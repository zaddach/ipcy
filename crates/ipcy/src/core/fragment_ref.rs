//! Port of `ipcz/fragment_ref.{h,cc}`.

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::Arc;

use super::buffer_id::BufferId;
use super::fragment::Fragment;
use super::node_link_memory::NodeLinkMemory;
use super::ref_counted_fragment::RefCountedFragment;
use super::router_link_state::RouterLinkState;

/// Implemented by objects which live in a shared [`Fragment`] and begin with a
/// [`RefCountedFragment`] (in C++, subclasses of `RefCountedFragment`). Such
/// objects must be `repr(C)` with the `RefCountedFragment` at offset zero.
///
/// # Safety
/// `ref_counted_fragment()` must return a reference to a `RefCountedFragment`
/// located at the very start of `self`.
pub unsafe trait RefCountedFragmentObject {
    fn ref_counted_fragment(&self) -> &RefCountedFragment;
}

// SAFETY: it is the object itself.
unsafe impl RefCountedFragmentObject for RefCountedFragment {
    fn ref_counted_fragment(&self) -> &RefCountedFragment {
        self
    }
}

// SAFETY: `ref_counted_fragment` is the first field of this `repr(C)` struct.
unsafe impl RefCountedFragmentObject for RouterLinkState {
    fn ref_counted_fragment(&self) -> &RefCountedFragment {
        &self.ref_counted_fragment
    }
}

/// Holds a reference to a [`RefCountedFragment`] object. When this object is
/// destroyed, the underlying ref count is decreased. If the ref count is
/// decreased to zero, the underlying Fragment is returned to its NodeLinkMemory.
///
/// Some FragmentRefs may be designated as "unmanaged", meaning that they will
/// never attempt to free the underlying Fragment. These refs are used to
/// preserve type compatibility with other similar (but managed) FragmentRefs
/// when the underlying Fragment isn't dynamically allocated and can't be freed.
///
/// For example most RouterLinkState fragments are dynamically allocated and
/// managed by FragmentRefs, but some instances are allocated at fixed locations
/// within the NodeLinkMemory and cannot be freed or reused. In both cases, ipcz
/// can refer to these objects using a `FragmentRef<RouterLinkState>`.
pub struct FragmentRef<T: RefCountedFragmentObject> {
    /// The NodeLinkMemory who ultimately owns this fragment's memory. `None` if
    /// the FragmentRef is null or unmanaged.
    memory: Option<Arc<NodeLinkMemory>>,
    fragment: Fragment,
    _object: PhantomData<fn() -> T>,
}

// SAFETY: a FragmentRef is a counted pointer into shared memory; the object it
// points at is shared by definition, so sending a ref is as safe as sharing the
// object.
unsafe impl<T: RefCountedFragmentObject + Sync> Send for FragmentRef<T> {}
unsafe impl<T: RefCountedFragmentObject + Sync> Sync for FragmentRef<T> {}

impl<T: RefCountedFragmentObject> Default for FragmentRef<T> {
    fn default() -> Self {
        Self {
            memory: None,
            fragment: Fragment::default(),
            _object: PhantomData,
        }
    }
}

impl<T: RefCountedFragmentObject> FragmentRef<T> {
    /// Adopts an existing ref to the RefCountedFragment located at the beginning
    /// of `fragment`, which is a Fragment owned by `memory`. (`kAdoptExistingRef`)
    pub fn adopt_existing_ref(memory: Arc<NodeLinkMemory>, fragment: Fragment) -> Self {
        assert!(fragment.is_null() || fragment.size() as usize >= std::mem::size_of::<T>());
        Self {
            memory: Some(memory),
            fragment,
            _object: PhantomData,
        }
    }

    /// Constructs an unmanaged FragmentRef, which references `fragment` but
    /// never attempts to release it back to its NodeLinkMemory. This is only
    /// safe to use with Fragments which cannot be freed. (`kUnmanagedRef`)
    pub fn unmanaged(fragment: Fragment) -> Self {
        assert!(fragment.is_null() || fragment.size() as usize >= std::mem::size_of::<T>());
        Self {
            memory: None,
            fragment,
            _object: PhantomData,
        }
    }

    pub fn memory(&self) -> Option<&Arc<NodeLinkMemory>> {
        self.memory.as_ref()
    }

    pub fn fragment(&self) -> &Fragment {
        &self.fragment
    }

    pub fn is_null(&self) -> bool {
        self.fragment.is_null()
    }

    pub fn is_addressable(&self) -> bool {
        self.fragment.is_addressable()
    }

    pub fn is_pending(&self) -> bool {
        self.fragment.is_pending()
    }

    pub fn buffer_id(&self) -> BufferId {
        self.fragment.buffer_id()
    }

    /// Pointer to the referenced object. Null unless addressable.
    pub fn get(&self) -> *mut T {
        self.fragment.address().cast()
    }

    fn ref_counted(&self) -> &RefCountedFragment {
        assert!(self.is_addressable());
        // SAFETY: addressable fragments referenced by a FragmentRef<T> hold a
        // `T` (which starts with a RefCountedFragment) at their address.
        unsafe { (*self.get()).ref_counted_fragment() }
    }

    pub fn ref_count_for_testing(&self) -> i32 {
        self.ref_counted().ref_count_for_testing()
    }

    /// Drops this reference, decrementing the object's ref count and freeing the
    /// fragment if that was the last reference and the ref is managed.
    pub fn reset(&mut self) {
        let memory = self.memory.take();
        if self.fragment.is_null() {
            return;
        }

        let fragment = std::mem::take(&mut self.fragment);
        if !fragment.is_addressable() {
            return;
        }

        // SAFETY: see `ref_counted()`.
        let ref_counted = unsafe { &*(fragment.address() as *const RefCountedFragment) };
        if ref_counted.release_ref() > 1 {
            return;
        }
        if let Some(memory) = memory {
            memory.free_fragment(&fragment);
        }
    }

    /// Relinquishes this reference without releasing it, returning the fragment.
    pub fn release(&mut self) -> Fragment {
        self.memory = None;
        std::mem::take(&mut self.fragment)
    }
}

impl<T: RefCountedFragmentObject> Clone for FragmentRef<T> {
    fn clone(&self) -> Self {
        if !self.fragment.is_null() {
            assert!(self.fragment.is_addressable());
            self.ref_counted().add_ref();
        }
        Self {
            memory: self.memory.clone(),
            fragment: self.fragment,
            _object: PhantomData,
        }
    }
}

impl<T: RefCountedFragmentObject> Deref for FragmentRef<T> {
    type Target = T;

    fn deref(&self) -> &T {
        assert!(self.is_addressable());
        // SAFETY: see `ref_counted()`.
        unsafe { &*self.get() }
    }
}

impl<T: RefCountedFragmentObject> Drop for FragmentRef<T> {
    fn drop(&mut self) {
        self.reset();
    }
}

impl<T: RefCountedFragmentObject> std::fmt::Debug for FragmentRef<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FragmentRef")
            .field("fragment", &self.fragment)
            .field("managed", &self.memory.is_some())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::buffer_id::BufferId;
    use crate::core::features::Features;
    use crate::core::fragment_descriptor::FragmentDescriptor;
    use crate::core::link_side::LinkSide;
    use crate::core::testing::heap_memory_driver::HeapMemoryDriver;
    use crate::core::testing::test_memory_node::TestMemoryNode;

    type TestObject = RefCountedFragment;

    fn unmanaged_ref(object: &TestObject) -> FragmentRef<TestObject> {
        FragmentRef::unmanaged(unsafe {
            Fragment::from_descriptor_unsafe(
                &FragmentDescriptor::new(BufferId::new(0), 0, std::mem::size_of::<TestObject>() as u32),
                object as *const TestObject as *mut u8,
            )
        })
    }

    fn assert_null(r: &FragmentRef<TestObject>) {
        assert!(r.is_null());
        assert!(!r.is_addressable());
    }

    fn assert_live(r: &FragmentRef<TestObject>) {
        assert!(!r.is_null());
        assert!(r.is_addressable());
    }

    #[test]
    fn null_ref() {
        let mut r = FragmentRef::<TestObject>::default();
        assert_null(&r);
        r.reset();
        assert_null(&r);

        let other1 = r.clone();
        assert_null(&r);
        assert_null(&other1);

        let other2 = std::mem::take(&mut r);
        assert_null(&r);
        assert_null(&other2);

        r = other1.clone();
        assert_null(&r);
        assert_null(&other1);

        r = other2;
        assert_null(&r);
    }

    #[test]
    fn simple_ref() {
        let object = TestObject::new();
        let mut r = unmanaged_ref(&object);
        assert_eq!(object.ref_count_for_testing(), 1);
        r.reset();
        assert_eq!(object.ref_count_for_testing(), 0);
    }

    #[test]
    #[allow(unused_assignments)] // The overwritten value's drop is the point.
    fn copy() {
        let object1 = TestObject::new();
        let mut ref1 = unmanaged_ref(&object1);
        assert_eq!(object1.ref_count_for_testing(), 1);

        let mut other1 = ref1.clone();
        assert_eq!(object1.ref_count_for_testing(), 2);
        other1.reset();
        assert_eq!(object1.ref_count_for_testing(), 1);
        assert_null(&other1);

        let object2 = TestObject::new();
        let mut ref2 = unmanaged_ref(&object2);
        assert_eq!(object1.ref_count_for_testing(), 1);
        assert_eq!(object2.ref_count_for_testing(), 1);

        ref2 = ref1.clone();
        assert_eq!(object1.ref_count_for_testing(), 2);
        assert_eq!(object2.ref_count_for_testing(), 0);
        assert_live(&ref1);
        assert_live(&ref2);

        ref1.reset();
        assert_eq!(object1.ref_count_for_testing(), 1);
        assert_eq!(object2.ref_count_for_testing(), 0);
        assert_null(&ref1);

        ref2.reset();
        assert_eq!(object1.ref_count_for_testing(), 0);
        assert_eq!(object2.ref_count_for_testing(), 0);
        assert_null(&ref2);
    }

    #[test]
    #[allow(unused_assignments)] // The overwritten value's drop is the point.
    fn move_semantics() {
        let object1 = TestObject::new();
        let mut ref1 = unmanaged_ref(&object1);
        assert_eq!(ref1.ref_count_for_testing(), 1);

        let mut other1 = std::mem::take(&mut ref1);
        assert_eq!(object1.ref_count_for_testing(), 1);
        assert_live(&other1);
        assert_null(&ref1);

        other1.reset();
        assert_null(&other1);
        assert_eq!(object1.ref_count_for_testing(), 0);

        let (object2, object3) = (TestObject::new(), TestObject::new());
        let mut ref2 = unmanaged_ref(&object2);
        let mut ref3 = unmanaged_ref(&object3);
        assert_live(&ref2);
        assert_live(&ref3);
        assert_eq!(object2.ref_count_for_testing(), 1);
        assert_eq!(object3.ref_count_for_testing(), 1);

        ref3 = std::mem::take(&mut ref2);
        assert_eq!(object2.ref_count_for_testing(), 1);
        assert_eq!(object3.ref_count_for_testing(), 0);
        assert_null(&ref2);
        assert_live(&ref3);

        ref3.reset();
        assert_null(&ref3);
        assert_eq!(object2.ref_count_for_testing(), 0);
        assert_eq!(object3.ref_count_for_testing(), 0);
    }

    #[test]
    fn free() {
        let driver = HeapMemoryDriver::new();
        let node = TestMemoryNode::new(&driver.as_driver(), Features::default(), 0);
        let buffer = NodeLinkMemory::allocate_memory(&driver.as_driver());
        let memory = NodeLinkMemory::create(node, LinkSide::A, &Features::default(), buffer.mapping);

        // Allocate a ton of fragments and let them be released by FragmentRef on
        // destruction. If the fragments aren't freed properly, allocations will
        // fail and so will the test.
        const NUM_ALLOCATIONS: usize = 100_000;
        (0..NUM_ALLOCATIONS).for_each(|_| {
            let fragment = memory.allocate_fragment(std::mem::size_of::<TestObject>());
            assert!(fragment.is_addressable());
            let _ref = FragmentRef::<TestObject>::adopt_existing_ref(memory.clone(), fragment);
        });
    }
}
