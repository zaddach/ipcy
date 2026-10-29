//! Port of `ipcz/fragment.{h,cc}`.

use super::buffer_id::BufferId;
use super::driver_memory_mapping::DriverMemoryMapping;
use super::fragment_descriptor::FragmentDescriptor;
use super::safe_math::saturated_add;

/// Represents a span of memory located within the shared memory regions owned by
/// a NodeLinkMemory, via BufferPool. This is essentially a [`FragmentDescriptor`]
/// plus the actual mapped address of the given buffer and offset.
#[derive(Clone, Copy, Debug)]
pub struct Fragment {
    descriptor: FragmentDescriptor,
    /// The actual mapped address corresponding to `descriptor`.
    address: *mut u8,
}

// SAFETY: a Fragment is a descriptor plus an address into shared memory; it
// confers no access by itself, and users synchronize access to the memory.
unsafe impl Send for Fragment {}
unsafe impl Sync for Fragment {}

impl Default for Fragment {
    fn default() -> Self {
        Self {
            descriptor: FragmentDescriptor::default(),
            address: std::ptr::null_mut(),
        }
    }
}

impl Fragment {
    // Constructs a new Fragment over `descriptor`, mapped to `address`. If
    // `address` is null, the Fragment is considered "pending" -- it has a
    // potentially valid descriptor, but could not be resolved to a mapped
    // address yet (e.g. because the relevant BufferPool doesn't have the
    // identified buffer mapped yet.)
    fn new(descriptor: FragmentDescriptor, address: *mut u8) -> Self {
        // If `address` is non-null, the descriptor must also be. The inverse is
        // not true, as a pending fragment may have a null mapped address but a
        // non-null descriptor.
        assert!(address.is_null() || !descriptor.is_null());
        Self { descriptor, address }
    }

    /// Returns a new concrete Fragment corresponding to `descriptor` within the
    /// context of `mapping`. This validates that the fragment's bounds fall
    /// within the bounds of `mapping`. If `descriptor` was null or validation
    /// fails, this returns a null Fragment.
    pub fn mapped_from_descriptor(descriptor: &FragmentDescriptor, mapping: &DriverMemoryMapping) -> Fragment {
        if descriptor.is_null() {
            return Fragment::default();
        }

        let end = saturated_add(descriptor.offset(), descriptor.size());
        if end as usize > mapping.size() {
            return Fragment::default();
        }

        // SAFETY: `offset <= end <= mapping.size()`, so the result stays within
        // (or one past the end of) the mapped region.
        let address = unsafe { mapping.address().add(descriptor.offset() as usize) };
        Fragment::new(*descriptor, address)
    }

    /// Returns a pending Fragment corresponding to `descriptor`.
    pub fn pending_from_descriptor(descriptor: &FragmentDescriptor) -> Fragment {
        Fragment::new(*descriptor, std::ptr::null_mut())
    }

    /// Returns a Fragment corresponding to `descriptor`, with the starting
    /// address already mapped to `address`.
    ///
    /// # Safety
    /// `address` must (if non-null) point to at least `descriptor.size()` bytes
    /// that remain valid for as long as the fragment's bytes are accessed.
    pub unsafe fn from_descriptor_unsafe(descriptor: &FragmentDescriptor, address: *mut u8) -> Fragment {
        Fragment::new(*descriptor, address)
    }

    /// A null fragment is a fragment with a null descriptor, meaning it does not
    /// reference a valid buffer ID.
    pub fn is_null(&self) -> bool {
        self.descriptor.is_null()
    }

    /// An addressable fragment is a fragment with a non-null mapped address
    /// (which also implies a non-null descriptor.)
    pub fn is_addressable(&self) -> bool {
        !self.address.is_null()
    }

    /// A pending fragment is one with a non-null descriptor but a null mapped
    /// address. Pending fragments may be resolved later, but are not themselves
    /// addressable.
    pub fn is_pending(&self) -> bool {
        !self.is_null() && !self.is_addressable()
    }

    pub fn buffer_id(&self) -> BufferId {
        self.descriptor.buffer_id()
    }

    pub fn offset(&self) -> u32 {
        self.descriptor.offset()
    }

    pub fn size(&self) -> u32 {
        self.descriptor.size()
    }

    pub fn descriptor(&self) -> &FragmentDescriptor {
        &self.descriptor
    }

    /// The mapped base address for this fragment. Null unless
    /// [`Self::is_addressable`] is true.
    pub fn address(&self) -> *mut u8 {
        self.address
    }

    /// The mapped bytes corresponding to this fragment.
    ///
    /// # Safety
    /// The fragment must be addressable, its memory must still be mapped, and no
    /// one may write to the bytes for the lifetime `'a`.
    pub unsafe fn bytes<'a>(&self) -> &'a [u8] {
        assert!(self.is_addressable());
        // SAFETY: guaranteed by the caller.
        unsafe { std::slice::from_raw_parts(self.address, self.descriptor.size() as usize) }
    }

    /// The mapped (mutable) bytes corresponding to this fragment.
    ///
    /// # Safety
    /// The fragment must be addressable, its memory must still be mapped, and
    /// the caller must have exclusive access to the bytes for the lifetime `'a`.
    pub unsafe fn mutable_bytes<'a>(&self) -> &'a mut [u8] {
        assert!(self.is_addressable());
        // SAFETY: guaranteed by the caller.
        unsafe { std::slice::from_raw_parts_mut(self.address, self.descriptor.size() as usize) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::buffer_id::INVALID_BUFFER_ID;
    use crate::core::driver_memory::DriverMemory;
    use crate::core::testing::heap_memory_driver::HeapMemoryDriver;

    fn descriptor(buffer_id: u64, offset: u32, size: u32) -> FragmentDescriptor {
        FragmentDescriptor::new(BufferId::new(buffer_id), offset, size)
    }

    #[test]
    fn from_descriptor_unsafe() {
        let mut buffer = *b"Hello, world!\0";
        let base = buffer.as_mut_ptr();

        let f = unsafe { Fragment::from_descriptor_unsafe(&descriptor(0, 1, 4), base.add(1)) };
        assert!(!f.is_null());
        assert!(!f.is_pending());
        assert_eq!(f.offset(), 1);
        assert_eq!(f.size(), 4);
        assert_eq!(unsafe { f.bytes() }, b"ello");

        let f = unsafe { Fragment::from_descriptor_unsafe(&descriptor(0, 7, 6), base.add(7)) };
        assert!(!f.is_null());
        assert!(!f.is_pending());
        assert_eq!(f.offset(), 7);
        assert_eq!(f.size(), 6);
        assert_eq!(unsafe { f.bytes() }, b"world!");
    }

    #[test]
    fn pending_from_descriptor() {
        let f = Fragment::pending_from_descriptor(&descriptor(0, 5, 42));
        assert!(f.is_pending());
        assert!(!f.is_null());
        assert_eq!(f.offset(), 5);
        assert_eq!(f.size(), 42);

        let f = Fragment::pending_from_descriptor(&FragmentDescriptor::new(INVALID_BUFFER_ID, 0, 0));
        assert!(f.is_null());
        assert!(!f.is_pending());
    }

    #[test]
    fn null_mapped_from_descriptor() {
        let driver = HeapMemoryDriver::new();
        let mapping = DriverMemory::allocate(&driver.as_driver(), 32).map();
        let f = Fragment::mapped_from_descriptor(&FragmentDescriptor::new(INVALID_BUFFER_ID, 0, 0), &mapping);
        assert!(f.is_null());
    }

    #[test]
    fn invalid_mapped_from_descriptor() {
        const DATA_SIZE: u32 = 32;
        let driver = HeapMemoryDriver::new();
        let mapping = DriverMemory::allocate(&driver.as_driver(), DATA_SIZE as usize).map();

        // Offset out of bounds
        assert!(Fragment::mapped_from_descriptor(&descriptor(0, DATA_SIZE, 1), &mapping).is_null());

        // Tail out of bounds
        assert!(Fragment::mapped_from_descriptor(&descriptor(0, 0, DATA_SIZE + 5), &mapping).is_null());

        // Tail overflow
        assert!(Fragment::mapped_from_descriptor(&descriptor(0, u32::MAX, 2), &mapping).is_null());
    }

    #[test]
    fn valid_mapped_from_descriptor() {
        const DATA: &[u8] = b"0123456789abcdef\0";
        let driver = HeapMemoryDriver::new();
        let mapping = DriverMemory::allocate(&driver.as_driver(), DATA.len()).map();
        unsafe { std::ptr::copy_nonoverlapping(DATA.as_ptr(), mapping.address(), DATA.len()) };

        let f = Fragment::mapped_from_descriptor(&descriptor(0, 2, 11), &mapping);
        assert!(!f.is_null());
        assert!(!f.is_pending());
        assert_eq!(f.offset(), 2);
        assert_eq!(f.size(), 11);
        assert_eq!(unsafe { f.bytes() }, b"23456789abc");
    }
}
