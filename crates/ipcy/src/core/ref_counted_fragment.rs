//! Port of `ipcz/ref_counted_fragment.{h,cc}`.

use std::sync::atomic::{AtomicI32, Ordering};

/// An object allocated within a shared Fragment from NodeLinkMemory, and which
/// is automatically freed when its last reference is released. Consumers can
/// hold onto references to RefCountedFragment objects by holding a FragmentRef.
///
/// This lives in shared memory, so it must stay `repr(C)` with a single
/// 4-byte-aligned atomic counter.
#[derive(Debug)]
#[repr(C, align(4))]
pub struct RefCountedFragment {
    ref_count: AtomicI32,
}

const _: () = assert!(std::mem::size_of::<RefCountedFragment>() == 4);

impl Default for RefCountedFragment {
    fn default() -> Self {
        Self::new()
    }
}

impl RefCountedFragment {
    pub const fn new() -> Self {
        Self {
            ref_count: AtomicI32::new(1),
        }
    }

    pub fn ref_count_for_testing(&self) -> i32 {
        self.ref_count.load(Ordering::SeqCst)
    }

    /// Increments the reference count for this object.
    pub fn add_ref(&self) {
        self.ref_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Releases a reference and returns the previous reference count. If this
    /// returns 1, the underlying Fragment can be safely freed.
    pub fn release_ref(&self) -> i32 {
        self.ref_count.fetch_sub(1, Ordering::AcqRel)
    }
}
