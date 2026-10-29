//! Port of `ipcz/block_allocator_pool.{h,cc}`.

use std::collections::HashMap;
use std::ptr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::{Mutex, MutexGuard};

use super::block_allocator::BlockAllocator;
use super::buffer_id::BufferId;
use super::fragment::Fragment;
use super::fragment_descriptor::FragmentDescriptor;
use super::safe_math::checked_cast;

struct Entry {
    buffer_id: BufferId,
    /// The entire mapped region associated with `buffer_id`, not just the span
    /// managed by `allocator`.
    buffer_memory: *mut u8,
    buffer_len: usize,
    allocator: BlockAllocator,
    /// Cached pointer to the next entry. Entries are never removed from the
    /// pool, so this stays valid for the pool's lifetime.
    next: AtomicPtr<Entry>,
}

#[derive(Default)]
struct Guarded {
    /// List of all allocators added to this pool. Once added, elements are never
    /// removed from this list, and boxing keeps every `Entry` at a stable
    /// address.
    #[allow(clippy::vec_box)] // Boxing is what keeps entry addresses stable.
    entries: Vec<Box<Entry>>,
    /// Maps BufferId to a specific entry in the pool, so individual Fragments
    /// can be freed efficiently by the pool.
    entry_map: HashMap<BufferId, *mut Entry>,
    /// The total capacity of all BlockAllocators added to the pool so far.
    capacity: usize,
}

/// Manages a pool of BlockAllocators for a specific block size. This maintains
/// the allocators in a list along with a cached reference to the allocator most
/// recently used for a successful allocation.
///
/// The pool cycles through its allocators as needed to satisfy new requests,
/// failing only once a complete pass over all allocators fails to yield a
/// successful allocation.
///
/// BlockAllocatorPool is thread-safe.
pub struct BlockAllocatorPool {
    guarded: Mutex<Guarded>,
    /// An atomic cache of the most recently used Entry, for fast unsynchronized
    /// access in the common case.
    active_entry: AtomicPtr<Entry>,
}

// SAFETY: the raw pointers refer to boxed entries owned by `guarded.entries`,
// which are never moved or freed before the pool is dropped; `Entry`'s mutable
// state is atomic and the rest is immutable. The memory the entries describe is
// shared memory whose access is mediated by the thread-safe `BlockAllocator`.
unsafe impl Send for BlockAllocatorPool {}
unsafe impl Sync for BlockAllocatorPool {}

impl Default for BlockAllocatorPool {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockAllocatorPool {
    pub fn new() -> Self {
        Self {
            guarded: Mutex::new(Guarded::default()),
            active_entry: AtomicPtr::new(ptr::null_mut()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Guarded> {
        self.guarded.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Returns the total allocable capacity (in bytes) within this pool. Note
    /// that this counts all blocks, including ones which are currently
    /// allocated.
    pub fn get_capacity(&self) -> usize {
        self.lock().capacity
    }

    /// Registers a new allocator. `buffer_memory`/`buffer_len` describe the
    /// entire mapped region associated with `buffer_id`, not just the subspan
    /// managed by `allocator`, and must stay mapped for the pool's lifetime.
    ///
    /// Note that each BlockAllocatorPool allows only one allocator per unique
    /// `buffer_id`. This returns false if an allocator was already registered
    /// for `buffer_id`, and true otherwise to indicate success.
    pub fn add(
        &self,
        buffer_id: BufferId,
        buffer_memory: *mut u8,
        buffer_len: usize,
        allocator: &BlockAllocator,
    ) -> bool {
        let mut guarded = self.lock();
        if guarded.entry_map.contains_key(&buffer_id) {
            // DEVIATION: upstream bumps `capacity_` before checking for a
            // duplicate, leaving it inflated when the add is rejected.
            return false;
        }

        let previous_tail = guarded
            .entries
            .last_mut()
            .map(|entry| &mut **entry as *mut Entry);
        guarded.capacity += allocator.capacity() * allocator.block_size();
        let mut entry = Box::new(Entry {
            buffer_id,
            buffer_memory,
            buffer_len,
            allocator: *allocator,
            next: AtomicPtr::new(ptr::null_mut()),
        });
        let new_entry: *mut Entry = &mut *entry;
        guarded.entries.push(entry);
        guarded.entry_map.insert(buffer_id, new_entry);

        match previous_tail {
            // SAFETY: `previous_tail` points into a live boxed entry.
            Some(tail) => unsafe { (*tail).next.store(new_entry, Ordering::Release) },
            // Balanced by a load-acquire in `allocate()`.
            None => self.active_entry.store(new_entry, Ordering::Release),
        }
        true
    }

    /// Allocates a block from the pool and returns a reference to it as a
    /// Fragment. Returns a null Fragment if a block could not be allocated.
    pub fn allocate(&self) -> Fragment {
        // For the fast common case, we always start by trying to reuse the most
        // recently used allocator. This load-acquire is balanced by a
        // store-release below (via compare_exchange_weak) and in `add()` above.
        let starting_entry = self.active_entry.load(Ordering::Acquire);
        let mut entry = starting_entry;
        while !entry.is_null() {
            // SAFETY: entries are never freed or moved while the pool lives.
            let e = unsafe { &*entry };
            if let Some(block) = e.allocator.allocate() {
                // Success! Compute a FragmentDescriptor based on the block
                // address and the mapped region's location.
                let offset = (block.as_ptr() as usize).wrapping_sub(e.buffer_memory as usize);
                if e.buffer_len.wrapping_sub(e.allocator.block_size()) < offset {
                    // Allocator did something bad and this span would fall
                    // outside of the mapped region's bounds.
                    log::error!("Invalid address from BlockAllocator.");
                    return Fragment::default();
                }

                if entry != starting_entry {
                    // Attempt to update the active entry to reflect our success.
                    // Since this is only meant as a helpful hint for future
                    // allocations, we don't care whether it succeeds.
                    let _ = self.active_entry.compare_exchange_weak(
                        starting_entry,
                        entry,
                        Ordering::Release,
                        Ordering::Relaxed,
                    );
                }

                let descriptor = FragmentDescriptor::new(
                    e.buffer_id,
                    checked_cast(offset),
                    checked_cast(e.allocator.block_size()),
                );
                // SAFETY: `block` is a freshly allocated block of
                // `block_size()` bytes within the mapped region.
                return unsafe { Fragment::from_descriptor_unsafe(&descriptor, block.as_ptr()) };
            }

            // Allocation from the active allocator failed. Try another if
            // available. (The lock mirrors upstream, which reads `next` under
            // the pool mutex.)
            let _lock = self.lock();
            entry = e.next.load(Ordering::Acquire);
            if entry == starting_entry {
                break;
            }
        }
        Fragment::default()
    }

    /// Frees a fragment previously allocated from one of this pool's
    /// allocators. Returns true if and only if `fragment` was a valid fragment
    /// to free.
    pub fn free(&self, fragment: &Fragment) -> bool {
        let entry = match self.lock().entry_map.get(&fragment.buffer_id()) {
            Some(&entry) => entry,
            None => {
                log::debug!("Invalid free() call on BlockAllocatorPool");
                return false;
            }
        };
        // SAFETY: entries are never freed or moved while the pool lives.
        unsafe { (*entry).allocator.free(fragment.address()) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver_memory::DriverMemory;
    use crate::core::driver_memory_mapping::DriverMemoryMapping;
    use crate::core::testing::heap_memory_driver::HeapMemoryDriver;

    const BUFFER_SIZE: usize = 1024;
    const BLOCK_SIZE: u32 = 64;

    fn new_buffer(driver: &std::sync::Arc<HeapMemoryDriver>) -> (DriverMemoryMapping, BlockAllocator) {
        let mapping = DriverMemory::allocate(&driver.as_driver(), BUFFER_SIZE).map();
        let allocator = unsafe { BlockAllocator::new(mapping.address(), mapping.size(), BLOCK_SIZE) };
        allocator.initialize_region();
        (mapping, allocator)
    }

    #[test]
    fn empty_pool_cannot_allocate() {
        let pool = BlockAllocatorPool::new();
        assert_eq!(pool.get_capacity(), 0);
        assert!(pool.allocate().is_null());
    }

    #[test]
    fn rejects_duplicate_buffer_ids() {
        let driver = HeapMemoryDriver::new();
        let (m0, a0) = new_buffer(&driver);
        let (m1, a1) = new_buffer(&driver);
        let pool = BlockAllocatorPool::new();
        assert!(pool.add(BufferId::new(7), m0.address(), m0.size(), &a0));
        assert!(!pool.add(BufferId::new(7), m1.address(), m1.size(), &a1));
        // The rejected add leaves capacity untouched.
        assert_eq!(pool.get_capacity(), a0.capacity() * BLOCK_SIZE as usize);
    }

    #[test]
    fn allocates_across_allocators_and_frees() {
        let driver = HeapMemoryDriver::new();
        let (m0, a0) = new_buffer(&driver);
        let (m1, a1) = new_buffer(&driver);
        let pool = BlockAllocatorPool::new();
        assert!(pool.add(BufferId::new(0), m0.address(), m0.size(), &a0));
        assert!(pool.add(BufferId::new(1), m1.address(), m1.size(), &a1));
        assert_eq!(pool.get_capacity(), (a0.capacity() + a1.capacity()) * BLOCK_SIZE as usize);

        // The pool serves allocations from its active (first) allocator until it
        // runs dry, then moves on to the next.
        let fragments = std::iter::repeat_with(|| pool.allocate())
            .take_while(|f| !f.is_null())
            .collect::<Vec<_>>();
        assert_eq!(fragments.len(), a0.capacity() + a1.capacity());
        assert!(fragments.iter().all(|f| f.size() == BLOCK_SIZE));
        assert_eq!(
            fragments.iter().filter(|f| f.buffer_id() == BufferId::new(1)).count(),
            a1.capacity()
        );

        assert!(fragments.iter().all(|f| pool.free(f)));
        assert!(!pool.allocate().is_null());
    }

    #[test]
    fn free_rejects_unknown_fragments() {
        let driver = HeapMemoryDriver::new();
        let (m0, a0) = new_buffer(&driver);
        let pool = BlockAllocatorPool::new();
        assert!(pool.add(BufferId::new(0), m0.address(), m0.size(), &a0));

        let unknown = Fragment::pending_from_descriptor(&FragmentDescriptor::new(BufferId::new(9), 0, 8));
        assert!(!pool.free(&unknown));
    }
}
