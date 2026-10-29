//! Port of `ipcz/buffer_pool.{h,cc}`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use super::block_allocator::BlockAllocator;
use super::block_allocator_pool::BlockAllocatorPool;
use super::buffer_id::BufferId;
use super::driver_memory_mapping::DriverMemoryMapping;
use super::fragment::Fragment;
use super::fragment_descriptor::FragmentDescriptor;
use super::metrics::{self, BlockAllocationSource};

/// Runs once the identified buffer is added to the pool.
pub type WaitForBufferCallback = Box<dyn FnOnce() + Send>;

#[derive(Default)]
struct Guarded {
    mappings: HashMap<BufferId, DriverMemoryMapping>,

    /// Mapping from block size to a pool of BlockAllocators for that size. When
    /// a new BlockAllocator is registered with this BufferPool, it's added to an
    /// appropriate BlockAllocatorPool within this map. Pools live as long as the
    /// BufferPool once added and are thread-safe, so they're shared out as
    /// `Arc`s to be used without the lock.
    block_allocator_pools: BTreeMap<usize, Arc<BlockAllocatorPool>>,

    /// Callbacks to be invoked when an identified buffer becomes available.
    buffer_callbacks: HashMap<BufferId, Vec<WaitForBufferCallback>>,
}

/// BufferPool maintains ownership of an extensible collection of mapped
/// DriverMemory buffers, exposing access to them for dynamic memory allocation
/// and arbitrary state sharing. Every buffer owned by a BufferPool is
/// identified by a unique BufferId, and once a buffer is added to the pool it
/// remains there indefinitely.
///
/// BufferPool objects are thread-safe.
#[derive(Default)]
pub struct BufferPool {
    guarded: Mutex<Guarded>,
}

impl BufferPool {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> MutexGuard<'_, Guarded> {
        self.guarded.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Resolves `descriptor` to a concrete Fragment. If the descriptor is null
    /// or describes a region of memory which exceeds the bounds of the
    /// identified buffer, this returns a null Fragment.
    ///
    /// If the descriptor's BufferId is not yet registered with this pool, this
    /// returns a pending Fragment with the same BufferId and dimensions as
    /// `descriptor`. Otherwise this returns a resolved Fragment which references
    /// an appropriate span of mapped memory.
    pub fn get_fragment(&self, descriptor: &FragmentDescriptor) -> Fragment {
        if descriptor.is_null() {
            return Fragment::default();
        }

        match self.lock().mappings.get(&descriptor.buffer_id()) {
            None => Fragment::pending_from_descriptor(descriptor),
            Some(mapping) => Fragment::mapped_from_descriptor(descriptor, mapping),
        }
    }

    /// Registers `mapping` under `id` within this pool, along with a collection
    /// of BlockAllocators that have already been initialized within the mapped
    /// memory, to support block allocation by the pool.
    ///
    /// Returns true if the mapping and BlockAllocators were successfully added
    /// to the pool, or false if the pool already had a buffer registered under
    /// the given `id` or if any allocator within `block_allocators` is not
    /// contained by `mapping` or is otherwise invalid.
    ///
    /// Note that every allocator in `block_allocators` must have a unique
    /// power-of-2 block size, as each buffer only supports at most one
    /// allocator per block size.
    pub fn add_block_buffer(
        &self,
        id: BufferId,
        mapping: DriverMemoryMapping,
        block_allocators: &[BlockAllocator],
    ) -> bool {
        assert!(mapping.is_valid());

        // Basic consistency checks before we change any pool state: ensure that
        // each given allocator actually lives within the memory mapped by
        // `mapping`, and that each has a unique power-of-2 block size.
        let mapping_start = mapping.address() as usize;
        let mapping_end = mapping_start + mapping.size();
        let mut block_sizes_present = 0usize;
        for allocator in block_allocators {
            let (region, region_len) = allocator.region();
            let (region_start, region_end) = (region as usize, region as usize + region_len);
            if region_start < mapping_start || region_end > mapping_end || region_len == 0 {
                // Not a valid allocator region for this mapping.
                return false;
            }

            let block_size = allocator.block_size();
            assert!((8..=(1 << 30)).contains(&block_size));
            if !block_size.is_power_of_two() {
                return false;
            }
            if block_sizes_present & block_size != 0 {
                // Duplicate allocator block size for this buffer.
                return false;
            }
            block_sizes_present |= block_size;
        }

        let callbacks = {
            let mut guarded = self.lock();
            if guarded.mappings.contains_key(&id) {
                return false;
            }
            let (address, size) = (mapping.address(), mapping.size());
            guarded.mappings.insert(id, mapping);
            let callbacks = guarded.buffer_callbacks.remove(&id).unwrap_or_default();

            for allocator in block_allocators {
                guarded
                    .block_allocator_pools
                    .entry(allocator.block_size())
                    .or_default()
                    .add(id, address, size, allocator);
            }
            callbacks
        };

        callbacks.into_iter().for_each(|callback| callback());
        true
    }

    /// Returns the total size in bytes of capacity available across all
    /// registered BlockAllocators for the given `block_size`.
    pub fn get_total_block_capacity(&self, block_size: usize) -> usize {
        let pool = self.lock().block_allocator_pools.get(&block_size).cloned();
        pool.map_or(0, |pool| pool.get_capacity())
    }

    /// Attempts to allocate an unused block of at least `block_size` bytes from
    /// any available block allocation buffer in the pool, preferring the smaller
    /// blocks over larger ones. If the BufferPool cannot accommodate the
    /// allocation request, this returns a null Fragment.
    pub fn allocate_block(&self, block_size: usize) -> Fragment {
        assert!(block_size.is_power_of_two());
        let pool = self
            .lock()
            .block_allocator_pools
            .range(block_size..)
            .next()
            .map(|(_, pool)| pool.clone());
        let fragment = pool.map_or_else(Fragment::default, |pool| pool.allocate());
        metrics::record_allocate_block_result(BlockAllocationSource::BufferPool, block_size, !fragment.is_null());
        fragment
    }

    /// Similar to `allocate_block()`, but this may allocate less space than
    /// requested if that's all that's available. May still return a null
    /// Fragment if the BufferPool has trouble finding available memory.
    pub fn allocate_block_best_effort(&self, preferred_block_size: usize) -> Fragment {
        assert!(preferred_block_size.is_power_of_two());

        // Limit the number of attempts we make to scale down the requested size
        // in search of an available fragment. This value was chosen arbitrarily.
        const MAX_ATTEMPTS: usize = 3;

        let (mut pool_size, mut pool) = {
            let guarded = self.lock();
            match guarded
                .block_allocator_pools
                .range(preferred_block_size..)
                .next()
                .or_else(|| guarded.block_allocator_pools.iter().next_back())
            {
                Some((size, pool)) => (*size, pool.clone()),
                None => return Fragment::default(),
            }
        };

        for _ in 0..MAX_ATTEMPTS {
            let fragment = pool.allocate();
            if !fragment.is_null() {
                return fragment;
            }

            let guarded = self.lock();
            match guarded.block_allocator_pools.range(..pool_size).next_back() {
                // We were already at the smallest block size.
                None => return Fragment::default(),
                Some((size, smaller)) => {
                    pool_size = *size;
                    pool = smaller.clone();
                }
            }
        }
        Fragment::default()
    }

    /// Frees a block previously allocated from this pool via `allocate_block()`
    /// or `allocate_block_best_effort()`. Returns true if successful, or false
    /// if `fragment` was not allocated from one of this pool's block buffers.
    pub fn free_block(&self, fragment: &Fragment) -> bool {
        let pool = self
            .lock()
            .block_allocator_pools
            .get(&(fragment.size() as usize))
            .cloned();
        pool.is_some_and(|pool| pool.free(fragment))
    }

    /// Runs `callback` as soon as the identified buffer is added to the
    /// underlying BufferPool. If the buffer is already present here, `callback`
    /// is run immediately. If this BufferPool's NodeLink is disconnected before
    /// the buffer becomes available, `callback` is destroyed and never run.
    pub fn wait_for_buffer_async(&self, id: BufferId, callback: WaitForBufferCallback) {
        {
            let mut guarded = self.lock();
            if !guarded.mappings.contains_key(&id) {
                guarded.buffer_callbacks.entry(id).or_default().push(callback);
                return;
            }
        }
        callback();
    }

    /// Purges all pending buffer callbacks enqueued by `wait_for_buffer_async()`,
    /// without invoking them.
    pub fn drop_pending_buffer_callbacks(&self) {
        // Dropping callbacks may have side-effects, so do it outside of a lock.
        let _callbacks = std::mem::take(&mut self.lock().buffer_callbacks);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver_memory::DriverMemory;
    use crate::core::testing::heap_memory_driver::HeapMemoryDriver;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Fixture {
        driver: Arc<HeapMemoryDriver>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                driver: HeapMemoryDriver::new(),
            }
        }

        fn allocate_driver_memory(&self, size: usize) -> DriverMemoryMapping {
            DriverMemory::allocate(&self.driver.as_driver(), size).map()
        }
    }

    fn allocator(mapping: &DriverMemoryMapping, offset: usize, block_size: u32) -> BlockAllocator {
        unsafe { BlockAllocator::new(mapping.address().add(offset), mapping.size() - offset, block_size) }
    }

    fn id(n: u64) -> BufferId {
        BufferId::new(n)
    }

    #[test]
    fn add_block_buffer() {
        const BUFFER_SIZE: usize = 4096;
        let f = Fixture::new();
        let mapping = f.allocate_driver_memory(BUFFER_SIZE);
        let base = mapping.address();
        let allocators = [allocator(&mapping, 0, 64)];

        let pool = BufferPool::new();
        assert!(pool.add_block_buffer(id(0), mapping, &allocators));

        let fragment = pool.get_fragment(&FragmentDescriptor::new(id(0), 0, BUFFER_SIZE as u32));
        assert!(fragment.is_addressable());
        assert_eq!(fragment.address(), base);
        assert_eq!(fragment.size() as usize, BUFFER_SIZE);
    }

    #[test]
    fn add_block_buffer_no_duplicate_buffer_id() {
        const BUFFER_SIZE: usize = 4096;
        let f = Fixture::new();
        let mapping = f.allocate_driver_memory(BUFFER_SIZE);
        let base = mapping.address();
        let allocators = [allocator(&mapping, 0, 64)];

        let pool = BufferPool::new();
        assert!(pool.add_block_buffer(id(0), mapping, &allocators));

        // Adding another buffer with the same ID as above must fail.
        let another_mapping = f.allocate_driver_memory(BUFFER_SIZE);
        let another_allocator = [allocator(&another_mapping, 0, 64)];
        assert!(!pool.add_block_buffer(id(0), another_mapping, &another_allocator));

        // Fragment resolution against buffer 0 should still map to the first
        // buffer.
        let fragment = pool.get_fragment(&FragmentDescriptor::new(id(0), 0, BUFFER_SIZE as u32));
        assert!(fragment.is_addressable());
        assert_eq!(fragment.address(), base);
    }

    #[test]
    fn add_block_buffer_no_duplicate_allocator_block_sizes() {
        const BUFFER_SIZE: usize = 4096;
        const BLOCK_SIZE: u32 = 64;
        let f = Fixture::new();
        let mapping = f.allocate_driver_memory(BUFFER_SIZE);

        // Carve up the buffer into two separate allocators for the same block
        // size, and try to register them both. This is unsupported.
        let first = unsafe { BlockAllocator::new(mapping.address(), BLOCK_SIZE as usize * 2, BLOCK_SIZE) };
        let second = allocator(&mapping, BLOCK_SIZE as usize * 2, BLOCK_SIZE);

        let pool = BufferPool::new();
        assert!(!pool.add_block_buffer(id(0), mapping, &[first, second]));

        // No buffer is registered to resolve this fragment.
        assert!(pool.get_fragment(&FragmentDescriptor::new(id(0), 0, 8)).is_pending());
    }

    #[test]
    fn add_block_buffer_require_block_size_power_of_two() {
        let f = Fixture::new();
        let mapping = f.allocate_driver_memory(4096);
        let bad_allocator = allocator(&mapping, 0, 80);

        let pool = BufferPool::new();
        assert!(!pool.add_block_buffer(id(0), mapping, &[bad_allocator]));

        // No buffer is registered to resolve this fragment.
        assert!(pool.get_fragment(&FragmentDescriptor::new(id(0), 0, 8)).is_pending());
    }

    #[test]
    fn add_block_buffer_requires_allocators_within_mapping() {
        let f = Fixture::new();
        let mapping = f.allocate_driver_memory(4096);
        let other = f.allocate_driver_memory(4096);
        let outside = allocator(&other, 0, 64);

        let pool = BufferPool::new();
        assert!(!pool.add_block_buffer(id(0), mapping, &[outside]));
    }

    #[test]
    fn get_fragment() {
        const BUFFER_SIZE_1: usize = 4096;
        const BUFFER_SIZE_2: usize = 2048;
        const BLOCK_SIZE: u32 = 64;
        let f = Fixture::new();
        let mapping1 = f.allocate_driver_memory(BUFFER_SIZE_1);
        let mapping2 = f.allocate_driver_memory(BUFFER_SIZE_2);
        let (bytes1, bytes2) = (mapping1.address(), mapping2.address());
        let allocators1 = [allocator(&mapping1, 0, BLOCK_SIZE)];
        let allocators2 = [allocator(&mapping2, 0, BLOCK_SIZE)];

        let pool = BufferPool::new();
        assert!(pool.add_block_buffer(id(1), mapping1, &allocators1));
        assert!(pool.add_block_buffer(id(2), mapping2, &allocators2));

        // We can resolve fragments covering entire buffers.
        let fragment = pool.get_fragment(&FragmentDescriptor::new(id(1), 0, BUFFER_SIZE_1 as u32));
        assert!(!fragment.is_null());
        assert!(fragment.is_addressable());
        assert_eq!(fragment.address(), bytes1);
        assert_eq!(fragment.size() as usize, BUFFER_SIZE_1);

        let fragment = pool.get_fragment(&FragmentDescriptor::new(id(2), 0, BUFFER_SIZE_2 as u32));
        assert!(!fragment.is_null());
        assert!(fragment.is_addressable());
        assert_eq!(fragment.address(), bytes2);
        assert_eq!(fragment.size() as usize, BUFFER_SIZE_2);

        // We can resolve fragments covering a subspan of a buffer.
        const PARTIAL_FRAGMENT_OFFSET: u32 = 4;
        const PARTIAL_FRAGMENT_SIZE: u32 = BUFFER_SIZE_2 as u32 / 2;
        let fragment = pool.get_fragment(&FragmentDescriptor::new(
            id(2),
            PARTIAL_FRAGMENT_OFFSET,
            PARTIAL_FRAGMENT_SIZE,
        ));
        assert!(!fragment.is_null());
        assert!(fragment.is_addressable());
        assert_eq!(fragment.address(), unsafe { bytes2.add(PARTIAL_FRAGMENT_OFFSET as usize) });
        assert_eq!(fragment.size(), PARTIAL_FRAGMENT_SIZE);

        // Unknown BufferIds resolve to pending fragments.
        let descriptor = FragmentDescriptor::new(id(42), 5, 1234);
        let fragment = pool.get_fragment(&descriptor);
        assert!(!fragment.is_null());
        assert!(!fragment.is_addressable());
        assert!(fragment.is_pending());
        assert!(fragment.address().is_null());
        assert_eq!(fragment.buffer_id(), descriptor.buffer_id());
        assert_eq!(fragment.offset(), descriptor.offset());
        assert_eq!(fragment.size(), descriptor.size());

        // Null descriptors resolve to null fragments.
        assert!(pool.get_fragment(&FragmentDescriptor::default()).is_null());

        // Out-of-bounds descriptors resolve to null fragments too.
        let fragment = pool.get_fragment(&FragmentDescriptor::new(id(1), 0, BUFFER_SIZE_1 as u32 + 1));
        assert!(fragment.is_null());
    }

    #[test]
    fn basic_block_allocation() {
        const BUFFER_SIZE: usize = 4096;
        const BLOCK_SIZE: u32 = 64;
        let f = Fixture::new();
        let mapping0 = f.allocate_driver_memory(BUFFER_SIZE);
        let mapping1 = f.allocate_driver_memory(BUFFER_SIZE);
        let bytes0 = mapping0.address();
        let allocator0 = allocator(&mapping0, 0, BLOCK_SIZE);
        allocator0.initialize_region();
        let allocator1 = allocator(&mapping1, 0, BLOCK_SIZE);
        allocator1.initialize_region();

        let pool = BufferPool::new();
        assert!(pool.add_block_buffer(id(0), mapping0, &[allocator0]));
        assert!(pool.add_block_buffer(id(1), mapping1, &[allocator1]));
        assert_eq!(
            pool.get_total_block_capacity(BLOCK_SIZE as usize),
            BLOCK_SIZE as usize * (allocator0.capacity() + allocator1.capacity())
        );

        // We can't free something that isn't a valid allocation.
        let unsafe_fragment = |buffer: u64, size: u32, address: *mut u8| unsafe {
            Fragment::from_descriptor_unsafe(&FragmentDescriptor::new(id(buffer), 0, size), address)
        };
        assert!(!pool.free_block(&Fragment::default()));
        assert!(!pool.free_block(&unsafe_fragment(1000, 1, std::ptr::null_mut())));
        assert!(!pool.free_block(&unsafe_fragment(0, 1, bytes0)));

        // Allocate all available capacity.
        let fragments = std::iter::repeat_with(|| pool.allocate_block(BLOCK_SIZE as usize))
            .take_while(|fragment| !fragment.is_null())
            .collect::<Vec<_>>();
        assert_eq!(fragments.len(), allocator0.capacity() + allocator1.capacity());
        assert!(fragments.iter().all(|fragment| pool.free_block(fragment)));
    }

    #[test]
    fn block_allocation_sizing() {
        const BUFFER_SIZE: usize = 4096;
        const BUFFER_1_BLOCK_SIZE: u32 = 64;
        const BUFFER_2_BLOCK_SIZE: u32 = BUFFER_1_BLOCK_SIZE * 4;
        let f = Fixture::new();
        let mapping1 = f.allocate_driver_memory(BUFFER_SIZE);
        let mapping2 = f.allocate_driver_memory(BUFFER_SIZE);
        let allocator1 = allocator(&mapping1, 0, BUFFER_1_BLOCK_SIZE);
        allocator1.initialize_region();
        let allocator2 = allocator(&mapping2, 0, BUFFER_2_BLOCK_SIZE);
        allocator2.initialize_region();

        let pool = BufferPool::new();
        assert!(pool.add_block_buffer(id(1), mapping1, &[allocator1]));
        assert!(pool.add_block_buffer(id(2), mapping2, &[allocator2]));

        let expect_block = |request: usize, buffer: u64, size: u32| {
            let fragment = pool.allocate_block(request);
            assert!(fragment.is_addressable());
            assert_eq!(fragment.buffer_id(), id(buffer));
            assert_eq!(fragment.size(), size);
        };

        // Allocations not larger than 64 bytes should be drawn from buffer 1.
        expect_block(1, 1, BUFFER_1_BLOCK_SIZE);
        expect_block(BUFFER_1_BLOCK_SIZE as usize / 2, 1, BUFFER_1_BLOCK_SIZE);
        expect_block(BUFFER_1_BLOCK_SIZE as usize, 1, BUFFER_1_BLOCK_SIZE);

        // Larger allocations which are still no larger than BUFFER_2_BLOCK_SIZE
        // bytes should be drawn from buffer 2.
        expect_block(BUFFER_1_BLOCK_SIZE as usize * 2, 2, BUFFER_2_BLOCK_SIZE);
        expect_block(BUFFER_2_BLOCK_SIZE as usize, 2, BUFFER_2_BLOCK_SIZE);

        // Anything larger than BUFFER_2_BLOCK_SIZE should fail to allocate.
        assert!(pool.allocate_block(BUFFER_2_BLOCK_SIZE as usize * 2).is_null());
    }

    #[test]
    fn allocate_block_result_metrics() {
        use crate::core::metrics::testing::RecordedEvents;
        use crate::core::metrics::{BlockAllocationSize, BlockAllocationSource::BufferPool as Source};

        let f = Fixture::new();
        let events = RecordedEvents::capture();
        let pool = BufferPool::new();

        // Check allocation result with an empty pool.
        assert!(pool.allocate_block(64).is_null());
        assert_eq!(1, events.count_result(Source, false));
        assert_eq!(1, events.count_size(Source, false, BlockAllocationSize::B64));
        assert_eq!(0, events.count_result(Source, true));

        // Check allocation failure with an unmapped block size falling into Other.
        assert!(pool.allocate_block(2 * 1024 * 1024).is_null());
        assert_eq!(2, events.count_result(Source, false));
        assert_eq!(1, events.count_size(Source, false, BlockAllocationSize::Other));
        assert_eq!(0, events.count_result(Source, true));

        // Check successful allocation.
        const BUFFER_SIZE: usize = 4096;
        let mapping = f.allocate_driver_memory(BUFFER_SIZE);
        let allocators = [allocator(&mapping, 0, 64)];
        assert!(pool.add_block_buffer(id(0), mapping, &allocators));

        assert!(!pool.allocate_block(64).is_null());
        assert_eq!(1, events.count_result(Source, true));
        assert_eq!(1, events.count_size(Source, true, BlockAllocationSize::B64));
    }

    #[test]
    fn best_effort_block_allocation() {
        const BUFFER_SIZE: usize = 4096;
        const BUFFER_1_BLOCK_SIZE: u32 = 64;
        const BUFFER_2_BLOCK_SIZE: u32 = 128;
        let f = Fixture::new();
        let mapping1 = f.allocate_driver_memory(BUFFER_SIZE);
        let mapping2 = f.allocate_driver_memory(BUFFER_SIZE);
        let allocator1 = allocator(&mapping1, 0, BUFFER_1_BLOCK_SIZE);
        allocator1.initialize_region();
        let allocator2 = allocator(&mapping2, 0, BUFFER_2_BLOCK_SIZE);
        allocator2.initialize_region();

        let pool = BufferPool::new();
        assert!(pool.add_block_buffer(id(1), mapping1, &[allocator1]));
        assert!(pool.add_block_buffer(id(2), mapping2, &[allocator2]));

        // Oversized best-effort allocations can succeed.
        let partial_fragment = pool.allocate_block_best_effort(BUFFER_2_BLOCK_SIZE as usize * 2);
        assert!(partial_fragment.is_addressable());
        assert_eq!(partial_fragment.buffer_id(), id(2));
        assert_eq!(partial_fragment.size(), BUFFER_2_BLOCK_SIZE);

        // If we exhaust a sufficient block size, we should fall back onto
        // smaller block sizes. First allocate all available capacity for
        // BUFFER_2_BLOCK_SIZE, and then a partial allocation of
        // BUFFER_2_BLOCK_SIZE should succeed with a smaller size.
        while !pool.allocate_block(BUFFER_2_BLOCK_SIZE as usize).is_null() {}
        let partial_fragment = pool.allocate_block_best_effort(BUFFER_2_BLOCK_SIZE as usize);
        assert!(partial_fragment.is_addressable());
        assert_eq!(partial_fragment.buffer_id(), id(1));
        assert_eq!(partial_fragment.size(), BUFFER_1_BLOCK_SIZE);
    }

    #[test]
    fn wait_for_buffer_async() {
        let f = Fixture::new();
        let pool = BufferPool::new();
        let ran = Arc::new(AtomicUsize::new(0));
        let callback = |ran: &Arc<AtomicUsize>| {
            let ran = ran.clone();
            Box::new(move || {
                ran.fetch_add(1, Ordering::SeqCst);
            }) as WaitForBufferCallback
        };

        // Callbacks for missing buffers wait, and dropped callbacks never run.
        pool.wait_for_buffer_async(id(5), callback(&ran));
        pool.wait_for_buffer_async(id(6), callback(&ran));
        pool.drop_pending_buffer_callbacks();
        let mapping = f.allocate_driver_memory(1024);
        assert!(pool.add_block_buffer(id(6), mapping, &[]));
        assert_eq!(ran.load(Ordering::SeqCst), 0);

        // Pending callbacks run when their buffer arrives; callbacks for present
        // buffers run immediately.
        pool.wait_for_buffer_async(id(7), callback(&ran));
        pool.wait_for_buffer_async(id(6), callback(&ran));
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        let mapping = f.allocate_driver_memory(1024);
        assert!(pool.add_block_buffer(id(7), mapping, &[]));
        assert_eq!(ran.load(Ordering::SeqCst), 2);
    }
}
