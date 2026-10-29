//! Port of `ipcz/block_allocator.{h,cc}`.

use std::ptr::NonNull;
use std::sync::atomic::{AtomicU32, Ordering};

use super::safe_math::checked_cast;

// The packed header representation below matches the in-memory layout of the
// C++ `struct { uint16_t version; int16_t next; }` only on little-endian hosts.
#[cfg(not(target_endian = "little"))]
compile_error!("BlockAllocator's shared-memory layout requires a little-endian target");

const FRONT_BLOCK_INDEX: i16 = 0;

/// A block header as stored in shared memory: a 4-byte, 4-aligned word of
/// `{ u16 version, i16 next }`, accessed atomically as a whole.
///
/// `version` is only meaningful within the first block. It is incremented any
/// time that block's header changes and resolves races between concurrent
/// `allocate()` or `free()` operations modifying the head of the free-list.
///
/// `next` is a relative index to the next free block in the list: relative to
/// the index of the block which physically follows this one (its "successor"),
/// not to this header's own block. This lets a region be initialized by zeroing
/// it and updating only the last block's header to terminate the list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BlockHeader {
    version: u16,
    next: i16,
}

impl BlockHeader {
    fn pack(self) -> u32 {
        u32::from(self.version) | (u32::from(self.next as u16) << 16)
    }

    fn unpack(bits: u32) -> Self {
        Self {
            version: bits as u16,
            next: (bits >> 16) as u16 as i16,
        }
    }
}

// Helper for legibility of relative/absolute index conversions, based around
// the successor to the block at `index`.
//
// NOTE: Overflow is intentionally ignored. Absolute indices are always
// validated before use anyway.
struct ForBaseIndex(i16);

impl ForBaseIndex {
    fn relative_from_absolute(&self, absolute_index: i16) -> i16 {
        absolute_index.wrapping_sub(self.0).wrapping_sub(1)
    }

    fn absolute_from_relative(&self, relative_index: i16) -> i16 {
        self.0.wrapping_add(relative_index).wrapping_add(1)
    }
}

/// Manages a region of memory, dividing it into dynamically allocable blocks of
/// a smaller fixed size. Allocation prioritizes reuse of the most recently
/// freed blocks.
///
/// This is a thread-safe, lock-free implementation which doesn't store heap
/// pointers within the managed region. Multiple BlockAllocators may therefore
/// cooperatively manage the same region of memory for the same block size
/// across different threads and processes.
///
/// For a region of N bytes with a block size B, the region is divided into N/B
/// contiguous blocks with a [`BlockHeader`] at the beginning of each. These
/// headers form a singly-linked free-list of available blocks. Block 0 can
/// never be allocated and is used exclusively for its header to point to the
/// first free block. The last free block references back to block 0 to
/// terminate the list. Once a block is allocated, its entire span of B bytes
/// remains untouched until freed.
#[derive(Clone, Copy, Debug)]
pub struct BlockAllocator {
    region: *mut u8,
    region_len: usize,
    block_size: u32,
    num_blocks: i16,
}

// SAFETY: all access to the managed region goes through atomics (headers) or
// is owned exclusively by the caller of `allocate()` (block contents).
unsafe impl Send for BlockAllocator {}
unsafe impl Sync for BlockAllocator {}

impl Default for BlockAllocator {
    /// An empty allocator with no memory to manage. It cannot allocate blocks.
    fn default() -> Self {
        Self {
            region: std::ptr::null_mut(),
            region_len: 0,
            block_size: 0,
            num_blocks: 0,
        }
    }
}

impl BlockAllocator {
    /// Constructs a BlockAllocator to manage the `region_len` bytes at
    /// `region`, allocating blocks of `block_size` bytes. This does NOT
    /// initialize the region. Before any BlockAllocators can allocate blocks
    /// from `region`, [`Self::initialize_region`] must be called once by a
    /// single BlockAllocator managing this region for the same block size.
    ///
    /// # Safety
    /// `region` must be 8-byte aligned and valid for reads and writes of
    /// `region_len` bytes for as long as this allocator (or any copy of it) is
    /// used. `block_size` must be a non-zero multiple of 8, and the region must
    /// hold between 1 and `i16::MAX` blocks.
    pub unsafe fn new(region: *mut u8, region_len: usize, block_size: u32) -> Self {
        // Require 8-byte alignment of the region and of block sizes, to ensure
        // that each BlockHeader is itself 8-byte aligned.
        assert_eq!(region as usize & 7, 0);
        assert!(block_size > 0);
        assert_eq!(block_size & 7, 0);

        // BlockHeader uses a signed 16-bit index to reference other blocks, and
        // block 0 must be able to reference any block; so the total number of
        // blocks must not exceed the max value of an i16.
        let num_blocks: i16 = checked_cast(region_len / block_size as usize);
        assert!(num_blocks > 0);
        Self {
            region,
            region_len,
            block_size,
            num_blocks,
        }
    }

    pub fn region(&self) -> (*mut u8, usize) {
        (self.region, self.region_len)
    }

    pub fn block_size(&self) -> usize {
        self.block_size as usize
    }

    pub fn capacity(&self) -> usize {
        // Note that the first block cannot be allocated, so real capacity is
        // one less than the total number of blocks which fit within the region.
        assert!(self.num_blocks > 0);
        self.num_blocks as usize - 1
    }

    /// Performs a one-time initialization of the memory region managed by this
    /// allocator. Many allocators may operate on the same region, but only one
    /// must initialize that region, and it must do so before any of them can
    /// allocate blocks.
    pub fn initialize_region(&self) {
        // By zeroing the entire region, every block effectively points to its
        // immediate successor as the next free block.
        // SAFETY: `new()` requires the region to be valid for `region_len`.
        unsafe { std::ptr::write_bytes(self.region, 0, self.region_len) };

        // Ensure that the last block points back to the unallocatable first
        // block, indicating the end of the free-list.
        self.set_next_free_block(self.last_block_index(), FRONT_BLOCK_INDEX);
    }

    /// Allocates a block and returns a pointer to its base address, where
    /// `block_size()` contiguous bytes are then owned by the caller. Returns
    /// `None` if out of blocks.
    pub fn allocate(&self) -> Option<NonNull<u8>> {
        let mut front = self.front_header();
        loop {
            let first_free_block_index =
                ForBaseIndex(FRONT_BLOCK_INDEX).absolute_from_relative(front.next);
            if first_free_block_index == FRONT_BLOCK_INDEX
                || !self.is_index_valid(first_free_block_index)
            {
                // The front block can never be allocated, so if that's where
                // the head of the free-list points then the free-list is empty.
                // If it otherwise points to an out-of-range index, the allocator
                // is in an invalid state. In either case, fail the allocation.
                return None;
            }

            // Extract the index of the *next* free block from the header of the
            // first.
            let first_free_block = self.block_header_at(first_free_block_index);

            // SUBTLE: multiple threads may race to load the front block header
            // and end up with the same `first_free_block`. Only one will claim
            // it via `try_update_front_header()` below, but all of them load the
            // maybe-free block's header here. The result is unused unless the
            // update succeeds, which it won't for the losers.
            let first_free_block_header =
                BlockHeader::unpack(first_free_block.load(Ordering::Acquire));
            let next_free_block_index = ForBaseIndex(first_free_block_index)
                .absolute_from_relative(first_free_block_header.next);
            if !self.is_index_valid(next_free_block_index) {
                // Invalid block header, so we cannot proceed.
                return None;
            }

            if self.try_update_front_header(&mut front, next_free_block_index) {
                // Updating the front header to point at the second free block
                // effectively allocates the first by removing it from the
                // free-list; the allocator won't touch it until it's freed.
                return NonNull::new(first_free_block as *const AtomicU32 as *mut u8);
            }

            // Another thread modified the front block header since we fetched
            // it; `front` now has a fresh copy, so retry.
        }
    }

    /// Frees a block back to the allocator, given its base address as returned
    /// by a prior call to [`Self::allocate`]. Returns false if `ptr` was not a
    /// valid block to free.
    pub fn free(&self, ptr: *mut u8) -> bool {
        // Derive a block index from the given address, relative to the start of
        // this allocator's managed region.
        //
        // DEVIATION: the C++ narrows the quotient to int16_t before validating
        // it, so a far-away pointer could wrap into range. Here the quotient is
        // range-checked at full width first.
        let byte_offset = (ptr as isize).wrapping_sub(self.region as isize);
        let new_free_index = byte_offset / self.block_size as isize;
        let Ok(new_free_index) = i16::try_from(new_free_index) else {
            return false;
        };
        if new_free_index == FRONT_BLOCK_INDEX || !self.is_index_valid(new_free_index) {
            // The first block cannot be freed, and obviously neither can any
            // block out of range for this allocator.
            return false;
        }

        let mut front = self.front_header();
        loop {
            let first_free_index =
                ForBaseIndex(FRONT_BLOCK_INDEX).absolute_from_relative(front.next);
            if !self.is_index_valid(first_free_index) {
                // The front block header is in an invalid state.
                return false;
            }

            // The application calling free() implies that it's done using the
            // block, so overwrite its contents with a new header which points
            // to the current head of the free-list.
            self.set_next_free_block(new_free_index, first_free_index);

            // Now try to make this block the new head of the free-list. On
            // failure `front` has an updated copy, so loop and try again.
            if self.try_update_front_header(&mut front, new_free_index) {
                return true;
            }
        }
    }

    // Attempts to update the front block's header to point to `first_free_block`
    // as the free-list's new head. `last_known_header` is the most recently
    // known value of the first block's header; the call can only succeed if that
    // value can be atomically swapped out for a new one. On failure it's
    // updated to the current value.
    fn try_update_front_header(
        &self,
        last_known_header: &mut BlockHeader,
        first_free_block: i16,
    ) -> bool {
        // `version` overflow is acceptable: it is only a tag protecting against
        // the ABA problem. Every alloc and free completes via an atomic exchange
        // of the front block header which also increments the version.
        let new_version = last_known_header.version.wrapping_add(1);
        let relative_next =
            ForBaseIndex(FRONT_BLOCK_INDEX).relative_from_absolute(first_free_block);
        let new_header = BlockHeader {
            version: new_version,
            next: relative_next,
        };

        // A weak compare/exchange is used since this is always called within a
        // tight retry loop.
        match self.block_header_at(FRONT_BLOCK_INDEX).compare_exchange_weak(
            last_known_header.pack(),
            new_header.pack(),
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => true,
            Err(current) => {
                *last_known_header = BlockHeader::unpack(current);
                false
            }
        }
    }

    // Atomically stores a header for the free block at `index`, making it
    // reference `next_free_block` (an absolute index) as the next free block.
    fn set_next_free_block(&self, index: i16, next_free_block: i16) {
        assert!(index > 0);
        let relative_next = ForBaseIndex(index).relative_from_absolute(next_free_block);
        self.block_header_at(index).store(
            BlockHeader {
                version: 0,
                next: relative_next,
            }
            .pack(),
            Ordering::Release,
        );
    }

    fn front_header(&self) -> BlockHeader {
        BlockHeader::unpack(self.block_header_at(FRONT_BLOCK_INDEX).load(Ordering::Relaxed))
    }

    fn last_block_index(&self) -> i16 {
        self.num_blocks - 1
    }

    fn is_index_valid(&self, index: i16) -> bool {
        index >= 0 && index <= self.last_block_index()
    }

    // Returns a reference to the atomic header for the block at `index`.
    fn block_header_at(&self, index: i16) -> &AtomicU32 {
        assert!(self.is_index_valid(index));
        // SAFETY: `new()` guarantees the region is valid and 8-byte aligned,
        // and block sizes are multiples of 8, so every block start is a valid,
        // aligned location for an `AtomicU32`; the index is in range.
        unsafe {
            &*(self.region.add(self.block_size as usize * index as usize) as *const AtomicU32)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    const PAGE_SIZE: usize = 16384;
    const BLOCK_SIZE: usize = 32;

    #[repr(align(8))]
    struct Page([u8; PAGE_SIZE]);

    struct Fixture {
        // Boxed so the region keeps a stable address.
        page: Box<Page>,
        allocator: BlockAllocator,
    }

    impl Fixture {
        fn new() -> Self {
            let mut page = Box::new(Page([0; PAGE_SIZE]));
            let allocator =
                unsafe { BlockAllocator::new(page.0.as_mut_ptr(), PAGE_SIZE, BLOCK_SIZE as u32) };
            allocator.initialize_region();
            Self { page, allocator }
        }
    }

    #[test]
    fn header_packing_matches_c_layout() {
        // {.version = 0x1234, .next = -2} as a little-endian u16/i16 pair.
        let header = BlockHeader { version: 0x1234, next: -2 };
        assert_eq!(header.pack().to_le_bytes(), [0x34, 0x12, 0xfe, 0xff]);
        assert_eq!(BlockHeader::unpack(header.pack()), header);
    }

    #[test]
    fn basic() {
        // Basic consistency check for a single thread accessing the allocator
        // sequentially. Blocks are allocated, fully overwritten, and then freed.
        let f = Fixture::new();
        let a = f.allocator;
        let mut blocks = HashSet::new();
        (0..a.capacity()).for_each(|_| {
            let block = a.allocate().expect("block");
            unsafe { std::ptr::write_bytes(block.as_ptr(), 0xaa, BLOCK_SIZE) };
            assert!(blocks.insert(block.as_ptr() as usize));
        });

        // All capacity has been allocated, so allocate() should fail.
        assert!(a.allocate().is_none());

        // The entire region (except the first block) should be filled with 0xaa
        // bytes, as all blocks were allocated and filled completely.
        assert!(f.page.0[BLOCK_SIZE..].iter().all(|&b| b == 0xaa));

        assert!(blocks.into_iter().all(|b| a.free(b as *mut u8)));
    }

    #[test]
    fn free_rejects_invalid_pointers() {
        let f = Fixture::new();
        let a = f.allocator;
        let base = a.region().0;
        assert!(!a.free(base)); // front block
        assert!(!a.free(unsafe { base.add(PAGE_SIZE) })); // one past the end
        assert!(!a.free((base as usize).wrapping_sub(BLOCK_SIZE) as *mut u8));
        // A pointer so far away that narrowing to i16 would wrap into range.
        assert!(!a.free(unsafe { base.add(BLOCK_SIZE << 16).add(BLOCK_SIZE) }));
    }

    #[test]
    fn alloc_use_free_race() {
        // One worker allocates blocks and writes to them non-atomically while
        // another frees them, exercising the memory ordering constraints.
        const NUM_ITERATIONS: usize = 1000;
        let f = Fixture::new();
        let a = f.allocator;
        let allocated = (0..NUM_ITERATIONS)
            .map(|_| std::sync::atomic::AtomicPtr::new(std::ptr::null_mut::<usize>()))
            .collect::<Vec<_>>();

        std::thread::scope(|s| {
            s.spawn(|| {
                (0..NUM_ITERATIONS).for_each(|i| {
                    let data = std::iter::repeat_with(|| a.allocate())
                        .find_map(|p| p)
                        .unwrap()
                        .as_ptr() as *mut usize;
                    unsafe { *data = i };
                    allocated[i].store(data, Ordering::Release);
                })
            });
            s.spawn(|| {
                (0..NUM_ITERATIONS).for_each(|i| {
                    let data = std::iter::repeat_with(|| allocated[i].load(Ordering::Acquire))
                        .find(|p| !p.is_null())
                        .unwrap();
                    assert_eq!(i, unsafe { *data });
                    assert!(a.free(data as *mut u8));
                })
            });
        });
    }

    #[test]
    fn stress_test() {
        // Races allocate() and free() across workers. Workers mark their blocks
        // and verify the markers when freeing; at the end every block must still
        // be allocable, i.e. none were lost to racy accounting.
        const NUM_ITERATIONS_PER_WORKER: usize = 1000;
        const NUM_ALLOCATIONS_PER_ITERATION: usize = 50;
        const NUM_WORKERS: u32 = 4;
        let f = Fixture::new();
        let a = f.allocator;

        std::thread::scope(|s| {
            (0..NUM_WORKERS).for_each(|id| {
                s.spawn(move || {
                    (0..NUM_ITERATIONS_PER_WORKER).for_each(|_| {
                        let allocations = (0..NUM_ALLOCATIONS_PER_ITERATION)
                            .filter_map(|_| a.allocate())
                            .inspect(|p| unsafe {
                                (*(p.as_ptr() as *const AtomicU32)).store(id, Ordering::Relaxed)
                            })
                            .collect::<Vec<_>>();
                        allocations.into_iter().for_each(|p| {
                            let marker = unsafe { (*(p.as_ptr() as *const AtomicU32)).load(Ordering::Relaxed) };
                            assert_eq!(id, marker);
                            assert!(a.free(p.as_ptr()));
                        });
                    })
                });
            })
        });

        let allocable_capacity = (0..a.capacity()).filter_map(|_| a.allocate()).count();
        assert_eq!(a.capacity(), allocable_capacity);
    }
}
