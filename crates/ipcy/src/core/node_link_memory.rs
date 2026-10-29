//! Port of `ipcz/node_link_memory.{h,cc}`.
//!
//! DEVIATION: upstream's NodeLinkMemory calls into `Node` and `NodeLink`
//! directly. Those arrive with the node layer (Phase 6), so the operations
//! NodeLinkMemory needs from them are expressed here as the [`MemoryNode`] and
//! [`MemoryLink`] traits, which `Node` and `NodeLink` will implement.

use std::collections::HashMap;
use std::mem::{offset_of, size_of};
use std::ptr::addr_of_mut;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use super::block_allocator::BlockAllocator;
use super::buffer_id::{BufferId, INVALID_BUFFER_ID};
use super::buffer_pool::{BufferPool, WaitForBufferCallback};
use super::driver::Driver;
use super::driver_memory::{DriverMemory, DriverMemoryWithMapping};
use super::driver_memory_mapping::DriverMemoryMapping;
use super::features::Features;
use super::fragment::Fragment;
use super::fragment_descriptor::FragmentDescriptor;
use super::fragment_ref::{FragmentRef, RefCountedFragmentObject};
use super::link_side::{LINK_SIDE_B_ID_BIT, LinkSide};
use super::metrics::{self, BlockAllocationSource};
use super::router_link_state::RouterLinkState;
use super::sublink_id::SublinkId;

/// `IPCZ_MEMORY_FIXED_PARCEL_CAPACITY` from ipcz.h.
pub const MEMORY_FIXED_PARCEL_CAPACITY: u32 = 1 << 0;

/// What a NodeLinkMemory needs from the `Node` using its link.
pub trait MemoryNode: Send + Sync {
    /// The node's enabled features.
    fn features(&self) -> Features;

    /// The `memory_flags` the node was created with.
    fn memory_flags(&self) -> u32;

    /// Allocates a new shared memory region of `size` bytes, either directly
    /// through the node's driver or via a delegate broker, and runs `callback`
    /// with it (possibly synchronously).
    fn allocate_shared_memory(&self, size: usize, callback: Box<dyn FnOnce(DriverMemory) + Send>);
}

/// What a NodeLinkMemory needs from the `NodeLink` using it.
pub trait MemoryLink: Send + Sync {
    /// Shares a new block buffer with the remote node, whose NodeLinkMemory
    /// will map it and call `add_block_buffer()`.
    fn add_block_buffer(&self, id: BufferId, block_size: usize, memory: DriverMemory);
}

type RequestBlockCapacityCallback = Box<dyn FnOnce(bool) + Send>;
pub type RouterLinkStateCallback = Box<dyn FnOnce(FragmentRef<RouterLinkState>) + Send>;

// Fixed allocation size for each NodeLink's primary shared buffer. (128 kB)
const PRIMARY_BUFFER_SIZE: usize = 128 * 1024;

// The front of the primary buffer is reserved for special current and future
// uses which require synchronous availability throughout a link's lifetime.
const PRIMARY_BUFFER_RESERVED_HEADER_SIZE: usize = 256;

// NodeLinkMemory may expand its BufferPool's capacity for each fragment size as
// needed. All newly allocated buffers for this purpose must be a multiple of
// BLOCK_ALLOCATOR_PAGE_SIZE. More specifically, a new buffer allocation for
// fragment size `n` will be the smallest multiple of BLOCK_ALLOCATOR_PAGE_SIZE
// which can still fit at least MIN_BLOCK_ALLOCATOR_CAPACITY blocks of size `n`.
const BLOCK_ALLOCATOR_PAGE_SIZE: usize = 64 * 1024;

// The minimum number of blocks which new BlockAllocator buffers must support.
const MIN_BLOCK_ALLOCATOR_CAPACITY: usize = 8;

// The maximum total BlockAllocator capacity to automatically reserve for any
// given fragment size within the BufferPool. This is not a hard cap on capacity
// per fragment size, but it sets a limit on how large the pool will grow
// automatically in response to failed allocation requests.
const MAX_BLOCK_ALLOCATOR_CAPACITY_PER_FRAGMENT_SIZE: usize = 2 * 1024 * 1024;

// The minimum fragment size (in bytes) to support with dedicated BufferPool
// capacity. All fragment sizes are powers of two. Fragment allocations below
// this size are rounded up to this size.
const MIN_FRAGMENT_SIZE: usize = 64;

// The maximum fragment size to support with dedicated BlockAllocator capacity
// within the BufferPool. Allocations beyond this size must fail or fall back
// onto a different allocation scheme which does not use a BlockAllocator.
const MAX_FRAGMENT_SIZE_FOR_BLOCK_ALLOCATION: usize = 1024 * 1024;

// The minimum fallback fragment size to attempt for best-effort allocations when
// the requested size cannot be accommodated.
const MIN_BEST_EFFORT_FALLBACK_BLOCK_SIZE: usize = 4096;

/// The maximum number of initial portals supported on ConnectNode() API calls.
/// The first `MAX_INITIAL_PORTALS` SublinkIds on a NodeLinkMemory will always be
/// reserved for use by initial portals.
pub const MAX_INITIAL_PORTALS: usize = 12;

// The number of fixed RouterLinkState locations in the primary buffer. This
// limits the maximum number of initial portals supported by the ConnectNode()
// API. Note that these states reside in a fixed location at the end of the
// reserved block.
type InitialRouterLinkStateArray = [RouterLinkState; MAX_INITIAL_PORTALS];
const _: () = assert!(size_of::<InitialRouterLinkStateArray>() == 768);

#[repr(C, align(8))]
struct PrimaryBufferHeader {
    /// Atomic generator for new unique BufferIds to use across the associated
    /// NodeLink. This allows each side of a NodeLink to generate new BufferIds
    /// spontaneously without synchronization or risk of collisions.
    next_buffer_id: AtomicU64,

    /// Atomic generator for new unique SublinkIds to use across the associated
    /// NodeLink, with the same purpose.
    next_sublink_id: AtomicU64,
}

const _: () = assert!(size_of::<PrimaryBufferHeader>() < PRIMARY_BUFFER_RESERVED_HEADER_SIZE);
const PRIMARY_BUFFER_HEADER_PADDING_SIZE: usize =
    PRIMARY_BUFFER_RESERVED_HEADER_SIZE - size_of::<PrimaryBufferHeader>();

const MEM_FOR_64_BYTE_BLOCKS: usize = 64 * 1484;
const MEM_FOR_256_BYTE_BLOCKS: usize = 256 * 9;
const MEM_FOR_512_BYTE_BLOCKS: usize = 512 * 8;
const MEM_FOR_1K_BLOCKS: usize = 1024 * 4;
const MEM_FOR_2K_BLOCKS: usize = 2048 * 4;
const MEM_FOR_4K_BLOCKS: usize = 4096 * 4;

/// This structure always sits at offset 0 in the primary buffer and has a fixed
/// layout according to the NodeLink's agreed upon protocol version. This is the
/// layout for version 0 (currently the only version.)
#[repr(C, align(8))]
struct PrimaryBuffer {
    /// Header + padding occupies the first 256 bytes.
    header: PrimaryBufferHeader,
    reserved_header_padding: [u8; PRIMARY_BUFFER_HEADER_PADDING_SIZE],

    /// Reserved RouterLinkState instances for use only by the NodeLink's initial
    /// portals.
    initial_link_states: InitialRouterLinkStateArray,

    /// Reserved memory for 64-byte block allocators required for
    /// RouterLinkState allocation, as well as (optional) small message and
    /// parcel data buffer allocation. Additional allocators for this and other
    /// block sizes may be adopted by a NodeLinkMemory over its lifetime, but
    /// these remain fixed within the primary buffer.
    mem_for_64_byte_blocks: [u8; MEM_FOR_64_BYTE_BLOCKS],
    mem_for_256_byte_blocks: [u8; MEM_FOR_256_BYTE_BLOCKS],
    mem_for_512_byte_blocks: [u8; MEM_FOR_512_BYTE_BLOCKS],
    mem_for_1k_blocks: [u8; MEM_FOR_1K_BLOCKS],
    mem_for_2k_blocks: [u8; MEM_FOR_2K_BLOCKS],
    mem_for_4k_blocks: [u8; MEM_FOR_4K_BLOCKS],
}

const _: () = assert!(
    size_of::<PrimaryBuffer>() <= PRIMARY_BUFFER_SIZE,
    "PrimaryBuffer structure is too large."
);

impl PrimaryBuffer {
    /// The block allocators over this buffer's fixed regions: 64, 256, 512, 1k,
    /// 2k and 4k blocks.
    ///
    /// # Safety
    /// `this` must point to a PrimaryBuffer-sized, 8-aligned mapped region.
    unsafe fn block_allocators(this: *mut PrimaryBuffer) -> [BlockAllocator; 6] {
        // SAFETY: the offsets and sizes are in-bounds fields of the buffer, and
        // 8-aligned (checked below).
        unsafe {
            [
                BlockAllocator::new(addr_of_mut!((*this).mem_for_64_byte_blocks).cast(), MEM_FOR_64_BYTE_BLOCKS, 64),
                BlockAllocator::new(addr_of_mut!((*this).mem_for_256_byte_blocks).cast(), MEM_FOR_256_BYTE_BLOCKS, 256),
                BlockAllocator::new(addr_of_mut!((*this).mem_for_512_byte_blocks).cast(), MEM_FOR_512_BYTE_BLOCKS, 512),
                BlockAllocator::new(addr_of_mut!((*this).mem_for_1k_blocks).cast(), MEM_FOR_1K_BLOCKS, 1024),
                BlockAllocator::new(addr_of_mut!((*this).mem_for_2k_blocks).cast(), MEM_FOR_2K_BLOCKS, 2048),
                BlockAllocator::new(addr_of_mut!((*this).mem_for_4k_blocks).cast(), MEM_FOR_4K_BLOCKS, 4096),
            ]
        }
    }
}

// The fixed regions exactly fill the 128 kB primary buffer, as in the C++ layout.
const _: () = assert!(size_of::<PrimaryBuffer>() == PRIMARY_BUFFER_SIZE);
const _: () = assert!(offset_of!(PrimaryBuffer, initial_link_states) == 256);
const _: () = assert!(offset_of!(PrimaryBuffer, mem_for_64_byte_blocks) % 8 == 0);
const _: () = assert!(offset_of!(PrimaryBuffer, mem_for_256_byte_blocks) % 8 == 0);
const _: () = assert!(offset_of!(PrimaryBuffer, mem_for_512_byte_blocks) % 8 == 0);
const _: () = assert!(offset_of!(PrimaryBuffer, mem_for_1k_blocks) % 8 == 0);
const _: () = assert!(offset_of!(PrimaryBuffer, mem_for_2k_blocks) % 8 == 0);
const _: () = assert!(offset_of!(PrimaryBuffer, mem_for_4k_blocks) % 8 == 0);

fn get_block_size_for_fragment_size(fragment_size: usize) -> usize {
    MIN_FRAGMENT_SIZE.max(fragment_size.next_power_of_two())
}

#[derive(Default)]
struct Guarded {
    /// The NodeLink which is using this NodeLinkMemory. Used to communicate with
    /// the NodeLinkMemory on the other side of the link.
    node_link: Option<Arc<dyn MemoryLink>>,

    /// Callbacks to invoke when a pending capacity request is fulfilled for a
    /// specific block size. Also used to prevent stacking of capacity requests
    /// for the same block size.
    capacity_callbacks: HashMap<usize, Vec<RequestBlockCapacityCallback>>,
}

/// Owns and manages all shared memory resource allocation on a single NodeLink.
/// Each end of a NodeLink has its own NodeLinkMemory instance cooperatively
/// managing the same dynamic pool of memory, shared exclusively between the two
/// endpoint nodes.
pub struct NodeLinkMemory {
    node: Arc<dyn MemoryNode>,
    link_side: LinkSide,
    available_features: Features,
    allow_memory_expansion_for_parcel_data: bool,

    /// Atomic ID generators for buffers and sublinks allocated by this side of
    /// the link when memv2 is enabled.
    next_buffer_id: AtomicU64,
    next_sublink_id: AtomicU64,

    /// The underlying BufferPool. Note that this object is itself thread-safe,
    /// so access to it is not synchronized by NodeLinkMemory.
    buffer_pool: BufferPool,

    /// This link's fixed primary buffer, which the BufferPool keeps mapped.
    primary_buffer_memory: *mut u8,
    primary_buffer: *mut PrimaryBuffer,

    guarded: Mutex<Guarded>,
}

// SAFETY: the raw pointers refer to the primary buffer mapping owned by
// `buffer_pool` for the lifetime of this object, and all access through them is
// atomic or confined to the thread-safe block allocators.
unsafe impl Send for NodeLinkMemory {}
unsafe impl Sync for NodeLinkMemory {}

impl NodeLinkMemory {
    pub const PRIMARY_BUFFER_ID: BufferId = BufferId::new(0);

    /// The side of the node link to which this NodeLinkMemory belongs.
    pub fn link_side(&self) -> LinkSide {
        self.link_side
    }

    /// The set of runtime features available to both nodes using this memory.
    pub fn available_features(&self) -> &Features {
        &self.available_features
    }

    fn lock(&self) -> MutexGuard<'_, Guarded> {
        self.guarded.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Sets a reference to the NodeLink using this NodeLinkMemory. This is called
    /// by the NodeLink itself before any other methods can be called on the
    /// NodeLinkMemory, and it's only reset to `None` once the NodeLink is
    /// deactivated. This link may be used to share information with the remote
    /// node, where another NodeLinkMemory is cooperatively managing the same
    /// memory pool as this one. `link` must belong to the same side of the node
    /// link as this object.
    pub fn set_node_link(self: &Arc<Self>, link: Option<Arc<dyn MemoryLink>>) {
        let block_sizes_needed = {
            let mut guarded = self.lock();
            guarded.node_link = link;
            if guarded.node_link.is_none() {
                return;
            }

            // Any capacity requests accumulated before NodeLink activation can
            // be carried out now.
            guarded.capacity_callbacks.keys().copied().collect::<Vec<_>>()
        };

        block_sizes_needed
            .into_iter()
            .for_each(|size| self.request_block_capacity(size, Box::new(|_| {})));
    }

    /// Allocates a new DriverMemory object and initializes its contents to be
    /// suitable as the primary buffer of a new NodeLinkMemory. Returns the
    /// memory along with a mapping of it. Both are invalid on failure.
    pub fn allocate_memory(driver: &Arc<dyn Driver>) -> DriverMemoryWithMapping {
        let memory = DriverMemory::allocate(driver, PRIMARY_BUFFER_SIZE);
        if !memory.is_valid() {
            return DriverMemoryWithMapping::default();
        }

        let mapping = memory.map();
        if !mapping.is_valid() {
            return DriverMemoryWithMapping::default();
        }

        let primary_buffer: *mut PrimaryBuffer = mapping.address().cast();
        // SAFETY: the mapping is a fresh, 8-aligned, PRIMARY_BUFFER_SIZE region.
        unsafe {
            // The first allocable BufferId is 1, because the primary buffer
            // uses 0.
            (*primary_buffer).header.next_buffer_id.store(1, Ordering::Relaxed);

            // The first allocable SublinkId is MAX_INITIAL_PORTALS. This way it
            // doesn't matter whether the two ends of a NodeLink initiate their
            // connection with a different initial portal count: neither can
            // request more than MAX_INITIAL_PORTALS, so neither will be assuming
            // initial ownership of any SublinkIds at or above this value.
            (*primary_buffer)
                .header
                .next_sublink_id
                .store(MAX_INITIAL_PORTALS as u64, Ordering::Relaxed);

            // Note: initialize_region() performs an atomic release, so atomic
            // stores before this section can be relaxed.
            PrimaryBuffer::block_allocators(primary_buffer)
                .iter()
                .for_each(BlockAllocator::initialize_region);
        }
        DriverMemoryWithMapping::new(memory, mapping)
    }

    /// Constructs a new NodeLinkMemory with BufferId 0 (the primary buffer)
    /// mapped as `primary_buffer_memory`. The buffer must have been created and
    /// initialized by a prior call to [`Self::allocate_memory`].
    pub fn create(
        node: Arc<dyn MemoryNode>,
        side: LinkSide,
        remote_features: &Features,
        primary_buffer_memory: DriverMemoryMapping,
    ) -> Arc<NodeLinkMemory> {
        let available_features = node.features().intersect(remote_features);
        let allow_memory_expansion_for_parcel_data =
            node.memory_flags() & MEMORY_FIXED_PARCEL_CAPACITY == 0;
        let primary_buffer_ptr = primary_buffer_memory.address();

        assert!(primary_buffer_memory.size() >= PRIMARY_BUFFER_SIZE);
        let primary_buffer: *mut PrimaryBuffer = primary_buffer_ptr.cast();
        // SAFETY: just checked the size; alignment is checked by BlockAllocator.
        let allocators = unsafe { PrimaryBuffer::block_allocators(primary_buffer) };

        let buffer_pool = BufferPool::new();
        buffer_pool.add_block_buffer(Self::PRIMARY_BUFFER_ID, primary_buffer_memory, &allocators);

        Arc::new(NodeLinkMemory {
            node,
            link_side: side,
            available_features,
            allow_memory_expansion_for_parcel_data,
            next_buffer_id: AtomicU64::new(1),
            next_sublink_id: AtomicU64::new(MAX_INITIAL_PORTALS as u64),
            buffer_pool,
            primary_buffer_memory: primary_buffer_ptr,
            primary_buffer,
            guarded: Mutex::new(Guarded::default()),
        })
    }

    fn header(&self) -> &PrimaryBufferHeader {
        // SAFETY: the primary buffer outlives self; the header holds atomics.
        unsafe { &(*self.primary_buffer).header }
    }

    fn side_bit(&self) -> u64 {
        if self.link_side.is_side_b() { 1 << LINK_SIDE_B_ID_BIT } else { 0 }
    }

    /// Returns a new BufferId which should still be unused by any buffer in this
    /// NodeLinkMemory's BufferPool, or that of its peer NodeLinkMemory. When
    /// allocating a new buffer to add to the BufferPool, its BufferId should be
    /// procured by calling this method.
    pub fn allocate_new_buffer_id(&self) -> BufferId {
        if self.available_features.mem_v2() {
            return BufferId::new(self.next_buffer_id.fetch_add(1, Ordering::Relaxed) | self.side_bit());
        }
        BufferId::new(self.header().next_buffer_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Returns the first of `count` newly allocated, contiguous sublink IDs for
    /// use on the corresponding NodeLink.
    pub fn allocate_sublink_ids(&self, count: usize) -> SublinkId {
        if self.available_features.mem_v2() {
            return SublinkId::new(
                self.next_sublink_id.fetch_add(count as u64, Ordering::Relaxed) | self.side_bit(),
            );
        }
        SublinkId::new(self.header().next_sublink_id.fetch_add(count as u64, Ordering::Relaxed))
    }

    /// Returns a ref to the RouterLinkState for the `i`th initial portal on the
    /// NodeLink, established by the Connect() call which created this link.
    /// Unlike other RouterLinkStates which are allocated dynamically, these have
    /// a fixed location within the NodeLinkMemory's primary buffer. The returned
    /// FragmentRef is unmanaged and will never free its underlying fragment.
    pub fn get_initial_router_link_state(&self, i: usize) -> FragmentRef<RouterLinkState> {
        assert!(i < MAX_INITIAL_PORTALS);
        // SAFETY: `i` is in bounds of the fixed array within the primary buffer.
        let state = unsafe {
            addr_of_mut!((*self.primary_buffer).initial_link_states)
                .cast::<RouterLinkState>()
                .add(i)
        };
        let offset = (state as usize - self.primary_buffer_memory as usize) as u32;
        let descriptor =
            FragmentDescriptor::new(Self::PRIMARY_BUFFER_ID, offset, size_of::<RouterLinkState>() as u32);
        // SAFETY: the state lives in the primary buffer for this object's life.
        FragmentRef::unmanaged(unsafe { Fragment::from_descriptor_unsafe(&descriptor, state.cast()) })
    }

    /// Resolves `descriptor` to a concrete Fragment. If the descriptor is null or
    /// describes a region of memory which exceeds the bounds of the identified
    /// buffer, this returns a null Fragment. If the descriptor's BufferId is not
    /// yet registered with this NodeLinkMemory, this returns a pending Fragment
    /// with the same BufferId and dimensions as `descriptor`.
    pub fn get_fragment(&self, descriptor: &FragmentDescriptor) -> Fragment {
        self.buffer_pool.get_fragment(descriptor)
    }

    /// Adopts an existing reference to a RefCountedFragment within `fragment`,
    /// which must be a valid, properly aligned, and sufficiently sized fragment
    /// to hold a `T`. This does NOT increment the ref count of the
    /// RefCountedFragment.
    pub fn adopt_fragment_ref<T: RefCountedFragmentObject>(self: &Arc<Self>, fragment: Fragment) -> FragmentRef<T> {
        assert!(size_of::<T>() <= fragment.size() as usize);
        FragmentRef::adopt_existing_ref(self.clone(), fragment)
    }

    /// Attempts to adopt an existing reference to a RefCountedFragment located
    /// at `descriptor`. Returns a null ref if the fragment descriptor is null,
    /// misaligned, or of insufficient size. This does NOT increment the ref
    /// count of the RefCountedFragment.
    pub fn adopt_fragment_ref_if_valid<T: RefCountedFragmentObject>(
        self: &Arc<Self>,
        descriptor: &FragmentDescriptor,
    ) -> FragmentRef<T> {
        if descriptor.is_null()
            || (descriptor.size() as usize) < size_of::<T>()
            || !descriptor.offset().is_multiple_of(8)
        {
            return FragmentRef::default();
        }

        let fragment = self.get_fragment(descriptor);
        if fragment.is_null() {
            return FragmentRef::default();
        }
        self.adopt_fragment_ref(fragment)
    }

    /// Adds a new buffer to the underlying BufferPool to use as additional
    /// allocation capacity for blocks of size `block_size`. Note that the
    /// contents of the mapped region must already be initialized as a
    /// BlockAllocator.
    pub fn add_block_buffer(&self, id: BufferId, block_size: usize, mapping: DriverMemoryMapping) -> bool {
        if id == INVALID_BUFFER_ID {
            return false;
        }

        if block_size < MIN_FRAGMENT_SIZE || block_size & 7 != 0 {
            return false;
        }

        let block_capacity = mapping.size() / block_size;
        if block_capacity < MIN_BLOCK_ALLOCATOR_CAPACITY {
            return false;
        }

        // SAFETY: the mapping is valid, 8-aligned shared memory and block_size
        // is a multiple of 8; the pool re-validates containment.
        let allocator = unsafe {
            BlockAllocator::new(mapping.address(), mapping.size(), block_size as u32)
        };
        self.buffer_pool.add_block_buffer(id, mapping, &[allocator])
    }

    /// Allocates a Fragment of `size` bytes from the underlying BufferPool. May
    /// return a null Fragment if there was no readily available capacity.
    pub fn allocate_fragment(self: &Arc<Self>, size: usize) -> Fragment {
        let fragment = self.allocate_fragment_internal(size);
        if size == 0 {
            return fragment;
        }

        let block_size = if size > MAX_FRAGMENT_SIZE_FOR_BLOCK_ALLOCATION {
            0
        } else {
            get_block_size_for_fragment_size(size)
        };
        metrics::record_allocate_block_result(BlockAllocationSource::Parcel, block_size, !fragment.is_null());
        fragment
    }

    /// As `allocate_fragment()`, without recording any metrics for the attempt.
    fn allocate_fragment_internal(self: &Arc<Self>, size: usize) -> Fragment {
        if size == 0 || size > MAX_FRAGMENT_SIZE_FOR_BLOCK_ALLOCATION {
            // TODO: Support an alternative allocation scheme for large requests.
            return Fragment::default();
        }

        let block_size = get_block_size_for_fragment_size(size);
        let fragment = self.buffer_pool.allocate_block(block_size);
        if fragment.is_null() {
            // Use failure as a hint to possibly expand the pool's capacity. The
            // caller's allocation will still fail, but maybe future allocations
            // won't.
            if self.can_expand_block_capacity(block_size) {
                self.request_block_capacity(
                    block_size,
                    Box::new(|success| {
                        if !success {
                            log::debug!("Failed to allocate new block capacity.");
                        }
                    }),
                );
            }
        }
        fragment
    }

    /// Attempts to allocate a Fragment of at least `size` bytes. If there are no
    /// readily available fragments large enough, this may return a fragment
    /// smaller than `size`.
    pub fn allocate_fragment_best_effort(self: &Arc<Self>, size: usize) -> Fragment {
        // TODO: Support an alternative allocation scheme for larger requests.
        let ideal_block_size = get_block_size_for_fragment_size(size);
        let largest_block_size = ideal_block_size.min(MAX_FRAGMENT_SIZE_FOR_BLOCK_ALLOCATION);
        let smallest_block_size = MIN_BEST_EFFORT_FALLBACK_BLOCK_SIZE;

        std::iter::successors(Some(largest_block_size), |block_size| Some(block_size / 2))
            .take_while(|&block_size| block_size >= smallest_block_size)
            .map(|block_size| self.allocate_fragment_internal(block_size))
            .find(|fragment| !fragment.is_null())
            .unwrap_or_default()
    }

    /// Frees a Fragment previously allocated through this NodeLinkMemory.
    /// Returns true on success. Returns false if `fragment` does not represent
    /// an allocated fragment within this NodeLinkMemory.
    pub fn free_fragment(&self, fragment: &Fragment) -> bool {
        if fragment.is_null() || fragment.size() as usize > MAX_FRAGMENT_SIZE_FOR_BLOCK_ALLOCATION {
            // TODO: Once we support larger non-block-based allocations, support
            // freeing them from here as well.
            return false;
        }

        assert!(fragment.is_addressable());
        self.buffer_pool.free_block(fragment)
    }

    /// Allocates a fragment to store a new RouterLinkState and initializes a new
    /// RouterLinkState instance there. If no capacity is currently available to
    /// allocate an appropriate fragment, this may return null.
    pub fn try_allocate_router_link_state(self: &Arc<Self>) -> FragmentRef<RouterLinkState> {
        let fragment = self.buffer_pool.allocate_block(size_of::<RouterLinkState>());
        if !fragment.is_null() {
            return self.initialize_router_link_state_fragment(fragment);
        }

        // Unlike with the more generic allocate_fragment(), we unconditionally
        // lobby for additional capacity when RouterLinkState allocation fails.
        self.request_block_capacity(size_of::<RouterLinkState>(), Box::new(|_| {}));
        FragmentRef::default()
    }

    /// Allocates a fragment to store a new RouterLinkState and initializes a new
    /// RouterLinkState instance there. Calls `callback` with a reference to the
    /// new fragment once allocated. Unlike [`Self::try_allocate_router_link_state`],
    /// this allocation always succeeds eventually unless driver memory allocation
    /// itself begins to fail unrecoverably. If the allocation can succeed
    /// synchronously, `callback` may be called before this method returns.
    pub fn allocate_router_link_state(self: &Arc<Self>, callback: RouterLinkStateCallback) {
        let fragment = self.buffer_pool.allocate_block(size_of::<RouterLinkState>());
        if !fragment.is_null() {
            callback(self.initialize_router_link_state_fragment(fragment));
            return;
        }

        let memory = self.clone();
        self.request_block_capacity(
            size_of::<RouterLinkState>(),
            Box::new(move |ok| {
                if !ok {
                    log::debug!("Could not allocate a new RouterLinkState");
                    callback(FragmentRef::default());
                    return;
                }
                memory.allocate_router_link_state(callback);
            }),
        );
    }

    /// Runs `callback` as soon as the identified buffer is added to the
    /// underlying BufferPool. If the buffer is already present here, `callback`
    /// is run immediately.
    pub fn wait_for_buffer_async(&self, id: BufferId, callback: WaitForBufferCallback) {
        self.buffer_pool.wait_for_buffer_async(id, callback);
    }

    /// Called when the corresponding NodeLink has had its transport
    /// disconnected. This should clean up any resources that might have been
    /// retained pending future transport activity.
    pub fn notify_link_disconnected(&self) {
        self.buffer_pool.drop_pending_buffer_callbacks();
    }

    // Indicates whether the NodeLinkMemory should be allowed to expand its
    // allocation capacity further for blocks of size `block_size`.
    fn can_expand_block_capacity(&self, block_size: usize) -> bool {
        self.allow_memory_expansion_for_parcel_data
            && self.buffer_pool.get_total_block_capacity(block_size)
                < MAX_BLOCK_ALLOCATOR_CAPACITY_PER_FRAGMENT_SIZE
    }

    // Attempts to expand the total block allocation capacity for blocks of
    // `block_size` bytes. `callback` may be called synchronously or
    // asynchronously with a result indicating whether the expansion succeeded.
    fn request_block_capacity(self: &Arc<Self>, block_size: usize, callback: RequestBlockCapacityCallback) {
        assert!(block_size >= MIN_FRAGMENT_SIZE);
        let min_buffer_size = block_size * MIN_BLOCK_ALLOCATOR_CAPACITY;
        let num_pages = min_buffer_size.div_ceil(BLOCK_ALLOCATOR_PAGE_SIZE);
        let buffer_size = num_pages * BLOCK_ALLOCATOR_PAGE_SIZE;

        let link = {
            let mut guarded = self.lock();
            let need_new_request = !guarded.capacity_callbacks.contains_key(&block_size);
            guarded.capacity_callbacks.entry(block_size).or_default().push(callback);
            if !need_new_request {
                // There was already a request pending for this block size.
                // `callback` will be run when that request completes.
                return;
            }

            match &guarded.node_link {
                Some(link) => link.clone(),
                // Allocation requests will be fulfilled once the NodeLink is
                // activated and we're given a reference to it via
                // set_node_link().
                None => return,
            }
        };

        let this = self.clone();
        self.node.allocate_shared_memory(
            buffer_size,
            Box::new(move |memory: DriverMemory| {
                let mapping = memory.map();
                if !mapping.is_valid() {
                    this.on_capacity_request_complete(block_size, false);
                    return;
                }

                // SAFETY: freshly mapped, 8-aligned memory of `buffer_size`
                // bytes; `block_size` is a power of two >= 64.
                let allocator =
                    unsafe { BlockAllocator::new(mapping.address(), mapping.size(), block_size as u32) };
                allocator.initialize_region();

                // SUBTLE: We first share the new buffer with the remote node,
                // then register it locally. If we registered the buffer locally
                // first, this could lead to a deadlock on the remote node:
                // another thread on this node could race to send a message which
                // uses a fragment from the new buffer before the message below
                // is sent to share the new buffer with the remote node.
                //
                // The remote node would not be able to dispatch the first
                // message until its pending fragment was resolved, and it
                // wouldn't be able to resolve the pending fragment until it
                // received the new buffer. But the message carrying the new
                // buffer would have been queued after the first message and
                // therefore could not be dispatched until after the first
                // message. Hence, deadlock.
                let id = this.allocate_new_buffer_id();
                link.add_block_buffer(id, block_size, memory);
                this.add_block_buffer(id, block_size, mapping);
                this.on_capacity_request_complete(block_size, true);
            }),
        );
    }

    fn on_capacity_request_complete(&self, block_size: usize, success: bool) {
        let callbacks = match self.lock().capacity_callbacks.remove(&block_size) {
            Some(callbacks) => callbacks,
            None => return,
        };
        callbacks.into_iter().for_each(|callback| callback(success));
    }

    // Initializes `fragment` as a new RouterLinkState and returns a ref to it.
    fn initialize_router_link_state_fragment(self: &Arc<Self>, fragment: Fragment) -> FragmentRef<RouterLinkState> {
        assert!(!fragment.is_null());
        let router_link_state = self.adopt_fragment_ref::<RouterLinkState>(fragment);
        // SAFETY: the block is exclusively ours, large enough and 8-aligned.
        unsafe { RouterLinkState::initialize(router_link_state.get()) };
        router_link_state
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::heap_memory_driver::HeapMemoryDriver;
    use crate::core::testing::test_memory_node::{TestMemoryLink, TestMemoryNode};
    use std::sync::atomic::AtomicBool;

    struct Fixture {
        driver: Arc<HeapMemoryDriver>,
        memory_a: Arc<NodeLinkMemory>,
        memory_b: Arc<NodeLinkMemory>,
        links: Vec<Arc<TestMemoryLink>>,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_flags(0, 0)
        }

        // Connects a pair of NodeLinkMemory instances sharing a primary buffer,
        // like NodeLinkMemoryTest::ConnectNodes().
        fn with_flags(memory_flags_a: u32, memory_flags_b: u32) -> Self {
            let driver = HeapMemoryDriver::new();
            let dyn_driver = driver.as_driver();
            let node_a = TestMemoryNode::new(&dyn_driver, Features::default(), memory_flags_a);
            let node_b = TestMemoryNode::new(&dyn_driver, Features::default(), memory_flags_b);

            let buffer = NodeLinkMemory::allocate_memory(&dyn_driver);
            let memory_a = NodeLinkMemory::create(node_a, LinkSide::A, &Features::default(), buffer.mapping);
            let memory_b = NodeLinkMemory::create(
                node_b,
                LinkSide::B,
                &Features::default(),
                buffer.memory.map(),
            );

            // Each side's link delivers new buffers to the other side's memory.
            let link_a = TestMemoryLink::new(&memory_b);
            let link_b = TestMemoryLink::new(&memory_a);
            memory_a.set_node_link(Some(link_a.clone()));
            memory_b.set_node_link(Some(link_b.clone()));
            Self {
                driver,
                memory_a,
                memory_b,
                links: vec![link_a, link_b],
            }
        }

        fn add_blocks_to_memory(&self, memory: &Arc<NodeLinkMemory>, block_size: usize) {
            const NUM_BLOCKS: usize = 32;
            let mapping =
                DriverMemory::allocate(&self.driver.as_driver(), block_size * NUM_BLOCKS).map();
            let allocator = unsafe {
                BlockAllocator::new(mapping.address(), mapping.size(), block_size as u32)
            };
            allocator.initialize_region();
            let id = memory.allocate_new_buffer_id();
            assert!(memory.add_block_buffer(id, block_size, mapping));
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Break the memory <-> link reference cycles.
            self.memory_a.set_node_link(None);
            self.memory_b.set_node_link(None);
            self.links.iter().for_each(|link| link.disconnect());
        }
    }

    #[test]
    fn basic_alloc_and_free() {
        let f = Fixture::new();
        let fragment = f.memory_a.allocate_fragment(64);
        assert!(fragment.is_addressable());
        assert!(!fragment.address().is_null());
        assert_eq!(fragment.size(), 64);
        assert!(f.memory_a.free_fragment(&fragment));
    }

    #[test]
    fn zero() {
        // Zero-sized fragments cannot be allocated.
        assert!(Fixture::new().memory_a.allocate_fragment(0).is_null());
    }

    #[test]
    fn minimum_size() {
        // Very small fragment sizes a minimum of 64 bytes.
        let f = Fixture::new();
        [1, 2, 3, 4, 17, 63].into_iter().for_each(|size| {
            let fragment = f.memory_a.allocate_fragment(size);
            assert!(fragment.is_addressable());
            assert_eq!(fragment.size(), 64);
        });
    }

    #[test]
    fn round_up_size() {
        let f = Fixture::new();
        f.add_blocks_to_memory(&f.memory_a, 256);
        f.add_blocks_to_memory(&f.memory_a, 512);

        // Fragment sizes are rounded up to the nearest power of 2.
        [(32, 64), (250, 256), (257, 512)].into_iter().for_each(|(request, expected)| {
            let fragment = f.memory_a.allocate_fragment(request);
            assert!(fragment.is_addressable());
            assert_eq!(fragment.size(), expected);
        });
    }

    #[test]
    fn shared_primary_buffer() {
        // Test basic allocation from the primary buffer which both
        // NodeLinkMemory instances share from the moment they're constructed.
        // Each NodeLinkMemory should be able to resolve and free fragments
        // allocated by the other.
        let f = Fixture::new();
        let from_a = f.memory_a.allocate_fragment(8);
        assert!(from_a.is_addressable());
        assert_eq!(from_a.buffer_id(), BufferId::new(0));
        assert!(from_a.size() >= 8);

        let same = f.memory_b.get_fragment(from_a.descriptor());
        assert!(same.is_addressable());
        assert_eq!(same.buffer_id(), from_a.buffer_id());
        assert_eq!(same.offset(), from_a.offset());
        assert_eq!(same.size(), from_a.size());

        let from_b = f.memory_b.allocate_fragment(16);
        assert!(from_b.is_addressable());
        assert_eq!(from_b.buffer_id(), BufferId::new(0));
        assert!(from_b.size() >= 16);

        let same = f.memory_a.get_fragment(from_b.descriptor());
        assert!(same.is_addressable());
        assert_eq!(same.buffer_id(), from_b.buffer_id());
        assert_eq!(same.offset(), from_b.offset());
        assert_eq!(same.size(), from_b.size());

        assert!(f.memory_a.free_fragment(&from_b));
        assert!(f.memory_b.free_fragment(&from_a));
    }

    #[test]
    fn expand_capacity() {
        // If we deplete a NodeLinkMemory's capacity to allocate fragments of a
        // given size, it should automatically acquire new capacity for future
        // allocations.
        const SIZE: usize = 64;
        let f = Fixture::new();
        let has_new_capacity = Arc::new(AtomicBool::new(false));
        let flag = has_new_capacity.clone();
        f.memory_a
            .wait_for_buffer_async(BufferId::new(1), Box::new(move || flag.store(true, Ordering::SeqCst)));
        while !f.memory_a.allocate_fragment(SIZE).is_null() {}

        // Since we're using a synchronous driver, this should have already been
        // true by the time the most recent failed allocation returned.
        assert!(has_new_capacity.load(Ordering::SeqCst));

        // And a subsequent allocation request should now succeed with a
        // fragment from the new buffer.
        let fragment = f.memory_a.allocate_fragment(SIZE);
        assert!(!fragment.is_null());
        assert!(fragment.is_addressable());
        assert_eq!(fragment.buffer_id(), BufferId::new(1));

        // The new buffer should have also been shared with the other
        // NodeLinkMemory already.
        assert!(f.memory_b.free_fragment(&fragment));
    }

    #[test]
    fn limited_capacity_expansion() {
        // A NodeLinkMemory will eventually stop expanding its capacity for new
        // fragments of a given size.
        const SIZE: usize = 64;
        let f = Fixture::new();
        let mut fragments = Vec::new();
        let mut try_alloc = || {
            let fragment = f.memory_a.allocate_fragment(SIZE);
            let ok = !fragment.is_null();
            if ok {
                fragments.push(fragment);
            }
            ok
        };
        loop {
            // Deplete the current capacity.
            while try_alloc() {}
            // Because we're using a synchronous driver, if the NodeLinkMemory
            // will expand its capacity at all, it will have already done so by
            // the time the failed allocation returns above. So if allocation
            // fails again here, we've reached the capacity limit for this
            // fragment size and we can end the test.
            if !try_alloc() {
                break;
            }
        }

        // Any additionally allocated buffers should already have been shared
        // with the other NodeLinkMemory. Let it free all of the fragments and
        // verify success in every case.
        assert!(fragments.iter().all(|fragment| f.memory_b.free_fragment(fragment)));
    }

    #[test]
    fn oversized_allocation() {
        // Allocations which are too large for block-based allocation will fail
        // for now. This may change as new allocation schemes are supported.
        const WAY_TOO_BIG: usize = 64 * 1024 * 1024;
        assert!(Fixture::new().memory_a.allocate_fragment(WAY_TOO_BIG).is_null());
    }

    #[test]
    fn new_block_sizes() {
        // NodeLinkMemory begins life with a fixed set of block allocators
        // available for certain common block sizes. Verify that we can trigger
        // new capacity for larger sizes by attempting to allocate them.
        const PRETTY_BIG: usize = 512 * 1024;
        let f = Fixture::new();

        // No initial capacity for 512 kB fragments.
        assert!(f.memory_a.allocate_fragment(PRETTY_BIG).is_null());

        // But the failure above should have triggered expansion of capacity for
        // that size. This request should succeed.
        let fragment = f.memory_a.allocate_fragment(PRETTY_BIG);
        assert!(!fragment.is_null());
        assert!(fragment.is_addressable());
        assert!(fragment.size() as usize >= PRETTY_BIG);

        // And as with other cases, the new capacity should have already been
        // shared with the other NodeLinkMemory.
        assert!(f.memory_b.free_fragment(&fragment));
    }

    #[test]
    fn fixed_parcel_capacity_disables_expansion() {
        // Dynamic expansion of the allocation capacity can be disabled when
        // configuring a new node. (Upstream's ParcelDataAllocation test drives
        // this through Parcel::AllocateData(), which arrives with the parcel
        // port; this exercises the same NodeLinkMemory behavior directly.)
        const PARCEL_SIZE: usize = 32;
        let f = Fixture::with_flags(0, MEMORY_FIXED_PARCEL_CAPACITY);

        // Allocating from side B, where expansion is disabled, must eventually
        // fail and must only ever draw from the primary buffer.
        let fragments = std::iter::repeat_with(|| f.memory_b.allocate_fragment(PARCEL_SIZE))
            .take_while(|fragment| !fragment.is_null())
            .collect::<Vec<_>>();
        assert!(!fragments.is_empty());
        assert!(fragments.iter().all(|fragment| {
            fragment.size() as usize >= PARCEL_SIZE
                && fragment.buffer_id() == NodeLinkMemory::PRIMARY_BUFFER_ID
        }));
    }

    #[repr(C)]
    struct TestObject {
        ref_counted: crate::core::ref_counted_fragment::RefCountedFragment,
        x: i32,
        y: i32,
    }

    // SAFETY: `ref_counted` is the first field of this repr(C) struct.
    unsafe impl RefCountedFragmentObject for TestObject {
        fn ref_counted_fragment(&self) -> &crate::core::ref_counted_fragment::RefCountedFragment {
            &self.ref_counted
        }
    }

    #[test]
    fn adopt_fragment_ref_if_valid() {
        let f = Fixture::new();
        let object = f
            .memory_a
            .adopt_fragment_ref::<TestObject>(f.memory_a.allocate_fragment(size_of::<TestObject>()));
        unsafe {
            (*object.get()).x = 5;
            (*object.get()).y = 42;
        }

        let valid = FragmentDescriptor::new(
            object.fragment().buffer_id(),
            object.fragment().offset(),
            size_of::<TestObject>() as u32,
        );

        let null = FragmentDescriptor::new(INVALID_BUFFER_ID, valid.offset(), valid.size());
        assert!(f.memory_a.adopt_fragment_ref_if_valid::<TestObject>(&null).is_null());

        let empty = FragmentDescriptor::new(valid.buffer_id(), valid.offset(), 0);
        assert!(f.memory_a.adopt_fragment_ref_if_valid::<TestObject>(&empty).is_null());

        let short = FragmentDescriptor::new(valid.buffer_id(), valid.offset(), size_of::<TestObject>() as u32 - 4);
        assert!(f.memory_a.adopt_fragment_ref_if_valid::<TestObject>(&short).is_null());

        let unaligned = FragmentDescriptor::new(valid.buffer_id(), valid.offset() + 2, valid.size() - 2);
        assert!(f.memory_a.adopt_fragment_ref_if_valid::<TestObject>(&unaligned).is_null());

        let adopted = f.memory_a.adopt_fragment_ref_if_valid::<TestObject>(&valid);
        assert!(adopted.is_addressable());
        assert_eq!((adopted.x, adopted.y), (5, 42));
        // Both refs adopted the same single reference; let only one release it.
        std::mem::forget(adopted);
    }

    #[test]
    fn id_allocation_without_mem_v2_uses_shared_counters() {
        let f = Fixture::new();
        // Both sides draw from the counters in the shared primary buffer.
        assert_eq!(f.memory_a.allocate_new_buffer_id(), BufferId::new(1));
        assert_eq!(f.memory_b.allocate_new_buffer_id(), BufferId::new(2));
        assert_eq!(f.memory_a.allocate_sublink_ids(3), SublinkId::new(MAX_INITIAL_PORTALS as u64));
        assert_eq!(f.memory_b.allocate_sublink_ids(1), SublinkId::new(MAX_INITIAL_PORTALS as u64 + 3));
    }

    #[test]
    fn id_allocation_with_mem_v2_is_per_side() {
        use crate::core::features::IPCZ_FEATURE_MEM_V2;
        let driver = HeapMemoryDriver::new();
        let dyn_driver = driver.as_driver();
        let features = Features::from_node_options(&[IPCZ_FEATURE_MEM_V2], &[]);
        let node = TestMemoryNode::new(&dyn_driver, features, 0);
        let buffer = NodeLinkMemory::allocate_memory(&dyn_driver);
        let a = NodeLinkMemory::create(node.clone(), LinkSide::A, &features, buffer.mapping);
        let b = NodeLinkMemory::create(node, LinkSide::B, &features, buffer.memory.map());

        assert!(a.available_features().mem_v2());
        // Side B IDs carry the high bit; each side counts independently.
        assert_eq!(a.allocate_new_buffer_id(), BufferId::new(1));
        assert_eq!(b.allocate_new_buffer_id(), BufferId::new(1 | 1 << LINK_SIDE_B_ID_BIT));
        assert_eq!(b.allocate_new_buffer_id(), BufferId::new(2 | 1 << LINK_SIDE_B_ID_BIT));
        assert_eq!(a.allocate_sublink_ids(2), SublinkId::new(12));
        assert_eq!(b.allocate_sublink_ids(2), SublinkId::new(12 | 1 << LINK_SIDE_B_ID_BIT));
    }

    #[test]
    fn initial_router_link_states_are_fixed_and_unmanaged() {
        let f = Fixture::new();
        let a0 = f.memory_a.get_initial_router_link_state(0);
        let b0 = f.memory_b.get_initial_router_link_state(0);
        let a11 = f.memory_a.get_initial_router_link_state(11);
        assert_eq!(a0.fragment().offset(), 256);
        assert_eq!(a11.fragment().offset(), 256 + 11 * 64);
        assert_eq!(a0.fragment().size(), 64);
        assert!(a0.memory().is_none());

        // Both sides see the same shared state through their own mappings.
        a0.status.store(0x12, Ordering::SeqCst);
        assert_eq!(b0.status.load(Ordering::SeqCst), 0x12);
        std::mem::forget((a0, b0, a11));
    }

    #[test]
    fn router_link_state_allocation() {
        let f = Fixture::new();
        let state = f.memory_a.try_allocate_router_link_state();
        assert!(state.is_addressable());
        assert_eq!(state.ref_count_for_testing(), 1);
        assert_eq!(state.status.load(Ordering::SeqCst), RouterLinkState::UNSTABLE);

        // Freeing happens when the last ref drops; the block is reusable.
        let fragment = *state.fragment();
        drop(state);
        let again = f.memory_a.try_allocate_router_link_state();
        assert_eq!(again.fragment().offset(), fragment.offset());

        // The async variant can run out of primary capacity and expand.
        let received = Arc::new(Mutex::new(Vec::new()));
        for _ in 0..2000 {
            let received = received.clone();
            f.memory_a.allocate_router_link_state(Box::new(move |state| {
                assert!(state.is_addressable());
                received.lock().unwrap().push(state);
            }));
        }
        assert_eq!(received.lock().unwrap().len(), 2000);
        assert!(received.lock().unwrap().iter().any(|s| s.buffer_id() != NodeLinkMemory::PRIMARY_BUFFER_ID));
        received.lock().unwrap().clear();
    }

    #[test]
    fn allocate_fragment_metrics() {
        use crate::core::metrics::testing::RecordedEvents;
        use crate::core::metrics::{BlockAllocationSize, BlockAllocationSource};

        let f = Fixture::new();
        let events = RecordedEvents::capture();

        // Successful allocation (64 bytes).
        let fragment = f.memory_a.allocate_fragment(64);
        assert!(fragment.is_addressable());
        assert_eq!(1, events.count_result(BlockAllocationSource::BufferPool, true));
        assert_eq!(
            1,
            events.count_size(BlockAllocationSource::BufferPool, true, BlockAllocationSize::B64)
        );

        // Small requests (<= MAX_FRAGMENT_SIZE_FOR_BLOCK_ALLOCATION) go through
        // the BufferPool, so they land in both sets of metrics.
        assert_eq!(1, events.count_result(BlockAllocationSource::Parcel, true));

        // Failed allocation for an unregistered size (8192 bytes).
        assert!(f.memory_a.allocate_fragment(8192).is_null());
        assert_eq!(1, events.count_result(BlockAllocationSource::BufferPool, false));
        assert_eq!(
            1,
            events.count_size(BlockAllocationSource::BufferPool, false, BlockAllocationSize::KB8)
        );
    }

    #[test]
    fn allocate_router_link_state_metrics() {
        use crate::core::metrics::testing::RecordedEvents;
        use crate::core::metrics::{BlockAllocationSize, BlockAllocationSource};

        let f = Fixture::new();
        let events = RecordedEvents::capture();

        let callback_run = Arc::new(AtomicBool::new(false));
        let ran = callback_run.clone();
        f.memory_a.allocate_router_link_state(Box::new(move |state| {
            assert!(state.is_addressable());
            ran.store(true, Ordering::SeqCst);
        }));
        assert!(callback_run.load(Ordering::SeqCst));
        assert_eq!(1, events.count_result(BlockAllocationSource::BufferPool, true));
        assert_eq!(
            1,
            events.count_size(BlockAllocationSource::BufferPool, true, BlockAllocationSize::B64)
        );
        assert_eq!(
            0,
            events.count_size(BlockAllocationSource::BufferPool, false, BlockAllocationSize::B64)
        );

        // RouterLinkState allocation goes straight to the BufferPool, so it must
        // not show up in the parcel metrics.
        assert_eq!(0, events.total(BlockAllocationSource::Parcel));
    }

    #[test]
    fn parcel_allocate_block_metrics() {
        use crate::core::metrics::testing::RecordedEvents;
        use crate::core::metrics::{BlockAllocationSize, BlockAllocationSource::{BufferPool, Parcel as Source}};
        use crate::core::parcel::Parcel;

        let f = Fixture::new();
        let events = RecordedEvents::capture();

        // A zero-sized request is not a meaningful allocation attempt.
        assert!(f.memory_a.allocate_fragment(0).is_null());
        assert_eq!(0, events.total(Source));

        // Successful parcel shared memory allocation (50 bytes plus a
        // FragmentHeader fits in a 64B block).
        let mut parcel1 = Parcel::default();
        parcel1.allocate_data(50, false, Some(&f.memory_a));
        assert!(parcel1.has_data_fragment());
        assert_eq!(1, events.count_result(Source, true));
        assert_eq!(1, events.count_size(Source, true, BlockAllocationSize::B64));

        // Best-effort parcel shared memory allocation (allow_partial) must NOT
        // record to the parcel metrics.
        let mut parcel_best_effort = Parcel::default();
        parcel_best_effort.allocate_data(4000, true, Some(&f.memory_a));
        assert!(parcel_best_effort.has_data_fragment());
        assert_eq!(1, events.total(Source));

        // Failed parcel shared memory allocation for an unregistered size (8192
        // bytes plus a FragmentHeader requires a 16KB block).
        let mut parcel2 = Parcel::default();
        parcel2.allocate_data(8192, false, Some(&f.memory_a));
        assert!(!parcel2.has_data_fragment());
        assert_eq!(1, events.count_result(Source, false));
        assert_eq!(1, events.count_size(Source, false, BlockAllocationSize::KB16));

        // Failed allocation for a size exceeding 1MB (2MB maps to Other). The
        // request never reaches the BufferPool, so it records no BufferPool
        // sample.
        let buffer_pool_total = events.total(BufferPool);
        assert!(buffer_pool_total > 0);
        let mut parcel3 = Parcel::default();
        parcel3.allocate_data(2 * 1024 * 1024, false, Some(&f.memory_a));
        assert!(!parcel3.has_data_fragment());
        assert_eq!(buffer_pool_total, events.total(BufferPool));
        assert_eq!(2, events.count_result(Source, false));
        assert_eq!(1, events.count_size(Source, false, BlockAllocationSize::Other));

        assert_eq!(3, events.total(Source));
        assert_eq!(1, events.count_result(Source, true));
        assert_eq!(2, events.count_result(Source, false));
    }
}
