//! Port of `ipcz/parcel.{h,cc}` and `ipcz/parcel_queue.h`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use super::api_object::{self, ApiObject};
use super::driver_object::DriverObject;
use super::fragment::Fragment;
use super::ipcz_types::IpczHandle;
use super::node_link::NodeLink;
use super::node_link_memory::NodeLinkMemory;
use super::sequence_number::SequenceNumber;
use super::sequenced_queue::{SequencedQueue, SequencedQueueTraits};

/// Arbitrary hard cap on the number of subparcels which can be embedded within a
/// parcel, to mitigate the potential for abuse.
pub const MAX_SUBPARCELS_PER_PARCEL: usize = 1024;

/// When Parcel data is in a shared memory fragment, this header sits at the
/// front of the fragment.
#[repr(C, align(8))]
struct FragmentHeader {
    /// The size in bytes of the parcel data which immediately follows this
    /// header. Must not extend beyond the bounds of the fragment itself.
    ///
    /// Access to this atomic is also used to synchronize access to the parcel
    /// data. Writes to the fragment must be finalized by calling
    /// `commit_data()`, and any node receiving this parcel must adopt the
    /// fragment by calling `adopt_data_fragment()`.
    size: AtomicU32,

    /// Reserved padding for 8-byte parcel data alignment.
    reserved: AtomicU32,
}

const FRAGMENT_HEADER_SIZE: usize = std::mem::size_of::<FragmentHeader>();
const _: () = assert!(FRAGMENT_HEADER_SIZE == 8);

/// Holds a Fragment and a reference to its backing memory. Used when a Parcel's
/// data lives in shared memory. This implements exclusive ownership of the
/// underlying fragment, and upon destruction it releases the fragment back into
/// the memory pool.
struct DataFragment {
    /// `None` once the fragment has been released.
    memory: Option<Arc<NodeLinkMemory>>,
    fragment: Fragment,
}

impl DataFragment {
    fn new(memory: Arc<NodeLinkMemory>, fragment: Fragment) -> Self {
        // Parcels can only be given data fragments which are already
        // addressable.
        assert!(fragment.is_addressable());
        Self {
            memory: Some(memory),
            fragment,
        }
    }

    /// Relinquishes the fragment without freeing it.
    fn release(mut self) -> Fragment {
        self.memory = None;
        self.fragment
    }
}

impl Drop for DataFragment {
    fn drop(&mut self) {
        if let Some(memory) = &self.memory {
            memory.free_fragment(&self.fragment);
        }
    }
}

/// A variant backing type for the parcel's data. Data may be in shared memory,
/// heap-allocated and initialized from within the Parcel, or heap-allocated by a
/// received Message and moved into the Parcel from there.
enum DataStorage {
    Empty,
    Fragment(DataFragment),
    Heap(#[allow(dead_code)] Vec<u8>),
    Received(#[allow(dead_code)] Vec<u8>),
}

/// Represents a parcel queued within a portal, either for inbound retrieval or
/// outgoing transfer.
pub struct Parcel {
    sequence_number: SequenceNumber,

    /// If this Parcel was received from a remote node, this tracks the NodeLink
    /// which received it.
    remote_source: Option<Arc<NodeLink>>,

    /// Concrete storage for the parcel's data, along with a view of the data not
    /// yet consumed. The view may reference only a subset of the storage; that
    /// subset is the Parcel's data. It always points into memory owned by
    /// `data_storage` (or is empty).
    data_storage: DataStorage,
    data_view: (*mut u8, usize),

    /// The set of APIObjects attached to this parcel (some slots may be empty
    /// if their object was moved out), and the index of the first object not yet
    /// consumed.
    objects: Vec<Option<Arc<dyn ApiObject>>>,
    objects_start: usize,

    /// By default, all parcels have a single subparcel (themselves) at index 0.
    /// On any Parcel that exists as a subparcel of another, these fields will be
    /// updated by the containing Parcel as needed.
    num_subparcels: usize,
    subparcel_index: usize,

    envelope: DriverObject,
}

// SAFETY: `data_view` points into memory exclusively owned by this Parcel (or
// shared memory kept alive by `data_storage`), and everything else is `Send`.
unsafe impl Send for Parcel {}
unsafe impl Sync for Parcel {}

impl Default for Parcel {
    fn default() -> Self {
        Self::new(SequenceNumber::new(0))
    }
}

impl Parcel {
    pub fn new(sequence_number: SequenceNumber) -> Self {
        Self {
            sequence_number,
            remote_source: None,
            data_storage: DataStorage::Empty,
            data_view: (std::ptr::null_mut(), 0),
            objects: Vec::new(),
            objects_start: 0,
            num_subparcels: 1,
            subparcel_index: 0,
            envelope: DriverObject::default(),
        }
    }

    pub fn set_sequence_number(&mut self, n: SequenceNumber) {
        self.sequence_number = n;
    }

    pub fn sequence_number(&self) -> SequenceNumber {
        self.sequence_number
    }

    pub fn set_num_subparcels(&mut self, num_subparcels: usize) {
        self.num_subparcels = num_subparcels;
    }

    pub fn num_subparcels(&self) -> usize {
        self.num_subparcels
    }

    pub fn set_subparcel_index(&mut self, index: usize) {
        self.subparcel_index = index;
    }

    pub fn subparcel_index(&self) -> usize {
        self.subparcel_index
    }

    /// Indicates whether this Parcel is empty, meaning its data and objects have
    /// been fully consumed.
    pub fn is_empty(&self) -> bool {
        self.data_view().is_empty() && self.objects_view().is_empty()
    }

    /// Sets this Parcel's data to `range` of `buffer`, which the Parcel takes
    /// ownership of. Any prior data in the Parcel is discarded.
    pub fn set_data_from_message(&mut self, mut buffer: Vec<u8>, range: std::ops::Range<usize>) {
        assert!(range.start <= range.end && range.end <= buffer.len());
        let view = if range.is_empty() {
            (std::ptr::null_mut(), 0)
        } else {
            // SAFETY: just bounds-checked.
            (unsafe { buffer.as_mut_ptr().add(range.start) }, range.len())
        };
        self.data_storage = DataStorage::Received(buffer);
        self.data_view = view;
    }

    /// Attaches the given set of `objects` to this Parcel.
    pub fn set_objects(&mut self, objects: Vec<Option<Arc<dyn ApiObject>>>) {
        self.objects = objects;
        self.objects_start = 0;
    }

    /// Allocates `num_bytes` of storage for this Parcel's data. If `memory` is
    /// provided then its fragment pool is the preferred allocation source.
    /// Otherwise memory is allocated and zero-initialized on the heap, and the
    /// data placed therein will be inlined within any message that transmits
    /// this parcel.
    ///
    /// If `memory` is provided and `allow_partial` is true, this may allocate
    /// less memory than requested if some reasonable amount of space is still
    /// available within `memory`.
    ///
    /// Upon return, `data_view()` references the allocated memory wherever it
    /// resides.
    pub fn allocate_data(&mut self, num_bytes: usize, allow_partial: bool, memory: Option<&Arc<NodeLinkMemory>>) {
        assert!(matches!(self.data_storage, DataStorage::Empty));

        let fragment = match memory {
            Some(memory) if num_bytes > 0 => {
                let requested_fragment_size = num_bytes + FRAGMENT_HEADER_SIZE;
                if allow_partial {
                    memory.allocate_fragment_best_effort(requested_fragment_size)
                } else {
                    memory.allocate_fragment(requested_fragment_size)
                }
            }
            _ => Fragment::default(),
        };

        if fragment.is_null() {
            let mut bytes = vec![0u8; num_bytes];
            self.data_view = (bytes.as_mut_ptr(), bytes.len());
            self.data_storage = DataStorage::Heap(bytes);
            return;
        }

        // The smallest possible Fragment we could allocate above is still
        // substantially larger than a FragmentHeader.
        assert!(fragment.size() as usize > FRAGMENT_HEADER_SIZE);

        // Leave room for a FragmentHeader at the start of the fragment. This
        // header is not written until `commit_data()`.
        let data_size = num_bytes.min(fragment.size() as usize - FRAGMENT_HEADER_SIZE);
        // SAFETY: the fragment is addressable and larger than the header.
        self.data_view = (unsafe { fragment.address().add(FRAGMENT_HEADER_SIZE) }, data_size);
        self.data_storage = DataStorage::Fragment(DataFragment::new(
            memory.expect("fragments only come from memory").clone(),
            fragment,
        ));
    }

    /// Configures this Parcel to adopt its data fragment from the `fragment`
    /// belonging to `memory`. `fragment` must be addressable and must have a
    /// valid FragmentHeader at the start describing the data which follows.
    /// Otherwise this returns false.
    pub fn adopt_data_fragment(&mut self, memory: Arc<NodeLinkMemory>, fragment: Fragment) -> bool {
        if !fragment.is_addressable()
            || fragment.size() as usize <= FRAGMENT_HEADER_SIZE
            || !fragment.offset().is_multiple_of(8)
        {
            return false;
        }

        // This load-acquire is balanced by a store-release in `commit_data()`
        // by the producer of this data.
        // SAFETY: addressable, at least a header long, and 8-aligned (offset
        // checked above, base mapping is 8-aligned).
        let header = unsafe { &*fragment.address().cast::<FragmentHeader>() };
        let data_size = header.size.load(Ordering::Acquire) as usize;
        let max_data_size = fragment.size() as usize - FRAGMENT_HEADER_SIZE;
        if data_size > max_data_size {
            return false;
        }

        // SAFETY: header size is within the fragment.
        self.data_view = (unsafe { fragment.address().add(FRAGMENT_HEADER_SIZE) }, data_size);
        self.data_storage = DataStorage::Fragment(DataFragment::new(memory, fragment));
        true
    }

    pub fn set_remote_source(&mut self, source: Option<Arc<NodeLink>>) {
        self.remote_source = source;
    }

    pub fn remote_source(&self) -> Option<&Arc<NodeLink>> {
        self.remote_source.as_ref()
    }

    pub fn data_view(&self) -> &[u8] {
        match self.data_view {
            (_, 0) => &[],
            // SAFETY: the view points into storage owned by `self`.
            (ptr, len) => unsafe { std::slice::from_raw_parts(ptr, len) },
        }
    }

    pub fn data_view_mut(&mut self) -> &mut [u8] {
        match self.data_view {
            (_, 0) => &mut [],
            // SAFETY: as above, with unique access.
            (ptr, len) => unsafe { std::slice::from_raw_parts_mut(ptr, len) },
        }
    }

    /// The address of the start of the data view (null if there's no data).
    pub fn data_ptr(&self) -> *mut u8 {
        self.data_view.0
    }

    pub fn data_size(&self) -> usize {
        self.data_view.1
    }

    pub fn has_data_fragment(&self) -> bool {
        matches!(self.data_storage, DataStorage::Fragment(_))
    }

    pub fn data_fragment(&self) -> &Fragment {
        match &self.data_storage {
            DataStorage::Fragment(f) => &f.fragment,
            _ => panic!("parcel has no data fragment"),
        }
    }

    pub fn data_fragment_memory(&self) -> &Arc<NodeLinkMemory> {
        match &self.data_storage {
            DataStorage::Fragment(DataFragment { memory: Some(memory), .. }) => memory,
            _ => panic!("parcel has no data fragment"),
        }
    }

    /// The objects not yet consumed. Slots are `None` for objects which have
    /// been moved out (or are placeholders yet to be filled in).
    pub fn objects_view(&self) -> &[Option<Arc<dyn ApiObject>>] {
        &self.objects[self.objects_start..]
    }

    pub fn objects_view_mut(&mut self) -> &mut [Option<Arc<dyn ApiObject>>] {
        &mut self.objects[self.objects_start..]
    }

    pub fn num_objects(&self) -> usize {
        self.objects_view().len()
    }

    /// Commits `num_bytes` of data to this Parcel's data fragment. This MUST be
    /// called after populating the Parcel's data, and it must be called by the
    /// same thread that populated the data. If the parcel's data is inlined
    /// rather than stored in a fragment, this just trims the view.
    pub fn commit_data(&mut self, num_bytes: usize) {
        assert!(num_bytes <= self.data_view.1);
        self.data_view.1 = num_bytes;
        let DataStorage::Fragment(storage) = &self.data_storage else {
            return;
        };

        assert!(num_bytes + FRAGMENT_HEADER_SIZE <= storage.fragment.size() as usize);
        // SAFETY: the fragment is addressable, at least a header long, and
        // 8-aligned.
        let header = unsafe { &*storage.fragment.address().cast::<FragmentHeader>() };
        header.reserved.store(0, Ordering::Relaxed);

        // This store-release is balanced by the load-acquire in
        // `adopt_data_fragment()` by the eventual consumer of this data.
        header.size.store(num_bytes as u32, Ordering::Release);
    }

    /// Relinquishes ownership of this Parcel's data fragment, if applicable.
    /// This prevents the fragment from being freed upon Parcel destruction.
    pub fn release_data_fragment(&mut self) {
        let DataStorage::Fragment(storage) = std::mem::replace(&mut self.data_storage, DataStorage::Empty) else {
            panic!("parcel has no data fragment");
        };
        let _fragment = storage.release();
        self.data_view = (std::ptr::null_mut(), 0);
    }

    /// Fills `out_handles` (of size N) with handles to the first N APIObjects in
    /// `objects_view()`. The front of `objects_view()` is also advanced by N,
    /// effectively removing the objects from this parcel.
    ///
    /// Note that the size of `out_handles` must not be larger than the size of
    /// `objects_view()`.
    pub fn consume_handles(&mut self, out_handles: &mut [IpczHandle]) {
        assert!(out_handles.len() <= self.num_objects());
        let start = self.objects_start;
        for (i, handle) in out_handles.iter_mut().enumerate() {
            let object = self.objects[start + i].take().expect("consumed object present");
            *handle = api_object::release_as_handle(object);
        }
        self.objects_start += out_handles.len();
    }

    pub fn set_envelope(&mut self, envelope: DriverObject) {
        self.envelope = envelope;
    }

    /// Produces a log-friendly description of the Parcel, useful for various
    /// debugging log messages.
    pub fn describe(&self) -> String {
        let mut description = format!("parcel {} (", self.sequence_number());
        if let Some(&first) = self.data_view().first() {
            // Cheesy heuristic: if the first character is an ASCII letter or
            // number, assume the parcel data is human-readable and print a few
            // characters.
            if first.is_ascii_alphanumeric() {
                let preview = &self.data_view()[..self.data_size().min(8)];
                description.push('"');
                description.push_str(&String::from_utf8_lossy(preview));
                if preview.len() < self.data_size() {
                    description.push_str(&format!("...\", {} bytes", self.data_size()));
                } else {
                    description.push('"');
                }
            }
        } else {
            description.push_str("no data");
        }
        if !self.objects_view().is_empty() {
            description.push_str(&format!(", {} handles", self.num_objects()));
        }
        description.push(')');
        description
    }
}

impl Drop for Parcel {
    fn drop(&mut self) {
        // Objects not consumed or explicitly released with the parcel are closed.
        self.objects.drain(..).flatten().for_each(|object| {
            object.close();
        });
    }
}

impl std::fmt::Debug for Parcel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.describe())
    }
}

/// Attributes the data size of each parcel to a [`ParcelQueue`].
pub struct ParcelQueueTraits;

impl SequencedQueueTraits<Box<Parcel>> for ParcelQueueTraits {
    fn get_element_size(parcel: &Box<Parcel>) -> usize {
        parcel.data_view().len()
    }
}

/// A SequencedQueue of Parcel objects which also tracks the total data size (in
/// bytes) of available parcels at the head of the queue.
pub type ParcelQueue = SequencedQueue<Box<Parcel>, ParcelQueueTraits>;
