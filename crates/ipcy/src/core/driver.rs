//! The driver abstraction.
//!
//! Upstream ipcz talks to its embedder through the `IpczDriver` C function
//! table. This port replaces the table with the safe [`Driver`] trait; there is
//! no C vtable adapter. The semantics of each method follow the documentation of
//! the corresponding `IpczDriver` function in ipcz.h.
//!
//! DEVIATION: driver functions take Rust slices and return typed results
//! instead of out-pointers, and `flags`/`options` arguments (always zero/null
//! in this version of ipcz) are dropped.

use std::sync::Arc;

use super::ipcz_result::IpczResult;

/// An opaque handle created by a [`Driver`] implementation, identifying a
/// driver-side object (transport, shared memory region, mapping, or any
/// application-boxed object). Zero is never a valid handle.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct DriverHandle(usize);

pub const INVALID_DRIVER_HANDLE: DriverHandle = DriverHandle(0);

impl DriverHandle {
    pub const fn new(value: usize) -> Self {
        Self(value)
    }

    pub const fn value(self) -> usize {
        self.0
    }

    pub const fn is_valid(self) -> bool {
        self.0 != 0
    }
}

/// Information about a driver shared memory region.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SharedMemoryInfo {
    pub region_num_bytes: usize,
}

/// The data and transmissible driver handle capacity required to serialize a
/// driver object.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SerializedDimensions {
    pub num_bytes: usize,
    pub num_driver_handles: usize,
}

/// Output buffers for [`Driver::serialize`].
pub struct SerializeBuffers<'a> {
    pub data: &'a mut [u8],
    pub handles: &'a mut [DriverHandle],
}

/// Outcomes of [`Driver::serialize`], one per distinguished `IpczResult` in the
/// C API's documentation of `Serialize()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SerializeOutcome {
    /// The object was serialized into the provided buffers and the driver
    /// relinquished the handle (`IPCZ_RESULT_OK`).
    Serialized,

    /// The object is serializable but the buffers were absent or too small. The
    /// exact required capacities are provided (`IPCZ_RESULT_RESOURCE_EXHAUSTED`).
    Dimensions(SerializedDimensions),

    /// The object can be serialized, but whether that succeeds depends on the
    /// transport, and none was given (`IPCZ_RESULT_ABORTED`).
    NeedsTransport,

    /// The object is serializable, but the serialized outputs would not be
    /// transmissible over the given transport, so ipcz may instead try to relay
    /// the object through a broker (`IPCZ_RESULT_PERMISSION_DENIED`).
    NotTransmissible,

    /// The object is invalid or unserializable (`IPCZ_RESULT_INVALID_ARGUMENT`),
    /// or serialization failed for some other reason.
    Error(IpczResult),
}

/// The result of mapping a shared memory region.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MappedMemory {
    /// The base address of the mapping.
    pub address: *mut u8,
    /// A handle which can be passed to [`Driver::close`] to unmap the region.
    /// Its lifetime is independent of the memory object's.
    pub mapping: DriverHandle,
}

// SAFETY: the address is just a number identifying a mapped region owned by the
// driver; dereferencing it is the (unsafe) responsibility of its users.
unsafe impl Send for MappedMemory {}
unsafe impl Sync for MappedMemory {}

/// Raw contents of an incoming transport message.
#[derive(Clone, Copy, Debug)]
pub struct RawMessage<'a> {
    pub data: &'a [u8],
    pub handles: &'a [DriverHandle],
}

/// Receives activity notifications for an activated transport. This replaces
/// the `IpczTransportActivityHandler` callback plus listener handle. Calls
/// may come from any thread but are mutually exclusive for a given transport.
pub trait TransportActivityHandler: Send + Sync {
    /// Incoming data and driver handles. An `Err` or non-`Ok` result tells the
    /// driver the message was rejected. `envelope` is the optional driver
    /// envelope, or `INVALID_DRIVER_HANDLE`.
    fn on_message(&self, message: RawMessage<'_>, envelope: DriverHandle) -> IpczResult;

    /// An unrecoverable error occurred on the transport
    /// (`IPCZ_TRANSPORT_ACTIVITY_ERROR`).
    fn on_error(&self);

    /// The driver has finalized deactivation; no further calls will be made
    /// and the driver has dropped its reference to this handler
    /// (`IPCZ_TRANSPORT_ACTIVITY_DEACTIVATED`).
    fn on_deactivated(&self);
}

/// The set of operations ipcz needs from its embedder to perform I/O and manage
/// shared memory. Implementations are shared across threads.
pub trait Driver: Send + Sync + 'static {
    /// Requests that the driver release the object identified by `handle`.
    fn close(&self, handle: DriverHandle) -> IpczResult;

    /// Serializes the driver object identified by `handle` into bytes and
    /// readily transmissible driver handles, for eventual transmission over
    /// `transport` (`INVALID_DRIVER_HANDLE` if unknown). With `buffers` absent,
    /// this only queries serializability and dimensions. See [`SerializeOutcome`].
    fn serialize(
        &self,
        handle: DriverHandle,
        transport: DriverHandle,
        buffers: Option<SerializeBuffers<'_>>,
    ) -> SerializeOutcome;

    /// Deserializes a driver object from bytes and transmissible driver handles
    /// originally produced by `serialize()` and received by activity on
    /// `transport`.
    fn deserialize(
        &self,
        data: &[u8],
        driver_handles: &[DriverHandle],
        transport: DriverHandle,
    ) -> Result<DriverHandle, IpczResult>;

    /// Creates a new pair of entangled bidirectional transports. The first will
    /// be sent over `transport0` and the second over `transport1` (either may be
    /// `INVALID_DRIVER_HANDLE` if the pair won't be sent anywhere). Returned
    /// transports may be used immediately for `transmit()`, even if not yet
    /// activated.
    fn create_transports(
        &self,
        transport0: DriverHandle,
        transport1: DriverHandle,
    ) -> Result<(DriverHandle, DriverHandle), IpczResult>;

    /// Activates `transport`. The driver notifies `handler` of incoming data and
    /// state changes until deactivated, and must eventually call
    /// `handler.on_deactivated()` after which it drops its reference.
    fn activate_transport(
        &self,
        transport: DriverHandle,
        handler: Arc<dyn TransportActivityHandler>,
    ) -> IpczResult;

    /// Deactivates a transport that is no longer needed. The driver need not
    /// finish synchronously, but must eventually invoke `on_deactivated()`.
    fn deactivate_transport(&self, transport: DriverHandle) -> IpczResult;

    /// Transmits `data` and `driver_handles` (all obtained from this driver's
    /// `serialize()`) over `transport`. A result other than `Ok` severs the
    /// transport's connection. Every call must result in a corresponding
    /// activity notification on the peer, in order.
    fn transmit(
        &self,
        transport: DriverHandle,
        data: &[u8],
        driver_handles: &[DriverHandle],
    ) -> IpczResult;

    /// Reports that the application rejected a parcel received via `transport`.
    fn report_bad_transport_activity(&self, transport: DriverHandle, context: usize) -> IpczResult;

    /// Allocates a shared memory region.
    fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult>;

    /// Returns information about the shared memory region `driver_memory`.
    fn get_shared_memory_info(&self, driver_memory: DriverHandle) -> Result<SharedMemoryInfo, IpczResult>;

    /// Duplicates a shared memory region handle into a new distinct handle
    /// referencing the same underlying region.
    fn duplicate_shared_memory(&self, driver_memory: DriverHandle) -> Result<DriverHandle, IpczResult>;

    /// Maps the shared memory region `driver_memory`.
    fn map_shared_memory(&self, driver_memory: DriverHandle) -> Result<MappedMemory, IpczResult>;

    /// Fills `buffer` with random bytes.
    fn generate_random_bytes(&self, buffer: &mut [u8]) -> IpczResult;
}
