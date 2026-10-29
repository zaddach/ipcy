//! Rust-native equivalents of the public ipcz API types in `ipcz.h` which are
//! used throughout the core: handles, flags, portal status and trap
//! definitions. Struct `size` fields and `options` pointers are dropped.

use std::sync::Arc;

use bitflags::bitflags;

use super::features::IpczFeature;

/// An opaque handle to an API object. Zero is never valid. See
/// [`super::api_object`].
pub type IpczHandle = u64;
pub const INVALID_HANDLE: IpczHandle = 0;

/// An opaque identifier for a two-phase put or get transaction.
pub type IpczTransaction = usize;

bitflags! {
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct CreateNodeFlags: u32 {
        const AS_BROKER = 1 << 0;
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct ConnectNodeFlags: u32 {
        const TO_BROKER = 1 << 0;
        const INHERIT_BROKER = 1 << 1;
        const SHARE_BROKER = 1 << 2;
        const TO_ALLOCATION_DELEGATE = 1 << 3;
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct BeginPutFlags: u32 {
        const ALLOW_PARTIAL = 1 << 0;
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct EndPutFlags: u32 {
        const ABORT = 1 << 0;
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct GetFlags: u32 {
        const PARTIAL = 1 << 0;
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct BeginGetFlags: u32 {
        const PARTIAL = 1 << 0;
        const OVERLAPPED = 1 << 1;
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct EndGetFlags: u32 {
        const ABORT = 1 << 0;
    }

    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct UnboxFlags: u32 {
        const PEEK = 1 << 0;
    }

    /// Status flags of a portal (`IpczPortalStatusFlags`).
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct PortalStatusFlags: u32 {
        const PEER_CLOSED = 1 << 0;
        const DEAD = 1 << 1;
    }

    /// Trap condition flags (`IpczTrapConditionFlags`).
    #[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
    pub struct TrapConditionFlags: u32 {
        const REMOVED = 1 << 0;
        const PEER_CLOSED = 1 << 1;
        const DEAD = 1 << 2;
        const ABOVE_MIN_LOCAL_PARCELS = 1 << 3;
        const ABOVE_MIN_LOCAL_BYTES = 1 << 4;
        const NEW_LOCAL_PARCEL = 1 << 7;
        const WITHIN_API_CALL = 1 << 9;
    }
}

/// The state of a portal, as reported by status queries and trap events.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PortalStatus {
    pub flags: PortalStatusFlags,
    pub num_local_parcels: usize,
    pub num_local_bytes: usize,
}

/// Conditions to watch for with a trap (`IpczTrapConditions`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TrapConditions {
    pub flags: TrapConditionFlags,
    pub min_local_parcels: usize,
    pub min_local_bytes: usize,
}

/// An event delivered to a trap's handler (`IpczTrapEvent`).
#[derive(Clone, Copy, Debug)]
pub struct TrapEvent {
    pub context: u64,
    pub condition_flags: TrapConditionFlags,
    pub status: PortalStatus,
}

/// Handles trap events. May be invoked from any thread, including from within
/// an API call on the thread which caused the event.
pub type TrapEventHandler = Arc<dyn Fn(&TrapEvent) + Send + Sync>;

/// Options for creating a node (`IpczCreateNodeOptions`).
#[derive(Clone, Debug, Default)]
pub struct CreateNodeOptions {
    /// `IpczMemoryFlags`; see [`super::node_link_memory::MEMORY_FIXED_PARCEL_CAPACITY`].
    pub memory_flags: u32,
    pub enabled_features: Vec<IpczFeature>,
    pub disabled_features: Vec<IpczFeature>,
}

/// The outcome of a `get()` call. `num_bytes` and `num_handles` report how much
/// data and how many handles were (or, on `ResourceExhausted`, would need to
/// be) transferred.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GetOutput {
    pub result: super::ipcz_result::IpczResult,
    pub num_bytes: usize,
    pub num_handles: usize,
    /// A handle to the parcel which was consumed, if one was requested.
    pub parcel: Option<IpczHandle>,
}

impl GetOutput {
    pub fn failure(result: super::ipcz_result::IpczResult) -> Self {
        Self {
            result,
            num_bytes: 0,
            num_handles: 0,
            parcel: None,
        }
    }
}

/// The outcome of a `begin_get()` call. `data` points at `num_bytes` bytes
/// which remain valid until the transaction ends.
#[derive(Clone, Copy, Debug)]
pub struct BeginGetOutput {
    pub result: super::ipcz_result::IpczResult,
    pub data: *const u8,
    pub num_bytes: usize,
    pub num_handles: usize,
    pub transaction: IpczTransaction,
}

impl BeginGetOutput {
    pub fn failure(result: super::ipcz_result::IpczResult) -> Self {
        Self {
            result,
            data: std::ptr::null(),
            num_bytes: 0,
            num_handles: 0,
            transaction: 0,
        }
    }
}

/// The outcome of a `begin_put()` call. `data` points at `num_bytes` writable
/// bytes which remain valid until the transaction ends.
#[derive(Clone, Copy, Debug)]
pub struct BeginPutOutput {
    pub result: super::ipcz_result::IpczResult,
    pub data: *mut u8,
    pub num_bytes: usize,
    pub transaction: IpczTransaction,
}

impl BeginPutOutput {
    pub fn failure(result: super::ipcz_result::IpczResult) -> Self {
        Self {
            result,
            data: std::ptr::null_mut(),
            num_bytes: 0,
            transaction: 0,
        }
    }
}
