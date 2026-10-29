use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::api as core_api;
use crate::core::application_object::ApplicationObject;
use crate::core::box_object::BoxContents as CoreBoxContents;
pub use crate::core::box_object::PeekedBoxContents;
use crate::core::driver::DriverHandle;
use crate::core::ipcz_result::IpczResult;
use crate::core::ipcz_types::{
    BeginGetFlags, BeginPutFlags, EndGetFlags, EndPutFlags, GetFlags, IpczHandle, IpczTransaction, PortalStatusFlags,
    TrapConditionFlags, TrapConditions,
};
use crate::core::trap_set::TrapAlreadySatisfied;
use crate::error::{IpczError, Result};
use crate::handle::{to_error, BoxHandle, Handle, Parcel, Portal};

/// Error returned by `Portal::put()` and `PutTransaction::commit()`.
///
/// On failure, ipcz guarantees that passed handles remain owned by the caller.
/// This error returns the original handles so they can be retried or closed.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct HandleTransferError {
    #[source]
    pub error: IpczError,
    pub handles: Vec<Handle>,
}

/// Error returned by `Node::box_contents()`. On failure the contents remain
/// owned by the caller.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct BoxError {
    #[source]
    pub error: IpczError,
    pub contents: BoxContents,
}

/// Error returned by `BoxHandle::unbox()`.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct UnboxError {
    #[source]
    pub error: IpczError,
    pub handle: BoxHandle,
}

/// Error returned by `Portal::merge_with()`. On failure the portals remain
/// owned by the caller.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct MergePortalsError {
    #[source]
    pub error: IpczError,
    pub first: Portal,
    pub second: Portal,
}

/// Error returned by `Parcel::reject()`. On failure the parcel remains owned
/// by the caller.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct RejectError {
    #[source]
    pub error: IpczError,
    pub parcel: Parcel,
}

/// Error returned by `Node::connect_node()`. On failure the transport remains
/// owned by the caller.
#[derive(Debug, thiserror::Error)]
#[error("{error}")]
pub struct ConnectNodeError {
    #[source]
    pub error: IpczError,
    pub transport: DriverHandle,
}

/// Error returned by `Portal::trap()`.
#[derive(Debug, thiserror::Error)]
pub enum TrapInstallError {
    /// The requested trap conditions were already satisfied, so the trap was
    /// not installed.
    #[error("trap conditions already satisfied: {satisfied_condition_flags:?}")]
    AlreadySatisfied {
        satisfied_condition_flags: TrapConditionFlags,
        status: PortalStatus,
    },

    #[error("{0}")]
    Ipcz(#[from] IpczError),
}

/// An ipcz object which can act as a source for get-transactions.
#[derive(Debug, Clone, Copy)]
pub enum GetSource<'s> {
    Portal(&'s Portal),
    Parcel(&'s Parcel),
}

impl GetSource<'_> {
    fn raw(self) -> IpczHandle {
        match self {
            Self::Portal(p) => p.raw(),
            Self::Parcel(p) => p.raw(),
        }
    }
}

/// Result of a one-shot `get()` call.
#[derive(Debug)]
pub struct GetOutput {
    pub data: Vec<u8>,
    pub handles: Vec<Handle>,
    pub parcel: Option<Parcel>,
}

/// A snapshot of portal state.
#[derive(Debug, Clone, Copy)]
pub struct PortalStatus {
    pub flags: PortalStatusFlags,
    pub num_local_parcels: usize,
    pub num_local_bytes: usize,
}

impl From<crate::core::ipcz_types::PortalStatus> for PortalStatus {
    fn from(status: crate::core::ipcz_types::PortalStatus) -> Self {
        Self {
            flags: status.flags,
            num_local_parcels: status.num_local_parcels,
            num_local_bytes: status.num_local_bytes,
        }
    }
}

/// A trap event delivered by ipcz.
#[derive(Debug, Clone)]
pub struct TrapEvent {
    pub condition_flags: TrapConditionFlags,
    pub status: PortalStatus,
}

type TrapCallback = Box<dyn FnOnce(TrapEvent) + Send>;

struct TrapState {
    cancelled: AtomicBool,
    callback: Mutex<Option<TrapCallback>>,
}

/// A one-shot trap installed on a portal.
///
/// Dropping this value cancels delivery of the callback, but does not remove
/// the underlying trap. The underlying trap will still eventually fire (e.g.
/// when the portal is closed).
pub struct Trap {
    state: Arc<TrapState>,
}

impl Trap {
    pub(crate) fn install<F>(
        portal: &Portal,
        conditions: TrapConditions,
        handler: F,
    ) -> std::result::Result<Trap, TrapInstallError>
    where
        F: FnOnce(TrapEvent) + Send + 'static,
    {
        let state = Arc::new(TrapState {
            cancelled: AtomicBool::new(false),
            callback: Mutex::new(Some(Box::new(handler))),
        });

        let handler_state = state.clone();
        let result = core_api::trap(
            portal.raw(),
            conditions,
            Arc::new(move |event| {
                if handler_state.cancelled.load(Ordering::Acquire) {
                    return;
                }
                let callback = handler_state.callback.lock().ok().and_then(|mut g| g.take());
                if let Some(callback) = callback {
                    callback(TrapEvent {
                        condition_flags: event.condition_flags,
                        status: event.status.into(),
                    });
                }
            }),
            0,
        );

        match result {
            Ok(()) => Ok(Trap { state }),
            Err(core_api::TrapError::AlreadySatisfied(TrapAlreadySatisfied {
                satisfied_condition_flags,
                status,
            })) => Err(TrapInstallError::AlreadySatisfied {
                satisfied_condition_flags,
                status: status.into(),
            }),
            Err(core_api::TrapError::Failed(result)) => Err(TrapInstallError::Ipcz(to_error(result))),
        }
    }

    /// Cancels callback delivery for this trap.
    pub fn cancel(&self) {
        self.state.cancelled.store(true, Ordering::Release);
        if let Ok(mut callback) = self.state.callback.lock() {
            let _ = callback.take();
        }
    }
}

impl Drop for Trap {
    fn drop(&mut self) {
        self.cancel();
    }
}

/// The contents of a box, used for both boxing and unboxing.
#[derive(Debug)]
pub enum BoxContents {
    /// An opaque driver object.
    DriverObject(DriverHandle),

    /// An application-defined object, which may be serializable.
    ApplicationObject(Arc<dyn ApplicationObject>),

    /// A parcel describing a collection of bytes and ipcz handles.
    Subparcel(Parcel),
}

impl std::fmt::Debug for dyn ApplicationObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ApplicationObject")
    }
}

impl BoxContents {
    pub(crate) fn from_core(contents: CoreBoxContents) -> BoxContents {
        match contents {
            CoreBoxContents::DriverObject(mut object) => BoxContents::DriverObject(object.release()),
            CoreBoxContents::ApplicationObject(object) => BoxContents::ApplicationObject(object),
            CoreBoxContents::Subparcel(raw) => {
                BoxContents::Subparcel(unsafe { Parcel::from_raw(raw) }.expect("subparcel handle is valid"))
            }
        }
    }
}

/// A guard for a put-transaction started by `Portal::begin_put()`.
///
/// Dropping this guard aborts the underlying transaction.
#[derive(Debug)]
pub struct PutTransaction<'p> {
    portal: &'p Portal,
    transaction: IpczTransaction,
    data: NonNull<u8>,
    capacity: usize,
    finished: bool,
}

/// A guard for a get-transaction started by `begin_get()`.
///
/// Dropping this guard commits the underlying transaction, unless the
/// transaction was started with `begin_get_partial()`, in which case it's
/// aborted.
#[derive(Debug)]
pub struct GetTransaction<'s> {
    source: GetSource<'s>,
    transaction: IpczTransaction,
    data: NonNull<u8>,
    num_bytes: usize,
    handles: Vec<Handle>,
    drop_flags: EndGetFlags,
    finished: bool,
}

fn end_get(source: GetSource<'_>, transaction: IpczTransaction, flags: EndGetFlags) -> Result<Option<Parcel>> {
    let want_parcel = matches!(source, GetSource::Parcel(_)) || flags.contains(EndGetFlags::ABORT);
    let (result, parcel) = core_api::end_get(source.raw(), transaction, flags, want_parcel);
    IpczError::check(result)?;
    Ok(parcel.and_then(|raw| unsafe { Parcel::from_raw(raw) }))
}

/// Wraps raw handles produced by the core into owned handles.
fn own_handles(raws: &[IpczHandle]) -> Result<Vec<Handle>> {
    raws.iter()
        .map(|&raw| unsafe { Handle::from_raw(raw) }.ok_or(IpczError::Internal))
        .collect()
}

impl<'s> GetTransaction<'s> {
    /// Returns the readable data view for this transaction. Only valid until
    /// the transaction ends.
    pub fn data(&self) -> &[u8] {
        // SAFETY: the core guarantees `num_bytes` readable bytes for the
        // duration of the transaction.
        unsafe { std::slice::from_raw_parts(self.data.as_ptr(), self.num_bytes) }
    }

    /// Returns handles extracted from the parcel by `begin_get()`. These are
    /// owned by the caller immediately, even if the transaction is aborted.
    pub fn handles(&self) -> &[Handle] {
        &self.handles
    }

    /// Takes ownership of all extracted handles.
    pub fn take_handles(&mut self) -> Vec<Handle> {
        std::mem::take(&mut self.handles)
    }

    /// Commits (ends) this transaction. For non-overlapped transactions on
    /// portals, this dequeues the parcel.
    pub fn commit(mut self) -> Result<Option<Parcel>> {
        let parcel = end_get(self.source, self.transaction, EndGetFlags::empty())?;
        self.finished = true;
        Ok(parcel)
    }

    /// Aborts (ends) this transaction without dequeuing the parcel.
    pub fn abort(mut self) -> Result<Option<Parcel>> {
        let parcel = end_get(self.source, self.transaction, EndGetFlags::ABORT)?;
        self.finished = true;
        Ok(parcel)
    }
}

impl Drop for GetTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = end_get(self.source, self.transaction, self.drop_flags);
            self.finished = true;
        }
    }
}

impl<'p> PutTransaction<'p> {
    /// The capacity (in bytes) of the writable buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns a writable view of the buffer. Only valid until the transaction
    /// is committed or aborted.
    pub fn buffer(&mut self) -> &mut [u8] {
        // SAFETY: the core guarantees `capacity` writable bytes for the
        // duration of the transaction.
        unsafe { std::slice::from_raw_parts_mut(self.data.as_ptr(), self.capacity) }
    }

    /// Commits this transaction with `num_bytes_produced` bytes written and
    /// `handles` attached. This always ends the transaction.
    pub fn commit(mut self, num_bytes_produced: usize, handles: Vec<Handle>) -> std::result::Result<(), HandleTransferError> {
        let raw_handles: Vec<IpczHandle> = handles.iter().map(Handle::raw).collect();
        let result = core_api::end_put(
            self.portal.raw(),
            self.transaction,
            num_bytes_produced,
            &raw_handles,
            EndPutFlags::empty(),
        );
        self.finished = true;
        match result {
            IpczResult::Ok => {
                handles.into_iter().for_each(|handle| {
                    handle.into_raw();
                });
                Ok(())
            }
            result => {
                // The transaction may still be alive (e.g. after an invalid
                // argument), so make sure it isn't leaked.
                let _ = core_api::end_put(self.portal.raw(), self.transaction, 0, &[], EndPutFlags::ABORT);
                Err(HandleTransferError {
                    error: to_error(result),
                    handles,
                })
            }
        }
    }
}

impl Drop for PutTransaction<'_> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = core_api::end_put(self.portal.raw(), self.transaction, 0, &[], EndPutFlags::ABORT);
            self.finished = true;
        }
    }
}

fn non_null(data: *const u8, len: usize) -> Result<NonNull<u8>> {
    if len == 0 {
        Ok(NonNull::dangling())
    } else {
        NonNull::new(data.cast_mut()).ok_or(IpczError::Internal)
    }
}

/// One-shot get of the whole next parcel from `source`.
fn get_whole(source: GetSource<'_>) -> Result<GetOutput> {
    let first = core_api::get(source.raw(), GetFlags::empty(), &mut [], &mut [], false);
    match first.result {
        IpczResult::Ok => {
            return Ok(GetOutput {
                data: Vec::new(),
                handles: Vec::new(),
                parcel: None,
            });
        }
        IpczResult::ResourceExhausted => {}
        other => return Err(to_error(other)),
    }

    let mut data = vec![0u8; first.num_bytes];
    let mut raw_handles = vec![0; first.num_handles];
    let second = core_api::get(source.raw(), GetFlags::empty(), &mut data, &mut raw_handles, false);
    IpczError::check(second.result)?;
    data.truncate(second.num_bytes);
    raw_handles.truncate(second.num_handles);
    Ok(GetOutput {
        data,
        handles: own_handles(&raw_handles)?,
        parcel: None,
    })
}

/// One-shot partial get with the given output capacities.
fn get_partial(source: GetSource<'_>, data_capacity: usize, handles_capacity: usize) -> Result<GetOutput> {
    let mut data = vec![0u8; data_capacity];
    let mut raw_handles = vec![0; handles_capacity];
    let out = core_api::get(source.raw(), GetFlags::PARTIAL, &mut data, &mut raw_handles, true);
    IpczError::check(out.result)?;
    data.truncate(out.num_bytes);
    raw_handles.truncate(out.num_handles);
    Ok(GetOutput {
        data,
        handles: own_handles(&raw_handles)?,
        parcel: out.parcel.and_then(|raw| unsafe { Parcel::from_raw(raw) }),
    })
}

fn begin_get<'s>(source: GetSource<'s>, flags: BeginGetFlags) -> Result<GetTransaction<'s>> {
    if flags.contains(BeginGetFlags::PARTIAL) {
        return Err(IpczError::InvalidArgument);
    }

    // Probe for the required handle capacity.
    let first = core_api::begin_get(source.raw(), flags, &mut []);
    let (out, raw_handles) = match first.result {
        IpczResult::ResourceExhausted => {
            let mut raw_handles = vec![0; first.num_handles];
            let out = core_api::begin_get(source.raw(), flags, &mut raw_handles);
            IpczError::check(out.result)?;
            raw_handles.truncate(out.num_handles);
            (out, raw_handles)
        }
        result => {
            IpczError::check(result)?;
            (first, Vec::new())
        }
    };

    Ok(GetTransaction {
        source,
        transaction: out.transaction,
        data: non_null(out.data, out.num_bytes)?,
        num_bytes: out.num_bytes,
        handles: own_handles(&raw_handles)?,
        drop_flags: EndGetFlags::empty(),
        finished: false,
    })
}

fn begin_get_partial<'s>(
    source: GetSource<'s>,
    handles_capacity: usize,
    flags: BeginGetFlags,
) -> Result<GetTransaction<'s>> {
    let mut raw_handles = vec![0; handles_capacity];
    let out = core_api::begin_get(source.raw(), flags | BeginGetFlags::PARTIAL, &mut raw_handles);
    IpczError::check(out.result)?;
    raw_handles.truncate(out.num_handles);
    Ok(GetTransaction {
        source,
        transaction: out.transaction,
        data: non_null(out.data, out.num_bytes)?,
        num_bytes: out.num_bytes,
        handles: own_handles(&raw_handles)?,
        drop_flags: EndGetFlags::ABORT,
        finished: false,
    })
}

impl Portal {
    /// Sends a parcel with `data` and `handles`. On success ownership of the
    /// handles transfers to ipcz; on failure the original handles are returned.
    pub fn put(&self, data: &[u8], handles: Vec<Handle>) -> std::result::Result<(), HandleTransferError> {
        let raw_handles: Vec<IpczHandle> = handles.iter().map(Handle::raw).collect();
        match core_api::put(self.raw(), data, &raw_handles) {
            IpczResult::Ok => {
                handles.into_iter().for_each(|handle| {
                    handle.into_raw();
                });
                Ok(())
            }
            result => Err(HandleTransferError {
                error: to_error(result),
                handles,
            }),
        }
    }

    /// Begins a put-transaction with room for `requested_bytes`.
    pub fn begin_put(&self, requested_bytes: usize, flags: BeginPutFlags) -> Result<PutTransaction<'_>> {
        let out = core_api::begin_put(self.raw(), flags, requested_bytes);
        IpczError::check(out.result)?;
        Ok(PutTransaction {
            portal: self,
            transaction: out.transaction,
            data: non_null(out.data, out.num_bytes)?,
            capacity: out.num_bytes,
            finished: false,
        })
    }

    /// Retrieves the whole next parcel.
    pub fn get(&self) -> Result<GetOutput> {
        get_whole(GetSource::Portal(self))
    }

    /// Retrieves part of the next parcel, within the given capacities. Also
    /// returns the parcel object.
    pub fn get_partial(&self, data_capacity: usize, handles_capacity: usize) -> Result<GetOutput> {
        get_partial(GetSource::Portal(self), data_capacity, handles_capacity)
    }

    /// Begins a get-transaction for the next parcel, retrieving all handles.
    pub fn begin_get(&self, flags: BeginGetFlags) -> Result<GetTransaction<'_>> {
        begin_get(GetSource::Portal(self), flags)
    }

    /// Like [`Self::begin_get`], retrieving at most `handles_capacity` handles.
    pub fn begin_get_partial(&self, handles_capacity: usize, flags: BeginGetFlags) -> Result<GetTransaction<'_>> {
        begin_get_partial(GetSource::Portal(self), handles_capacity, flags)
    }
}

impl Parcel {
    /// Retrieves the data and handles of this parcel.
    pub fn get(&self) -> Result<GetOutput> {
        get_whole(GetSource::Parcel(self))
    }

    pub fn get_partial(&self, data_capacity: usize, handles_capacity: usize) -> Result<GetOutput> {
        get_partial(GetSource::Parcel(self), data_capacity, handles_capacity)
    }

    pub fn begin_get(&self, flags: BeginGetFlags) -> Result<GetTransaction<'_>> {
        begin_get(GetSource::Parcel(self), flags)
    }

    pub fn begin_get_partial(&self, handles_capacity: usize, flags: BeginGetFlags) -> Result<GetTransaction<'_>> {
        begin_get_partial(GetSource::Parcel(self), handles_capacity, flags)
    }
}
