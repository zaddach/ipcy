//! Port of `ipcz/../api.cc`: the ipcz API operations, over the native core.
//!
//! DEVIATIONS: instead of a C function table with out-pointers, each operation
//! is a free function taking slices and returning typed results. `size`
//! fields and `options` pointers are gone. Handles are keys into the global
//! table in [`super::api_object`], so stale or mistyped handles fail with
//! `InvalidArgument` rather than being undefined behavior.

use std::sync::Arc;

use super::api_context::ApiContext;
use super::api_object;
use super::application_object::ApplicationObject;
use super::box_object::{ApiBox, BoxContents, PeekedBoxContents};
use super::driver::{Driver, DriverHandle};
use super::driver_object::DriverObject;
use super::ipcz_result::IpczResult;
use super::ipcz_types::{
    BeginGetFlags, BeginGetOutput, BeginPutFlags, BeginPutOutput, ConnectNodeFlags, CreateNodeFlags,
    CreateNodeOptions, EndGetFlags, EndPutFlags, GetFlags, GetOutput, IpczHandle, IpczTransaction, PortalStatus,
    TrapConditions, TrapEventHandler, UnboxFlags,
};
use super::node::Node;
use super::node_link_memory::MAX_INITIAL_PORTALS;
use super::node_type::NodeType;
use super::parcel_wrapper::ParcelWrapper;
use super::router::Router;
use super::trap_set::TrapAlreadySatisfied;

/// The result of a failed `trap()`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrapError {
    /// The call itself was invalid.
    Failed(IpczResult),
    /// The trap could not be installed because its conditions are already met.
    AlreadySatisfied(TrapAlreadySatisfied),
}

/// Contents of a box when peeked at or unboxed.
pub enum UnboxedContents {
    Peeked(PeekedBoxContents),
    Unboxed(BoxContents),
}

fn node_from_handle(handle: IpczHandle) -> Option<Arc<Node>> {
    api_object::from_handle_typed::<Node>(handle)
}

fn router_from_handle(handle: IpczHandle) -> Option<Arc<Router>> {
    api_object::from_handle_typed::<Router>(handle)
}

fn wrapper_from_handle(handle: IpczHandle) -> Option<Arc<ParcelWrapper>> {
    api_object::from_handle_typed::<ParcelWrapper>(handle)
}

/// Closes the object referenced by `handle`, invalidating the handle.
pub fn close(handle: IpczHandle) -> IpczResult {
    let _api_context = ApiContext::new();
    match api_object::take_from_handle(handle) {
        Some(doomed_object) => doomed_object.close(),
        None => IpczResult::InvalidArgument,
    }
}

/// Creates a new node using `driver`.
pub fn create_node(
    driver: Arc<dyn Driver>,
    flags: CreateNodeFlags,
    options: Option<&CreateNodeOptions>,
) -> Result<IpczHandle, IpczResult> {
    let _api_context = ApiContext::new();
    let node_type = if flags.contains(CreateNodeFlags::AS_BROKER) {
        NodeType::Broker
    } else {
        NodeType::Normal
    };
    Ok(api_object::release_as_handle_typed(Node::new(node_type, driver, options)))
}

/// Connects `node_handle` to another node over `driver_transport`, returning
/// `num_initial_portals` portals.
pub fn connect_node(
    node_handle: IpczHandle,
    driver_transport: DriverHandle,
    num_initial_portals: usize,
    flags: ConnectNodeFlags,
) -> Result<Vec<IpczHandle>, IpczResult> {
    let Some(node) = node_from_handle(node_handle) else {
        return Err(IpczResult::InvalidArgument);
    };
    if !driver_transport.is_valid() || num_initial_portals == 0 {
        return Err(IpczResult::InvalidArgument);
    }
    if num_initial_portals > MAX_INITIAL_PORTALS {
        return Err(IpczResult::OutOfRange);
    }

    let _api_context = ApiContext::new();
    node.connect_node(driver_transport, flags, num_initial_portals)
}

/// Opens a new pair of directly connected portals on `node_handle`.
pub fn open_portals(node_handle: IpczHandle) -> Result<(IpczHandle, IpczHandle), IpczResult> {
    if node_from_handle(node_handle).is_none() {
        return Err(IpczResult::InvalidArgument);
    }

    let _api_context = ApiContext::new();
    let (first, second) = Router::create_pair();
    Ok((
        api_object::release_as_handle_typed(first),
        api_object::release_as_handle_typed(second),
    ))
}

/// Merges two portals, consuming both handles on success.
pub fn merge_portals(portal0: IpczHandle, portal1: IpczHandle) -> IpczResult {
    let (Some(first), Some(second)) = (router_from_handle(portal0), router_from_handle(portal1)) else {
        return IpczResult::InvalidArgument;
    };

    let _api_context = ApiContext::new();
    let result = first.merge_route(&second);
    if result == IpczResult::Ok {
        // The merged portals are owned by the route now.
        drop(api_object::take_from_handle(portal0));
        drop(api_object::take_from_handle(portal1));
    }
    result
}

pub fn query_portal_status(portal: IpczHandle) -> Result<PortalStatus, IpczResult> {
    let router = router_from_handle(portal).ok_or(IpczResult::InvalidArgument)?;
    let _api_context = ApiContext::new();
    Ok(router.query_status())
}

pub fn put(portal: IpczHandle, data: &[u8], handles: &[IpczHandle]) -> IpczResult {
    let Some(router) = router_from_handle(portal) else {
        return IpczResult::InvalidArgument;
    };

    let _api_context = ApiContext::new();
    router.put(data, handles)
}

/// Begins a two-phase put. The returned `data` pointer is valid for
/// `num_bytes` bytes until the transaction ends.
pub fn begin_put(portal: IpczHandle, flags: BeginPutFlags, num_bytes: usize) -> BeginPutOutput {
    let Some(router) = router_from_handle(portal) else {
        return BeginPutOutput::failure(IpczResult::InvalidArgument);
    };

    let _api_context = ApiContext::new();
    router.begin_put(flags, num_bytes)
}

pub fn end_put(
    portal: IpczHandle,
    transaction: IpczTransaction,
    num_bytes_produced: usize,
    handles: &[IpczHandle],
    flags: EndPutFlags,
) -> IpczResult {
    let Some(router) = router_from_handle(portal) else {
        return IpczResult::InvalidArgument;
    };
    if transaction == 0 {
        return IpczResult::InvalidArgument;
    }

    let _api_context = ApiContext::new();
    router.end_put(transaction, num_bytes_produced, handles, flags)
}

/// Gets the next parcel from a portal or a parcel handle. If `want_parcel` is
/// set, a handle to the consumed parcel is returned in the output.
pub fn get(
    source: IpczHandle,
    flags: GetFlags,
    data: &mut [u8],
    handles: &mut [IpczHandle],
    want_parcel: bool,
) -> GetOutput {
    let _api_context = ApiContext::new();
    if let Some(router) = router_from_handle(source) {
        return router.get(flags, data, handles, want_parcel);
    }
    if let Some(wrapper) = wrapper_from_handle(source) {
        return wrapper.get(flags, data, handles, want_parcel);
    }
    GetOutput::failure(IpczResult::InvalidArgument)
}

pub fn begin_get(source: IpczHandle, flags: BeginGetFlags, handles: &mut [IpczHandle]) -> BeginGetOutput {
    let _api_context = ApiContext::new();
    if let Some(router) = router_from_handle(source) {
        return router.begin_get(flags, handles);
    }
    if let Some(wrapper) = wrapper_from_handle(source) {
        return wrapper.begin_get(flags, handles);
    }
    BeginGetOutput::failure(IpczResult::InvalidArgument)
}

/// Ends a two-phase get, optionally producing a handle to the consumed parcel.
pub fn end_get(
    source: IpczHandle,
    transaction: IpczTransaction,
    flags: EndGetFlags,
    want_parcel: bool,
) -> (IpczResult, Option<IpczHandle>) {
    let _api_context = ApiContext::new();
    if let Some(router) = router_from_handle(source) {
        return router.end_get(transaction, flags, want_parcel);
    }
    if let Some(wrapper) = wrapper_from_handle(source) {
        return wrapper.end_get(transaction, flags, want_parcel);
    }
    (IpczResult::InvalidArgument, None)
}

pub fn trap(
    portal: IpczHandle,
    conditions: TrapConditions,
    handler: TrapEventHandler,
    context: u64,
) -> Result<(), TrapError> {
    let router = router_from_handle(portal).ok_or(TrapError::Failed(IpczResult::InvalidArgument))?;
    let _api_context = ApiContext::new();
    router
        .trap(conditions, handler, context)
        .map_err(TrapError::AlreadySatisfied)
}

pub fn reject(parcel: IpczHandle, context: usize) -> IpczResult {
    let Some(wrapper) = wrapper_from_handle(parcel) else {
        return IpczResult::InvalidArgument;
    };

    let _api_context = ApiContext::new();
    wrapper.reject(context)
}

/// Boxes a driver object owned by `node_handle`'s driver.
pub fn box_driver_object(node_handle: IpczHandle, driver_object: DriverHandle) -> Result<IpczHandle, IpczResult> {
    let node = node_from_handle(node_handle).ok_or(IpczResult::InvalidArgument)?;
    if !driver_object.is_valid() {
        return Err(IpczResult::InvalidArgument);
    }

    let _api_context = ApiContext::new();
    let object = DriverObject::new(node.driver().clone(), driver_object);
    Ok(api_object::release_as_handle_typed(ApiBox::new_driver_object(object)))
}

/// Boxes an application-defined object.
pub fn box_application_object(
    node_handle: IpczHandle,
    object: Arc<dyn ApplicationObject>,
) -> Result<IpczHandle, IpczResult> {
    if node_from_handle(node_handle).is_none() {
        return Err(IpczResult::InvalidArgument);
    }

    let _api_context = ApiContext::new();
    Ok(api_object::release_as_handle_typed(ApiBox::new_application_object(
        object,
    )))
}

/// Unboxes (or, with `UnboxFlags::PEEK`, peeks at) a box. Unboxing invalidates
/// the handle even on failure.
pub fn unbox(handle: IpczHandle, flags: UnboxFlags) -> Result<UnboxedContents, IpczResult> {
    let _api_context = ApiContext::new();
    if flags.contains(UnboxFlags::PEEK) {
        let api_box = api_object::from_handle_typed::<ApiBox>(handle).ok_or(IpczResult::InvalidArgument)?;
        return api_box.peek().map(UnboxedContents::Peeked);
    }

    let api_box = api_object::take_from_handle_typed::<ApiBox>(handle).ok_or(IpczResult::InvalidArgument)?;
    api_box.unbox().map(UnboxedContents::Unboxed)
}
