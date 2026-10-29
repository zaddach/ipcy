//! Port of `ipcz/router.{h,cc}`.
//!
//! The Router is the main primitive responsible for routing parcels between ipcz
//! portals. This type is thread-safe.
//!
//! Before a Router can participate in any actual routing, it must have an
//! outward link to another Router (see [`Router::set_outward_link`]). To
//! establish a locally connected pair of Routers, use [`Router::create_pair`].
//!
//! Each ipcz portal handle directly controls a terminal Router along its route,
//! and all routes stabilize to eventually consist of only two interconnected
//! terminal Routers. When a portal moves, its side of the route is extended by
//! creating a new terminal Router at the portal's new location. The previous
//! terminal Router remains as a proxying hop to be phased out eventually.
//!
//! DEVIATIONS: all guarded state lives in [`RouterState`] behind a single
//! `Mutex`; operations which hold several routers' locks at once use the
//! address-ordered `lock2/3/4` helpers. `IpczHandle` out-parameters become
//! returned output structs.

use std::sync::{Arc, Mutex, MutexGuard};

use super::api_object::{self, ApiObject, ApiObjectType};
use super::fragment_descriptor::FragmentDescriptor;
use super::fragment_ref::FragmentRef;
use super::ipcz_result::IpczResult;
use super::ipcz_types::{
    BeginGetFlags, BeginGetOutput, BeginPutFlags, BeginPutOutput, EndGetFlags, EndPutFlags, GetFlags, GetOutput,
    IpczHandle, IpczTransaction, PortalStatus, PortalStatusFlags, TrapConditions, TrapEventHandler,
};
use super::link_side::LinkSide;
use super::link_type::LinkType;
use super::local_router_link::{InitialState, LocalRouterLink};
use super::multi_mutex_lock::{lock2, lock3, lock4};
use super::node_link::NodeLink;
use super::node_name::NodeName;
use super::parcel::{Parcel, ParcelQueue};
use super::parcel_wrapper::ParcelWrapper;
use super::pending_transaction_set::PendingTransactionSet;
use super::remote_router_link::RemoteRouterLink;
use super::route_edge::RouteEdge;
use super::router_descriptor::RouterDescriptor;
use super::router_link::RouterLink;
use super::router_link_state::RouterLinkState;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;
use super::trap_event_dispatcher::TrapEventDispatcher;
use super::trap_set::{TrapAlreadySatisfied, TrapSet};

/// Determines whether a `flush()` will unconditionally attempt to initiate
/// bypass of the Router or its outward peer after performing all other flushing
/// operations. By default, bypass progress is only attempted if the flush itself
/// resulted in an unstable central link becoming potentially stable. But various
/// operations which invoke `flush()` may also elicit state changes that can
/// unblock a bypass operation. These operations may specify
/// `ForceProxyBypassAttempt` in such cases.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FlushBehavior {
    #[default]
    Default,
    ForceProxyBypassAttempt,
}

// A parcel accumulated within `flush()`, along with the link over which to flush
// it.
struct ParcelToFlush {
    link: Arc<dyn RouterLink>,
    parcel: Box<Parcel>,
}

// Attempts to pop elements from `queue` for transmission along `edge`. This
// terminates either when `queue` is exhausted, or the next parcel in `queue` is
// to be transmitted over a link that is not yet known to `edge`. Any
// successfully popped elements are accumulated at the end of `parcels`.
fn collect_parcels_to_flush(queue: &mut ParcelQueue, edge: &RouteEdge, parcels: &mut Vec<ParcelToFlush>) {
    let decaying_link = edge.decaying_link();
    let primary_link = edge.primary_link();
    while queue.has_next_element() {
        let n = queue.current_sequence_number();
        let link = match (decaying_link, primary_link) {
            (Some(link), _) if edge.should_transmit_on_decaying_link(n) => link,
            (_, Some(link)) if !edge.should_transmit_on_decaying_link(n) => link,
            _ => return,
        };

        let parcel = queue.pop().expect("next element is available");
        parcels.push(ParcelToFlush {
            link: link.clone(),
            parcel,
        });
    }
}

type Objects = Vec<Option<Arc<dyn ApiObject>>>;

fn validate_and_acquire_objects_for_transit_from(sender: &Router, handles: &[IpczHandle]) -> Option<Objects> {
    handles
        .iter()
        .map(|&handle| {
            api_object::from_handle(handle)
                .filter(|object| object.can_send_from(sender))
                .map(Some)
        })
        .collect()
}

fn is_remote_link(link: &Arc<dyn RouterLink>, remote: &RemoteRouterLink) -> bool {
    link.as_remote_router_link().is_some_and(|r| std::ptr::eq(r, remote))
}

#[derive(Default)]
struct RouterState {
    /// Indicates whether the opposite end of the route has been closed. This is
    /// the source of truth for peer closure status. The status bit
    /// (`PEER_CLOSED`) within `status_flags`, and the corresponding trap
    /// condition are only raised when this is true AND we are not expecting any
    /// more in-flight parcels.
    is_peer_closed: bool,

    /// Tracks whether this router has been unexpectedly disconnected from its
    /// links. This may be used to prevent additional links from being
    /// established.
    is_disconnected: bool,

    /// If `pending_gets` has only one transaction, this indicates whether it's
    /// exclusive. An exclusive transaction must return its Parcel to the head
    /// element of `inbound_parcels` if aborted.
    is_pending_get_exclusive: bool,

    /// The current computed portal status flags state, to be reflected by a
    /// portal controlling this router iff this is a terminal router.
    status_flags: PortalStatusFlags,

    /// A set of traps installed via a controlling portal where applicable.
    /// These traps are notified about any interesting state changes within the
    /// router.
    traps: TrapSet,

    /// The edge connecting this router outward to another, toward the portal on
    /// the other side of the route.
    outward_edge: RouteEdge,

    /// The edge connecting this router inward to another, closer to the portal
    /// on our own side of the route. Only present for proxying routers:
    /// terminal routers by definition can have no inward edge.
    inward_edge: Option<Box<RouteEdge>>,

    /// A special inward edge which when present bridges this route with another
    /// route. This is used only to implement route merging.
    bridge: Option<Box<RouteEdge>>,

    /// Parcels received from the other end of the route. If this is a terminal
    /// router, these may be retrieved by the application via a controlling
    /// portal; otherwise they will be forwarded along `inward_edge` as soon as
    /// possible.
    inbound_parcels: ParcelQueue,

    /// Parcels transmitted directly from this router (if sent by a controlling
    /// portal) or received from an inward peer which sent them outward toward
    /// this Router. These parcels generally only accumulate if there is no
    /// outward link present when attempting to transmit them, and they are
    /// forwarded along `outward_edge` as soon as possible.
    outbound_parcels: ParcelQueue,

    /// The set of pending get transactions in progress on this router.
    pending_gets: Option<Box<PendingTransactionSet>>,

    /// The set of pending put transactions in progress on this router.
    pending_puts: Option<Box<PendingTransactionSet>>,
}

// Pops the next inbound parcel and updates portal status accordingly.
fn take_next_inbound_parcel(state: &mut RouterState, dispatcher: &mut TrapEventDispatcher) -> Box<Parcel> {
    let parcel = state.inbound_parcels.pop().expect("next inbound parcel is available");
    if state.inbound_parcels.is_sequence_fully_consumed() {
        state.status_flags |= PortalStatusFlags::PEER_CLOSED | PortalStatusFlags::DEAD;
    }
    state
        .traps
        .notify_local_parcel_consumed(state.status_flags, &state.inbound_parcels, dispatcher);
    parcel
}

pub struct Router {
    state: Mutex<RouterState>,
}

impl Router {
    pub fn new() -> Arc<Router> {
        log::trace!("Creating Router");
        Arc::new(Router {
            state: Mutex::new(RouterState::default()),
        })
    }

    /// Creates a new pair of terminal routers which are directly connected to
    /// each other by a LocalRouterLink.
    pub fn create_pair() -> (Arc<Router>, Arc<Router>) {
        let routers = (Router::new(), Router::new());
        let links = LocalRouterLink::create_pair(LinkType::Central, routers.clone(), InitialState::Stable);
        routers.0.set_outward_link(links.0);
        routers.1.set_outward_link(links.1);
        routers
    }

    fn lock(&self) -> MutexGuard<'_, RouterState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Indicates whether the terminal router on the other side of the central
    /// link is known to be closed.
    pub fn is_peer_closed(&self) -> bool {
        self.lock().status_flags.contains(PortalStatusFlags::PEER_CLOSED)
    }

    /// Indicates whether the terminal router on the other side of the central
    /// link is known to be closed AND there are no more inbound parcels to be
    /// retrieved.
    pub fn is_route_dead(&self) -> bool {
        self.lock().status_flags.contains(PortalStatusFlags::DEAD)
    }

    /// Indicates whether this Router is currently on a central link which is
    /// connected to a router on another node. Used by tests to verify route
    /// reduction behavior, and may only be called on terminal Routers.
    pub fn is_on_central_remote_link(&self) -> bool {
        let state = self.lock();
        // This may only be called on terminal Routers.
        assert!(state.inward_edge.is_none());
        state.outward_edge.primary_link().is_some_and(|link| {
            state.outward_edge.is_stable() && link.get_type().is_central() && link.get_local_peer().is_none()
        })
    }

    /// Returns the current computed status of this Router.
    pub fn query_status(&self) -> PortalStatus {
        let state = self.lock();
        PortalStatus {
            flags: state.status_flags,
            num_local_parcels: state.inbound_parcels.get_num_available_elements(),
            num_local_bytes: state.inbound_parcels.get_total_available_element_size(),
        }
    }

    /// Returns true iff this Router's outward link is a LocalRouterLink between
    /// `self` and `router`.
    pub fn has_local_peer(&self, router: &Router) -> bool {
        self.lock()
            .outward_edge
            .get_local_peer()
            .is_some_and(|peer| std::ptr::eq(&*peer, router))
    }

    // Allocates an outbound parcel with the intention of eventually sending it
    // from this Router via `send_outbound_parcel()`. This will always try to
    // allocate exactly `num_bytes` capacity unless `allow_partial` is true; in
    // which case the allocated size may be less than requested. If available,
    // this will also attempt to allocate the parcel data as a fragment of the
    // router's outward link memory.
    fn allocate_outbound_parcel(&self, num_bytes: usize, allow_partial: bool) -> Box<Parcel> {
        let outward_link = self.lock().outward_edge.primary_link().cloned();

        let mut parcel = Box::new(Parcel::default());
        match outward_link {
            Some(link) => link.allocate_parcel_data(num_bytes, allow_partial, &mut parcel),
            None => parcel.allocate_data(num_bytes, allow_partial, None),
        }
        parcel
    }

    // Attempts to send an outbound parcel originating from this Router. Called
    // only as a direct result of a `put()` or `end_put()` call on the router's
    // owning portal.
    fn send_outbound_parcel(self: &Arc<Self>, mut parcel: Box<Parcel>) -> IpczResult {
        let link;
        {
            let mut state = self.lock();
            if state.inbound_parcels.final_sequence_length().is_some() {
                // If the inbound sequence is finalized, the peer portal must be
                // gone.
                return IpczResult::NotFound;
            }

            let sequence_number = state.outbound_parcels.get_current_sequence_length();
            parcel.set_sequence_number(sequence_number);
            match state.outward_edge.primary_link().cloned() {
                Some(primary_link) if state.outbound_parcels.skip_element(sequence_number) => {
                    // If there are no unsent parcels ahead of this one in the
                    // outbound sequence, and we have an active outward link, we
                    // can immediately transmit the parcel without any
                    // intermediate queueing step. That is the most common case.
                    link = Some(primary_link);
                }
                _ => {
                    // Otherwise we have to queue the parcel here and it will be
                    // flushed out ASAP.
                    log::trace!("Queuing outbound {}", parcel.describe());
                    let push_ok = state.outbound_parcels.push(sequence_number, parcel);
                    assert!(push_ok);
                    drop(state);
                    self.flush(FlushBehavior::Default);
                    return IpczResult::Ok;
                }
            }
        }

        link.expect("immediate transmission").accept_parcel(parcel);
        IpczResult::Ok
    }

    /// Closes this side of the Router's own route. Only called on terminal
    /// Routers.
    pub fn close_route(self: &Arc<Self>) {
        let mut dispatcher = TrapEventDispatcher::new();
        {
            let mut state = self.lock();
            let length = state.outbound_parcels.get_current_sequence_length();
            state.outbound_parcels.set_final_sequence_length(length);
            state.traps.remove_all(&mut dispatcher);
        }
        self.flush(FlushBehavior::Default);
    }

    /// Uses `link` as this Router's new outward link. This is the primary link
    /// on which the router transmits parcels and control messages directed
    /// toward the other side of its route. Must only be called on a Router which
    /// has no outward link.
    ///
    /// NOTE: This is NOT safe to call when the other side of the link is already
    /// in active use by another Router, as `self` Router may already be in a
    /// transitional state and must be able to block decay around `link` from
    /// within this call.
    pub fn set_outward_link(self: &Arc<Self>, link: Arc<dyn RouterLink>) {
        let rejected = {
            let mut state = self.lock();

            // If we have a stable inward edge (or none at all), and the outward
            // edge is stable too, our new link can be marked stable from our
            // side.
            if link.get_type().is_central()
                && state.outward_edge.is_stable()
                && state.inward_edge.as_ref().is_none_or(|edge| edge.is_stable())
            {
                link.mark_side_stable();
            }

            if state.is_disconnected {
                Some(link)
            } else {
                state.outward_edge.set_primary_link(link);
                None
            }
        };

        if let Some(link) = rejected {
            // The link wasn't adopted: this Router has already been
            // disconnected.
            link.accept_route_disconnected();
            link.deactivate();
            return;
        }

        self.flush(FlushBehavior::ForceProxyBypassAttempt);
    }

    /// Accepts an inbound parcel from the outward edge of this router, either to
    /// queue it for retrieval or forward it further inward.
    pub fn accept_inbound_parcel(self: &Arc<Self>, parcel: Box<Parcel>) -> bool {
        let mut dispatcher = TrapEventDispatcher::new();
        {
            let mut state = self.lock();
            let state = &mut *state;
            let sequence_number = parcel.sequence_number();
            if !state.inbound_parcels.push(sequence_number, parcel) {
                // Unexpected route disconnection can cut off inbound sequences,
                // so don't treat an out-of-bounds parcel as a validation
                // failure.
                return true;
            }

            if state.inward_edge.is_none() {
                // If this is a terminal router, we may have trap events to fire.
                if sequence_number < state.inbound_parcels.get_current_sequence_length() {
                    // Only notify traps if the new parcel is actually available
                    // for reading, which may not be the case if some preceding
                    // parcels have yet to be received.
                    state
                        .traps
                        .notify_new_local_parcel(state.status_flags, &state.inbound_parcels, &mut dispatcher);
                }
            }
        }

        self.flush(FlushBehavior::Default);
        true
    }

    /// Accepts an outbound parcel here from some other Router. The parcel is
    /// transmitted immediately or queued for later transmission over the
    /// Router's outward link. Called only on proxying Routers.
    pub fn accept_outbound_parcel(self: &Arc<Self>, parcel: Box<Parcel>) -> bool {
        {
            let mut state = self.lock();

            // Proxied outbound parcels are always queued in a ParcelQueue even
            // if they will be forwarded immediately. This allows us to track the
            // full sequence of forwarded parcels so we can know with certainty
            // when we're done forwarding.
            //
            // TODO: Using a queue here may increase latency along the route,
            // because it unnecessarily forces in-order forwarding.
            let sequence_number = parcel.sequence_number();
            if !state.outbound_parcels.push(sequence_number, parcel) {
                // Unexpected route disconnection can cut off outbound sequences,
                // so don't treat an out-of-bounds parcel as a validation
                // failure.
                return true;
            }
        }

        self.flush(FlushBehavior::Default);
        true
    }

    /// Accepts notification that the other end of the route has been closed and
    /// that the closed end transmitted a total of `sequence_length` parcels
    /// before closing.
    pub fn accept_route_closure_from(self: &Arc<Self>, link_type: LinkType, sequence_length: SequenceNumber) -> bool {
        let mut dispatcher = TrapEventDispatcher::new();
        {
            let mut state = self.lock();
            let state = &mut *state;
            if link_type.is_outward() {
                if !state.inbound_parcels.set_final_sequence_length(sequence_length) {
                    // Ignore if and only if the sequence was terminated early.
                    log::trace!("Discarding inbound route closure notification");
                    return state
                        .inbound_parcels
                        .final_sequence_length()
                        .is_some_and(|length| length <= sequence_length);
                }

                if state.inward_edge.is_none() && state.bridge.is_none() {
                    state.is_peer_closed = true;
                    if state.inbound_parcels.is_sequence_fully_consumed() {
                        state.status_flags |= PortalStatusFlags::PEER_CLOSED | PortalStatusFlags::DEAD;
                    }
                    state
                        .traps
                        .notify_peer_closed(state.status_flags, &state.inbound_parcels, &mut dispatcher);
                }
            } else if link_type.is_peripheral_inward() {
                if !state.outbound_parcels.set_final_sequence_length(sequence_length) {
                    // Ignore if and only if the sequence was terminated early.
                    log::trace!("Discarding outbound route closure notification");
                    return state
                        .outbound_parcels
                        .final_sequence_length()
                        .is_some_and(|length| length <= sequence_length);
                }
            } else if link_type.is_bridge() {
                if !state.outbound_parcels.set_final_sequence_length(sequence_length) {
                    return false;
                }
                state.bridge = None;
            }
        }

        self.flush(FlushBehavior::Default);
        true
    }

    /// Accepts notification from a link bound to this Router that some node along
    /// the route (in the direction of that link) has been disconnected, e.g. due
    /// to a crash, and that the route is no longer functional as a result. This
    /// is similar to route closure, except no effort can realistically be made to
    /// deliver the complete sequence of parcels transmitted from that end of the
    /// route. `link_type` specifies the type of link which is propagating the
    /// notification to this router.
    pub fn accept_route_disconnected_from(self: &Arc<Self>, link_type: LinkType) -> bool {
        let mut dispatcher = TrapEventDispatcher::new();
        let mut forwarding_links: Vec<Arc<dyn RouterLink>> = Vec::new();
        {
            // If we have to drop any undeliverable parcels, make sure they're
            // destroyed outside of the lock in case they have any attached
            // Routers.
            let _dropped_parcels;
            let mut state = self.lock();
            let state = &mut *state;

            log::trace!("Router disconnected from {link_type} link");

            state.is_disconnected = true;
            _dropped_parcels = if link_type.is_peripheral_inward() {
                state.outbound_parcels.force_terminate_sequence()
            } else {
                state.inbound_parcels.force_terminate_sequence()
            };

            // Wipe out all remaining links and propagate the disconnection over
            // them.
            forwarding_links.extend(state.outward_edge.release_primary_link());
            forwarding_links.extend(state.outward_edge.release_decaying_link());
            if let Some(inward_edge) = state.inward_edge.as_mut() {
                forwarding_links.extend(inward_edge.release_primary_link());
                forwarding_links.extend(inward_edge.release_decaying_link());
            } else if let Some(bridge) = state.bridge.as_mut() {
                forwarding_links.extend(bridge.release_primary_link());
                forwarding_links.extend(bridge.release_decaying_link());
            } else {
                // Terminal routers may have trap events to fire.
                state.is_peer_closed = true;
                if state.inbound_parcels.is_sequence_fully_consumed() {
                    state.status_flags |= PortalStatusFlags::PEER_CLOSED | PortalStatusFlags::DEAD;
                }
                state
                    .traps
                    .notify_peer_closed(state.status_flags, &state.inbound_parcels, &mut dispatcher);
            }
        }

        for link in &forwarding_links {
            log::trace!("Forwarding disconnection over {}", link.describe());
            link.accept_route_disconnected();
            link.deactivate();
        }

        self.flush(FlushBehavior::Default);
        true
    }

    /// Sends a parcel with `data` and `handles` through this portal.
    pub fn put(self: &Arc<Self>, data: &[u8], handles: &[IpczHandle]) -> IpczResult {
        let Some(objects) = validate_and_acquire_objects_for_transit_from(self, handles) else {
            return IpczResult::InvalidArgument;
        };

        if self.is_peer_closed() {
            return IpczResult::NotFound;
        }

        let mut parcel = self.allocate_outbound_parcel(data.len(), /*allow_partial=*/ false);
        if !data.is_empty() {
            parcel.data_view_mut()[..data.len()].copy_from_slice(data);
        }
        parcel.commit_data(data.len());
        parcel.set_objects(objects);
        let result = self.send_outbound_parcel(parcel);
        if result == IpczResult::Ok {
            // If the parcel was sent, the sender relinquishes handle ownership
            // and therefore implicitly releases its ref to each object.
            handles.iter().for_each(|&handle| drop(api_object::take_from_handle(handle)));
        }

        result
    }

    /// Begins a two-phase put, allocating up to `num_bytes_requested` of parcel
    /// capacity.
    pub fn begin_put(&self, flags: BeginPutFlags, num_bytes_requested: usize) -> BeginPutOutput {
        let allow_partial = flags.contains(BeginPutFlags::ALLOW_PARTIAL);
        if self.is_peer_closed() {
            return BeginPutOutput::failure(IpczResult::NotFound);
        }

        let mut parcel = self.allocate_outbound_parcel(num_bytes_requested, allow_partial);
        let (data, num_bytes) = (parcel.data_view_mut().as_mut_ptr(), parcel.data_size());
        let mut state = self.lock();
        let transaction = state
            .pending_puts
            .get_or_insert_with(|| Box::new(PendingTransactionSet::new()))
            .add(parcel);
        BeginPutOutput {
            result: IpczResult::Ok,
            data,
            num_bytes,
            transaction,
        }
    }

    /// Completes a two-phase put.
    pub fn end_put(
        self: &Arc<Self>,
        transaction: IpczTransaction,
        num_bytes_produced: usize,
        handles: &[IpczHandle],
        flags: EndPutFlags,
    ) -> IpczResult {
        let aborted = flags.contains(EndPutFlags::ABORT);
        let objects = if aborted {
            Vec::new()
        } else {
            match validate_and_acquire_objects_for_transit_from(self, handles) {
                Some(objects) => objects,
                None => return IpczResult::InvalidArgument,
            }
        };

        let parcel = {
            let mut state = self.lock();
            let Some(pending_puts) = state.pending_puts.as_mut() else {
                return IpczResult::InvalidArgument;
            };

            if aborted {
                pending_puts.finalize_for_put(transaction, 0)
            } else {
                pending_puts.finalize_for_put(transaction, num_bytes_produced)
            }
        };

        let Some(mut parcel) = parcel else {
            return IpczResult::InvalidArgument;
        };

        if aborted {
            return IpczResult::Ok;
        }

        parcel.commit_data(num_bytes_produced);
        parcel.set_objects(objects);
        let result = self.send_outbound_parcel(parcel);
        if result == IpczResult::Ok {
            // If the parcel was sent, the sender relinquishes handle ownership
            // and therefore implicitly releases its ref to each object.
            handles.iter().for_each(|&handle| drop(api_object::take_from_handle(handle)));
        }

        result
    }

    /// Retrieves the next inbound parcel, if any.
    pub fn get(&self, flags: GetFlags, data: &mut [u8], handles: &mut [IpczHandle], want_parcel: bool) -> GetOutput {
        let mut dispatcher = TrapEventDispatcher::new();
        let mut output;
        let consumed_parcel;
        {
            let mut state = self.lock();
            let state = &mut *state;
            if state.inbound_parcels.is_sequence_fully_consumed() {
                return GetOutput::failure(IpczResult::NotFound);
            }
            if !state.inbound_parcels.has_next_element() {
                return GetOutput::failure(IpczResult::Unavailable);
            }

            let allow_partial = flags.contains(GetFlags::PARTIAL);
            let (data_capacity, handles_capacity) = (data.len(), handles.len());

            if state.pending_gets.as_ref().is_some_and(|gets| !gets.is_empty()) && state.is_pending_get_exclusive {
                return GetOutput::failure(IpczResult::AlreadyExists);
            }

            let p = state.inbound_parcels.next_element();
            let data_size = if allow_partial { p.data_size().min(data_capacity) } else { p.data_size() };
            let handles_size = if allow_partial {
                p.num_objects().min(handles_capacity)
            } else {
                p.num_objects()
            };
            output = GetOutput {
                result: IpczResult::Ok,
                num_bytes: data_size,
                num_handles: handles_size,
                parcel: None,
            };

            let consuming_whole_parcel = data_capacity >= data_size && handles_capacity >= handles_size;
            if !consuming_whole_parcel && !allow_partial {
                output.result = IpczResult::ResourceExhausted;
                return output;
            }

            if data_size > 0 {
                data[..data_size].copy_from_slice(&p.data_view()[..data_size]);
            }

            let mut parcel = state.inbound_parcels.pop().expect("next element is available");
            parcel.consume_handles(&mut handles[..handles_size]);

            if state.inbound_parcels.is_sequence_fully_consumed() {
                state.status_flags |= PortalStatusFlags::PEER_CLOSED | PortalStatusFlags::DEAD;
            }
            state
                .traps
                .notify_local_parcel_consumed(state.status_flags, &state.inbound_parcels, &mut dispatcher);
            consumed_parcel = parcel;
        }

        if want_parcel {
            output.parcel = Some(api_object::release_as_handle(ParcelWrapper::new(Some(consumed_parcel))));
        }

        output
    }

    /// Begins a two-phase get of the next inbound parcel.
    pub fn begin_get(&self, flags: BeginGetFlags, handles: &mut [IpczHandle]) -> BeginGetOutput {
        let mut dispatcher = TrapEventDispatcher::new();
        let mut state = self.lock();
        let state = &mut *state;
        if state.inward_edge.is_some() {
            return BeginGetOutput::failure(IpczResult::InvalidArgument);
        }

        let overlapped = flags.contains(BeginGetFlags::OVERLAPPED);
        let allow_partial = flags.contains(BeginGetFlags::PARTIAL);
        if !overlapped && state.pending_gets.as_ref().is_some_and(|gets| !gets.is_empty()) {
            return BeginGetOutput::failure(IpczResult::AlreadyExists);
        }
        if overlapped && state.is_pending_get_exclusive {
            return BeginGetOutput::failure(IpczResult::AlreadyExists);
        }
        if !state.inbound_parcels.has_next_element() {
            return BeginGetOutput::failure(IpczResult::Unavailable);
        }

        let p = state.inbound_parcels.next_element();
        let num_objects = p.num_objects();
        let handle_capacity = handles.len();
        let num_handles_to_consume = handle_capacity.min(num_objects);
        if num_handles_to_consume < num_objects && !allow_partial {
            let mut output = BeginGetOutput::failure(IpczResult::ResourceExhausted);
            output.num_handles = num_objects;
            return output;
        }

        p.consume_handles(&mut handles[..num_handles_to_consume]);
        let (data, num_bytes) = (p.data_ptr() as *const u8, p.data_size());

        let parcel = if overlapped {
            take_next_inbound_parcel(state, &mut dispatcher)
        } else {
            // The parcel stays logically at the head of the inbound queue (the
            // slot is filled with an empty placeholder) until the transaction
            // ends, at which point it's put back or consumed.
            state.is_pending_get_exclusive = true;
            std::mem::replace(state.inbound_parcels.next_element(), Box::new(Parcel::default()))
        };
        let transaction = state
            .pending_gets
            .get_or_insert_with(|| Box::new(PendingTransactionSet::new()))
            .add(parcel);

        BeginGetOutput {
            result: IpczResult::Ok,
            data,
            num_bytes,
            num_handles: num_handles_to_consume,
            transaction,
        }
    }

    /// Completes a two-phase get.
    pub fn end_get(
        &self,
        transaction: IpczTransaction,
        flags: EndGetFlags,
        want_parcel: bool,
    ) -> (IpczResult, Option<IpczHandle>) {
        let mut dispatcher = TrapEventDispatcher::new();
        let mut state = self.lock();
        let state = &mut *state;
        let Some(pending_gets) = state.pending_gets.as_mut() else {
            return (IpczResult::InvalidArgument, None);
        };

        let Some(mut parcel) = pending_gets.finalize_for_get(transaction) else {
            return (IpczResult::InvalidArgument, None);
        };

        let aborted = flags.contains(EndGetFlags::ABORT);
        let mut consumed = None;
        if state.is_pending_get_exclusive {
            assert!(state.inbound_parcels.has_next_element());
            assert_eq!(state.inbound_parcels.current_sequence_number(), parcel.sequence_number());
            *state.inbound_parcels.next_element() = std::mem::take(&mut parcel);
            if !aborted {
                consumed = Some(take_next_inbound_parcel(state, &mut dispatcher));
            }
            state.is_pending_get_exclusive = false;
        } else if !aborted {
            consumed = Some(parcel);
        }

        let handle = match (aborted, want_parcel, consumed) {
            (false, true, Some(parcel)) => Some(api_object::release_as_handle(ParcelWrapper::new(Some(parcel)))),
            _ => None,
        };
        (IpczResult::Ok, handle)
    }

    /// Attempts to install a new trap on this Router, to invoke `handler` as soon
    /// as one or more conditions in `conditions` is met. This method effectively
    /// implements the ipcz Trap() API.
    pub fn trap(
        &self,
        conditions: TrapConditions,
        handler: TrapEventHandler,
        context: u64,
    ) -> Result<(), TrapAlreadySatisfied> {
        let mut state = self.lock();
        let state = &mut *state;
        state
            .traps
            .add(conditions, handler, context, state.status_flags, &state.inbound_parcels)
    }

    /// Attempts to merge this Router's route with the route terminated by
    /// `other`. Both `other` and this Router must be terminal routers on their
    /// own separate routes, and neither Router must have transmitted or
    /// retrieved any parcels via Put or Get APIs.
    pub fn merge_route(self: &Arc<Self>, other: &Arc<Router>) -> IpczResult {
        let mut dispatcher = TrapEventDispatcher::new();
        if self.has_local_peer(other) || Arc::ptr_eq(self, other) {
            return IpczResult::InvalidArgument;
        }

        {
            let (mut this, mut that) = lock2(&self.state, &other.state);
            if this.inward_edge.is_some() || that.inward_edge.is_some() || this.bridge.is_some() || that.bridge.is_some()
            {
                // It's not legal to call this on non-terminal routers.
                return IpczResult::InvalidArgument;
            }

            let zero = SequenceNumber::new(0);
            if this.inbound_parcels.current_sequence_number() > zero
                || this.outbound_parcels.get_current_sequence_length() > zero
                || that.inbound_parcels.current_sequence_number() > zero
                || that.outbound_parcels.get_current_sequence_length() > zero
            {
                // It's not legal to call this on a router which has transmitted
                // outbound parcels to its peer or retrieved inbound parcels from
                // its queue.
                return IpczResult::FailedPrecondition;
            }

            this.traps.remove_all(&mut dispatcher);
            that.traps.remove_all(&mut dispatcher);

            this.bridge = Some(Box::new(RouteEdge::new()));
            that.bridge = Some(Box::new(RouteEdge::new()));

            let links = LocalRouterLink::create_pair(LinkType::Bridge, (self.clone(), other.clone()), InitialState::Unstable);
            this.bridge.as_mut().expect("bridge").set_primary_link(links.0);
            that.bridge.as_mut().expect("bridge").set_primary_link(links.1);
        }

        self.flush(FlushBehavior::Default);
        IpczResult::Ok
    }

    /// Deserializes a new Router from `descriptor` received over
    /// `from_node_link` on `receiving_sublink`.
    pub fn deserialize(
        descriptor: &RouterDescriptor,
        from_node_link: &Arc<NodeLink>,
        receiving_sublink: SublinkId,
    ) -> Option<Arc<Router>> {
        let new_decaying_sublink = descriptor.proxy_already_bypassed().then_some(descriptor.new_decaying_sublink);

        if descriptor.new_sublink == receiving_sublink
            || Some(descriptor.new_sublink) == new_decaying_sublink
            || new_decaying_sublink == Some(receiving_sublink)
        {
            // New sublink IDs must be unique among each other, and must not
            // identify the new Router as its own recipient.
            return None;
        }

        // Resolve and validate the link state fragment before acquiring the
        // Router lock. This avoids a potential lock order inversion between the
        // Router's lock and the BufferPool's, as `adopt_fragment_ref_if_valid`
        // may acquire the BufferPool lock.
        let link_state = if new_decaying_sublink.is_some() {
            let link_state = from_node_link
                .memory()
                .adopt_fragment_ref_if_valid::<RouterLinkState>(&descriptor.new_link_state_fragment);
            if link_state.is_null() {
                // Central links require a valid link state fragment.
                return None;
            }
            link_state
        } else {
            if !descriptor.new_link_state_fragment.is_null() {
                // No RouterLinkState fragment should be provided for this new
                // peripheral link.
                return None;
            }
            FragmentRef::default()
        };

        let router = Router::new();
        let new_outward_link: Option<Arc<RemoteRouterLink>>;
        {
            let mut state = router.lock();
            let state = &mut *state;
            state.outbound_parcels.reset_sequence(descriptor.next_outgoing_sequence_number);
            state.inbound_parcels.reset_sequence(descriptor.next_incoming_sequence_number);
            if descriptor.peer_closed() {
                state.is_peer_closed = true;
                if !state
                    .inbound_parcels
                    .set_final_sequence_length(descriptor.closed_peer_sequence_length)
                {
                    return None;
                }
                if state.inbound_parcels.is_sequence_fully_consumed() {
                    state.status_flags |= PortalStatusFlags::PEER_CLOSED | PortalStatusFlags::DEAD;
                }
            }

            if let Some(new_decaying_sublink) = new_decaying_sublink {
                // When split from a local peer, our remote counterpart (our
                // remote peer's former local peer) will use this link to forward
                // parcels it already received from our peer. This link decays
                // like any other decaying link once its usefulness has expired.
                //
                // The sequence length toward this link is the current outbound
                // sequence length, which is to say, we will not be sending any
                // parcels that way. The sequence length from the link is
                // whatever had already been sent to our counterpart back on the
                // peer's node.
                let new_decaying_link = from_node_link.add_remote_router_link(
                    new_decaying_sublink,
                    FragmentRef::default(),
                    LinkType::PeripheralOutward,
                    LinkSide::B,
                    router.clone(),
                )?;
                state.outward_edge.set_primary_link(new_decaying_link);
                state.outward_edge.begin_primary_link_decay();
                state
                    .outward_edge
                    .set_length_to_decaying_link(state.outbound_parcels.current_sequence_number());
                state.outward_edge.set_length_from_decaying_link(
                    if descriptor.decaying_incoming_sequence_length > SequenceNumber::new(0) {
                        descriptor.decaying_incoming_sequence_length
                    } else {
                        descriptor.next_incoming_sequence_number
                    },
                );

                let link = from_node_link.add_remote_router_link(
                    descriptor.new_sublink,
                    link_state,
                    LinkType::Central,
                    LinkSide::B,
                    router.clone(),
                )?;
                state.outward_edge.set_primary_link(link.clone());
                new_outward_link = Some(link);

                log::trace!(
                    "Route extended from {} to {} via sublink {} and decaying sublink {}",
                    from_node_link.remote_node_name(),
                    from_node_link.local_node_name(),
                    descriptor.new_sublink,
                    new_decaying_sublink
                );
            } else {
                new_outward_link = from_node_link.add_remote_router_link(
                    descriptor.new_sublink,
                    FragmentRef::default(),
                    LinkType::PeripheralOutward,
                    LinkSide::B,
                    router.clone(),
                );
                if let Some(link) = &new_outward_link {
                    state.outward_edge.set_primary_link(link.clone());

                    log::trace!(
                        "Route extended from {} to {} via sublink {}",
                        from_node_link.remote_node_name(),
                        from_node_link.local_node_name(),
                        descriptor.new_sublink
                    );
                }
            }
        }

        from_node_link.accept_early_parcels_for_sublink(descriptor.new_sublink);

        match &new_outward_link {
            None => {
                // The new portal is DOA, either because the associated NodeLink
                // is dead, or the sublink ID was already in use. The latter
                // implies a bug or bad behavior, but it should be harmless to
                // ignore beyond this point.
                log::trace!("Disconnected new Router immediately after deserialization");
                router.accept_route_disconnected_from(LinkType::PeripheralOutward);
            }
            Some(link) if descriptor.proxy_peer_node_name.is_valid() => {
                // The source router rolled some peer bypass details into our
                // descriptor to avoid some IPC overhead. We can begin bypassing
                // the proxy now.
                router.bypass_peer(link, &descriptor.proxy_peer_node_name, descriptor.proxy_peer_sublink);
            }
            Some(_) => {}
        }

        router.flush(FlushBehavior::ForceProxyBypassAttempt);
        Some(router)
    }

    /// Serializes a description of a new Router which will be used to extend this
    /// Router's route across `to_node_link` by introducing a new Router on the
    /// remote node.
    pub fn serialize_new_router(self: &Arc<Self>, to_node_link: &Arc<NodeLink>, descriptor: &mut RouterDescriptor) {
        let mut dispatcher = TrapEventDispatcher::new();
        let local_peer;
        let initiate_proxy_bypass;
        {
            let mut state = self.lock();
            state.traps.remove_all(&mut dispatcher);
            local_peer = state.outward_edge.get_local_peer();
            initiate_proxy_bypass = state
                .outward_edge
                .primary_link()
                .is_some_and(|link| link.try_lock_for_bypass(*to_node_link.remote_node_name()));
        }

        if let Some(local_peer) = local_peer.filter(|_| initiate_proxy_bypass)
            && self.serialize_new_router_with_local_peer(to_node_link, descriptor, local_peer.clone()) {
                local_peer.flush(FlushBehavior::ForceProxyBypassAttempt);
                return;
            }

        self.serialize_new_router_and_configure_proxy(to_node_link, descriptor, initiate_proxy_bypass);
    }

    // Optimized Router serialization case when the Router's peer is local to the
    // same node and the existing (local) central link can be replaced with a new
    // remote link, without establishing an intermediate proxy. Returns true on
    // success, or false indicating that the caller must fall back onto the
    // slower Router serialization path.
    fn serialize_new_router_with_local_peer(
        self: &Arc<Self>,
        to_node_link: &Arc<NodeLink>,
        descriptor: &mut RouterDescriptor,
        local_peer: Arc<Router>,
    ) -> bool {
        let (mut this, mut peer) = lock2(&self.state, &local_peer.state);
        let peer_links_to_us = peer
            .outward_edge
            .get_local_peer()
            .is_some_and(|router| Arc::ptr_eq(&router, self));
        if !peer_links_to_us {
            // If the peer was closed, its link to us may already be
            // invalidated.
            return false;
        }

        let mut new_link_state = to_node_link.memory().try_allocate_router_link_state();
        if !new_link_state.is_addressable() {
            // If we couldn't allocate a RouterLinkState for a new central link,
            // then we can't replace the central link yet. Fall back to the
            // proxying case.
            return false;
        }

        let proxy_inbound_sequence_length = peer.outbound_parcels.current_sequence_number();

        // The local peer no longer needs its link to us. We'll give it a new
        // outward link in `begin_proxying_to_new_router()` after this
        // descriptor is transmitted.
        peer.outward_edge.release_primary_link();

        // The primary new sublink to the destination node will act as the
        // route's new central link between our local peer and the new remote
        // router.
        //
        // An additional sublink is allocated to act as a decaying inward link
        // from this router to the new one, so we can forward any inbound parcels
        // that have already been queued here.
        let new_sublink = to_node_link.memory().allocate_sublink_ids(2);
        let decaying_sublink = SublinkId::new(new_sublink.value() + 1);

        // Register the new routes on the NodeLink. Note that we don't provide
        // them to any routers yet since we don't want the routers using them
        // until this descriptor is transmitted to its destination node. The
        // links will be adopted after transmission in
        // `begin_proxying_to_new_router()`.
        let _new_link = to_node_link.add_remote_router_link(
            new_sublink,
            new_link_state.clone(),
            LinkType::Central,
            LinkSide::A,
            local_peer.clone(),
        );

        to_node_link.add_remote_router_link(
            decaying_sublink,
            FragmentRef::default(),
            LinkType::PeripheralInward,
            LinkSide::A,
            self.clone(),
        );

        descriptor.new_sublink = new_sublink;
        descriptor.new_link_state_fragment = *new_link_state.release().descriptor();
        descriptor.new_decaying_sublink = decaying_sublink;
        descriptor.set_proxy_already_bypassed(true);
        descriptor.next_outgoing_sequence_number = this.outbound_parcels.get_current_sequence_length();
        descriptor.next_incoming_sequence_number = this.inbound_parcels.current_sequence_number();
        descriptor.decaying_incoming_sequence_length = proxy_inbound_sequence_length;

        log::trace!(
            "Splitting local pair to move router with outbound sequence length {} and current inbound sequence number {}",
            descriptor.next_outgoing_sequence_number,
            descriptor.next_incoming_sequence_number
        );

        if let Some(length) = this.inbound_parcels.final_sequence_length() {
            descriptor.set_peer_closed(true);
            descriptor.closed_peer_sequence_length = length;
        }

        // Initialize an inward edge that will immediately begin decaying once it
        // has a link (established in `begin_proxying_to_new_router()`).
        let mut inward_edge = Box::new(RouteEdge::new());
        inward_edge.begin_primary_link_decay();
        inward_edge.set_length_to_decaying_link(proxy_inbound_sequence_length);
        inward_edge.set_length_from_decaying_link(this.outbound_parcels.get_current_sequence_length());
        this.inward_edge = Some(inward_edge);
        true
    }

    // Default Router serialization case when the serializing Router must stay
    // behind as an intermediate proxy between its (remote) peer and the newly
    // established Router that will result from this serialization. As an
    // optimization, `initiate_proxy_bypass` may be true if the serializing router
    // is on the central link and was able to lock that link for bypass prior to
    // serialization.
    fn serialize_new_router_and_configure_proxy(
        self: &Arc<Self>,
        to_node_link: &Arc<NodeLink>,
        descriptor: &mut RouterDescriptor,
        initiate_proxy_bypass: bool,
    ) {
        let new_sublink = to_node_link.memory().allocate_sublink_ids(1);

        let mut state = self.lock();
        let state = &mut *state;
        descriptor.new_sublink = new_sublink;
        descriptor.new_link_state_fragment = FragmentDescriptor::default();
        descriptor.set_proxy_already_bypassed(false);
        descriptor.next_outgoing_sequence_number = state.outbound_parcels.get_current_sequence_length();
        descriptor.next_incoming_sequence_number = state.inbound_parcels.current_sequence_number();

        // Initialize an inward edge but with no link yet. This ensures that we
        // don't look like a terminal router while waiting for a link to be set,
        // which can only happen after `descriptor` is transmitted.
        let mut inward_edge = Box::new(RouteEdge::new());

        if state.is_peer_closed {
            let final_length = state.inbound_parcels.final_sequence_length().expect("closed peer has a final length");
            descriptor.set_peer_closed(true);
            descriptor.closed_peer_sequence_length = final_length;

            // Ensure that the new edge decays its link as soon as it has one,
            // since we know the link will not be used.
            inward_edge.begin_primary_link_decay();
            inward_edge.set_length_to_decaying_link(final_length);
            inward_edge.set_length_from_decaying_link(state.outbound_parcels.current_sequence_number());
        } else if initiate_proxy_bypass && state.outward_edge.primary_link().is_some() {
            let outward_link = state.outward_edge.primary_link().expect("outward link").clone();
            match outward_link.as_remote_router_link() {
                Some(remote_link) => {
                    descriptor.proxy_peer_node_name = *remote_link.node_link().remote_node_name();
                    descriptor.proxy_peer_sublink = remote_link.sublink();
                    log::trace!(
                        "Will initiate proxy bypass immediately on deserialization with peer at {} and peer route to proxy on sublink {}",
                        descriptor.proxy_peer_node_name,
                        descriptor.proxy_peer_sublink
                    );

                    inward_edge.begin_primary_link_decay();
                    state.outward_edge.begin_primary_link_decay();
                }
                None => {
                    // The link was locked in anticipation of initiating a proxy
                    // bypass, but that's no longer going to happen.
                    outward_link.unlock();
                }
            }
        }
        state.inward_edge = Some(inward_edge);

        // Once `descriptor` is transmitted to the destination node and the new
        // Router is created there, it may immediately begin transmitting
        // messages back to this node regarding `new_sublink`. We establish a new
        // RemoteRouterLink now and register it to `new_sublink` on
        // `to_node_link`, so that any such incoming messages are routed to
        // `self`.
        //
        // NOTE: We do not yet provide `self` itself with a reference to the new
        // RemoteRouterLink, because it's not yet safe for us to send messages to
        // the remote node regarding `new_sublink`. `descriptor` must be
        // transmitted first.
        let new_link = to_node_link.add_remote_router_link(
            new_sublink,
            FragmentRef::default(),
            LinkType::PeripheralInward,
            LinkSide::A,
            self.clone(),
        );
        if let Some(new_link) = new_link {
            log::trace!("Router extending route with tentative new {}", new_link.describe());
        }
    }

    /// Configures this Router to begin proxying incoming parcels toward (and
    /// outgoing parcels from) the Router described by `descriptor`, living on the
    /// remote node of `to_node_link`.
    pub fn begin_proxying_to_new_router(
        self: &Arc<Self>,
        to_node_link: &Arc<NodeLink>,
        descriptor: &RouterDescriptor,
    ) {
        let mut local_peer: Option<Arc<Router>> = None;

        // Acquire references to RemoteRouterLink(s) created by an earlier call
        // to `serialize_new_router()`. If the NodeLink has already been
        // disconnected, these may be `None`.
        let new_sublink = to_node_link.get_sublink(descriptor.new_sublink);
        let new_decaying_sublink = to_node_link.get_sublink(descriptor.new_decaying_sublink);
        let Some(new_sublink) = new_sublink else {
            self.accept_route_disconnected_from(LinkType::PeripheralInward);
            self.flush(FlushBehavior::ForceProxyBypassAttempt);
            return;
        };

        let mut new_primary_link: Option<Arc<RemoteRouterLink>> = Some(new_sublink.router_link);
        let mut new_decaying_link: Option<Arc<RemoteRouterLink>> = None;
        {
            let mut state = self.lock();
            let state = &mut *state;
            assert!(state.inward_edge.is_some());

            if descriptor.proxy_already_bypassed() {
                let peer_link = state.outward_edge.release_primary_link();
                local_peer = peer_link.as_ref().and_then(|link| link.get_local_peer());
                new_decaying_link = new_decaying_sublink.map(|sublink| sublink.router_link);
            }

            if local_peer.is_some() && new_decaying_link.is_some() && !state.is_disconnected {
                // We've already bypassed this router. Use the new decaying link
                // for our inward edge in case we need to forward parcels to the
                // new router. The new primary link will be adopted by our peer
                // further below.
                state
                    .inward_edge
                    .as_mut()
                    .expect("inward edge")
                    .set_primary_link(new_decaying_link.take().expect("decaying link"));
            } else if state.outbound_parcels.final_sequence_length().is_none()
                && new_decaying_link.is_none()
                && !state.is_disconnected
            {
                let link = new_primary_link.take().expect("primary link");
                log::trace!("Router will proxy to new router over {}", link.describe());
                state.inward_edge.as_mut().expect("inward edge").set_primary_link(link);

                if let Some(outward_link) = state.outward_edge.primary_link()
                    && state.outward_edge.is_stable() && state.inward_edge.as_ref().expect("inward edge").is_stable() {
                        outward_link.mark_side_stable();
                    }
            }
        }

        // NOTE: if the decaying link was adopted above, the primary link is
        // still present here.
        if let (Some(peer), true, None) = (&local_peer, new_primary_link.is_some(), &new_decaying_link) {
            // If we have a `local_peer` and no decaying link, this means the
            // decaying link was successfully adopted for our own inward edge; and
            // the primary link is therefore meant to serve as our local peer's
            // new outward link directly to the new remote router.
            peer.set_outward_link(new_primary_link.take().expect("primary link"));
        }

        // New links that were not adopted imply that the new router has already
        // been closed or disconnected.
        if let Some(link) = new_primary_link {
            log::trace!("Dropping link to new router {}", link.describe());
            link.accept_route_disconnected();
            link.deactivate();
        }
        if let Some(link) = new_decaying_link {
            log::trace!("Dropping link to new router {}", link.describe());
            link.accept_route_disconnected();
            link.deactivate();
        }

        // We may have inbound parcels queued which need to be forwarded to the
        // new Router, so give them a chance to be flushed out.
        self.flush(FlushBehavior::ForceProxyBypassAttempt);
        if let Some(peer) = local_peer {
            peer.flush(FlushBehavior::ForceProxyBypassAttempt);
        }
    }

    /// Notifies this router that it should reach out to its outward peer's own
    /// outward peer in order to establish a direct link. `requestor` is the link
    /// over which this request arrived, and it must be this router's current
    /// outward peer in order for the request to be valid.
    ///
    /// Returns true if the BypassPeer() request was valid, or false if it was
    /// invalid. Note that a return value of true does not necessarily imply that
    /// bypass was or will be successful (e.g. it may silently fail due to lost
    /// node connections).
    pub fn bypass_peer(
        self: &Arc<Self>,
        requestor: &Arc<RemoteRouterLink>,
        bypass_target_node: &NodeName,
        bypass_target_sublink: SublinkId,
    ) -> bool {
        let from_node_link = requestor.node_link().clone();

        // Validate that the source of this request is actually our peripheral
        // outward peer, and that we are therefore its inward peer.
        {
            let state = self.lock();
            let Some(outward_link) = state.outward_edge.primary_link() else {
                // This Router may have been disconnected already due to some
                // other failure along the route. This is not the fault of the
                // requestor, so we silently ignore the request.
                return true;
            };

            if !is_remote_link(outward_link, requestor) {
                log::error!(
                    "Rejecting BypassPeer received on {} with existing {}",
                    requestor.describe(),
                    outward_link.describe()
                );
                return false;
            }
        }

        // There are two distinct cases to handle. The first case here is when
        // the proxy's outward peer lives on a different node from us.
        if bypass_target_node != from_node_link.local_node_name() {
            if let Some(link_to_bypass_target) = from_node_link.node().get_link(bypass_target_node) {
                let new_link_state = link_to_bypass_target.memory().try_allocate_router_link_state();
                return self.bypass_peer_with_new_remote_link(
                    requestor,
                    &link_to_bypass_target,
                    bypass_target_sublink,
                    new_link_state,
                );
            }

            // We need to establish a link to the target node before we can
            // proceed.
            let router = self.clone();
            let requestor = requestor.clone();
            from_node_link.node().establish_link(
                bypass_target_node,
                Box::new(move |link_to_bypass_target| {
                    let Some(link_to_bypass_target) = link_to_bypass_target else {
                        log::error!("Disconnecting Router due to failed introduction");
                        router.accept_route_disconnected_from(LinkType::PeripheralOutward);
                        return;
                    };

                    let new_link_state = link_to_bypass_target.memory().try_allocate_router_link_state();
                    router.bypass_peer_with_new_remote_link(
                        &requestor,
                        &link_to_bypass_target,
                        bypass_target_sublink,
                        new_link_state,
                    );
                }),
            );
            return true;
        }

        // The second case is when the proxy's outward peer lives on our own node.
        self.bypass_peer_with_new_local_link(requestor, bypass_target_sublink)
    }

    /// Begins decaying this router's outward link and replaces it with a new
    /// link over `new_node_link` via `new_sublink`, and using `new_link_state`
    /// for its shared state.
    ///
    /// `inbound_sequence_length_from_bypassed_link` conveys the final length of
    /// sequence of inbound parcels to expect over the decaying link from the
    /// peer.
    ///
    /// Returns true if the request was valid, or false if it was invalid. An
    /// invalid request implies that a remote node tried to do something bad and
    /// should be disconnected ASAP.
    pub fn accept_bypass_link(
        self: &Arc<Self>,
        new_node_link: &Arc<NodeLink>,
        new_sublink: SublinkId,
        new_link_state: FragmentRef<RouterLinkState>,
        inbound_sequence_length_from_bypassed_link: SequenceNumber,
    ) -> bool {
        let length_to_proxy_from_us;
        let old_link: Arc<RemoteRouterLink>;
        let new_link: Option<Arc<RemoteRouterLink>>;
        {
            let mut state = self.lock();
            let state = &mut *state;
            let Some(outward_link) = state.outward_edge.primary_link().cloned().filter(|_| !state.is_disconnected)
            else {
                // We've already been unexpectedly disconnected from the proxy,
                // so the route is dysfunctional. Don't establish new links.
                log::trace!("Discarding proxy bypass link due to peer disconnection");
                return true;
            };

            let Some(remote_link) = outward_link.into_remote_router_link() else {
                // It only makes sense to receive this at a router whose outward
                // link is remote. If we have a non-remote outward link,
                // something is wrong.
                log::trace!("Rejecting unexpected bypass link");
                return false;
            };
            old_link = remote_link;

            if !Arc::ptr_eq(old_link.node_link(), new_node_link)
                && !old_link.can_node_request_bypass(new_node_link.remote_node_name())
            {
                // The new link must either go to the same node as the old link,
                // or the the old link must have been expecting a bypass link to
                // the new node.
                log::error!("Rejecting unauthorized BypassProxy");
                return false;
            }

            length_to_proxy_from_us = state.outbound_parcels.current_sequence_number();
            if !state.outward_edge.begin_primary_link_decay() {
                log::error!("Rejecting BypassProxy on failure to decay link");
                return false;
            }

            // By convention the initiator of a bypass assumes side A of the
            // bypass link, so we assume side B.
            new_link = new_node_link.add_remote_router_link(
                new_sublink,
                new_link_state,
                LinkType::Central,
                LinkSide::B,
                self.clone(),
            );

            if let Some(link) = &new_link {
                log::trace!(
                    "Bypassing proxy on other end of {} using a new {} with length to proxy {} and length from proxy {}",
                    old_link.describe(),
                    link.describe(),
                    length_to_proxy_from_us,
                    inbound_sequence_length_from_bypassed_link
                );

                state.outward_edge.set_length_to_decaying_link(length_to_proxy_from_us);
                state
                    .outward_edge
                    .set_length_from_decaying_link(inbound_sequence_length_from_bypassed_link);
                state.outward_edge.set_primary_link(link.clone());
            }
        }

        let Some(new_link) = new_link else {
            self.accept_route_disconnected_from(LinkType::Central);
            return true;
        };

        if Arc::ptr_eq(new_link.node_link(), old_link.node_link()) {
            // If the new link goes to the same place as the old link, we only
            // need to tell the proxy there to stop proxying. It has already
            // conspired with its local outward peer.
            old_link.stop_proxying_to_local_peer(length_to_proxy_from_us);
        } else {
            // Otherwise, tell the proxy to stop proxying and let its inward peer
            // (our new outward peer) know that the proxy will stop.
            old_link.stop_proxying(length_to_proxy_from_us, inbound_sequence_length_from_bypassed_link);
            new_link.proxy_will_stop(length_to_proxy_from_us);
        }

        self.flush(FlushBehavior::Default);
        true
    }

    /// Configures the final inbound and outbound sequence lengths of this
    /// router's decaying links. Once these lengths are set and sequences have
    /// progressed to the specified length in each direction, those decaying links
    /// -- and eventually the router itself -- are dropped.
    ///
    /// Returns true if and only if this router is a proxy with decaying inward
    /// and outward links. Otherwise returns false, indicating an invalid request.
    pub fn stop_proxying(
        self: &Arc<Self>,
        inbound_sequence_length: SequenceNumber,
        outbound_sequence_length: SequenceNumber,
    ) -> bool {
        let bridge_peer: Option<Arc<Router>>;
        {
            let mut state = self.lock();
            let state = &mut *state;
            if state.outward_edge.is_stable() {
                // Proxies begin decaying their links before requesting to be
                // bypassed, and they don't adopt new links after that. So if
                // either edge is stable then someone is doing something wrong.
                log::error!("Rejecting StopProxying on invalid or non-proxying Router");
                return false;
            }

            if let Some(bridge) = &state.bridge {
                // If we have a bridge link, we also need to update the router on
                // the other side of the bridge.
                bridge_peer = bridge.get_decaying_local_peer();
                if bridge_peer.is_none() {
                    return false;
                }
            } else {
                bridge_peer = None;
                match state.inward_edge.as_mut() {
                    Some(inward_edge) if !inward_edge.is_stable() => {
                        if inward_edge.length_to_decaying_link().is_some()
                            || inward_edge.length_from_decaying_link().is_some()
                            || state.outward_edge.length_to_decaying_link().is_some()
                            || state.outward_edge.length_from_decaying_link().is_some()
                        {
                            return false;
                        }

                        inward_edge.set_length_to_decaying_link(inbound_sequence_length);
                        inward_edge.set_length_from_decaying_link(outbound_sequence_length);
                        state.outward_edge.set_length_to_decaying_link(outbound_sequence_length);
                        state.outward_edge.set_length_from_decaying_link(inbound_sequence_length);
                    }
                    // Not a proxy, so this request is invalid.
                    _ => return false,
                }
            }
        }

        if let Some(bridge_peer) = &bridge_peer {
            let (mut this, mut peer) = lock2(&self.state, &bridge_peer.state);
            let (this, peer) = (&mut *this, &mut *peer);
            match (&mut this.bridge, &mut peer.bridge) {
                (Some(bridge), Some(peer_bridge)) if !bridge.is_stable() && !peer_bridge.is_stable() => {
                    if bridge.length_to_decaying_link().is_some()
                        || bridge.length_from_decaying_link().is_some()
                        || this.outward_edge.length_to_decaying_link().is_some()
                        || this.outward_edge.length_from_decaying_link().is_some()
                        || peer_bridge.length_to_decaying_link().is_some()
                        || peer_bridge.length_from_decaying_link().is_some()
                        || peer.outward_edge.length_to_decaying_link().is_some()
                        || peer.outward_edge.length_from_decaying_link().is_some()
                    {
                        return false;
                    }

                    bridge.set_length_to_decaying_link(inbound_sequence_length);
                    bridge.set_length_from_decaying_link(outbound_sequence_length);
                    this.outward_edge.set_length_to_decaying_link(outbound_sequence_length);
                    this.outward_edge.set_length_from_decaying_link(inbound_sequence_length);
                    peer_bridge.set_length_to_decaying_link(outbound_sequence_length);
                    peer_bridge.set_length_from_decaying_link(inbound_sequence_length);
                    peer.outward_edge.set_length_to_decaying_link(inbound_sequence_length);
                    peer.outward_edge.set_length_from_decaying_link(outbound_sequence_length);
                }
                // The bridge is being or has already been torn down, so there's
                // nothing to do here.
                _ => return true,
            }
        }

        self.flush(FlushBehavior::Default);
        if let Some(bridge_peer) = bridge_peer {
            bridge_peer.flush(FlushBehavior::Default);
        }
        true
    }

    /// Configures the final length of the inbound parcel sequence coming from
    /// this router's decaying outward link. Once this length is set and the
    /// decaying link has forwarded the full sequence of parcels up to this limit,
    /// the decaying link can be dropped.
    ///
    /// Returns true if this router has a decaying outward link -- implying that
    /// its outward peer is a proxy -- or the router has been disconnected.
    /// Otherwise the request is invalid and this returns false.
    pub fn notify_proxy_will_stop(self: &Arc<Self>, inbound_sequence_length: SequenceNumber) -> bool {
        {
            let mut state = self.lock();
            if state.outward_edge.is_stable() {
                // If the outward edge is already stable, either this request is
                // invalid, or we've lost all links due to disconnection. In the
                // latter case we can silently ignore this, but the former case
                // is a validation failure.
                return state.is_disconnected;
            }

            if state.outward_edge.length_from_decaying_link().is_some() {
                return false;
            }

            log::trace!(
                "Bypassed proxy will stop forwarding inbound parcels after a sequence length of {inbound_sequence_length}"
            );

            state.outward_edge.set_length_from_decaying_link(inbound_sequence_length);
        }

        self.flush(FlushBehavior::Default);
        true
    }

    /// Configures the final sequence length of outbound parcels to expect on this
    /// proxying Router's decaying inward link. Once this is set and the decaying
    /// link has received the full sequence of parcels, the link can be dropped.
    ///
    /// Returns true if the request is valid, meaning that this Router is a proxy
    /// whose outward peer is local to the same node. Otherwise this returns
    /// false.
    pub fn stop_proxying_to_local_peer(self: &Arc<Self>, outbound_sequence_length: SequenceNumber) -> bool {
        let mut local_peer: Option<Arc<Router>> = None;
        let mut bridge_peer: Option<Arc<Router>> = None;
        {
            let state = self.lock();
            if let Some(bridge) = &state.bridge {
                bridge_peer = bridge.get_decaying_local_peer();
            } else if let Some(decaying_link) = state.outward_edge.decaying_link() {
                local_peer = decaying_link.get_local_peer();
            } else {
                // Ignore this request if we've been unexpectedly disconnected.
                return state.is_disconnected;
            }
        }

        if let (Some(local_peer), None) = (&local_peer, &bridge_peer) {
            // This is the common case, with no bridge link.
            let (mut this, mut peer) = lock2(&self.state, &local_peer.state);
            let (this, peer) = (&mut *this, &mut *peer);
            let (Some(our_link), Some(peer_link)) =
                (this.outward_edge.decaying_link().cloned(), peer.outward_edge.decaying_link().cloned())
            else {
                // Either Router may have been unexpectedly disconnected, in
                // which case we can ignore this request.
                return true;
            };

            let our_link_goes_to_peer = our_link.get_local_peer().is_some_and(|r| Arc::ptr_eq(&r, local_peer));
            let peer_link_goes_to_us = peer_link.get_local_peer().is_some_and(|r| Arc::ptr_eq(&r, self));
            let Some(inward_edge) = this.inward_edge.as_mut().filter(|_| our_link_goes_to_peer && peer_link_goes_to_us)
            else {
                // Consistency check: this must be a proxying router, and both
                // this router and its local peer must link to each other.
                log::error!("Rejecting StopProxyingToLocalPeer at invalid proxy");
                return false;
            };

            if peer.outward_edge.length_from_decaying_link().is_some()
                || this.outward_edge.length_to_decaying_link().is_some()
                || inward_edge.length_from_decaying_link().is_some()
            {
                return false;
            }

            log::trace!(
                "Stopping proxy with decaying {:?} and decaying {}",
                inward_edge.decaying_link().map(|link| link.describe()),
                our_link.describe()
            );

            peer.outward_edge.set_length_from_decaying_link(outbound_sequence_length);
            this.outward_edge.set_length_to_decaying_link(outbound_sequence_length);
            inward_edge.set_length_from_decaying_link(outbound_sequence_length);
        } else if let Some(bridge_peer_router) = &bridge_peer {
            // When a bridge peer is present we actually have three local routers
            // involved: this router, its outward peer, and its bridge peer. Both
            // this router and the bridge peer serve as "the" proxy being
            // bypassed in this case, so we'll be bypassing both of them below.
            {
                let peer_state = bridge_peer_router.lock();
                if peer_state.outward_edge.is_stable() {
                    return false;
                }
                local_peer = peer_state.outward_edge.get_decaying_local_peer();
                if local_peer.is_none() {
                    return false;
                }
            }
            let local_peer_router = local_peer.clone().expect("local peer");

            let (mut this, mut local, mut bridge_state) =
                lock3(&self.state, &local_peer_router.state, &bridge_peer_router.state);
            let (this, local, bridge_state) = (&mut *this, &mut *local, &mut *bridge_state);
            if this.outward_edge.is_stable()
                || local.outward_edge.is_stable()
                || bridge_state.outward_edge.is_stable()
            {
                return false;
            }

            let (Some(bridge), Some(peer_bridge)) = (this.bridge.as_mut(), bridge_state.bridge.as_mut()) else {
                return false;
            };
            if local.outward_edge.length_from_decaying_link().is_some()
                || this.outward_edge.length_from_decaying_link().is_some()
                || bridge.length_to_decaying_link().is_some()
                || bridge_state.outward_edge.length_to_decaying_link().is_some()
                || peer_bridge.length_from_decaying_link().is_some()
            {
                return false;
            }

            local.outward_edge.set_length_from_decaying_link(outbound_sequence_length);
            this.outward_edge.set_length_from_decaying_link(outbound_sequence_length);
            bridge.set_length_to_decaying_link(outbound_sequence_length);
            bridge_state.outward_edge.set_length_to_decaying_link(outbound_sequence_length);
            peer_bridge.set_length_from_decaying_link(outbound_sequence_length);
        } else {
            // It's invalid to call this on a Router with a non-local outward peer
            // or bridge link.
            log::error!("Rejecting StopProxyingToLocalPeer with no local peer");
            return false;
        }

        self.flush(FlushBehavior::Default);
        if let Some(local_peer) = local_peer {
            local_peer.flush(FlushBehavior::Default);
        }
        if let Some(bridge_peer) = bridge_peer {
            bridge_peer.flush(FlushBehavior::Default);
        }
        true
    }

    /// Notifies this Router that one of its links has been disconnected from a
    /// remote node. The link is identified by a combination of a specific
    /// NodeLink and SublinkId.
    ///
    /// Note that this is invoked if ANY RemoteRouterLink bound to this router is
    /// disconnected at its underlying NodeLink, and the result is aggressive
    /// teardown of the route in both directions across any remaining (i.e.
    /// primary and/or decaying) links.
    pub fn notify_link_disconnected(self: &Arc<Self>, link: &RemoteRouterLink) {
        {
            let mut state = self.lock();
            let state = &mut *state;
            let matches = |candidate: Option<&Arc<dyn RouterLink>>| candidate.is_some_and(|l| is_remote_link(l, link));
            if matches(state.outward_edge.primary_link()) {
                log::trace!("Primary {} disconnected", link.describe());
                state.outward_edge.release_primary_link();
            } else if matches(state.outward_edge.decaying_link()) {
                log::trace!("Decaying {} disconnected", link.describe());
                state.outward_edge.release_decaying_link();
            } else if state.inward_edge.as_ref().is_some_and(|edge| matches(edge.primary_link())) {
                log::trace!("Primary {} disconnected", link.describe());
                state.inward_edge.as_mut().expect("inward edge").release_primary_link();
            } else if state.inward_edge.as_ref().is_some_and(|edge| matches(edge.decaying_link())) {
                log::trace!("Decaying {} disconnected", link.describe());
                state.inward_edge.as_mut().expect("inward edge").release_decaying_link();
            }
        }

        if link.get_type().is_outward() {
            self.accept_route_disconnected_from(LinkType::PeripheralOutward);
        } else {
            self.accept_route_disconnected_from(LinkType::PeripheralInward);
        }
    }

    /// Flushes any inbound or outbound parcels, as well as any route closure
    /// notifications. RouterLinks which are no longer needed for the operation of
    /// this Router may be deactivated by this call.
    ///
    /// Since this may be called by many other Router methods, RouterLink
    /// implementations must exercise caution when calling into a Router to ensure
    /// that their own potentially reentrant deactivation by `flush()` won't end up
    /// dropping the last reference and deleting `self` before `flush()` returns.
    /// A safe way to ensure that is for RouterLink implementations to only call
    /// into Router using a reference held on the calling stack.
    pub fn flush(self: &Arc<Self>, behavior: FlushBehavior) {
        let outward_link;
        let inward_link;
        let mut bridge_link: Option<Arc<dyn RouterLink>> = None;
        let decaying_outward_link;
        let decaying_inward_link;
        let mut dead_inward_link = None;
        let mut dead_outward_link = None;
        let mut dead_bridge_link = None;
        let mut final_inward_sequence_length = None;
        let mut final_outward_sequence_length = None;
        let on_central_link;
        let mut inward_link_decayed = false;
        let mut outward_link_decayed = false;
        let mut dropped_last_decaying_link = false;
        let mut parcels_to_flush = Vec::new();
        let mut dispatcher = TrapEventDispatcher::new();
        {
            let mut state = self.lock();
            let state = &mut *state;

            // Acquire stack references to all links we might want to use, so
            // it's safe to use them after the lock is released.
            outward_link = state.outward_edge.primary_link().cloned();
            inward_link = state.inward_edge.as_ref().and_then(|edge| edge.primary_link().cloned());
            decaying_outward_link = state.outward_edge.decaying_link().cloned();
            decaying_inward_link = state.inward_edge.as_ref().and_then(|edge| edge.decaying_link().cloned());
            on_central_link = outward_link.as_ref().is_some_and(|link| link.get_type().is_central());
            if let Some(bridge) = &state.bridge {
                // Bridges have either a primary link or decaying link, but never
                // both.
                bridge_link = bridge.primary_link().cloned().or_else(|| bridge.decaying_link().cloned());
            }

            // Collect any parcels which are safe to transmit now. Note that we do
            // not transmit anything or generally call into any RouterLinks while
            // the lock is held, because such calls may ultimately re-enter this
            // Router (e.g. if a link is a LocalRouterLink, or even a
            // RemoteRouterLink with a fully synchronous driver.) Instead we
            // accumulate work within this block, and then perform any
            // transmissions or link deactivations after the lock is released
            // further below.

            collect_parcels_to_flush(&mut state.outbound_parcels, &state.outward_edge, &mut parcels_to_flush);
            let outbound_sequence_length_sent = state.outbound_parcels.current_sequence_number();
            let inbound_sequence_length_received = state.inbound_parcels.get_current_sequence_length();
            if state
                .outward_edge
                .maybe_finish_decay(outbound_sequence_length_sent, inbound_sequence_length_received)
            {
                log::trace!(
                    "Outward {:?} fully decayed at {} sent and {} received",
                    decaying_outward_link.as_ref().map(|l| l.describe()),
                    outbound_sequence_length_sent,
                    inbound_sequence_length_received
                );
                outward_link_decayed = true;
            }

            if let Some(inward_edge) = state.inward_edge.as_mut() {
                collect_parcels_to_flush(&mut state.inbound_parcels, inward_edge, &mut parcels_to_flush);
                let inbound_sequence_length_sent = state.inbound_parcels.current_sequence_number();
                let outbound_sequence_length_received = state.outbound_parcels.get_current_sequence_length();
                if inward_edge.maybe_finish_decay(inbound_sequence_length_sent, outbound_sequence_length_received) {
                    log::trace!(
                        "Inward {:?} fully decayed at {} sent and {} received",
                        decaying_inward_link.as_ref().map(|l| l.describe()),
                        inbound_sequence_length_sent,
                        outbound_sequence_length_received
                    );
                    inward_link_decayed = true;
                }
            } else if bridge_link.is_some()
                && let Some(bridge) = &state.bridge {
                    collect_parcels_to_flush(&mut state.inbound_parcels, bridge, &mut parcels_to_flush);
                }

            let drop_bridge = match state.bridge.as_mut() {
                Some(bridge) => bridge.maybe_finish_decay(
                    state.inbound_parcels.current_sequence_number(),
                    state.outbound_parcels.current_sequence_number(),
                ),
                None => false,
            };
            if drop_bridge {
                state.bridge = None;
            }

            if state.is_peer_closed
                && !state.status_flags.contains(PortalStatusFlags::PEER_CLOSED)
                && !state.inbound_parcels.expects_more_elements()
            {
                // Set the PEER_CLOSED bit and trigger any relevant traps, if and
                // only if the peer is actually closed and there are no more
                // inbound parcels in flight towards us.
                state.status_flags |= PortalStatusFlags::PEER_CLOSED;
                state
                    .traps
                    .notify_peer_closed(state.status_flags, &state.inbound_parcels, &mut dispatcher);
            }

            // If we're dropping the last of our decaying links, our outward link
            // may now be stable. This may unblock proxy bypass or other
            // operations.
            let inward_edge_stable = decaying_inward_link.is_none() || inward_link_decayed;
            let outward_edge_stable = outward_link.is_some() && (decaying_outward_link.is_none() || outward_link_decayed);
            let both_edges_stable = inward_edge_stable && outward_edge_stable;
            let either_link_decayed = inward_link_decayed || outward_link_decayed;
            if on_central_link && either_link_decayed && both_edges_stable {
                let link = outward_link.as_ref().expect("outward link");
                log::trace!("Router with fully decayed links may be eligible for bypass with outward {}", link.describe());
                link.mark_side_stable();
                dropped_last_decaying_link = true;
            }

            if on_central_link
                && state.outbound_parcels.is_sequence_fully_consumed()
                && outward_link.as_ref().expect("outward link").try_lock_for_closure()
            {
                // Notify the other end of the route that this end is closed. See
                // the `accept_route_closure()` invocation further below.
                final_outward_sequence_length = state.outbound_parcels.final_sequence_length();

                // We also have no more use for either outward or inward links:
                // trivially there are no more outbound parcels to send outward,
                // and there no longer exists an ultimate destination for any
                // forwarded inbound parcels. So we drop both links now.
                dead_outward_link = state.outward_edge.release_primary_link();
            } else if !state.inbound_parcels.expects_more_elements() {
                // If the other end of the route is gone and we've received all
                // its parcels, we can simply drop the outward link in that case.
                dead_outward_link = state.outward_edge.release_primary_link();
            }

            if state.inbound_parcels.is_sequence_fully_consumed() {
                // We won't be receiving anything new from our peer, and if we're
                // a proxy then we've also forwarded everything already. We can
                // propagate closure inward and drop the inward link, if
                // applicable.
                final_inward_sequence_length = state.inbound_parcels.final_sequence_length();
                if let Some(inward_edge) = state.inward_edge.as_mut() {
                    dead_inward_link = inward_edge.release_primary_link();
                } else {
                    dead_bridge_link = bridge_link.take();
                    state.bridge = None;
                }
            }
        }

        for ParcelToFlush { link, parcel } in parcels_to_flush {
            link.accept_parcel(parcel);
        }

        if outward_link_decayed {
            decaying_outward_link.as_ref().expect("decayed link").deactivate();
        }

        if inward_link_decayed {
            decaying_inward_link.as_ref().expect("decayed link").deactivate();
        }

        // If we have an outward link, and we have no decaying outward link (or
        // our decaying outward link has just finished decaying above), we
        // consider the outward link to be stable.
        let has_stable_outward_link = outward_link.is_some() && (decaying_outward_link.is_none() || outward_link_decayed);

        // If we have no primary inward link, and we have no decaying inward link
        // (or our decaying inward link has just finished decaying above), this
        // router has no inward-facing links.
        let has_no_inward_links = inward_link.is_none() && (decaying_inward_link.is_none() || inward_link_decayed);

        // Bridge bypass is only possible with no inward links and a stable
        // outward link.
        if bridge_link.is_some() && has_stable_outward_link && has_no_inward_links {
            self.maybe_start_bridge_bypass();
        }

        let had_dead_outward_link = dead_outward_link.is_some();
        if let Some(link) = dead_outward_link {
            if let Some(length) = final_outward_sequence_length {
                link.accept_route_closure(length);
            }
            link.deactivate();
        }

        if let Some(link) = dead_inward_link {
            if let Some(length) = final_inward_sequence_length {
                link.accept_route_closure(length);
            }
            link.deactivate();
        }

        if let Some(link) = dead_bridge_link
            && let Some(length) = final_inward_sequence_length {
                link.accept_route_closure(length);
            }

        if had_dead_outward_link || !on_central_link {
            // If we're not on a central link, there's no more work to do.
            return;
        }

        if !dropped_last_decaying_link && behavior != FlushBehavior::ForceProxyBypassAttempt {
            // No relevant state changes, so there are no new bypass
            // opportunities.
            return;
        }

        if inward_link.is_some() && self.maybe_start_self_bypass() {
            return;
        }

        if let Some(link) = outward_link {
            link.flush_other_side_if_waiting();
        }
    }

    // Attempts to initiate bypass of this router by its peers, and ultimately to
    // remove this router from its route.
    //
    // Called during a `flush()` if this is a proxying router which just dropped
    // its last decaying link, or if `flush()` was called with
    // `ForceProxyBypassAttempt`, indicating that some significant state has
    // changed on the route which might unblock our bypass.
    fn maybe_start_self_bypass(self: &Arc<Self>) -> bool {
        let remote_inward_link: Arc<RemoteRouterLink>;
        let mut remote_outward_link: Option<Arc<RemoteRouterLink>> = None;
        let local_outward_peer: Option<Arc<Router>>;
        {
            let state = self.lock();
            let Some(inward_edge) = state.inward_edge.as_ref() else {
                return false;
            };
            let Some(inward_primary) = inward_edge.primary_link().filter(|_| inward_edge.is_stable()) else {
                // Only a proxy with stable links can be bypassed.
                return false;
            };

            let outward_link = state.outward_edge.primary_link();
            let (Some(outward_link), Some(inward_link)) =
                (outward_link, inward_primary.as_remote_router_link().map(|_| inward_primary.clone()))
            else {
                return false;
            };

            let inward_remote = inward_link.into_remote_router_link().expect("remote inward link");
            let inward_peer_name = *inward_remote.node_link().remote_node_name();
            if !outward_link.try_lock_for_bypass(inward_peer_name) {
                log::trace!("Proxy bypass blocked by busy {}", outward_link.describe());
                return false;
            }

            remote_inward_link = inward_remote;
            local_outward_peer = outward_link.get_local_peer();
            if local_outward_peer.is_none() {
                remote_outward_link = outward_link.clone().into_remote_router_link();
            }
        }

        if let Some(remote_outward_link) = remote_outward_link {
            // The simpler case here: our outward peer is on another node, so we
            // begin decaying our inward and outward links and ask the inward peer
            // to bypass us ASAP.
            {
                let mut state = self.lock();
                let state = &mut *state;
                match (state.inward_edge.as_mut(), state.outward_edge.primary_link()) {
                    (Some(inward_edge), Some(_)) if inward_edge.primary_link().is_some() => {
                        state.outward_edge.begin_primary_link_decay();
                        inward_edge.begin_primary_link_decay();
                    }
                    // We've been disconnected since leaving the block above.
                    // Nothing to do.
                    _ => return false,
                }
            }

            log::trace!(
                "Proxy sending bypass request to inward peer over {} targeting outward peer on other side of {}",
                remote_inward_link.describe(),
                remote_outward_link.describe()
            );

            remote_inward_link.bypass_peer(remote_outward_link.node_link().remote_node_name(), remote_outward_link.sublink());
            return true;
        }

        // When the bypass target is local to the same node as this router, we can
        // establish the bypass link immediately and send it to the remote inward
        // peer.
        let new_link_state = remote_inward_link.node_link().memory().try_allocate_router_link_state();
        self.start_self_bypass_to_local_peer(
            local_outward_peer.expect("local outward peer"),
            remote_inward_link,
            new_link_state,
        )
    }

    // Starts bypass of this Router when its outward peer lives on the same node.
    // This must only be called once the central link is already locked. If
    // `new_link_state` is non-null, it will be used for the RouterLinkState of
    // the new RemoteRouterLink between this Routers inward and outward peers.
    // Otherwise one will be allocated asynchronously before proceeding.
    //
    // Returns true if and only if self-bypass has been initiated by reaching out
    // to this router's inward peer with with a BypassPeer() or
    // BypassPeerWithLink() request. Otherwise returns false.
    fn start_self_bypass_to_local_peer(
        self: &Arc<Self>,
        local_outward_peer: Arc<Router>,
        inward_link: Arc<RemoteRouterLink>,
        new_link_state: FragmentRef<RouterLinkState>,
    ) -> bool {
        if new_link_state.is_null() {
            let memory = inward_link.node_link().memory().clone();
            let router = self.clone();
            memory.allocate_router_link_state(Box::new(move |new_link_state| {
                if new_link_state.is_null() {
                    // If this fails once, it's unlikely to succeed afterwards.
                    return;
                }

                router.start_self_bypass_to_local_peer(local_outward_peer, inward_link, new_link_state);
            }));
            return true;
        }

        let new_link: Option<Arc<RemoteRouterLink>>;
        let length_from_outward_peer;
        let new_sublink = inward_link.node_link().memory().allocate_sublink_ids(1);
        {
            let (mut this, mut peer) = lock2(&self.state, &local_outward_peer.state);
            let (this, peer) = (&mut *this, &mut *peer);

            if this.outward_edge.primary_link().is_none()
                || peer.outward_edge.primary_link().is_none()
                || this.is_disconnected
                || peer.is_disconnected
            {
                log::trace!("Proxy bypass blocked due to peer closure or disconnection");
                return false;
            }

            log::trace!(
                "Proxy requesting own bypass from inward peer on {} to local outward peer",
                inward_link.node_link().remote_node_name()
            );

            debug_assert!(this
                .outward_edge
                .primary_link()
                .and_then(|link| link.get_local_peer())
                .is_some_and(|r| Arc::ptr_eq(&r, &local_outward_peer)));
            debug_assert!(peer
                .outward_edge
                .primary_link()
                .and_then(|link| link.get_local_peer())
                .is_some_and(|r| Arc::ptr_eq(&r, self)));

            // Decay both of our existing links, as well as the local peer's link
            // to us.
            length_from_outward_peer = peer.outbound_parcels.current_sequence_number();
            peer.outward_edge.begin_primary_link_decay();
            peer.outward_edge.set_length_to_decaying_link(length_from_outward_peer);
            this.outward_edge.begin_primary_link_decay();
            this.outward_edge.set_length_from_decaying_link(length_from_outward_peer);
            let inward_edge = this.inward_edge.as_mut().expect("proxy has an inward edge");
            inward_edge.begin_primary_link_decay();
            inward_edge.set_length_to_decaying_link(length_from_outward_peer);

            new_link = inward_link.node_link().add_remote_router_link(
                new_sublink,
                new_link_state.clone(),
                LinkType::Central,
                LinkSide::A,
                local_outward_peer.clone(),
            );
        }

        let Some(new_link) = new_link else {
            self.accept_route_disconnected_from(LinkType::Central);
            return false;
        };

        // Inform our inward peer on another node that they can bypass us using
        // the new link we just created to our own outward local peer. Once that
        // message is sent, it's safe for that local peer to adopt the new link.
        inward_link.bypass_peer_with_link(new_sublink, new_link_state, length_from_outward_peer);
        local_outward_peer.set_outward_link(new_link);
        true
    }

    // Attempts to start bypass of this Router, which must be on a bridge link, as
    // well bypassing the bridge link itself and the bridge peer router on its
    // other side. This method will attempt to lock this Router's outward link as
    // well as the outward link of this Router's bridge peer. If either fails,
    // both are left unlocked and this operation cannot yet proceed.
    fn maybe_start_bridge_bypass(self: &Arc<Self>) {
        let first_bridge = self.clone();
        let second_bridge: Arc<Router>;
        {
            let state = self.lock();
            let Some(bridge) = state.bridge.as_ref().filter(|bridge| bridge.is_stable()) else {
                return;
            };

            let Some(peer) = bridge.get_local_peer() else {
                return;
            };
            second_bridge = peer;
        }

        let first_local_peer: Option<Arc<Router>>;
        let second_local_peer: Option<Arc<Router>>;
        let first_remote_link: Option<Arc<RemoteRouterLink>>;
        let second_remote_link: Option<Arc<RemoteRouterLink>>;
        {
            let (this, second) = lock2(&self.state, &second_bridge.state);
            let (Some(link_to_first_peer), Some(link_to_second_peer)) =
                (this.outward_edge.primary_link(), second.outward_edge.primary_link())
            else {
                return;
            };

            let mut first_peer_node_name = NodeName::default();
            first_local_peer = link_to_first_peer.get_local_peer();
            first_remote_link = link_to_first_peer.clone().into_remote_router_link();
            if let Some(link) = &first_remote_link {
                first_peer_node_name = *link.node_link().remote_node_name();
            }

            let mut second_peer_node_name = NodeName::default();
            second_local_peer = link_to_second_peer.get_local_peer();
            second_remote_link = link_to_second_peer.clone().into_remote_router_link();
            if let Some(link) = &second_remote_link {
                second_peer_node_name = *link.node_link().remote_node_name();
            }

            if !link_to_first_peer.try_lock_for_bypass(second_peer_node_name) {
                return;
            }
            if !link_to_second_peer.try_lock_for_bypass(first_peer_node_name) {
                // Cancel the decay on this bridge's side, because we couldn't
                // decay the other side of the bridge yet.
                link_to_first_peer.unlock();
                return;
            }
        }

        // At this point, the outward links from each bridge router have been
        // locked for bypass. There are now three distinct cases to handle, based
        // around where the outward peer routers are located.

        // Case 1: Neither bridge router's outward peer is local to this node.
        // This is roughly equivalent to the normal proxy bypass case where the
        // proxy belongs to a different node from its inward and outward peers. We
        // send a message to our outward peer with sufficient information for it
        // to bypass both bridge routers with a new central link directly to the
        // other bridge router's outward peer.
        if first_local_peer.is_none() && second_local_peer.is_none() {
            {
                let (mut this, mut second) = lock2(&self.state, &second_bridge.state);
                let (this, second) = (&mut *this, &mut *second);
                let (Some(bridge), Some(second_bridge_edge)) = (this.bridge.as_mut(), second.bridge.as_mut()) else {
                    // If another thread raced to sever this link, we can give up
                    // immediately.
                    return;
                };
                this.outward_edge.begin_primary_link_decay();
                second.outward_edge.begin_primary_link_decay();
                bridge.begin_primary_link_decay();
                second_bridge_edge.begin_primary_link_decay();
            }
            let first_remote_link = first_remote_link.expect("remote link");
            second_remote_link
                .expect("remote link")
                .bypass_peer(first_remote_link.node_link().remote_node_name(), first_remote_link.sublink());
            return;
        }

        // Case 2: Only one of the bridge routers has a local outward peer. This
        // is roughly equivalent to the normal proxy bypass case where the proxy
        // and its outward peer belong to the same node. This case is handled
        // separately since it's a bit more complex than the cases above and
        // below.
        let (first_local_peer, second_local_peer) = match (first_local_peer, second_local_peer) {
            (Some(first), Some(second)) => (first, second),
            (Some(_), None) => {
                let remote = second_remote_link.expect("remote link");
                let link_state = remote.node_link().memory().try_allocate_router_link_state();
                self.start_bridge_bypass_from_local_peer(link_state);
                return;
            }
            (None, _) => {
                let remote = first_remote_link.expect("remote link");
                let link_state = remote.node_link().memory().try_allocate_router_link_state();
                second_bridge.start_bridge_bypass_from_local_peer(link_state);
                return;
            }
        };

        // Case 3: Both bridge routers' outward peers are local to this node. This
        // is a unique bypass case, as it's the only scenario where all involved
        // routers are local to the same node and bypass can be orchestrated
        // synchronously in a single step.
        {
            let (mut this, mut second, mut first_peer, mut second_peer) = lock4(
                &self.state,
                &second_bridge.state,
                &first_local_peer.state,
                &second_local_peer.state,
            );
            let (this, second, first_peer, second_peer) =
                (&mut *this, &mut *second, &mut *first_peer, &mut *second_peer);
            let (Some(bridge), Some(peer_bridge)) = (this.bridge.as_mut(), second.bridge.as_mut()) else {
                // If another thread raced to sever this link, we can give up
                // immediately.
                return;
            };

            let length_from_first_peer = first_peer.outbound_parcels.current_sequence_number();
            let length_from_second_peer = second_peer.outbound_parcels.current_sequence_number();

            let first_peer_edge = &mut first_peer.outward_edge;
            first_peer_edge.begin_primary_link_decay();
            first_peer_edge.set_length_to_decaying_link(length_from_first_peer);
            first_peer_edge.set_length_from_decaying_link(length_from_second_peer);

            let second_peer_edge = &mut second_peer.outward_edge;
            second_peer_edge.begin_primary_link_decay();
            second_peer_edge.set_length_to_decaying_link(length_from_second_peer);
            second_peer_edge.set_length_from_decaying_link(length_from_first_peer);

            this.outward_edge.begin_primary_link_decay();
            this.outward_edge.set_length_to_decaying_link(length_from_second_peer);
            this.outward_edge.set_length_from_decaying_link(length_from_first_peer);

            let peer_bridge_outward_edge = &mut second.outward_edge;
            peer_bridge_outward_edge.begin_primary_link_decay();
            peer_bridge_outward_edge.set_length_to_decaying_link(length_from_first_peer);
            peer_bridge_outward_edge.set_length_from_decaying_link(length_from_second_peer);

            bridge.begin_primary_link_decay();
            bridge.set_length_to_decaying_link(length_from_first_peer);
            bridge.set_length_from_decaying_link(length_from_second_peer);

            peer_bridge.begin_primary_link_decay();
            peer_bridge.set_length_to_decaying_link(length_from_second_peer);
            peer_bridge.set_length_from_decaying_link(length_from_first_peer);

            let links = LocalRouterLink::create_pair(
                LinkType::Central,
                (first_local_peer.clone(), second_local_peer.clone()),
                InitialState::Unstable,
            );
            first_peer.outward_edge.set_primary_link(links.0);
            second_peer.outward_edge.set_primary_link(links.1);
        }

        first_bridge.flush(FlushBehavior::Default);
        second_bridge.flush(FlushBehavior::Default);
        first_local_peer.flush(FlushBehavior::Default);
        second_local_peer.flush(FlushBehavior::Default);
    }

    // Starts bypass of this Router, which must be on a bridge link and must have
    // a local outward peer link. The router on the other side of the bridge must
    // have a remote outward peer, and `link_state` if non-null will be used to
    // establish a new remote link to that peer to bypass the entire bridge. If
    // `link_state` is null, the operation will be deferred until a fragment can
    // be allocated.
    fn start_bridge_bypass_from_local_peer(self: &Arc<Self>, link_state: FragmentRef<RouterLinkState>) {
        let local_peer: Arc<Router>;
        let other_bridge: Arc<Router>;
        {
            let state = self.lock();
            let Some(bridge) = state.bridge.as_ref().filter(|bridge| bridge.is_stable()) else {
                return;
            };

            let (Some(peer), Some(other)) = (state.outward_edge.get_local_peer(), bridge.get_local_peer()) else {
                return;
            };
            local_peer = peer;
            other_bridge = other;
        }

        let remote_link: Arc<RemoteRouterLink>;
        {
            let other_state = other_bridge.lock();
            let Some(link) = other_state.outward_edge.primary_link() else {
                return;
            };

            let Some(remote) = link.clone().into_remote_router_link() else {
                return;
            };
            remote_link = remote;
        }

        if link_state.is_null() {
            // We need a new RouterLinkState on the remote link before we can
            // complete this operation.
            let router = self.clone();
            remote_link
                .node_link()
                .memory()
                .clone()
                .allocate_router_link_state(Box::new(move |state| {
                    if !state.is_null() {
                        router.start_bridge_bypass_from_local_peer(state);
                    }
                }));
            return;
        }

        // At this point, we have a new RouterLinkState for a new link, we have
        // references to all three local routers (this bridge router, its local
        // peer, and the other bridge router), and we have a remote link to the
        // other bridge router's outward peer. This is sufficient to initiate
        // bypass.

        let node_link_to_peer = remote_link.node_link().clone();
        let length_from_local_peer;
        let bypass_sublink = node_link_to_peer.memory().allocate_sublink_ids(1);
        let new_link = node_link_to_peer.add_remote_router_link(
            bypass_sublink,
            link_state.clone(),
            LinkType::Central,
            LinkSide::A,
            local_peer.clone(),
        );
        {
            let (mut this, mut other, mut local) = lock3(&self.state, &other_bridge.state, &local_peer.state);
            let (this, other, local) = (&mut *this, &mut *other, &mut *local);
            let (Some(bridge), Some(other_bridge_edge)) = (this.bridge.as_mut(), other.bridge.as_mut()) else {
                // If another thread raced to sever this link, we can give up
                // immediately.
                return;
            };

            length_from_local_peer = local.outbound_parcels.current_sequence_number();

            let edge_from_local_peer = &mut local.outward_edge;
            edge_from_local_peer.begin_primary_link_decay();
            edge_from_local_peer.set_length_to_decaying_link(length_from_local_peer);

            let edge_to_other_peer = &mut other.outward_edge;
            edge_to_other_peer.begin_primary_link_decay();
            edge_to_other_peer.set_length_to_decaying_link(length_from_local_peer);

            bridge.begin_primary_link_decay();
            bridge.set_length_to_decaying_link(length_from_local_peer);

            this.outward_edge.begin_primary_link_decay();
            this.outward_edge.set_length_from_decaying_link(length_from_local_peer);

            other_bridge_edge.begin_primary_link_decay();
            other_bridge_edge.set_length_from_decaying_link(length_from_local_peer);
        }

        remote_link.bypass_peer_with_link(bypass_sublink, link_state, length_from_local_peer);
        if let Some(new_link) = new_link {
            local_peer.set_outward_link(new_link);
        }
        self.flush(FlushBehavior::Default);
        other_bridge.flush(FlushBehavior::Default);
        local_peer.flush(FlushBehavior::Default);
    }

    // Attempts to bypass the link identified by `requestor` in favor of a new
    // link that runs over `node_link`. If `new_link_state` is non-null, it will
    // be used for the RouterLinkState of the new RemoteRouterLink; otherwise one
    // will be allocated asynchronously before proceeding.
    //
    // Returns true if and only if this request was valid.
    fn bypass_peer_with_new_remote_link(
        self: &Arc<Self>,
        requestor: &Arc<RemoteRouterLink>,
        node_link: &Arc<NodeLink>,
        bypass_target_sublink: SublinkId,
        new_link_state: FragmentRef<RouterLinkState>,
    ) -> bool {
        if new_link_state.is_null() {
            // We can't proceed with bypass until we have a fragment allocated
            // for a new RouterLinkState.
            let router = self.clone();
            let requestor = requestor.clone();
            let node_link_for_callback = node_link.clone();
            node_link.memory().clone().allocate_router_link_state(Box::new(move |new_link_state| {
                if new_link_state.is_null() {
                    // If this fails once, it's unlikely to succeed afterwards.
                    return;
                }

                router.bypass_peer_with_new_remote_link(
                    &requestor,
                    &node_link_for_callback,
                    bypass_target_sublink,
                    new_link_state,
                );
            }));
            return true;
        }

        // Begin decaying our outward link.
        let length_to_decaying_link;
        let new_link: Option<Arc<RemoteRouterLink>>;
        let new_sublink = node_link.memory().allocate_sublink_ids(1);
        {
            let mut state = self.lock();
            if state.outward_edge.primary_link().is_none() || state.is_disconnected {
                // We've been disconnected since leaving the above block. Don't
                // bother to request a bypass. This is not the requestor's fault,
                // so it's not treated as an error.
                return true;
            }

            if !state.outward_edge.begin_primary_link_decay() {
                log::error!("Rejecting BypassPeer on failure to decay link");
                return false;
            }

            length_to_decaying_link = state.outbound_parcels.current_sequence_number();
            state.outward_edge.set_length_to_decaying_link(length_to_decaying_link);
            new_link = node_link.add_remote_router_link(
                new_sublink,
                new_link_state.clone(),
                LinkType::Central,
                LinkSide::A,
                self.clone(),
            );
        }

        let Some(new_link) = new_link else {
            // The NodeLink was disconnected before we could create a new link
            // for this Router. This is not the requestor's fault, so it's not
            // treated as an error.
            self.accept_route_disconnected_from(LinkType::Central);
            return true;
        };

        let proxy_node_name = *requestor.node_link().remote_node_name();
        log::trace!(
            "Sending AcceptBypassLink from {} to {} with new sublink {} to replace a link to proxy {} via sublink {}",
            node_link.local_node_name(),
            node_link.remote_node_name(),
            new_sublink,
            proxy_node_name,
            bypass_target_sublink
        );

        node_link.accept_bypass_link(
            &proxy_node_name,
            bypass_target_sublink,
            length_to_decaying_link,
            new_sublink,
            new_link_state,
        );

        // NOTE: This link is intentionally set *after* transmitting the above
        // message. Otherwise the router might race on another thread to send
        // messages via `new_sublink`, and the remote node would have no idea
        // where to route them.
        self.set_outward_link(new_link);
        true
    }

    // Attempts to bypass the link identified by `requestor` in favor of a new
    // LocalRouterLink to a Router bound to `bypass_target_sublink` on the same
    // NodeLink as `requestor`.
    //
    // Returns true if and only if this request was valid.
    fn bypass_peer_with_new_local_link(
        self: &Arc<Self>,
        requestor: &Arc<RemoteRouterLink>,
        bypass_target_sublink: SublinkId,
    ) -> bool {
        let from_node_link = requestor.node_link().clone();
        let Some(new_local_peer) = from_node_link.get_router(bypass_target_sublink) else {
            // The peer may have already been destroyed or disconnected from the
            // proxy by the time we get here.
            self.accept_route_disconnected_from(LinkType::PeripheralOutward);
            return true;
        };

        let link_from_new_local_peer_to_proxy: Arc<dyn RouterLink>;
        let length_to_proxy_from_us;
        let length_from_proxy_to_us;
        {
            let (mut this, mut peer) = lock2(&self.state, &new_local_peer.state);
            let (this, peer) = (&mut *this, &mut *peer);
            length_from_proxy_to_us = peer.outbound_parcels.current_sequence_number();
            length_to_proxy_from_us = this.outbound_parcels.current_sequence_number();

            log::trace!(
                "Proxy bypass requested with new local peer on {} and proxy on {} via sublinks {} and {}; length to the proxy is {} and length from the proxy {}",
                from_node_link.local_node_name(),
                from_node_link.remote_node_name(),
                bypass_target_sublink,
                requestor.sublink(),
                length_to_proxy_from_us,
                length_from_proxy_to_us
            );

            let (Some(_), Some(peer_link)) = (this.outward_edge.primary_link(), peer.outward_edge.primary_link())
            else {
                return true;
            };
            if this.is_disconnected || peer.is_disconnected {
                return true;
            }
            link_from_new_local_peer_to_proxy = peer_link.clone();

            // Otherwise immediately begin decay of both links to the proxy.
            if !this.outward_edge.begin_primary_link_decay() || !peer.outward_edge.begin_primary_link_decay() {
                log::error!("Rejecting BypassPeer on failure to decay link");
                return false;
            }
            this.outward_edge.set_length_to_decaying_link(length_to_proxy_from_us);
            this.outward_edge.set_length_from_decaying_link(length_from_proxy_to_us);
            peer.outward_edge.set_length_to_decaying_link(length_from_proxy_to_us);
            peer.outward_edge.set_length_from_decaying_link(length_to_proxy_from_us);

            // Finally, link the two routers with a new LocalRouterLink. This link
            // will remain unstable until the decaying proxy links are gone.
            let links = LocalRouterLink::create_pair(
                LinkType::Central,
                (self.clone(), new_local_peer.clone()),
                InitialState::Unstable,
            );
            this.outward_edge.set_primary_link(links.0);
            peer.outward_edge.set_primary_link(links.1);
        }

        link_from_new_local_peer_to_proxy.stop_proxying(length_from_proxy_to_us, length_to_proxy_from_us);

        self.flush(FlushBehavior::Default);
        new_local_peer.flush(FlushBehavior::Default);
        true
    }
}

impl ApiObject for Router {
    fn object_type(&self) -> ApiObjectType {
        ApiObjectType::Portal
    }

    fn close(self: Arc<Self>) -> IpczResult {
        self.close_route();
        IpczResult::Ok
    }

    fn can_send_from(&self, sender: &Router) -> bool {
        !std::ptr::eq(sender, self) && !sender.has_local_peer(self)
    }
}
