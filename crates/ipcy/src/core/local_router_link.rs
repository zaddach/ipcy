//! Port of `ipcz/local_router_link.{h,cc}`.

use std::sync::{Arc, Mutex, MutexGuard};

use super::fragment_ref::FragmentRef;
use super::link_side::LinkSide;
use super::link_type::LinkType;
use super::node_name::NodeName;
use super::parcel::Parcel;
use super::remote_router_link::RemoteRouterLink;
use super::router::{FlushBehavior, Router};
use super::router_link::RouterLink;
use super::router_link_state::RouterLinkState;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;

/// Determines whether a new local link starts in a stable state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitialState {
    Unstable,
    Stable,
}

#[derive(Default)]
struct Routers {
    router_a: Option<Arc<Router>>,
    router_b: Option<Arc<Router>>,
}

// This object is shared between the two Routers on either end of a
// LocalRouterLink. The Routers access each other through references owned by
// this object.
struct SharedState {
    link_type: LinkType,
    link_state: RouterLinkState,
    routers: Mutex<Routers>,
}

impl SharedState {
    fn new(link_type: LinkType, initial_state: InitialState, router_a: Arc<Router>, router_b: Arc<Router>) -> Self {
        let link_state = RouterLinkState::new();
        if initial_state == InitialState::Stable {
            link_state
                .status
                .store(RouterLinkState::STABLE, std::sync::atomic::Ordering::Relaxed);
        }
        Self {
            link_type,
            link_state,
            routers: Mutex::new(Routers {
                router_a: Some(router_a),
                router_b: Some(router_b),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Routers> {
        self.routers.lock().unwrap_or_else(|e| e.into_inner())
    }

    // Returns the Router on the given `side` of this link. Note that this may
    // return `None` if the Router in question has been deactivated, for example
    // due to the application closing the Router's controlling portal.
    fn get_router(&self, side: LinkSide) -> Option<Arc<Router>> {
        let routers = self.lock();
        match side {
            LinkSide::A => routers.router_a.clone(),
            LinkSide::B => routers.router_b.clone(),
        }
    }

    fn deactivate(&self, side: LinkSide) {
        // Drop the router outside of the lock: its destructor may run arbitrary
        // code.
        let _router = {
            let mut routers = self.lock();
            match side {
                LinkSide::A => routers.router_a.take(),
                LinkSide::B => routers.router_b.take(),
            }
        };
    }
}

/// Local link between two Routers on the same node. This type is thread-safe.
///
/// NOTE: This implementation must take caution when calling into any Router. See
/// the note on [`RouterLink`].
pub struct LocalRouterLink {
    side: LinkSide,
    state: Arc<SharedState>,
}

impl LocalRouterLink {
    /// Creates a new pair of LocalRouterLinks linking the given pair of Routers
    /// together. `link_type` must be either Central or Bridge, as local links
    /// may never be peripheral. `initial_state` determines whether the new link
    /// starts in a stable state.
    ///
    /// It is the caller's responsibility to give the returned links to their
    /// respective Routers.
    pub fn create_pair(
        link_type: LinkType,
        routers: (Arc<Router>, Arc<Router>),
        initial_state: InitialState,
    ) -> (Arc<dyn RouterLink>, Arc<dyn RouterLink>) {
        assert!(link_type == LinkType::Central || link_type == LinkType::Bridge);
        let state = Arc::new(SharedState::new(link_type, initial_state, routers.0, routers.1));
        (
            Arc::new(LocalRouterLink {
                side: LinkSide::A,
                state: state.clone(),
            }),
            Arc::new(LocalRouterLink {
                side: LinkSide::B,
                state,
            }),
        )
    }
}

impl RouterLink for LocalRouterLink {
    fn get_type(&self) -> LinkType {
        self.state.link_type
    }

    fn get_link_state(&self) -> Option<&RouterLinkState> {
        Some(&self.state.link_state)
    }

    fn wait_for_link_state_async(&self, callback: Box<dyn FnOnce() + Send>) {
        callback();
    }

    fn get_local_peer(&self) -> Option<Arc<Router>> {
        self.state.get_router(self.side.opposite())
    }

    fn as_remote_router_link(&self) -> Option<&RemoteRouterLink> {
        None
    }

    fn into_remote_router_link(self: Arc<Self>) -> Option<Arc<RemoteRouterLink>> {
        None
    }

    fn allocate_parcel_data(&self, num_bytes: usize, allow_partial: bool, parcel: &mut Parcel) {
        parcel.allocate_data(num_bytes, allow_partial, None);
    }

    fn accept_parcel(&self, parcel: Box<Parcel>) {
        if let Some(receiver) = self.state.get_router(self.side.opposite()) {
            if self.state.link_type == LinkType::Central {
                receiver.accept_inbound_parcel(parcel);
            } else {
                assert_eq!(self.state.link_type, LinkType::Bridge);
                receiver.accept_outbound_parcel(parcel);
            }
        }
    }

    fn accept_route_closure(&self, sequence_length: SequenceNumber) {
        if let Some(receiver) = self.state.get_router(self.side.opposite()) {
            receiver.accept_route_closure_from(self.state.link_type, sequence_length);
        }
    }

    fn accept_route_disconnected(&self) {
        if let Some(receiver) = self.state.get_router(self.side.opposite()) {
            receiver.accept_route_disconnected_from(self.state.link_type);
        }
    }

    fn mark_side_stable(&self) {
        self.state.link_state.set_side_stable(self.side);
    }

    fn try_lock_for_bypass(&self, bypass_request_source: NodeName) -> bool {
        if !self.state.link_state.try_lock(self.side) {
            return false;
        }

        self.state.link_state.store_allowed_bypass_request_source(bypass_request_source);
        true
    }

    fn try_lock_for_closure(&self) -> bool {
        self.state.link_state.try_lock(self.side)
    }

    fn unlock(&self) {
        self.state.link_state.unlock(self.side);
    }

    fn flush_other_side_if_waiting(&self) -> bool {
        let other_side = self.side.opposite();
        if self.state.link_state.reset_waiting_bit(other_side) {
            if let Some(receiver) = self.state.get_router(other_side) {
                receiver.flush(FlushBehavior::ForceProxyBypassAttempt);
            }
            return true;
        }
        false
    }

    fn can_node_request_bypass(&self, bypass_request_source: &NodeName) -> bool {
        // Balanced by a release in `try_lock_for_bypass()`.
        let allowed_source = self.state.link_state.load_allowed_bypass_request_source();
        self.state.link_state.is_locked_by(self.side.opposite()) && allowed_source == *bypass_request_source
    }

    // The following are not implemented, and never called on local links.
    fn bypass_peer(&self, _: &NodeName, _: SublinkId) {
        debug_assert!(false, "bypass_peer() on a local link");
    }

    fn stop_proxying(&self, _: SequenceNumber, _: SequenceNumber) {
        debug_assert!(false, "stop_proxying() on a local link");
    }

    fn proxy_will_stop(&self, _: SequenceNumber) {
        debug_assert!(false, "proxy_will_stop() on a local link");
    }

    fn bypass_peer_with_link(&self, _: SublinkId, _: FragmentRef<RouterLinkState>, _: SequenceNumber) {
        debug_assert!(false, "bypass_peer_with_link() on a local link");
    }

    fn stop_proxying_to_local_peer(&self, _: SequenceNumber) {
        debug_assert!(false, "stop_proxying_to_local_peer() on a local link");
    }

    fn deactivate(&self) {
        self.state.deactivate(self.side);
    }

    fn describe(&self) -> String {
        let peer = self.state.get_router(self.side.opposite());
        format!(
            "{}-side link to local peer {:?} on {}-side",
            self.side,
            peer.as_ref().map(Arc::as_ptr),
            self.side.opposite()
        )
    }
}
