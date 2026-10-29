//! Port of `ipcz/router_link.h`.

use std::sync::Arc;

use super::fragment_ref::FragmentRef;
use super::node_name::NodeName;
use super::parcel::Parcel;
use super::remote_router_link::RemoteRouterLink;
use super::router::Router;
use super::router_link_state::RouterLinkState;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;
use super::link_type::LinkType;

/// A RouterLink represents one endpoint of a link between two Routers. All
/// subclasses must be thread-safe.
///
/// NOTE: Implementations of this trait must take caution when calling into a
/// Router. Routers may call into links while holding their own lock (e.g. to
/// deactivate a link), and links may call into routers (e.g. a LocalRouterLink
/// passing a parcel to its peer); so links must only call into a Router using a
/// reference held on the calling stack, and the Router must never call a link
/// method with its lock held that could re-enter it.
pub trait RouterLink: Send + Sync {
    /// Indicates what type of link this is.
    fn get_type(&self) -> LinkType;

    /// Returns a reference to the RouterLinkState for this link, if available.
    /// Only central links ever have a RouterLinkState, and even then it may take
    /// a while to resolve (see [`Self::wait_for_link_state_async`]).
    fn get_link_state(&self) -> Option<&RouterLinkState>;

    /// Runs `callback` as soon as this link has a RouterLinkState. If it
    /// already does, `callback` is run immediately.
    fn wait_for_link_state_async(&self, callback: Box<dyn FnOnce() + Send>);

    /// If this link is a LocalRouterLink, this returns the Router on the other
    /// side of the link. Otherwise this returns `None`.
    fn get_local_peer(&self) -> Option<Arc<Router>>;

    /// If this link is a RemoteRouterLink, this returns it.
    fn as_remote_router_link(&self) -> Option<&RemoteRouterLink>;

    /// Like [`Self::as_remote_router_link`], but for a shared reference.
    fn into_remote_router_link(self: Arc<Self>) -> Option<Arc<RemoteRouterLink>>;

    /// Allocates space for the data of `parcel`, possibly in shared memory
    /// belonging to this link's NodeLink.
    fn allocate_parcel_data(&self, num_bytes: usize, allow_partial: bool, parcel: &mut Parcel);

    /// Passes a parcel to the Router on the other side of this link to be queued
    /// and/or routed further.
    fn accept_parcel(&self, parcel: Box<Parcel>);

    /// Notifies the Router on the other side of this link that the route has
    /// been closed from this side, after `sequence_length` parcels.
    fn accept_route_closure(&self, sequence_length: SequenceNumber);

    /// Notifies the Router on the other side that the route has been severed by
    /// disconnection somewhere along the way.
    fn accept_route_disconnected(&self);

    /// Notifies the other side of this link that this side is stable, i.e. has
    /// no decaying links.
    fn mark_side_stable(&self);

    /// Attempts to lock the link from this side for bypass, to be requested only
    /// by `bypass_request_source` (the default name implies a local bypass).
    /// Returns true on success.
    #[must_use]
    fn try_lock_for_bypass(&self, bypass_request_source: NodeName) -> bool;

    /// Attempts to lock the link from this side for propagation of route
    /// closure. Returns true on success.
    #[must_use]
    fn try_lock_for_closure(&self) -> bool;

    /// Unlocks a link previously locked by one of the above.
    fn unlock(&self);

    /// If the other side of this link is waiting for it to become stable (and
    /// it now is), notifies it with a Flush. Returns true if a flush was sent.
    fn flush_other_side_if_waiting(&self) -> bool;

    /// Indicates whether `bypass_request_source` is allowed to request bypass of
    /// this link, which must have been locked by the other side.
    fn can_node_request_bypass(&self, bypass_request_source: &NodeName) -> bool;

    /// Only valid on remote links. Asks the inward peer to bypass this proxy's
    /// outward link in favor of a direct link to the outward peer's node.
    fn bypass_peer(&self, bypass_target_node: &NodeName, bypass_target_sublink: SublinkId);

    /// Only valid on remote links. Informs the router on the other side of a
    /// bypassed link that it can stop proxying.
    fn stop_proxying(&self, inbound_sequence_length: SequenceNumber, outbound_sequence_length: SequenceNumber);

    /// Only valid on remote links. Informs a router that its outward peer is a
    /// proxy which will stop after forwarding `inbound_sequence_length` parcels.
    fn proxy_will_stop(&self, inbound_sequence_length: SequenceNumber);

    /// Only valid on remote links. Asks the other side to bypass its link using
    /// a new link to a (local) peer of the requester.
    fn bypass_peer_with_link(
        &self,
        new_sublink: SublinkId,
        new_link_state: FragmentRef<RouterLinkState>,
        inbound_sequence_length: SequenceNumber,
    );

    /// Only valid on remote links. Informs the other side that its proxy
    /// bypass is complete in the outbound direction.
    fn stop_proxying_to_local_peer(&self, outbound_sequence_length: SequenceNumber);

    /// Permanently deactivates this link, ceasing the flow of messages through
    /// it.
    fn deactivate(&self);

    /// Produces a log-friendly description of the link.
    fn describe(&self) -> String;
}

/// Compares two links for identity.
pub fn same_link(a: &Arc<dyn RouterLink>, b: &Arc<dyn RouterLink>) -> bool {
    Arc::ptr_eq(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::local_router_link::{InitialState, LocalRouterLink};
    use crate::core::router_link_state::RouterLinkState;
    use crate::core::testing::test_node_pair::*;
    use std::sync::atomic::Ordering;

    #[derive(Clone, Copy, Debug)]
    enum Mode {
        Local,
        Remote,
    }

    struct RouterLinkTest {
        // Declared first so the routers are closed before the nodes go away.
        a: Arc<Router>,
        b: Arc<Router>,
        a_link: Arc<dyn RouterLink>,
        b_link: Arc<dyn RouterLink>,
        link_state: *const RouterLinkState,
        _nodes: TestNodePair,
    }

    impl RouterLinkTest {
        fn new(mode: Mode) -> Self {
            let nodes = TestNodePair::new();
            let (a, b) = (Router::new(), Router::new());
            let (a_link, b_link): (Arc<dyn RouterLink>, Arc<dyn RouterLink>) = match mode {
                Mode::Local => {
                    let (a_link, b_link) =
                        LocalRouterLink::create_pair(LinkType::Central, (a.clone(), b.clone()), InitialState::Unstable);
                    a.set_outward_link(a_link.clone());
                    b.set_outward_link(b_link.clone());
                    (a_link, b_link)
                }
                Mode::Remote => {
                    let link_state = nodes.memory_a().try_allocate_router_link_state();
                    assert!(link_state.is_addressable());
                    let (a_link, b_link) = nodes.link_remote_routers(&a, link_state.clone(), &b, link_state);
                    (a_link, b_link)
                }
            };

            let link_state: *const RouterLinkState = a_link.get_link_state().expect("link state");
            assert!(std::ptr::eq(link_state, b_link.get_link_state().expect("link state")));
            nodes.activate_transports();
            Self {
                a,
                b,
                a_link,
                b_link,
                link_state,
                _nodes: nodes,
            }
        }

        fn link_state(&self) -> &RouterLinkState {
            // SAFETY: the links keep the link state alive.
            unsafe { &*self.link_state }
        }

        fn status(&self) -> u32 {
            self.link_state().status.load(Ordering::SeqCst)
        }
    }

    impl Drop for RouterLinkTest {
        fn drop(&mut self) {
            self.a.close_route();
            self.b.close_route();
        }
    }

    fn for_each_mode(test: impl Fn(Mode)) {
        [Mode::Local, Mode::Remote].into_iter().for_each(test);
    }

    #[test]
    fn locking() {
        for_each_mode(|mode| {
            let t = RouterLinkTest::new(mode);
            t.link_state().status.store(RouterLinkState::UNSTABLE, Ordering::SeqCst);

            // No locking can take place until both sides are marked stable.
            assert!(!t.a_link.try_lock_for_bypass(TEST_PEER1_NAME));
            assert!(!t.a_link.try_lock_for_closure());
            t.a_link.mark_side_stable();
            t.b_link.mark_side_stable();
            assert_eq!(RouterLinkState::STABLE, t.status());

            // Only one side can lock for bypass, and (only) the other side can
            // use this to validate the source of a future bypass request.
            assert!(!t.b_link.can_node_request_bypass(&TEST_PEER1_NAME));
            assert!(t.a_link.try_lock_for_bypass(TEST_PEER1_NAME));
            assert!(!t.b_link.try_lock_for_bypass(TEST_PEER2_NAME));
            assert!(!t.b_link.can_node_request_bypass(&TEST_PEER2_NAME));
            assert!(t.b_link.can_node_request_bypass(&TEST_PEER1_NAME));
            assert!(!t.a_link.can_node_request_bypass(&TEST_PEER1_NAME));
            assert_eq!(RouterLinkState::STABLE | RouterLinkState::LOCKED_BY_SIDE_A, t.status());
            t.a_link.unlock();
            assert_eq!(RouterLinkState::STABLE, t.status());

            assert!(t.b_link.try_lock_for_bypass(TEST_PEER2_NAME));
            assert!(!t.a_link.try_lock_for_bypass(TEST_PEER1_NAME));
            assert!(!t.a_link.can_node_request_bypass(&TEST_PEER1_NAME));
            assert!(!t.b_link.can_node_request_bypass(&TEST_PEER2_NAME));
            assert!(t.a_link.can_node_request_bypass(&TEST_PEER2_NAME));
            assert_eq!(RouterLinkState::STABLE | RouterLinkState::LOCKED_BY_SIDE_B, t.status());
            t.b_link.unlock();
            assert_eq!(RouterLinkState::STABLE, t.status());

            assert!(t.a_link.try_lock_for_closure());
            assert!(!t.b_link.try_lock_for_closure());
            assert_eq!(RouterLinkState::STABLE | RouterLinkState::LOCKED_BY_SIDE_A, t.status());
            t.a_link.unlock();
            assert_eq!(RouterLinkState::STABLE, t.status());

            assert!(t.b_link.try_lock_for_closure());
            assert!(!t.a_link.try_lock_for_closure());
            assert_eq!(RouterLinkState::STABLE | RouterLinkState::LOCKED_BY_SIDE_B, t.status());
            t.b_link.unlock();
            assert_eq!(RouterLinkState::STABLE, t.status());
        });
    }

    #[test]
    fn flush_other_side_if_waiting() {
        for_each_mode(|mode| {
            let t = RouterLinkTest::new(mode);
            t.link_state().status.store(RouterLinkState::UNSTABLE, Ordering::SeqCst);

            // `flush_other_side_if_waiting()` does nothing if the other side is
            // not, in fact, waiting for something.
            assert!(!t.a_link.flush_other_side_if_waiting());
            assert!(!t.b_link.flush_other_side_if_waiting());
            assert_eq!(RouterLinkState::UNSTABLE, t.status());

            // Mark B stable and try to lock the link. Since A is not yet stable,
            // this should fail and set B's waiting bit.
            t.b_link.mark_side_stable();
            assert!(!t.b_link.try_lock_for_bypass(TEST_PEER1_NAME));
            assert_eq!(RouterLinkState::SIDE_B_STABLE | RouterLinkState::SIDE_B_WAITING, t.status());

            // Now mark A stable. The `flush_other_side_if_waiting()` should
            // successfully flush B and clear its waiting bit.
            t.a_link.mark_side_stable();
            assert_eq!(RouterLinkState::STABLE | RouterLinkState::SIDE_B_WAITING, t.status());
            assert!(t.a_link.flush_other_side_if_waiting());
            assert_eq!(RouterLinkState::STABLE, t.status());
        });
    }

    fn create_test_router_pairs(n: usize) -> Vec<(Arc<Router>, Arc<Router>)> {
        (0..n).map(|_| (Router::new(), Router::new())).collect()
    }

    fn close_routes(routers: &[(Arc<Router>, Arc<Router>)]) {
        routers.iter().for_each(|(a, b)| {
            a.close_route();
            b.close_route();
        });
    }

    #[test]
    fn new_link_with_addressable_state() {
        let nodes = TestNodePair::new();
        nodes.activate_transports();

        let fragments = nodes.allocate_all_router_link_states();
        let router_pairs = create_test_router_pairs(fragments.len());
        let links: Vec<_> = router_pairs
            .iter()
            .zip(&fragments)
            .map(|((a, b), fragment)| {
                let (a_link, b_link) = nodes.link_remote_routers(a, fragment.clone(), b, fragment.clone());
                a_link.mark_side_stable();
                b_link.mark_side_stable();
                (a_link, b_link)
            })
            .collect();

        // We should be able to lock all links from either side, implying that
        // both sides have a valid reference to the same RouterLinkState.
        for (a_link, b_link) in &links {
            assert!(a_link.try_lock_for_closure());
            a_link.unlock();
            assert!(b_link.try_lock_for_closure());
        }

        close_routes(&router_pairs);
    }

    #[test]
    fn new_link_with_pending_state() {
        let nodes = TestNodePair::new();

        // Occupy all fragments in the primary buffer so they aren't usable.
        let _unused_fragments = nodes.allocate_all_router_link_states();

        // Now allocate another batch of fragments which must be in a newly
        // allocated buffer on node A. Because the nodes' transports are not
        // active yet, there is no way for node B to have had this buffer shared
        // with it yet. Hence all of these fragments will be seen as pending on
        // node B.
        let fragments = nodes.allocate_all_router_link_states();

        let router_pairs = create_test_router_pairs(fragments.len());
        let links: Vec<_> = router_pairs
            .iter()
            .zip(&fragments)
            .map(|((a, b), fragment)| {
                let a_state = fragment.clone();
                let b_fragment = nodes.memory_b().get_fragment(fragment.fragment().descriptor());
                let b_state = nodes.memory_b().adopt_fragment_ref::<RouterLinkState>(b_fragment);
                assert!(a_state.is_addressable());
                assert!(b_state.is_pending());
                let (a_link, b_link) = nodes.link_remote_routers(a, a_state, b, b_state);
                a_link.mark_side_stable();
                b_link.mark_side_stable();
                (a_link, b_link)
            })
            .collect();

        // Because side B of these links still cannot resolve its
        // RouterLinkState, the link still cannot be stabilized or locked yet.
        for (a_link, b_link) in &links {
            assert!(!a_link.try_lock_for_closure());
            assert!(!b_link.try_lock_for_closure());
        }

        // We're using the synchronous driver, so as soon as we activate our
        // transports, all pending NodeLink communications will complete before
        // this call returns. This also means side B of each link will resolve
        // its RouterLinkState.
        nodes.activate_transports();

        // Now all links should be lockable from either side, implying that both
        // sides have a valid reference to the same RouterLinkState.
        for (a_link, b_link) in &links {
            assert!(a_link.try_lock_for_closure());
            a_link.unlock();
            assert!(b_link.try_lock_for_closure());
        }

        close_routes(&router_pairs);
    }
}
