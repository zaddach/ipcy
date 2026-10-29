//! Port of `ipcz/route_edge.{h,cc}`.

use std::sync::Arc;

use super::router::Router;
use super::router_link::RouterLink;
use super::sequence_number::SequenceNumber;

struct DecayingLink {
    /// A link which used to be the edge's primary link but which is being
    /// phased out. The decaying link may continue to receive parcels, but once
    /// `incoming_length` is set, it will only expect to receive parcels with a
    /// SequenceNumber up to (but not including) that value. Similarly, the
    /// decaying link will be preferred for message transmission as long as
    /// `outgoing_length` remains unknown, but as soon as that value is set, only
    /// parcels with a SequenceNumber up to (but not including) that value will
    /// be transmitted over this link. Once both sequence lengths are known and
    /// surpassed, the edge will drop this link.
    ///
    /// This field may be `None`, indicating that the edge was marked for link
    /// decay but hasn't yet been assigned a primary link. When a primary link is
    /// assigned in this case, it will immediately begin to decay.
    link: Option<Arc<dyn RouterLink>>,

    /// If present, the length of the parcel sequence after which this edge must
    /// stop using the decaying link to transmit parcels. If this is 5, then the
    /// decaying link must be used to transmit any new parcels with a
    /// SequenceNumber in the range [0, 4] inclusive. Beyond that point the
    /// primary link must be used.
    outgoing_length: Option<SequenceNumber>,

    /// If present, the length of the parcel sequence after which this edge can
    /// stop expecting to receive parcels over the decaying link. If this is 7,
    /// then the Router using this edge should still expect to receive parcels
    /// from the decaying link as long as it is missing any parcel in the range
    /// [0, 6] inclusive. Beyond that point parcels should only be expected from
    /// the primary link.
    incoming_length: Option<SequenceNumber>,
}

/// A RouteEdge is responsible for message ingress and egress on one
/// (inward-facing or outward-facing) side of a router. Every functioning router
/// has one outward edge and (if proxying) one inward edge.
///
/// Over the course of its lifetime a RouteEdge may utilize many different
/// RouterLinks, but at any moment it has at most two: one "primary" link and one
/// "decaying" link.
///
/// The decaying link's usage is restricted to transmission and receipt of a
/// limited range of parcels based on SequenceNumber, and once all expected
/// parcels are sent and received, the link is dropped from the edge.
///
/// When a RouteEdge has no decaying link, it may be able to transition its
/// primary link to a decaying link, while adopting a new primary link to take
/// its place. This process of incremental link replacement is the basis for ipcz
/// route reduction.
///
/// This object is not thread-safe.
#[derive(Default)]
pub struct RouteEdge {
    /// The primary link over which this edge transmits and accepts parcels and
    /// other messages. If a decaying link is also present, then the decaying link
    /// is preferred for transmission of all parcels with a SequenceNumber up to
    /// (but not including) the decaying link's outgoing length. If that value is
    /// not set, the decaying link is always preferred when not null.
    primary_link: Option<Arc<dyn RouterLink>>,

    /// State regarding the edge's previous primary link which is currently
    /// decaying. Only present when the edge starts to decay (or will decay ASAP)
    /// its primary link.
    decaying_link: Option<DecayingLink>,
}

impl RouteEdge {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn primary_link(&self) -> Option<&Arc<dyn RouterLink>> {
        self.primary_link.as_ref()
    }

    pub fn decaying_link(&self) -> Option<&Arc<dyn RouterLink>> {
        self.decaying_link.as_ref().and_then(|decaying| decaying.link.as_ref())
    }

    /// Indicates whether this edge is stable, meaning it has no decaying link
    /// and it is not set to decay the next primary link it adopts.
    pub fn is_stable(&self) -> bool {
        self.decaying_link.is_none()
    }

    /// Indicates whether this edge has been marked for decay before being
    /// assigned a primary link. When this is true, any assigned primary link
    /// will immediately begin to decay.
    pub fn is_decay_deferred(&self) -> bool {
        self.decaying_link.as_ref().is_some_and(|decaying| decaying.link.is_none())
    }

    /// Sets the final length of the parcel sequence transmitted to the current
    /// (or deferred) decaying link. Once both this and the length from the link
    /// are set, the decaying link will be dropped from this edge as soon as the
    /// link transmits all messages up to (but not including) this length AND
    /// receives all messages up to (but not including) the other.
    pub fn set_length_to_decaying_link(&mut self, length: SequenceNumber) {
        let decaying = self.decaying_link.as_mut().expect("edge is not stable");
        assert!(decaying.outgoing_length.is_none());
        decaying.outgoing_length = Some(length);
    }

    pub fn set_length_from_decaying_link(&mut self, length: SequenceNumber) {
        let decaying = self.decaying_link.as_mut().expect("edge is not stable");
        assert!(decaying.incoming_length.is_none());
        decaying.incoming_length = Some(length);
    }

    pub fn length_to_decaying_link(&self) -> Option<SequenceNumber> {
        self.decaying_link.as_ref().and_then(|decaying| decaying.outgoing_length)
    }

    pub fn length_from_decaying_link(&self) -> Option<SequenceNumber> {
        self.decaying_link.as_ref().and_then(|decaying| decaying.incoming_length)
    }

    /// Sets the primary link for this edge. Only valid to call if the edge does
    /// not currently have a primary link.
    pub fn set_primary_link(&mut self, link: Arc<dyn RouterLink>) {
        assert!(self.primary_link.is_none());
        if self.is_decay_deferred() {
            // This edge was set to decay its primary link before it had a
            // primary link, so this primary link must be immediately set to
            // decay.
            log::trace!("Edge adopted decaying {}", link.describe());
            self.decaying_link.as_mut().expect("deferred decay").link = Some(link);
        } else {
            log::trace!("Edge adopted {}", link.describe());
            self.primary_link = Some(link);
        }
    }

    /// Releases this edge's primary link and returns it.
    pub fn release_primary_link(&mut self) -> Option<Arc<dyn RouterLink>> {
        self.primary_link.take()
    }

    /// Releases this edge's decaying link and returns it.
    pub fn release_decaying_link(&mut self) -> Option<Arc<dyn RouterLink>> {
        self.decaying_link.take().and_then(|decaying| decaying.link)
    }

    /// If the primary link is present and is a LocalRouterLink, this returns the
    /// Router on the other side of the link. Otherwise it returns `None`.
    pub fn get_local_peer(&self) -> Option<Arc<Router>> {
        self.primary_link.as_ref().and_then(|link| link.get_local_peer())
    }

    /// If the decaying link is present and is a LocalRouterLink, this returns
    /// the Router on the other side of the link. Otherwise it returns `None`.
    pub fn get_decaying_local_peer(&self) -> Option<Arc<Router>> {
        self.decaying_link().and_then(|link| link.get_local_peer())
    }

    /// Sets the current primary link to begin decay; or if there is no primary
    /// link yet, marks this edge for deferred decay. In the latter case, the
    /// next primary link adopted by this edge will immediately begin to decay.
    /// This may only be called while the edge has no decaying link.
    pub fn begin_primary_link_decay(&mut self) -> bool {
        if self.decaying_link.is_some() {
            return false;
        }

        self.decaying_link = Some(DecayingLink {
            link: self.primary_link.take(),
            outgoing_length: None,
            incoming_length: None,
        });
        true
    }

    /// Indicates whether a parcel with the given SequenceNumber should be
    /// transmitted over this edge's decaying link. If not, the parcel should be
    /// transmitted over this edge's primary link.
    pub fn should_transmit_on_decaying_link(&self, n: SequenceNumber) -> bool {
        self.decaying_link
            .as_ref()
            .is_some_and(|decaying| decaying.outgoing_length.is_none_or(|length| n < length))
    }

    /// Attempts to drop this edge's decaying link, given that it has already
    /// transmitted a parcel sequence up to `length_sent` and received a parcel
    /// sequence up to `length_received`. Returns true if the decaying link was
    /// dropped, and false otherwise.
    pub fn maybe_finish_decay(&mut self, length_sent: SequenceNumber, length_received: SequenceNumber) -> bool {
        let Some(decaying) = self.decaying_link.as_ref().filter(|decaying| decaying.link.is_some()) else {
            return false;
        };
        let described = || decaying.link.as_ref().map(|link| link.describe()).unwrap_or_default();

        let Some(outgoing_length) = decaying.outgoing_length else {
            log::trace!("Cannot decay yet with no known sequence length to {}", described());
            return false;
        };

        let Some(incoming_length) = decaying.incoming_length else {
            log::trace!("Cannot decay yet with no known sequence length from {}", described());
            return false;
        };

        if length_sent < outgoing_length {
            log::trace!(
                "Cannot decay yet without sending full sequence up to {outgoing_length} on {}",
                described()
            );
            return false;
        }

        if length_received < incoming_length {
            log::trace!(
                "Cannot decay yet without receiving full sequence up to {incoming_length} on {}",
                described()
            );
            return false;
        }

        self.decaying_link = None;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::link_type::LinkType;
    use crate::core::local_router_link::LocalRouterLink;

    fn sn(n: u64) -> SequenceNumber {
        SequenceNumber::new(n)
    }

    // This link does not need to be usable, it just needs to be unique. Hence we
    // create and link two throwaway routers, and return one of their links.
    fn create_link() -> Arc<dyn RouterLink> {
        let a = Router::new();
        let b = Router::new();
        let (a_link, _b_link) =
            LocalRouterLink::create_pair(LinkType::Central, (a, b), crate::core::local_router_link::InitialState::Unstable);
        a_link
    }

    #[test]
    fn stability() {
        let mut edge = RouteEdge::new();

        // Fresh edges are stable.
        assert!(edge.is_stable());

        // Edges with only a primary link are stable.
        let link = create_link();
        edge.set_primary_link(link.clone());
        assert!(edge.is_stable());
        edge.release_primary_link();

        // Edges with a deferred decaying link are not stable.
        edge.begin_primary_link_decay();
        assert!(!edge.is_stable());

        // Edges with only a decaying link are not stable. This link will be set
        // to decay immediately due to the deferred decay from above.
        edge.set_primary_link(link);
        assert!(!edge.is_stable());

        // Edges with both a primary and decaying link are still not stable.
        let new_link = create_link();
        edge.set_primary_link(new_link);
        assert!(!edge.is_stable());

        // But once the decaying link is dropped, the edge is stable again.
        edge.release_decaying_link();
        assert!(edge.is_stable());
    }

    #[test]
    fn link_selection() {
        let mut edge = RouteEdge::new();
        let first_link = create_link();
        let second_link = create_link();

        // With no primary or decaying link, the primary link is the default
        // choice.
        assert!(!edge.should_transmit_on_decaying_link(sn(0)));

        // Now with only a primary link, that link is always selected.
        edge.set_primary_link(first_link.clone());
        assert!(!edge.should_transmit_on_decaying_link(sn(0)));
        assert!(!edge.should_transmit_on_decaying_link(sn(5)));
        assert!(!edge.should_transmit_on_decaying_link(sn(10)));

        // With a decaying link but no outgoing sequence length limit, the
        // decaying link is always selected.
        edge.begin_primary_link_decay();
        edge.set_primary_link(second_link.clone());
        assert!(Arc::ptr_eq(edge.primary_link().unwrap(), &second_link));
        assert!(Arc::ptr_eq(edge.decaying_link().unwrap(), &first_link));
        assert!(edge.should_transmit_on_decaying_link(sn(0)));
        assert!(edge.should_transmit_on_decaying_link(sn(5)));
        assert!(edge.should_transmit_on_decaying_link(sn(10)));

        // Finally, with a limit on the decaying link's sequence length,
        // selection now depends on the specific SequenceNumber being
        // transmitted.
        edge.set_length_to_decaying_link(sn(5));
        assert!(edge.should_transmit_on_decaying_link(sn(0)));
        assert!(edge.should_transmit_on_decaying_link(sn(4)));
        assert!(!edge.should_transmit_on_decaying_link(sn(5)));
        assert!(!edge.should_transmit_on_decaying_link(sn(10)));
    }

    #[test]
    fn finish_decay() {
        let mut edge = RouteEdge::new();
        let link = create_link();
        edge.set_primary_link(link.clone());
        edge.begin_primary_link_decay();

        // Decay cannot finish until inbound and outbound sequence length limits
        // are set.
        assert!(!edge.maybe_finish_decay(sn(0), sn(0)));
        edge.set_length_to_decaying_link(sn(0));
        assert!(!edge.maybe_finish_decay(sn(0), sn(0)));
        edge.set_length_from_decaying_link(sn(0));
        assert!(edge.decaying_link().is_some());
        assert!(edge.maybe_finish_decay(sn(0), sn(0)));
        assert!(edge.decaying_link().is_none());

        // Decay also cannot finish while the sequence length limits have not yet
        // been met by messages transmitted and received over the decaying link.
        edge.set_primary_link(link);
        edge.begin_primary_link_decay();
        edge.set_length_to_decaying_link(sn(2));
        edge.set_length_from_decaying_link(sn(4));
        assert!(!edge.maybe_finish_decay(sn(1), sn(3)));
        assert!(!edge.maybe_finish_decay(sn(1), sn(4)));
        assert!(edge.decaying_link().is_some());
        assert!(edge.maybe_finish_decay(sn(2), sn(4)));
        assert!(edge.decaying_link().is_none());
    }
}
