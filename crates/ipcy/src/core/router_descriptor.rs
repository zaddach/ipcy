//! Port of `ipcz/router_descriptor.h`.

use super::fragment_descriptor::FragmentDescriptor;
use super::message::Pod;
use super::node_name::NodeName;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;

/// Serialized representation of a Router sent in a parcel. When a portal is
/// transferred to a new node, we use this structure to serialize a description
/// of the new Router that will back the portal at its new location. This new
/// router is an inward peer of the portal's previous router on the sending node.
///
/// NOTE: This is a wire structure and must remain backwards-compatible across
/// changes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct RouterDescriptor {
    /// If the other end of the route is already known to be closed when this
    /// router is serialized, this is the total number of parcels sent from that
    /// end.
    pub closed_peer_sequence_length: SequenceNumber,

    /// A new sublink allocated by the sender on the NodeLink which sends this
    /// descriptor. The sublink may be used as a peripheral link, inward to (and
    /// outward from) the new router, or it may be used the route's central link
    /// if and only if `proxy_already_bypassed` is true. In the latter case,
    /// `new_link_state_fragment` must be valid and is used as the new central
    /// link's RouterLinkState.
    pub new_sublink: SublinkId,
    pub new_link_state_fragment: FragmentDescriptor,

    /// When `proxy_already_bypassed` is true, this is another new sublink
    /// allocated by the sender on the NodeLink which sends this descriptor. This
    /// sublink is used as a peripheral link to the new router's outward peer
    /// back on the sending node, as a way for that router to forward any inbound
    /// parcels that were still queued or in flight when this router was
    /// serialized.
    pub new_decaying_sublink: SublinkId,

    /// The SequenceNumber of the next outbound parcel which can be produced by
    /// this router.
    pub next_outgoing_sequence_number: SequenceNumber,

    /// The SequenceNumber of the next inbound parcel expected by this router.
    pub next_incoming_sequence_number: SequenceNumber,

    /// The total length of the sequence of parcels expected on the decaying link
    /// established by `new_decaying_sublink`, if and only if
    /// `proxy_already_bypassed` is true. The decaying link is expected to
    /// receive only parcels between `next_incoming_sequence_number` (inclusive)
    /// and `decaying_incoming_sequence_length` (exclusive). If those fields are
    /// equal then the decaying link should be ignored and `new_decaying_sublink`
    /// may not be valid.
    pub decaying_incoming_sequence_length: SequenceNumber,

    /// Bit flags, in place of the C++ `bool : 1` bitfields (the first declared
    /// bitfield is the least significant bit). See [`Self::PEER_CLOSED`] and
    /// [`Self::PROXY_ALREADY_BYPASSED`].
    flags: u8,

    /// Reserved padding out to the next 8-byte boundary.
    pub reserved0: [u8; 7],

    /// These fields are set if and only if proxy bypass should be initiated
    /// immediately on deserialization of the new Router. The deserializing node
    /// must contact `proxy_peer_node_name` with the name of the node who sent
    /// this descriptor, along with `proxy_peer_sublink` (an existing sublink
    /// between those two nodes, identifying the link we want to bypass). These
    /// fields may be set as an optimization to avoid additional messaging
    /// overhead in the common case of transferring a yet-unused portal.
    pub proxy_peer_node_name: NodeName,
    pub proxy_peer_sublink: SublinkId,
}

// SAFETY: `repr(C)` with no padding bytes (checked below), and every bit
// pattern of every field is valid.
unsafe impl Pod for RouterDescriptor {}

const _: () = assert!(std::mem::size_of::<RouterDescriptor>() == 96);
const _: () = assert!(std::mem::align_of::<RouterDescriptor>() == 8);

impl RouterDescriptor {
    /// Indicates that the other end of the route is already known to be closed.
    /// In this case sending any new outbound parcels from this router would be
    /// pointless, but there may still be in-flight parcels to receive from the
    /// other end. `closed_peer_sequence_length` will indicate the total number
    /// of parcels sent from that end, and `next_incoming_sequence_number` can be
    /// used to determine whether there are any parcels left to receive.
    pub const PEER_CLOSED: u8 = 1 << 0;

    /// Indicates that, as an optimization, the sender was able to circumvent the
    /// usual process of first establishing a peripheral link and then
    /// initiating proxy bypass. Instead the outward peer of this new router is
    /// already configured to route messages directly to the new router, and its
    /// former (and local) outward peer is configured to proxy any previously
    /// queued or in-flight messages to us over the decaying link described
    /// above.
    pub const PROXY_ALREADY_BYPASSED: u8 = 1 << 1;

    pub fn peer_closed(&self) -> bool {
        self.flags & Self::PEER_CLOSED != 0
    }

    pub fn set_peer_closed(&mut self, value: bool) {
        self.set_flag(Self::PEER_CLOSED, value);
    }

    pub fn proxy_already_bypassed(&self) -> bool {
        self.flags & Self::PROXY_ALREADY_BYPASSED != 0
    }

    pub fn set_proxy_already_bypassed(&mut self, value: bool) {
        self.set_flag(Self::PROXY_ALREADY_BYPASSED, value);
    }

    fn set_flag(&mut self, flag: u8, value: bool) {
        self.flags = (self.flags & !flag) | if value { flag } else { 0 };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;

    #[test]
    fn layout_matches_cpp() {
        assert_eq!(offset_of!(RouterDescriptor, closed_peer_sequence_length), 0);
        assert_eq!(offset_of!(RouterDescriptor, new_sublink), 8);
        assert_eq!(offset_of!(RouterDescriptor, new_link_state_fragment), 16);
        assert_eq!(offset_of!(RouterDescriptor, new_decaying_sublink), 32);
        assert_eq!(offset_of!(RouterDescriptor, next_outgoing_sequence_number), 40);
        assert_eq!(offset_of!(RouterDescriptor, next_incoming_sequence_number), 48);
        assert_eq!(offset_of!(RouterDescriptor, decaying_incoming_sequence_length), 56);
        assert_eq!(offset_of!(RouterDescriptor, flags), 64);
        assert_eq!(offset_of!(RouterDescriptor, reserved0), 65);
        assert_eq!(offset_of!(RouterDescriptor, proxy_peer_node_name), 72);
        assert_eq!(offset_of!(RouterDescriptor, proxy_peer_sublink), 88);
    }

    #[test]
    fn flags() {
        let mut d = RouterDescriptor::default();
        assert!(!d.peer_closed() && !d.proxy_already_bypassed());
        d.set_proxy_already_bypassed(true);
        assert!(!d.peer_closed() && d.proxy_already_bypassed());
        d.set_peer_closed(true);
        d.set_proxy_already_bypassed(false);
        assert!(d.peer_closed() && !d.proxy_already_bypassed());
    }
}
