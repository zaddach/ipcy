//! Port of `ipcz/node_type.h`.

use super::message::WireEnum;

/// Indicates the role a Node plays in its network of nodes. Note that this is
/// used by internal wire messages, so values must not be changed or removed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum NodeType {
    /// A broker node assigns its own name and is able to assign names to other
    /// nodes upon connection. Brokers are trusted to introduce nodes to each
    /// other upon request, and brokers may connect to other brokers in order to
    /// share information and effectively bridge two node networks together.
    Broker = 0,

    /// A "normal" (i.e. non-broker) node is assigned a permanent name by the
    /// first broker node it connects to, and it can only make contact with other
    /// nodes by requesting an introduction from that broker.
    Normal = 1,
}

impl WireEnum for NodeType {
    type Raw = u8;
    const MAX_VALUE: u32 = NodeType::Normal as u32;

    fn from_raw(raw: u8) -> Option<Self> {
        match raw {
            0 => Some(NodeType::Broker),
            1 => Some(NodeType::Normal),
            _ => None,
        }
    }

    fn to_raw(self) -> u8 {
        self as u8
    }
}
