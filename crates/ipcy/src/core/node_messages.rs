//! The ipcz node-to-node protocol messages: the port of
//! `ipcz/node_messages_generator.h`.

use super::buffer_id::BufferId;
use super::fragment_descriptor::FragmentDescriptor;
use super::link_side::LinkSide;
use super::message::internal::DriverObjectArrayData;
use super::message_macros::define_messages;
use super::node_name::NodeName;
use super::node_type::NodeType;
use super::router_descriptor::RouterDescriptor;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;

/// Bump this version number up by 1 when adding new protocol features so that
/// they can be detected during NodeLink establishment.
pub const PROTOCOL_VERSION: u32 = 0;

// `Features::Bitfield` is `u64`, and arrays of `HandleType` are arrays of `u32`
// on the wire.
define_messages! {
    interface Node;

    // Initial greeting sent by a broker on a new transport to a non-broker.
    message ConnectFromBrokerToNonBroker = 0 {
        version 0 {
            param broker_name: NodeName;
            param receiver_name: NodeName;
            param protocol_version: u32;
            param num_initial_portals: u32;
            driver_object buffer: u32;
            param padding: u32;
        }
        version 1 {
            param_array features: u64;
        }
    }

    message ConnectFromNonBrokerToBroker = 1 {
        version 0 {
            param protocol_version: u32;
            param num_initial_portals: u32;
        }
        version 1 {
            param_array features: u64;
        }
    }

    message ReferNonBroker = 2 {
        version 0 {
            param referral_id: u64;
            param num_initial_portals: u32;
            driver_object transport: u32;
        }
    }

    message ConnectToReferredBroker = 3 {
        version 0 {
            param protocol_version: u32;
            param num_initial_portals: u32;
        }
        version 1 {
            param_array features: u64;
        }
    }

    message ConnectToReferredNonBroker = 4 {
        version 0 {
            param name: NodeName;
            param broker_name: NodeName;
            param referrer_name: NodeName;
            param broker_protocol_version: u32;
            param referrer_protocol_version: u32;
            param num_initial_portals: u32;
            driver_object broker_link_buffer: u32;
            driver_object referrer_link_transport: u32;
            driver_object referrer_link_buffer: u32;
        }
        version 1 {
            param_array broker_features: u64;
            param_array referrer_features: u64;
        }
    }

    message NonBrokerReferralAccepted = 5 {
        version 0 {
            param referral_id: u64;
            param protocol_version: u32;
            param num_initial_portals: u32;
            param name: NodeName;
            driver_object transport: u32;
            driver_object buffer: u32;
        }
        version 1 {
            param_array features: u64;
        }
    }

    message NonBrokerReferralRejected = 6 {
        version 0 {
            param referral_id: u64;
        }
    }

    message ConnectFromBrokerToBroker = 7 {
        version 0 {
            param name: NodeName;
            param protocol_version: u32;
            param num_initial_portals: u32;
            driver_object buffer: u32;
            param padding: u32;
        }
        version 1 {
            param_array features: u64;
        }
    }

    message RequestIntroduction = 10 {
        version 0 {
            param name: NodeName;
        }
    }

    message AcceptIntroduction = 11 {
        version 0 {
            param name: NodeName;
            param_enum link_side: LinkSide;
            param_enum remote_node_type: NodeType;
            param padding: u16;
            param remote_protocol_version: u32;
            driver_object transport: u32;
            driver_object memory: u32;
        }
        version 1 {
            param_array remote_features: u64;
        }
    }

    message RejectIntroduction = 12 {
        version 0 {
            param name: NodeName;
        }
    }

    message RequestIndirectIntroduction = 13 {
        version 0 {
            param source_node: NodeName;
            param target_node: NodeName;
        }
    }

    message AddBlockBuffer = 14 {
        version 0 {
            param id: BufferId;
            param block_size: u32;
            driver_object buffer: u32;
        }
    }

    message AcceptParcel = 20 {
        version 0 {
            param sublink: SublinkId;
            param sequence_number: SequenceNumber;
            param subparcel_index: u32;
            param num_subparcels: u32;
            param parcel_fragment: FragmentDescriptor;
            param_array parcel_data: u8;
            param_array handle_types: u32;
            param_array new_routers: RouterDescriptor;
            param padding: u32;
            driver_object_array driver_objects: DriverObjectArrayData;
        }
    }

    message AcceptParcelDriverObjects = 21 {
        version 0 {
            param sublink: SublinkId;
            param sequence_number: SequenceNumber;
            driver_object_array driver_objects: DriverObjectArrayData;
        }
    }

    message RouteClosed = 22 {
        version 0 {
            param sublink: SublinkId;
            param sequence_length: SequenceNumber;
        }
    }

    message RouteDisconnected = 23 {
        version 0 {
            param sublink: SublinkId;
        }
    }

    message BypassPeer = 30 {
        version 0 {
            param sublink: SublinkId;
            param reserved0: u64;
            param bypass_target_node: NodeName;
            param bypass_target_sublink: SublinkId;
        }
    }

    message AcceptBypassLink = 31 {
        version 0 {
            param current_peer_node: NodeName;
            param current_peer_sublink: SublinkId;
            param inbound_sequence_length_from_bypassed_link: SequenceNumber;
            param new_sublink: SublinkId;
            param new_link_state_fragment: FragmentDescriptor;
        }
    }

    message StopProxying = 32 {
        version 0 {
            param sublink: SublinkId;
            param inbound_sequence_length: SequenceNumber;
            param outbound_sequence_length: SequenceNumber;
        }
    }

    message ProxyWillStop = 33 {
        version 0 {
            param sublink: SublinkId;
            param inbound_sequence_length: SequenceNumber;
        }
    }

    message BypassPeerWithLink = 34 {
        version 0 {
            param sublink: SublinkId;
            param new_sublink: SublinkId;
            param new_link_state_fragment: FragmentDescriptor;
            param inbound_sequence_length: SequenceNumber;
        }
    }

    message StopProxyingToLocalPeer = 35 {
        version 0 {
            param sublink: SublinkId;
            param outbound_sequence_length: SequenceNumber;
        }
    }

    message FlushRouter = 36 {
        version 0 {
            param sublink: SublinkId;
        }
    }

    message RequestMemory = 64 {
        version 0 {
            param size: u32;
            param padding: u32;
        }
    }

    message ProvideMemory = 65 {
        version 0 {
            param size: u32;
            driver_object buffer: u32;
        }
    }

    message RelayMessage = 66 {
        version 0 {
            param destination: NodeName;
            param_array data: u8;
            param padding: u32;
            driver_object_array driver_objects: DriverObjectArrayData;
        }
    }

    message AcceptRelayedMessage = 67 {
        version 0 {
            param source: NodeName;
            param_array data: u8;
            param padding: u32;
            driver_object_array driver_objects: DriverObjectArrayData;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::message::{layout_dump, MessageParams};

    /// The layout of every node message, as printed by the C++ implementation's
    /// generated `node_messages.h` (captured once from upstream).
    const CPP_LAYOUT: &str = include_str!("../../tests/golden/node_messages_layout.txt");

    #[test]
    fn layout_matches_cpp_golden() {
        let mut dump = layout_dump(NODE_MESSAGE_INFOS);
        dump.push_str(&format!("sizeof RouterDescriptor={}\n", std::mem::size_of::<RouterDescriptor>()));
        // A Windows checkout may have converted the fixture to CRLF line endings.
        assert_eq!(dump, CPP_LAYOUT.replace("\r\n", "\n"));
    }

    #[test]
    fn message_ids_are_unique() {
        let mut ids = NODE_MESSAGE_INFOS.iter().map(|info| info.id).collect::<Vec<_>>();
        ids.sort_unstable();
        assert!(ids.windows(2).all(|pair| pair[0] != pair[1]));
        assert_eq!(ids.len(), 28);
    }

    #[test]
    fn versions_are_8_byte_blocks() {
        for info in NODE_MESSAGE_INFOS {
            assert_eq!(info.params_size % 8, 0, "{}", info.name);
            for version in info.versions {
                assert_eq!(version.offset % 8, 0, "{}", info.name);
                assert_eq!(version.size % 8, 0, "{}", info.name);
            }
        }
        // And a spot check of the typed ids.
        assert_eq!(<AcceptParcelParams as MessageParams>::ID, 20);
        assert_eq!(AcceptParcel::ID, 20);
    }
}
