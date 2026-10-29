//! Port of `ipcz/node_connector.{h,cc}`.
//!
//! A NodeConnector performs the initial handshake on a new transport, to
//! establish a NodeLink between two nodes.
//!
//! DEVIATIONS: upstream's class hierarchy of connectors is a single struct here
//! with a [`Kind`] enum carrying the per-handshake state. Connectors keep
//! themselves alive by being the transport's listener until the handshake
//! completes or fails.

use std::sync::{Arc, Mutex, MutexGuard, Weak};

use super::driver::{DriverHandle, RawMessage};
use super::driver_memory::{DriverMemory, DriverMemoryWithMapping};
use super::driver_transport::{DriverTransport, TransportListener};
use super::features::Features;
use super::ipcz_result::IpczResult;
use super::ipcz_types::ConnectNodeFlags;
use super::link_side::LinkSide;
use super::link_type::LinkType;
use super::node::{Connection, Node};
use super::node_link::NodeLink;
use super::node_link_memory::{MemoryNode, NodeLinkMemory};
use super::node_messages::{self as msg, NodeMessageListener, PROTOCOL_VERSION};
use super::node_name::NodeName;
use super::node_type::NodeType;
use super::router::Router;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;

/// Invoked once the handshake is complete. If it failed, the link is `None`.
pub type ConnectCallback = Box<dyn FnOnce(Option<Arc<NodeLink>>) + Send>;

fn take<T>(slot: &Mutex<Option<T>>) -> Option<T> {
    slot.lock().unwrap_or_else(|e| e.into_inner()).take()
}

fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

enum Kind {
    BrokerToNonBroker {
        broker_name: NodeName,
        new_remote_node_name: NodeName,
        link_memory_allocation: Mutex<DriverMemoryWithMapping>,
    },
    NonBrokerToBroker,

    /// Unlike other kinds, this one doesn't activate its transport or listen for
    /// any messages. Instead it uses an existing broker link to pass the
    /// transport along to a broker and wait for a reply to confirm either
    /// acceptance or rejection of the referral.
    Referrer {
        transport_for_broker: Mutex<Option<Arc<DriverTransport>>>,
        broker_link: Mutex<Option<Arc<NodeLink>>>,
    },

    /// Used by a referred node to await acceptance by the broker.
    ReferredNonBroker,

    /// Used by a broker to await a handshake from the referred node before
    /// responding to both that node and the referrer.
    BrokerReferral {
        referral_id: u64,
        num_initial_portals: u32,
        referrer: Arc<NodeLink>,
        broker_name: NodeName,
        referred_node_name: NodeName,
        link_memory: Mutex<Option<DriverMemoryWithMapping>>,
        client_link_memory: Mutex<Option<DriverMemoryWithMapping>>,
    },
    BrokerToBroker {
        local_name: NodeName,
        link_memory_allocation: Mutex<DriverMemoryWithMapping>,
    },
}

pub struct NodeConnector {
    weak_self: Weak<NodeConnector>,
    node: Arc<Node>,
    transport: Option<Arc<DriverTransport>>,
    flags: ConnectNodeFlags,
    waiting_routers: Vec<Arc<Router>>,
    callback: Mutex<Option<ConnectCallback>>,
    kind: Kind,
}

impl NodeConnector {
    /// Starts connecting `node` over `transport`. The specific type of connector
    /// to create is determined by a combination of the type of `node` and the
    /// value of `flags`.
    ///
    /// If a connection is successfully established, `transport` will eventually
    /// be adopted by a NodeLink and passed to `node` for use. Otherwise, all
    /// `initial_routers` will observe peer closure.
    ///
    /// In either case this invokes `callback` if given and then destroys itself
    /// once the handshake is complete. If this fails, the NodeLink given to the
    /// callback will be `None`.
    pub fn connect_node(
        node: Arc<Node>,
        transport: Arc<DriverTransport>,
        flags: ConnectNodeFlags,
        initial_routers: Vec<Arc<Router>>,
        callback: Option<ConnectCallback>,
    ) -> IpczResult {
        let from_broker = node.node_type() == NodeType::Broker;
        let to_broker = flags.contains(ConnectNodeFlags::TO_BROKER);
        let inherit_broker = flags.contains(ConnectNodeFlags::INHERIT_BROKER);
        let share_broker = flags.contains(ConnectNodeFlags::SHARE_BROKER);
        let broker_link = node.get_broker_link();
        if share_broker && (from_broker || to_broker || inherit_broker) {
            return IpczResult::InvalidArgument;
        }
        if (to_broker || from_broker) && (inherit_broker || share_broker) {
            return IpczResult::InvalidArgument;
        }

        let connector = match Self::create_connector(node, transport, flags, initial_routers, broker_link, callback) {
            Ok(connector) => connector,
            Err(result) => return result,
        };

        if !share_broker && !connector.activate_transport() {
            // Note that when referring another node to our own broker, we don't
            // activate the transport, since the transport will be passed to the
            // broker. See `Kind::Referrer`.
            return IpczResult::Unknown;
        }

        if !connector.connect() {
            return IpczResult::Unknown;
        }

        IpczResult::Ok
    }

    /// Handles a request on `node` (which must be a broker) to accept a new
    /// non-broker node referral from `referrer`, referring a new non-broker node
    /// on the remote end of `transport_to_referred_node`. This performs a
    /// handshake with the referred node before introducing it and the referrer
    /// to each other. `link_memory` and `client_link_memory` must be valid and
    /// will be passed respectively to the referred node (for its link to the
    /// broker) and the referring node (for its link to the referred node).
    pub fn handle_non_broker_referral(
        node: Arc<Node>,
        referral_id: u64,
        num_initial_portals: u32,
        referrer: Arc<NodeLink>,
        transport_to_referred_node: Arc<DriverTransport>,
        link_memory: DriverMemoryWithMapping,
        client_link_memory: DriverMemoryWithMapping,
    ) -> bool {
        assert_eq!(node.node_type(), NodeType::Broker);
        assert!(link_memory.mapping.is_valid());
        assert!(client_link_memory.mapping.is_valid());
        let broker_name = node.get_assigned_name();
        let referred_node_name = node.generate_random_name();
        let connector = Self::new(
            node,
            Some(transport_to_referred_node),
            ConnectNodeFlags::empty(),
            Vec::new(),
            None,
            Kind::BrokerReferral {
                referral_id,
                num_initial_portals,
                referrer,
                broker_name,
                referred_node_name,
                link_memory: Mutex::new(Some(link_memory)),
                client_link_memory: Mutex::new(Some(client_link_memory)),
            },
        );

        // The connector effectively owns itself and lives only until its
        // transport is disconnected or it receives a greeting from the referred
        // node.
        connector.activate_transport()
    }

    fn new(
        node: Arc<Node>,
        transport: Option<Arc<DriverTransport>>,
        flags: ConnectNodeFlags,
        waiting_routers: Vec<Arc<Router>>,
        callback: Option<ConnectCallback>,
        kind: Kind,
    ) -> Arc<NodeConnector> {
        Arc::new_cyclic(|weak_self| NodeConnector {
            weak_self: weak_self.clone(),
            node,
            transport,
            flags,
            waiting_routers,
            callback: Mutex::new(callback),
            kind,
        })
    }

    fn create_connector(
        node: Arc<Node>,
        transport: Arc<DriverTransport>,
        flags: ConnectNodeFlags,
        initial_routers: Vec<Arc<Router>>,
        broker_link: Option<Arc<NodeLink>>,
        callback: Option<ConnectCallback>,
    ) -> Result<Arc<NodeConnector>, IpczResult> {
        let from_broker = node.node_type() == NodeType::Broker;
        let to_broker = flags.contains(ConnectNodeFlags::TO_BROKER);
        let share_broker = flags.contains(ConnectNodeFlags::SHARE_BROKER);
        let inherit_broker = flags.contains(ConnectNodeFlags::INHERIT_BROKER);
        if from_broker {
            let memory = NodeLinkMemory::allocate_memory(node.driver());
            if !memory.mapping.is_valid() {
                return Err(IpczResult::ResourceExhausted);
            }

            let kind = if to_broker {
                Kind::BrokerToBroker {
                    local_name: node.get_assigned_name(),
                    link_memory_allocation: Mutex::new(memory),
                }
            } else {
                Kind::BrokerToNonBroker {
                    broker_name: node.get_assigned_name(),
                    new_remote_node_name: node.generate_random_name(),
                    link_memory_allocation: Mutex::new(memory),
                }
            };
            return Ok(Self::new(node, Some(transport), flags, initial_routers, callback, kind));
        }

        if to_broker {
            return Ok(Self::new(node, Some(transport), flags, initial_routers, callback, Kind::NonBrokerToBroker));
        }

        if share_broker {
            return Ok(Self::new(
                node,
                None,
                flags,
                initial_routers,
                callback,
                Kind::Referrer {
                    transport_for_broker: Mutex::new(Some(transport)),
                    broker_link: Mutex::new(broker_link),
                },
            ));
        }

        if inherit_broker {
            return Ok(Self::new(node, Some(transport), flags, initial_routers, callback, Kind::ReferredNonBroker));
        }

        Err(IpczResult::InvalidArgument)
    }

    fn arc(&self) -> Arc<NodeConnector> {
        self.weak_self.upgrade().expect("NodeConnector is alive while in use")
    }

    fn transport(&self) -> &Arc<DriverTransport> {
        self.transport.as_ref().expect("connector has a transport")
    }

    fn num_portals(&self) -> u32 {
        u32::try_from(self.waiting_routers.len()).expect("portal count fits in 32 bits")
    }

    fn connect(&self) -> bool {
        match &self.kind {
            Kind::BrokerToNonBroker {
                broker_name,
                new_remote_node_name,
                link_memory_allocation,
            } => {
                log::trace!(
                    "Sending direct ConnectFromBrokerToNonBroker from broker {broker_name} to new node {new_remote_node_name} with {} initial portals",
                    self.waiting_routers.len()
                );

                assert_eq!(self.node.node_type(), NodeType::Broker);
                let memory_object = guard(link_memory_allocation).memory.take_driver_object();
                let mut connect = msg::ConnectFromBrokerToNonBroker::new();
                let buffer = connect.append_driver_object(memory_object);
                let features = self.node.features().serialize(&mut connect);
                let v0 = connect.v0_mut().expect("v0");
                v0.broker_name = *broker_name;
                v0.receiver_name = *new_remote_node_name;
                v0.protocol_version = PROTOCOL_VERSION;
                v0.num_initial_portals = self.num_portals();
                v0.buffer = buffer;
                v0.padding = 0;
                connect.v1_mut().expect("v1").features = features;
                self.transport().transmit(&mut connect) == IpczResult::Ok
            }

            Kind::NonBrokerToBroker => {
                assert_eq!(self.node.node_type(), NodeType::Normal);
                let mut connect = msg::ConnectFromNonBrokerToBroker::new();
                let features = self.node.features().serialize(&mut connect);
                let v0 = connect.v0_mut().expect("v0");
                v0.protocol_version = PROTOCOL_VERSION;
                v0.num_initial_portals = self.num_portals();
                connect.v1_mut().expect("v1").features = features;
                self.transport().transmit(&mut connect) == IpczResult::Ok
            }

            Kind::Referrer {
                transport_for_broker,
                broker_link,
            } => {
                assert_eq!(self.node.node_type(), NodeType::Normal);
                let Some(broker) = guard(broker_link).clone() else {
                    // If there's no broker link yet, wait for one.
                    let connector = self.arc();
                    self.node.wait_for_broker_link_async(Box::new(move |broker_link| {
                        if let Kind::Referrer { broker_link: slot, .. } = &connector.kind {
                            *guard(slot) = Some(broker_link);
                        }
                        connector.connect();
                    }));
                    return true;
                };

                let connector = self.arc();
                let broker_for_callback = broker.clone();
                let transport = take(transport_for_broker).expect("transport not yet passed to the broker");
                broker.refer_non_broker(
                    transport,
                    self.num_portals(),
                    Box::new(move |link_to_referred_node, remote_num_initial_portals| match link_to_referred_node {
                        Some(link) => connector.accept_connection(
                            Connection {
                                link,
                                broker: Some(broker_for_callback),
                            },
                            remote_num_initial_portals,
                        ),
                        None => connector.reject_connection(),
                    }),
                );
                true
            }

            Kind::ReferredNonBroker => {
                assert_eq!(self.node.node_type(), NodeType::Normal);
                let mut connect = msg::ConnectToReferredBroker::new();
                let features = self.node.features().serialize(&mut connect);
                let v0 = connect.v0_mut().expect("v0");
                v0.protocol_version = PROTOCOL_VERSION;
                v0.num_initial_portals = self.num_portals();
                connect.v1_mut().expect("v1").features = features;
                self.transport().transmit(&mut connect) == IpczResult::Ok
            }

            Kind::BrokerReferral { .. } => true,

            Kind::BrokerToBroker {
                local_name,
                link_memory_allocation,
            } => {
                log::trace!(
                    "Sending direct ConnectFromBrokerToBroker from broker {local_name} with {} initial portals",
                    self.waiting_routers.len()
                );

                assert_eq!(self.node.node_type(), NodeType::Broker);
                let memory_object = guard(link_memory_allocation).memory.take_driver_object();
                let mut connect = msg::ConnectFromBrokerToBroker::new();
                let buffer = connect.append_driver_object(memory_object);
                let features = self.node.features().serialize(&mut connect);
                let v0 = connect.v0_mut().expect("v0");
                v0.name = *local_name;
                v0.protocol_version = PROTOCOL_VERSION;
                v0.num_initial_portals = self.num_portals();
                v0.buffer = buffer;
                v0.padding = 0;
                connect.v1_mut().expect("v1").features = features;
                self.transport().transmit(&mut connect) == IpczResult::Ok
            }
        }
    }

    // Invoked once by the implementation when it has completed its handshake.
    fn accept_connection(&self, connection: Connection, num_remote_portals: u32) {
        self.node
            .add_connection(*connection.link.remote_node_name(), connection.clone());
        if let Some(callback) = take(&self.callback) {
            callback(Some(connection.link.clone()));
        }
        self.establish_waiting_routers(Some(&connection.link), num_remote_portals as usize);
    }

    // Invoked if the transport observes an error before receiving the expected
    // handshake message, or if the implementation receives any message other
    // than the one handshake message it expects to see first.
    fn reject_connection(&self) {
        if let Some(callback) = take(&self.callback) {
            callback(None);
        }
        self.establish_waiting_routers(None, 0);
        if let Some(transport) = &self.transport {
            transport.deactivate();
        }
    }

    fn activate_transport(&self) -> bool {
        let transport = self.transport();
        transport.set_listener(Some(self.arc()));
        if transport.activate() != IpczResult::Ok {
            self.reject_connection();
            return false;
        }
        true
    }

    fn establish_waiting_routers(&self, to_link: Option<&Arc<NodeLink>>, max_valid_portals: usize) {
        assert!(to_link.is_some() || max_valid_portals == 0);
        let num_valid_portals = max_valid_portals.min(self.waiting_routers.len());
        for (i, router) in self.waiting_routers.iter().enumerate().take(num_valid_portals) {
            let to_link = to_link.expect("valid portals imply a link");
            let link = to_link.add_remote_router_link(
                SublinkId::new(i as u64),
                to_link.memory().get_initial_router_link_state(i),
                LinkType::Central,
                to_link.link_side(),
                router.clone(),
            );
            match link {
                Some(link) => router.set_outward_link(link),
                None => {
                    router.accept_route_disconnected_from(LinkType::Central);
                }
            }
        }

        // Elicit immediate peer closure on any surplus portals that were
        // established on this side of the link.
        for router in &self.waiting_routers[num_valid_portals..] {
            router.accept_route_closure_from(LinkType::Central, SequenceNumber::new(0));
        }
    }

    fn handle_transport_error(&self) {
        if let Kind::BrokerReferral {
            referral_id, referrer, ..
        } = &self.kind
        {
            let mut rejected = msg::NonBrokerReferralRejected::new();
            rejected.v0_mut().expect("v0").referral_id = *referral_id;
            referrer.transmit(&mut rejected);
        }

        self.reject_connection();
    }
}

fn deserialize_features<M: std::ops::Deref<Target = super::message::Message>>(message: &M, offset: Option<u32>) -> Features {
    offset.map_or_else(Features::default, |offset| Features::deserialize(message, offset))
}

// Protocol version negotiation is kept for parity with upstream, though there is
// only one protocol version for now.
#[allow(clippy::unnecessary_min_or_max)]
impl NodeMessageListener for NodeConnector {
    // Handled by `Kind::BrokerToNonBroker`.
    fn on_connect_from_non_broker_to_broker(&self, connect: msg::ConnectFromNonBrokerToBroker) -> bool {
        let Kind::BrokerToNonBroker {
            broker_name,
            new_remote_node_name,
            link_memory_allocation,
        } = &self.kind
        else {
            return false;
        };

        log::trace!("Accepting ConnectFromNonBrokerToBroker on broker {broker_name} from new node {new_remote_node_name}");

        let v0 = *connect.v0().expect("v0");
        let remote_features = deserialize_features(&connect, connect.v1().map(|v1| v1.features));
        let mapping = std::mem::take(&mut guard(link_memory_allocation).mapping);
        let node: Arc<dyn MemoryNode> = self.node.clone();
        let link = NodeLink::create_active(
            self.node.clone(),
            LinkSide::A,
            *broker_name,
            *new_remote_node_name,
            NodeType::Normal,
            v0.protocol_version,
            remote_features,
            self.transport().clone(),
            NodeLinkMemory::create(node, LinkSide::A, &remote_features, mapping),
        );
        self.accept_connection(Connection { link, broker: None }, v0.num_initial_portals);
        true
    }

    // Handled by `Kind::NonBrokerToBroker`.
    fn on_connect_from_broker_to_non_broker(&self, mut connect: msg::ConnectFromBrokerToNonBroker) -> bool {
        if !matches!(self.kind, Kind::NonBrokerToBroker) {
            return false;
        }

        let v0 = *connect.v0().expect("v0");
        log::trace!(
            "New node accepting ConnectFromBrokerToNonBroker with assigned name {} from broker {}",
            { v0.receiver_name },
            { v0.broker_name }
        );

        let mapping = DriverMemory::from_object(connect.take_driver_object(v0.buffer)).map();
        if !mapping.is_valid() {
            return false;
        }

        let remote_features = deserialize_features(&connect, connect.v1().map(|v1| v1.features));
        let node: Arc<dyn MemoryNode> = self.node.clone();
        let new_link = NodeLink::create_active(
            self.node.clone(),
            LinkSide::B,
            v0.receiver_name,
            v0.broker_name,
            NodeType::Broker,
            v0.protocol_version,
            remote_features,
            self.transport().clone(),
            NodeLinkMemory::create(node, LinkSide::B, &remote_features, mapping),
        );
        if self.flags.contains(ConnectNodeFlags::TO_ALLOCATION_DELEGATE) {
            self.node.set_allocation_delegate(Some(new_link.clone()));
        }

        self.accept_connection(
            Connection {
                link: new_link.clone(),
                broker: Some(new_link),
            },
            v0.num_initial_portals,
        );
        true
    }

    // Handled by `Kind::ReferredNonBroker`.
    fn on_connect_to_referred_non_broker(&self, mut connect: msg::ConnectToReferredNonBroker) -> bool {
        if !matches!(self.kind, Kind::ReferredNonBroker) {
            return false;
        }

        let v0 = *connect.v0().expect("v0");
        log::trace!(
            "New node accepting ConnectToReferredNonBroker with assigned name {} from broker {} as referred by {}",
            { v0.name },
            { v0.broker_name },
            { v0.referrer_name }
        );

        let broker_mapping = DriverMemory::from_object(connect.take_driver_object(v0.broker_link_buffer)).map();
        let referrer_mapping = DriverMemory::from_object(connect.take_driver_object(v0.referrer_link_buffer)).map();
        let referrer_transport = DriverTransport::new(connect.take_driver_object(v0.referrer_link_transport));
        if !broker_mapping.is_valid() || !referrer_mapping.is_valid() || !referrer_transport.driver_handle().is_valid() {
            return false;
        }

        let (broker_features, referrer_features) = match connect.v1() {
            Some(v1) => (
                Features::deserialize(&connect, v1.broker_features),
                Features::deserialize(&connect, v1.referrer_features),
            ),
            None => (Features::default(), Features::default()),
        };

        // Ensure this NodeConnector stays alive until this method returns.
        // Otherwise the last reference may be dropped when the new NodeLink
        // takes over listening on `transport`.
        let _self = self.arc();
        let broker_protocol_version = v0.broker_protocol_version.min(PROTOCOL_VERSION);
        let node: Arc<dyn MemoryNode> = self.node.clone();
        let broker_link = NodeLink::create_active(
            self.node.clone(),
            LinkSide::B,
            v0.name,
            v0.broker_name,
            NodeType::Broker,
            broker_protocol_version,
            broker_features,
            self.transport().clone(),
            NodeLinkMemory::create(node.clone(), LinkSide::B, &broker_features, broker_mapping),
        );
        if self.flags.contains(ConnectNodeFlags::TO_ALLOCATION_DELEGATE) {
            self.node.set_allocation_delegate(Some(broker_link.clone()));
        }
        self.node.add_connection(
            v0.broker_name,
            Connection {
                link: broker_link.clone(),
                broker: Some(broker_link.clone()),
            },
        );

        let referrer_protocol_version = v0.referrer_protocol_version.min(PROTOCOL_VERSION);
        let referrer_link = NodeLink::create_inactive(
            self.node.clone(),
            LinkSide::B,
            v0.name,
            v0.referrer_name,
            NodeType::Normal,
            referrer_protocol_version,
            referrer_features,
            referrer_transport,
            NodeLinkMemory::create(node, LinkSide::B, &referrer_features, referrer_mapping),
        );

        self.accept_connection(
            Connection {
                link: referrer_link.clone(),
                broker: Some(broker_link),
            },
            v0.num_initial_portals,
        );
        referrer_link.activate();
        true
    }

    // Handled by `Kind::BrokerReferral`.
    fn on_connect_to_referred_broker(&self, connect_to_broker: msg::ConnectToReferredBroker) -> bool {
        let Kind::BrokerReferral {
            referral_id,
            num_initial_portals,
            referrer,
            broker_name,
            referred_node_name,
            link_memory,
            client_link_memory,
        } = &self.kind
        else {
            return false;
        };

        log::trace!("Accepting ConnectToReferredBroker on broker {broker_name} from new referred node {referred_node_name}");

        // Ensure this NodeConnector stays alive until this method returns.
        // Otherwise the last reference may be dropped when the new NodeLink
        // below takes over listening on `transport`.
        let _self = self.arc();

        // First, accept the new non-broker client on this broker node. There are
        // no initial portals on this link, as this link was not established
        // directly by the application. Note that this takes over listening on
        // `transport`.
        let v0 = *connect_to_broker.v0().expect("v0");
        let remote_features = deserialize_features(&connect_to_broker, connect_to_broker.v1().map(|v1| v1.features));
        let protocol_version = v0.protocol_version.min(PROTOCOL_VERSION);
        let (Some(mut link_memory), Some(mut client_link_memory)) = (take(link_memory), take(client_link_memory)) else {
            return false;
        };
        let node: Arc<dyn MemoryNode> = self.node.clone();
        let mapping = std::mem::take(&mut link_memory.mapping);
        let link_to_referree = NodeLink::create_active(
            self.node.clone(),
            LinkSide::A,
            *broker_name,
            *referred_node_name,
            NodeType::Normal,
            protocol_version,
            remote_features,
            self.transport().clone(),
            NodeLinkMemory::create(node, LinkSide::A, &remote_features, mapping),
        );
        self.accept_connection(
            Connection {
                link: link_to_referree.clone(),
                broker: None,
            },
            0,
        );

        // Now we can create a new link to introduce both clients -- the referrer
        // and the referree -- to each other.
        let (referrer_transport, referree_transport) =
            DriverTransport::create_pair(self.node.driver(), Some(referrer.transport()), Some(self.transport()));

        // Give the referred node a reply with sufficient details for it to
        // establish links to both this broker and the referrer simultaneously.
        //
        // SUBTLE: It's important that this message is sent before the message to
        // the referrer below. Otherwise the referrer might begin relaying
        // messages through the broker to the referree before this handshake is
        // sent to the referree, which would be bad.
        let mut connect = msg::ConnectToReferredNonBroker::new();
        let broker_link_buffer = connect.append_driver_object(link_memory.memory.take_driver_object());
        let referrer_link_transport = connect.append_driver_object(referree_transport.take_driver_object());
        let referrer_link_buffer = connect.append_driver_object(client_link_memory.memory.clone_memory().take_driver_object());
        let broker_features = self.node.features().serialize(&mut connect);
        let referrer_features = referrer.remote_features().serialize(&mut connect);
        let v = connect.v0_mut().expect("v0");
        v.name = *referred_node_name;
        v.broker_name = *broker_name;
        v.referrer_name = *referrer.remote_node_name();
        v.broker_protocol_version = protocol_version;
        v.referrer_protocol_version = referrer.remote_protocol_version();
        v.num_initial_portals = *num_initial_portals;
        v.broker_link_buffer = broker_link_buffer;
        v.referrer_link_transport = referrer_link_transport;
        v.referrer_link_buffer = referrer_link_buffer;
        let v1 = connect.v1_mut().expect("v1");
        v1.broker_features = broker_features;
        v1.referrer_features = referrer_features;
        link_to_referree.transmit(&mut connect);

        // Finally, give the referrer a reply which includes details of its new
        // link to the referred node.
        let mut accepted = msg::NonBrokerReferralAccepted::new();
        let transport = accepted.append_driver_object(referrer_transport.take_driver_object());
        let buffer = accepted.append_driver_object(client_link_memory.memory.take_driver_object());
        let features = link_to_referree.remote_features().serialize(&mut accepted);
        let v = accepted.v0_mut().expect("v0");
        v.referral_id = *referral_id;
        v.protocol_version = v0.protocol_version;
        v.num_initial_portals = v0.num_initial_portals;
        v.name = *referred_node_name;
        v.transport = transport;
        v.buffer = buffer;
        accepted.v1_mut().expect("v1").features = features;
        referrer.transmit(&mut accepted);
        true
    }

    // Handled by `Kind::BrokerToBroker`.
    fn on_connect_from_broker_to_broker(&self, mut connect: msg::ConnectFromBrokerToBroker) -> bool {
        let Kind::BrokerToBroker {
            local_name,
            link_memory_allocation,
        } = &self.kind
        else {
            return false;
        };

        let v0 = *connect.v0().expect("v0");
        let remote_name = v0.name;
        log::trace!("Accepting ConnectFromBrokerToBroker on broker {local_name} from other broker {remote_name}");

        let this_side = if remote_name < *local_name { LinkSide::A } else { LinkSide::B };
        let their_memory = DriverMemory::from_object(connect.take_driver_object(v0.buffer));
        let primary_buffer_mapping = if this_side.is_side_a() {
            Some(std::mem::take(&mut guard(link_memory_allocation).mapping))
        } else {
            Some(their_memory.map())
        };
        let Some(primary_buffer_mapping) = primary_buffer_mapping.filter(|mapping| mapping.is_valid()) else {
            return false;
        };

        let remote_features = deserialize_features(&connect, connect.v1().map(|v1| v1.features));
        let node: Arc<dyn MemoryNode> = self.node.clone();
        let link = NodeLink::create_active(
            self.node.clone(),
            this_side,
            *local_name,
            remote_name,
            NodeType::Broker,
            v0.protocol_version,
            remote_features,
            self.transport().clone(),
            NodeLinkMemory::create(node, this_side, &remote_features, primary_buffer_mapping),
        );
        self.accept_connection(
            Connection {
                link: link.clone(),
                broker: Some(link),
            },
            v0.num_initial_portals,
        );
        true
    }
}

impl TransportListener for NodeConnector {
    fn on_transport_message(&self, message: &RawMessage<'_>, transport: &DriverTransport, envelope: DriverHandle) -> bool {
        self.handle_transport_message(message, transport, envelope)
    }

    fn on_transport_error(&self) {
        self.handle_transport_error();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::Driver;
    use crate::core::driver_object::DriverObject;
    use crate::core::features::IPCZ_FEATURE_MEM_V2;
    use crate::core::ipcz_types::{CreateNodeOptions, PortalStatusFlags};
    use crate::core::reference_drivers::sync::SyncDriver;
    use crate::core::testing::test_transport_listener::TestTransportListener;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct NodeConnectorTest {
        driver: Arc<SyncDriver>,
    }

    impl NodeConnectorTest {
        fn new() -> Self {
            Self {
                driver: SyncDriver::new(),
            }
        }

        fn broker(&self, options: Option<&CreateNodeOptions>) -> Arc<Node> {
            Node::new(NodeType::Broker, self.driver.clone(), options)
        }

        fn non_broker(&self, options: Option<&CreateNodeOptions>) -> Arc<Node> {
            Node::new(NodeType::Normal, self.driver.clone(), options)
        }

        fn create_transports(&self) -> (Arc<DriverTransport>, Arc<DriverTransport>) {
            let driver: Arc<dyn Driver> = self.driver.clone();
            let (handle0, handle1) = self.driver.create_transports(Default::default(), Default::default()).unwrap();
            (
                DriverTransport::new(DriverObject::new(driver.clone(), handle0)),
                DriverTransport::new(DriverObject::new(driver, handle1)),
            )
        }
    }

    fn mem_v2_options() -> CreateNodeOptions {
        CreateNodeOptions {
            enabled_features: vec![IPCZ_FEATURE_MEM_V2],
            ..Default::default()
        }
    }

    /// Records the result of a connection attempt as a rejection flag.
    fn rejection_flag() -> (Arc<AtomicBool>, Option<ConnectCallback>) {
        let rejected = Arc::new(AtomicBool::new(false));
        let flag = rejected.clone();
        (rejected, Some(Box::new(move |link| flag.store(link.is_none(), Ordering::SeqCst))))
    }

    /// Listens on `transport` for a single `ConnectFromBrokerToNonBroker`, so
    /// that no driver objects are leaked.
    fn listen_for_broker_connect(
        transport: &Arc<DriverTransport>,
        on_connect: impl Fn(&msg::ConnectFromBrokerToNonBroker) + Send + 'static,
    ) -> (TestTransportListener, Arc<AtomicBool>) {
        let received = Arc::new(AtomicBool::new(false));
        let listener = TestTransportListener::new(transport.clone());
        let (flag, transport) = (received.clone(), transport.clone());
        listener.on_raw_message(move |raw| {
            let mut connect = msg::ConnectFromBrokerToNonBroker::new_incoming();
            assert!(connect.deserialize(raw, &transport));
            on_connect(&connect);
            flag.store(true, Ordering::SeqCst);
            true
        });
        (listener, received)
    }

    #[test]
    fn connect_broker_to_non_broker() {
        let t = NodeConnectorTest::new();
        let broker = t.broker(Some(&mem_v2_options()));
        assert!(broker.features().mem_v2());
        let non_broker = t.non_broker(None);
        assert!(!non_broker.features().mem_v2());

        let (broker_transport, non_broker_transport) = t.create_transports();

        let broker_name = broker.get_assigned_name();
        let (_listener, received) = listen_for_broker_connect(&non_broker_transport, move |connect| {
            let v0 = *connect.v0().unwrap();
            assert_eq!(broker_name, { v0.broker_name });
            assert_eq!(1, { v0.num_initial_portals });

            let features = Features::deserialize(connect, connect.v1().unwrap().features);
            assert!(features.mem_v2());
        });

        // Initiate connection from the broker side. The non-broker's transport
        // should receive an appropriate connection message.
        assert!(!received.load(Ordering::SeqCst));
        NodeConnector::connect_node(broker.clone(), broker_transport, ConnectNodeFlags::empty(), vec![Router::new()], None);
        assert!(received.load(Ordering::SeqCst));

        broker.shut_down();
        non_broker.shut_down();
    }

    #[test]
    fn connect_non_broker_to_broker() {
        let t = NodeConnectorTest::new();
        let broker = t.broker(None);
        assert!(!broker.features().mem_v2());
        let non_broker = t.non_broker(Some(&mem_v2_options()));
        assert!(non_broker.features().mem_v2());

        let (broker_transport, non_broker_transport) = t.create_transports();

        let received = Arc::new(AtomicBool::new(false));
        let listener = TestTransportListener::new(broker_transport.clone());
        {
            let (flag, transport) = (received.clone(), broker_transport.clone());
            listener.on_raw_message(move |raw| {
                let mut connect = msg::ConnectFromNonBrokerToBroker::new_incoming();
                assert!(connect.deserialize(raw, &transport));
                assert_eq!(1, { connect.v0().unwrap().num_initial_portals });

                let features = Features::deserialize(&connect, connect.v1().unwrap().features);
                assert!(features.mem_v2());

                flag.store(true, Ordering::SeqCst);
                true
            });
        }

        // Initiate connection from the non-broker side. The broker's transport
        // should receive an appropriate connection message.
        assert!(!received.load(Ordering::SeqCst));
        NodeConnector::connect_node(
            non_broker.clone(),
            non_broker_transport,
            ConnectNodeFlags::TO_BROKER,
            vec![Router::new()],
            None,
        );
        assert!(received.load(Ordering::SeqCst));

        broker.shut_down();
        non_broker.shut_down();
    }

    #[test]
    fn broker_reject_invalid_message() {
        let t = NodeConnectorTest::new();
        let broker = t.broker(None);
        let non_broker = t.non_broker(None);

        {
            let (broker_transport, non_broker_transport) = t.create_transports();

            let (rejected, callback) = rejection_flag();
            NodeConnector::connect_node(
                broker.clone(),
                broker_transport,
                ConnectNodeFlags::empty(),
                vec![Router::new()],
                callback,
            );

            // Make sure we receive and deserialize the message so no driver
            // objects are leaked.
            let (_listener, received) = listen_for_broker_connect(&non_broker_transport, |_| {});
            assert!(received.load(Ordering::SeqCst));

            // Now try sending a non-connection message, which should be
            // rejected by the NodeConnector.
            assert!(!rejected.load(Ordering::SeqCst));
            non_broker_transport.transmit(&mut msg::RouteClosed::new());
            assert!(rejected.load(Ordering::SeqCst));
        }

        {
            let (broker_transport, non_broker_transport) = t.create_transports();

            let (rejected, callback) = rejection_flag();
            NodeConnector::connect_node(
                broker.clone(),
                broker_transport,
                ConnectNodeFlags::empty(),
                vec![Router::new()],
                callback,
            );

            let (_listener, received) = listen_for_broker_connect(&non_broker_transport, |_| {});
            assert!(received.load(Ordering::SeqCst));

            // The recipient here is a broker node, but we're sending it a
            // message that is only expected to be sent from a broker to a
            // non-broker. Its NodeConnector should reject this message.
            assert!(!rejected.load(Ordering::SeqCst));
            let driver: Arc<dyn Driver> = t.driver.clone();
            let mut message = msg::ConnectFromBrokerToNonBroker::new();
            let buffer = message.append_driver_object(DriverMemory::allocate(&driver, 64).take_driver_object());
            message.v0_mut().unwrap().buffer = buffer;
            non_broker_transport.transmit(&mut message);
            assert!(rejected.load(Ordering::SeqCst));
        }

        broker.shut_down();
        non_broker.shut_down();
    }

    #[test]
    fn non_broker_reject_invalid_message() {
        let t = NodeConnectorTest::new();
        let broker = t.broker(None);
        let non_broker = t.non_broker(None);

        {
            let (broker_transport, non_broker_transport) = t.create_transports();

            let (rejected, callback) = rejection_flag();
            NodeConnector::connect_node(
                non_broker.clone(),
                non_broker_transport,
                ConnectNodeFlags::TO_BROKER,
                vec![Router::new()],
                callback,
            );

            // Try sending a non-connection message, which should be rejected by
            // the NodeConnector.
            assert!(!rejected.load(Ordering::SeqCst));
            broker_transport.transmit(&mut msg::RouteClosed::new());
            assert!(rejected.load(Ordering::SeqCst));
        }

        {
            let (broker_transport, non_broker_transport) = t.create_transports();

            let (rejected, callback) = rejection_flag();
            NodeConnector::connect_node(
                non_broker.clone(),
                non_broker_transport,
                ConnectNodeFlags::TO_BROKER,
                vec![Router::new()],
                callback,
            );

            // For good measure, also try sending an inappropriate connection
            // message, which should have the same result as above. This is a
            // message which should only be accepted by broker nodes from a
            // transport expected to link to a non-broker node. Since the
            // receiving NodeConnector is for a non-broker, it must reject this
            // message.
            assert!(!rejected.load(Ordering::SeqCst));
            broker_transport.transmit(&mut msg::ConnectFromNonBrokerToBroker::new());
            assert!(rejected.load(Ordering::SeqCst));
        }

        broker.shut_down();
        non_broker.shut_down();
    }

    fn connect_both(
        t: &NodeConnectorTest,
        broker: &Arc<Node>,
        non_broker: &Arc<Node>,
        broker_routers: Vec<Arc<Router>>,
        non_broker_routers: Vec<Arc<Router>>,
        broker_first: bool,
    ) -> (Arc<NodeLink>, Arc<NodeLink>) {
        let (broker_transport, non_broker_transport) = t.create_transports();
        let broker_link = Arc::new(Mutex::new(None));
        let non_broker_link = Arc::new(Mutex::new(None));
        let connect_broker = || {
            let slot = broker_link.clone();
            NodeConnector::connect_node(
                broker.clone(),
                broker_transport.clone(),
                ConnectNodeFlags::empty(),
                broker_routers.clone(),
                Some(Box::new(move |link| *slot.lock().unwrap() = link)),
            );
        };
        let connect_non_broker = || {
            let slot = non_broker_link.clone();
            NodeConnector::connect_node(
                non_broker.clone(),
                non_broker_transport.clone(),
                ConnectNodeFlags::TO_BROKER,
                non_broker_routers.clone(),
                Some(Box::new(move |link| *slot.lock().unwrap() = link)),
            );
        };
        if broker_first {
            connect_broker();
            connect_non_broker();
        } else {
            connect_non_broker();
            connect_broker();
        }
        let broker_link = broker_link.lock().unwrap().take().expect("broker link");
        let non_broker_link = non_broker_link.lock().unwrap().take().expect("non-broker link");
        (broker_link, non_broker_link)
    }

    fn end_to_end_success(broker_first: bool) {
        let t = NodeConnectorTest::new();
        let broker = t.broker(None);
        let non_broker = t.non_broker(None);

        let broker_routers = vec![Router::new()];
        let non_broker_routers = vec![Router::new()];
        let (broker_link, non_broker_link) = connect_both(
            &t,
            &broker,
            &non_broker,
            broker_routers.clone(),
            non_broker_routers.clone(),
            broker_first,
        );

        // Verify that sublink 0 on both NodeLinks corresponds to the
        // appropriate initial portals.
        assert!(Arc::ptr_eq(&broker_routers[0], &broker_link.get_router(SublinkId::new(0)).unwrap()));
        assert!(Arc::ptr_eq(
            &non_broker_routers[0],
            &non_broker_link.get_router(SublinkId::new(0)).unwrap()
        ));

        broker_routers[0].close_route();
        non_broker_routers[0].close_route();
        broker.shut_down();
        non_broker.shut_down();
    }

    #[test]
    fn end_to_end_success_broker_first() {
        end_to_end_success(true);
    }

    #[test]
    fn end_to_end_success_non_broker_first() {
        end_to_end_success(false);
    }

    #[test]
    fn multiple_initial_portals() {
        let t = NodeConnectorTest::new();
        let broker = t.broker(None);
        let non_broker = t.non_broker(None);

        // We establish multiple initial portals on connection, with a surplus on
        // the broker side to test that behavior as well.
        const NUM_BROKER_PORTALS: usize = 5;
        const NUM_NON_BROKER_PORTALS: usize = 3;

        let broker_routers: Vec<_> = (0..NUM_BROKER_PORTALS).map(|_| Router::new()).collect();
        let non_broker_routers: Vec<_> = (0..NUM_NON_BROKER_PORTALS).map(|_| Router::new()).collect();
        let (broker_link, non_broker_link) = connect_both(
            &t,
            &broker,
            &non_broker,
            broker_routers.clone(),
            non_broker_routers.clone(),
            true,
        );

        // Verify that the first sublinks of both NodeLinks correspond to
        // appropriate initial portals. Any surplus sublinks on the broker's
        // NodeLink should also correspond to an appropriate initial portal on
        // the broker side, but they should see that their peer is closed since
        // the non-broker established a smaller set of initial portals.
        for i in 0..NUM_NON_BROKER_PORTALS {
            let sublink = SublinkId::new(i as u64);
            assert!(Arc::ptr_eq(&broker_routers[i], &broker_link.get_router(sublink).unwrap()));
            assert!(Arc::ptr_eq(&non_broker_routers[i], &non_broker_link.get_router(sublink).unwrap()));
            broker_routers[i].close_route();
            non_broker_routers[i].close_route();
        }

        for router in &broker_routers[NUM_NON_BROKER_PORTALS..] {
            assert!(router.query_status().flags.contains(PortalStatusFlags::PEER_CLOSED));
        }

        broker.shut_down();
        non_broker.shut_down();
    }
}
