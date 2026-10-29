//! Port of `ipcz/node.{h,cc}`.
//!
//! DEVIATIONS: the driver is an `Arc<dyn Driver>` trait object instead of an
//! `IpczDriver` vtable; callbacks are boxed `FnOnce` closures; node creation
//! options are the plain [`CreateNodeOptions`] struct; `ConnectNode` returns
//! its initial portals as a vector of handles instead of filling a span.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use super::api_object::{self, ApiObject, ApiObjectType};
use super::driver::{Driver, DriverHandle};
use super::driver_memory::DriverMemory;
use super::driver_object::DriverObject;
use super::driver_transport::DriverTransport;
use super::features::Features;
use super::ipcz_result::IpczResult;
use super::ipcz_types::{ConnectNodeFlags, CreateNodeOptions, IpczHandle};
use super::link_side::LinkSide;
use super::node_connector::NodeConnector;
use super::node_link::NodeLink;
use super::node_link_memory::{MemoryNode, NodeLinkMemory};
use super::node_messages as msg;
use super::node_name::NodeName;
use super::node_type::NodeType;
use super::router::Router;


/// A callback invoked with the link established by `establish_link()`, or
/// `None` on failure.
pub type EstablishLinkCallback = Box<dyn FnOnce(Option<Arc<NodeLink>>) + Send>;

/// A callback invoked once a broker link is available.
pub type BrokerLinkCallback = Box<dyn FnOnce(Arc<NodeLink>) + Send>;

/// A callback invoked with newly allocated shared memory.
pub type AllocateSharedMemoryCallback = Box<dyn FnOnce(DriverMemory) + Send>;

/// Tracks a connection to a remote node: the link itself, and the broker who
/// introduced it (if any). A connection without a broker is a direct connection
/// established by the application.
#[derive(Clone)]
pub struct Connection {
    pub link: Arc<NodeLink>,
    pub broker: Option<Arc<NodeLink>>,
}

/// A pending introduction tracks progress of one or more outstanding
/// introduction requests for a single node in the system.
struct PendingIntroduction {
    /// Set of brokers from whom we're still expecting a reply for this pending
    /// introduction request.
    pending_replies: HashSet<NodeName>,

    /// Callbacks to be invoked once the introduction is finished.
    callbacks: Vec<EstablishLinkCallback>,
}

impl PendingIntroduction {
    /// Constructs a new object to track introduction of a specific node.
    /// `brokers` is the sequence of broker nodes queried for the introduction.
    fn new(brokers: &[Arc<NodeLink>]) -> Self {
        Self {
            pending_replies: brokers.iter().map(|broker| *broker.remote_node_name()).collect(),
            callbacks: Vec::new(),
        }
    }

    /// Indicates that all pending responses have come back indicating failure,
    /// and that the pending introduction itself has failed.
    fn failed(&self) -> bool {
        self.pending_replies.is_empty()
    }

    /// Marks a specific broker as having responded with rejection.
    fn notify_failure_from(&mut self, rejecting_broker: &NodeLink) {
        self.pending_replies.remove(rejecting_broker.remote_node_name());
    }

    /// Registers a new callback to be invoked once the introduction process has
    /// completed, regardless of success or failure.
    fn add_callback(&mut self, callback: EstablishLinkCallback) {
        self.callbacks.push(callback);
    }

    /// Runs all callbacks associated with this introduction. If the introduction
    /// failed, `result` will be `None`. Otherwise it's a link to the newly
    /// introduced remote node.
    fn finish(self, result: Option<&Arc<NodeLink>>) {
        self.callbacks.into_iter().for_each(|callback| callback(result.cloned()));
    }
}

#[derive(Default)]
struct NodeState {
    /// The name assigned to this node by its broker (or by itself, for a broker
    /// node).
    assigned_name: NodeName,

    /// A link to the node's broker, if this is a non-broker node.
    broker_link: Option<Arc<NodeLink>>,

    /// A link to the node which will be used to allocate shared memory on behalf
    /// of this node, if any.
    allocation_delegate_link: Option<Arc<NodeLink>>,

    /// Active connections to remote nodes, keyed by their names.
    connections: HashMap<NodeName, Connection>,

    /// Outstanding introduction requests, keyed by the name of the node being
    /// introduced.
    pending_introductions: HashMap<NodeName, PendingIntroduction>,

    /// Introductions this (broker) node is in the middle of performing between
    /// two other nodes, keyed by the pair of names in sorted order.
    in_progress_introductions: HashSet<(NodeName, NodeName)>,

    /// Callbacks waiting for this node to be connected to its broker.
    broker_link_callbacks: Vec<BrokerLinkCallback>,

    /// Links to other brokers, if this is a broker node.
    other_brokers: HashMap<NodeName, Arc<NodeLink>>,
}

/// A Node controls creation and interconnection of a group of routers which can
/// establish routes to and from other routers in other nodes.
pub struct Node {
    node_type: NodeType,
    driver: Arc<dyn Driver>,
    options: CreateNodeOptions,
    features: Features,
    state: Mutex<NodeState>,
}

impl Node {
    pub fn new(node_type: NodeType, driver: Arc<dyn Driver>, options: Option<&CreateNodeOptions>) -> Arc<Node> {
        let options = options.cloned().unwrap_or_default();
        let features = Features::from_node_options(&options.enabled_features, &options.disabled_features);
        let node = Arc::new(Node {
            node_type,
            driver,
            options,
            features,
            state: Mutex::new(NodeState::default()),
        });
        if node_type == NodeType::Broker {
            // Only brokers assign their own names.
            let name = node.generate_random_name();
            node.lock().assigned_name = name;
            log::trace!("Created new broker node {name}");
        } else {
            log::trace!("Created new non-broker node");
        }
        node
    }

    fn lock(&self) -> MutexGuard<'_, NodeState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn node_type(&self) -> NodeType {
        self.node_type
    }

    pub fn driver(&self) -> &Arc<dyn Driver> {
        &self.driver
    }

    pub fn options(&self) -> &CreateNodeOptions {
        &self.options
    }

    pub fn features(&self) -> &Features {
        &self.features
    }

    /// Connects this node to another node via `driver_transport`, which must be
    /// a transport handle created by this node's driver. Returns `num_portals`
    /// initial portals on success.
    pub fn connect_node(
        self: &Arc<Self>,
        driver_transport: DriverHandle,
        flags: ConnectNodeFlags,
        num_portals: usize,
    ) -> Result<Vec<IpczHandle>, IpczResult> {
        let routers: Vec<Arc<Router>> = (0..num_portals).map(|_| Router::new()).collect();
        let transport = DriverTransport::new(DriverObject::new(self.driver.clone(), driver_transport));
        let node = self.clone();
        let result = NodeConnector::connect_node(
            self.clone(),
            transport,
            flags,
            routers.clone(),
            Some(Box::new(move |link| {
                // If we fail to connect to a broker and we're not a broker
                // ourselves, we won't have luck communicating with anyone else
                // either.
                let is_remote_broker = flags.contains(ConnectNodeFlags::TO_BROKER);
                let is_local_broker = node.node_type() == NodeType::Broker;
                if link.is_none() && is_remote_broker && !is_local_broker {
                    node.notify_broker_link_dropped();
                }
            })),
        );
        match result {
            IpczResult::Ok => Ok(routers.into_iter().map(api_object::release_as_handle_typed).collect()),
            // The routers are invalid and effectively not returned to the caller
            // on failure, so they're simply dropped.
            error => Err(error),
        }
    }

    /// Returns the name assigned to this node, either by itself if it's a
    /// broker, or by its broker.
    pub fn get_assigned_name(&self) -> NodeName {
        self.lock().assigned_name
    }

    /// Returns the link to this node's broker, if any.
    pub fn get_broker_link(&self) -> Option<Arc<NodeLink>> {
        self.lock().broker_link.clone()
    }

    /// Registers a new connection to a remote node. Returns true on success.
    pub fn add_connection(self: &Arc<Self>, remote_node_name: NodeName, connection: Connection) -> bool {
        let callbacks = loop {
            let mut state = self.lock();
            if let Some(existing) = state.connections.get(&remote_node_name) {
                // Assume that if we're getting a new connection to an
                // already-known node, it must be because the application has
                // explicitly initiated a new connection to the same node and it
                // expects the previous connection to be replaced.
                let link = existing.link.clone();
                drop(state);
                self.drop_connection(&link);
                continue;
            }

            state.connections.insert(remote_node_name, connection.clone());
            let remote_is_broker = connection.link.remote_node_type() == NodeType::Broker;
            let local_is_broker = self.node_type == NodeType::Broker;
            if local_is_broker && remote_is_broker {
                // We're a broker, and this is a link to some other broker. We
                // retain a separate mapping of other brokers so they can be
                // consulted for introductions.
                state.other_brokers.insert(remote_node_name, connection.link.clone());
                break Vec::new();
            } else if remote_is_broker {
                // The first connection accepted by a non-broker must be a
                // connection to its own broker.
                debug_assert_eq!(state.connections.len(), 1);
                debug_assert!(state.broker_link.is_none());
                state.broker_link = Some(connection.link.clone());
                state.assigned_name = *connection.link.local_node_name();
                break std::mem::take(&mut state.broker_link_callbacks);
            }
            break Vec::new();
        };

        callbacks.into_iter().for_each(|callback| callback(connection.link.clone()));
        true
    }

    /// Retrieves the connection to a remote node, if any.
    pub fn get_connection(&self, name: &NodeName) -> Option<Connection> {
        self.lock().connections.get(name).cloned()
    }

    /// Retrieves only the link of the connection to a remote node, if any.
    pub fn get_link(&self, name: &NodeName) -> Option<Arc<NodeLink>> {
        self.lock().connections.get(name).map(|connection| connection.link.clone())
    }

    /// Generates a new random node name using this node's driver.
    pub fn generate_random_name(&self) -> NodeName {
        let mut bytes = [0u8; 16];
        let result = self.driver.generate_random_bytes(&mut bytes);
        assert_eq!(result, IpczResult::Ok);
        let (high, low) = bytes.split_at(8);
        NodeName::new(
            u64::from_ne_bytes(high.try_into().expect("8 bytes")),
            u64::from_ne_bytes(low.try_into().expect("8 bytes")),
        )
    }

    /// Sets the link over which shared memory is allocated on this node's
    /// behalf.
    pub fn set_allocation_delegate(&self, link: Option<Arc<NodeLink>>) {
        self.lock().allocation_delegate_link = link;
    }

    /// Allocates shared memory of `size` bytes, either directly or via the
    /// allocation delegate. `callback` may be invoked synchronously.
    pub fn allocate_shared_memory(&self, size: usize, callback: AllocateSharedMemoryCallback) {
        let delegate = self.lock().allocation_delegate_link.clone();
        match delegate {
            Some(delegate) => delegate.request_memory(size, callback),
            None => callback(DriverMemory::allocate(&self.driver, size)),
        }
    }

    /// Eventually invokes `callback` with a link to the node named `name`, which
    /// may involve requesting an introduction from one or more brokers. If a
    /// link already exists, `callback` is invoked immediately.
    pub fn establish_link(self: &Arc<Self>, name: &NodeName, callback: EstablishLinkCallback) {
        let mut brokers_to_query: Vec<Arc<NodeLink>> = Vec::new();
        let existing_link;
        {
            let mut state = self.lock();
            existing_link = state.connections.get(name).map(|connection| connection.link.clone());
            if existing_link.is_none() {
                match &state.broker_link {
                    Some(broker_link) if self.node_type == NodeType::Normal => {
                        brokers_to_query.push(broker_link.clone());
                    }
                    _ => brokers_to_query.extend(state.other_brokers.values().cloned()),
                }

                if !brokers_to_query.is_empty() {
                    let mut inserted = false;
                    state
                        .pending_introductions
                        .entry(*name)
                        .or_insert_with(|| {
                            inserted = true;
                            PendingIntroduction::new(&brokers_to_query)
                        })
                        .add_callback(callback);
                    if !inserted {
                        // There was already a pending introduction we can wait
                        // for.
                        return;
                    }
                    drop(state);
                    brokers_to_query.iter().for_each(|broker| broker.request_introduction(name));
                    return;
                }
            }
        }

        // NOTE: `existing_link` may be `None` here, implying that we have
        // failed.
        callback(existing_link);
    }

    /// Handles an introduction request from `from_node_link` for `for_node`.
    /// Only brokers receive these.
    pub fn handle_introduction_request(self: &Arc<Self>, from_node_link: &Arc<NodeLink>, for_node: &NodeName) {
        // NodeLink must never accept these requests on non-broker nodes.
        assert_eq!(self.node_type, NodeType::Broker);

        let requestor = *from_node_link.remote_node_name();
        log::trace!(
            "Broker {} received introduction request for {} from {}",
            from_node_link.local_node_name(),
            for_node,
            requestor
        );

        let Some(target_connection) = self.get_connection(for_node) else {
            // We are not familiar with the requested node. Attempt to establish
            // our own link to it first, then try again.
            let node = self.clone();
            let requestor_link = from_node_link.clone();
            let name = *for_node;
            self.establish_link(
                for_node,
                Box::new(move |link| {
                    if link.is_none() {
                        requestor_link.reject_introduction(&name);
                        return;
                    }

                    node.handle_introduction_request(&requestor_link, &name);
                }),
            );
            return;
        };

        let is_target_in_network = target_connection.broker.is_none();
        let is_target_broker = target_connection
            .broker
            .as_ref()
            .is_some_and(|broker| Arc::ptr_eq(&target_connection.link, broker));
        let is_requestor_broker = from_node_link.remote_node_type() == NodeType::Broker;
        if is_requestor_broker && is_target_broker {
            log::error!("Invalid introduction request from broker {requestor} for broker {for_node}");
            return;
        }

        let target_broker = target_connection.broker.as_ref();
        if is_target_broker
            || is_requestor_broker
            || is_target_in_network
            || target_broker.is_some_and(|broker| broker.link_side().is_side_a())
        {
            // If one of the two nodes being introduced is a broker, or if the
            // target is in-network (which implies the requestor is too, if it's
            // not a broker) then we are the only node that can introduce these
            // two nodes.
            //
            // Otherwise if this is an introduction between two non-brokers in
            // separate networks, by convention we can only perform the
            // introduction if we're on side A of the link between the two
            // relevant brokers.
            self.introduce_remote_nodes(from_node_link, &target_connection.link);
            return;
        }

        // This is an introduction between two non-brokers in separate networks,
        // and we (one of the networks' brokers) are on side B of the link to the
        // other network's broker. This introduction is therefore the other
        // broker's responsibility.
        let mut request = msg::RequestIndirectIntroduction::new();
        let v0 = request.v0_mut().expect("v0");
        v0.source_node = *from_node_link.remote_node_name();
        v0.target_node = *target_connection.link.remote_node_name();
        target_broker.expect("broker").transmit(&mut request);
    }

    /// Accepts an introduction from a broker to another node.
    #[allow(clippy::too_many_arguments)]
    pub fn accept_introduction(
        self: &Arc<Self>,
        from_node_link: &Arc<NodeLink>,
        name: &NodeName,
        side: LinkSide,
        remote_node_type: NodeType,
        remote_protocol_version: u32,
        remote_features: &Features,
        transport: Arc<DriverTransport>,
        memory: Arc<NodeLinkMemory>,
    ) {
        // NodeLink should never dispatch this method to a node if the
        // introduction didn't come from a broker, so this assertion should
        // always hold.
        assert_eq!(from_node_link.remote_node_type(), NodeType::Broker);

        let local_name = *from_node_link.local_node_name();
        log::trace!(
            "Node {local_name} received introduction to {name} from broker {}",
            from_node_link.remote_node_name()
        );

        let new_link = NodeLink::create_inactive(
            self.clone(),
            side,
            local_name,
            *name,
            remote_node_type,
            remote_protocol_version,
            *remote_features,
            transport,
            memory,
        );

        let pending_introduction;
        {
            let mut state = self.lock();
            if self.node_type == NodeType::Normal && state.broker_link.is_none() {
                // If we've lost our broker connection, we should ignore any
                // further introductions that arrive.
                return;
            }

            if state.connections.contains_key(name) {
                // If both nodes race to request an introduction to each other,
                // the broker may send redundant introductions. It does however
                // take care to ensure that they're ordered consistently across
                // both nodes, so redundant introductions can be safely ignored
                // by convention.
                return;
            }
            state.connections.insert(
                *name,
                Connection {
                    link: new_link.clone(),
                    broker: Some(from_node_link.clone()),
                },
            );

            // If this node requested this introduction, we may have callbacks to
            // run. Note that it is not an error to receive an unrequested
            // introduction, since it is only necessary for one of the introduced
            // nodes to have requested it.
            pending_introduction = state.pending_introductions.remove(name);
        }

        new_link.activate();
        if let Some(pending_introduction) = pending_introduction {
            pending_introduction.finish(Some(&new_link));
        }
    }

    /// Handles a rejection of an introduction request from `from_broker`.
    pub fn notify_introduction_failed(&self, from_broker: &NodeLink, name: &NodeName) {
        let failed_introduction;
        {
            let mut state = self.lock();
            let Some(intro) = state.pending_introductions.get_mut(name) else {
                return;
            };

            intro.notify_failure_from(from_broker);
            if !intro.failed() {
                // We're still waiting for replies from one or more other
                // brokers.
                return;
            }

            failed_introduction = state.pending_introductions.remove(name);
        }

        if let Some(intro) = failed_introduction {
            intro.finish(None);
        }
    }

    /// Relays a message from `from_node` to its destination. Only brokers do
    /// this.
    pub fn relay_message(&self, from_node: &NodeName, relay: &mut msg::RelayMessage) -> bool {
        assert_eq!(self.node_type, NodeType::Broker);
        let (destination, data_offset) = {
            let v0 = relay.v0().expect("v0");
            (v0.destination, v0.data)
        };
        let Some(link) = self.get_link(&destination) else {
            return true;
        };

        let data = relay.get_array_view::<u8>(data_offset as usize).to_vec();
        let mut accept = msg::AcceptRelayedMessage::new();
        let data_offset = accept.allocate_and_set_array(&data);
        let objects = accept.append_driver_objects(relay.driver_objects());
        let v0 = accept.v0_mut().expect("v0");
        v0.source = *from_node;
        v0.data = data_offset;
        v0.padding = 0;
        v0.driver_objects = objects;
        link.transmit(&mut accept);
        true
    }

    /// Dispatches a message which was relayed by the broker.
    pub fn accept_relayed_message(&self, accept: &mut msg::AcceptRelayedMessage) -> bool {
        let source = accept.v0().expect("v0").source;
        match self.get_link(&source) {
            Some(link) => link.dispatch_relayed_message(accept),
            None => true,
        }
    }

    /// Drops the connection identified by `connection_link`, if it's still the
    /// registered connection to its remote node.
    pub fn drop_connection(self: &Arc<Self>, connection_link: &Arc<NodeLink>) {
        let link;
        let pending_introductions: Vec<NodeName>;
        let mut lost_broker = false;
        {
            let mut state = self.lock();
            match state.connections.get(connection_link.remote_node_name()) {
                Some(connection) if Arc::ptr_eq(&connection.link, connection_link) => {}
                _ => return,
            }
            link = state
                .connections
                .remove(connection_link.remote_node_name())
                .expect("connection exists")
                .link;

            let local_name = link.local_node_name();
            log::trace!("Node {local_name} dropping link to {}", link.remote_node_name());
            if state.broker_link.as_ref().is_some_and(|broker| Arc::ptr_eq(broker, &link)) {
                log::trace!("Node {local_name} lost its broker link");
                state.broker_link = None;
                lost_broker = true;
            } else if link.remote_node_type() == NodeType::Broker {
                state.other_brokers.remove(link.remote_node_name());
            }

            if state.allocation_delegate_link.as_ref().is_some_and(|delegate| Arc::ptr_eq(delegate, &link)) {
                log::trace!("Node {local_name} lost its allocation delegate");
                state.allocation_delegate_link = None;
            }

            // Accumulate the set of currently pending introductions. If any of
            // them are awaiting a response from the dropped link, their
            // expectations will be updated accordingly by
            // `notify_introduction_failed()` below.
            pending_introductions = state.pending_introductions.keys().copied().collect();
        }

        link.deactivate();

        if lost_broker {
            self.notify_broker_link_dropped();
        } else {
            pending_introductions
                .iter()
                .for_each(|target| self.notify_introduction_failed(&link, target));
        }
    }

    /// Invokes `callback` as soon as this node has a broker link.
    pub fn wait_for_broker_link_async(&self, callback: BrokerLinkCallback) {
        let broker_link = {
            let mut state = self.lock();
            match state.broker_link.clone() {
                Some(link) => link,
                None => {
                    state.broker_link_callbacks.push(callback);
                    return;
                }
            }
        };

        callback(broker_link);
    }

    /// Handles a request from another broker (`from_node_link`) to introduce
    /// `their_node` to `our_node`, one of this broker's own connections.
    pub fn handle_indirect_introduction_request(
        self: &Arc<Self>,
        from_node_link: &Arc<NodeLink>,
        our_node: &NodeName,
        their_node: &NodeName,
    ) -> bool {
        // Enforced by NodeLink before dispatching to this Node.
        assert_eq!(self.node_type, NodeType::Broker);
        assert_eq!(from_node_link.remote_node_type(), NodeType::Broker);
        assert!(from_node_link.link_side().is_side_a());

        let Some(connection_to_their_node) = self.get_connection(their_node) else {
            // We need to establish our own direct connection to `their_node`
            // before we can help introduce it to `our_node`.
            let node = self.clone();
            let their_broker = from_node_link.clone();
            let (our_node, their_node) = (*our_node, *their_node);
            self.establish_link(
                &their_node,
                Box::new(move |link| {
                    if link.is_none() {
                        // If we could not get our own link to the identified
                        // node, then we can't complete the introduction request.
                        // Notify our own node of the failure in case they were
                        // awaiting such an introduction.
                        if let Some(connection_to_our_node) = node.get_connection(&our_node) {
                            connection_to_our_node.link.reject_introduction(&their_node);
                        }
                        return;
                    }
                    node.handle_indirect_introduction_request(&their_broker, &our_node, &their_node);
                }),
            );
            return true;
        };

        let Some(connection_to_our_node) = self.get_connection(our_node) else {
            // We may have lost this connection for any number of reasons, but in
            // any case we can't fulfill the request.
            connection_to_their_node.link.reject_introduction(our_node);
            return true;
        };

        self.introduce_remote_nodes(&connection_to_our_node.link, &connection_to_their_node.link);
        true
    }

    /// Closes the node, dropping all of its connections.
    pub fn shut_down(&self) {
        let connections;
        {
            let mut state = self.lock();
            connections = std::mem::take(&mut state.connections);
            state.broker_link = None;
            // Callbacks waiting for a broker link are discarded, not run.
            state.broker_link_callbacks.clear();
            state.allocation_delegate_link = None;
            state.other_brokers.clear();
            state.assigned_name = NodeName::default();
        }

        connections.values().for_each(|connection| connection.link.deactivate());
        self.cancel_all_introductions();
    }

    fn cancel_all_introductions(&self) {
        let introductions = std::mem::take(&mut self.lock().pending_introductions);
        introductions.into_values().for_each(|intro| intro.finish(None));
    }

    fn introduce_remote_nodes(&self, first: &Arc<NodeLink>, second: &Arc<NodeLink>) {
        // Ensure that no other thread does the same introduction concurrently.
        let first_name = *first.remote_node_name();
        let second_name = *second.remote_node_name();
        let key = if first_name < second_name {
            (first_name, second_name)
        } else {
            (second_name, first_name)
        };
        if !self.lock().in_progress_introductions.insert(key) {
            return;
        }

        struct RemoveIntroKey<'a>(&'a Node, (NodeName, NodeName));
        impl Drop for RemoveIntroKey<'_> {
            fn drop(&mut self) {
                self.0.lock().in_progress_introductions.remove(&self.1);
            }
        }
        let _remove_intro_key = RemoveIntroKey(self, key);

        let buffer = NodeLinkMemory::allocate_memory(&self.driver);
        if !buffer.memory.is_valid() {
            return;
        }

        let cloned_buffer = buffer.memory.clone_memory();
        if !cloned_buffer.is_valid() {
            return;
        }

        let (transport_for_first_node, transport_for_second_node) =
            DriverTransport::create_pair(&self.driver, Some(first.transport()), Some(second.transport()));
        first.accept_introduction(
            &second_name,
            LinkSide::A,
            second.remote_node_type(),
            second.remote_protocol_version(),
            second.remote_features(),
            transport_for_first_node,
            cloned_buffer,
        );
        second.accept_introduction(
            &first_name,
            LinkSide::B,
            first.remote_node_type(),
            first.remote_protocol_version(),
            first.remote_features(),
            transport_for_second_node,
            buffer.memory,
        );
    }

    fn notify_broker_link_dropped(&self) {
        // Break all connections if the broker is lost. In practice we should
        // only need to break connections which were introduced by the lost
        // broker, but there's less risk of weird future inconsistencies if we
        // just say that as a rule, primary broker disconnection serves as a sort
        // of "reset" for a node. The node can be re-connected to a broker and
        // continue operating normally from there.
        self.shut_down();
    }
}

impl MemoryNode for Node {
    fn features(&self) -> Features {
        self.features
    }

    fn memory_flags(&self) -> u32 {
        self.options.memory_flags
    }

    fn allocate_shared_memory(&self, size: usize, callback: Box<dyn FnOnce(DriverMemory) + Send>) {
        Node::allocate_shared_memory(self, size, callback);
    }
}

impl ApiObject for Node {
    fn object_type(&self) -> ApiObjectType {
        ApiObjectType::Node
    }

    fn close(self: Arc<Self>) -> IpczResult {
        self.shut_down();
        IpczResult::Ok
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::Driver;
    use crate::core::reference_drivers::sync::SyncDriver;
    use std::sync::atomic::{AtomicBool, Ordering};

    const NODE_A_NAME: NodeName = NodeName::new(0, 1);
    const NODE_B_NAME: NodeName = NodeName::new(1, 2);
    const NODE_C_NAME: NodeName = NodeName::new(3, 5);

    struct NodeTest {
        broker: Arc<Node>,
        node_a: Arc<Node>,
        node_b: Arc<Node>,
    }

    impl NodeTest {
        fn new() -> Self {
            let driver = SyncDriver::new();
            let test = Self {
                broker: Node::new(NodeType::Broker, driver.clone(), None),
                node_a: Node::new(NodeType::Normal, driver.clone(), None),
                node_b: Node::new(NodeType::Normal, driver.clone(), None),
            };
            test.connect_broker_to_node(&driver, &test.node_a, NODE_A_NAME);
            test.connect_broker_to_node(&driver, &test.node_b, NODE_B_NAME);
            test
        }

        fn broker_name(&self) -> NodeName {
            self.broker.get_assigned_name()
        }

        fn connect_broker_to_node(&self, driver: &Arc<SyncDriver>, node: &Arc<Node>, name: NodeName) {
            let dyn_driver: Arc<dyn Driver> = driver.clone();
            let (handle0, handle1) = driver.create_transports(Default::default(), Default::default()).unwrap();
            let transport0 = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle0));
            let transport1 = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle1));
            let buffer = NodeLinkMemory::allocate_memory(&dyn_driver);
            let broker_name = self.broker_name();
            let broker_memory_node: Arc<dyn MemoryNode> = self.broker.clone();
            let node_memory_node: Arc<dyn MemoryNode> = node.clone();
            let broker_link = NodeLink::create_inactive(
                self.broker.clone(),
                LinkSide::A,
                broker_name,
                name,
                NodeType::Normal,
                0,
                Features::default(),
                transport0,
                NodeLinkMemory::create(broker_memory_node, LinkSide::A, &Features::default(), buffer.mapping),
            );
            let node_link = NodeLink::create_inactive(
                node.clone(),
                LinkSide::B,
                name,
                broker_name,
                NodeType::Broker,
                0,
                Features::default(),
                transport1,
                NodeLinkMemory::create(node_memory_node, LinkSide::B, &Features::default(), buffer.memory.map()),
            );
            self.broker.add_connection(
                name,
                Connection {
                    link: broker_link.clone(),
                    broker: None,
                },
            );
            node.add_connection(
                broker_name,
                Connection {
                    link: node_link.clone(),
                    broker: Some(node_link.clone()),
                },
            );
            broker_link.activate();
            node_link.activate();
        }
    }

    impl Drop for NodeTest {
        fn drop(&mut self) {
            self.node_b.shut_down();
            self.node_a.shut_down();
            self.broker.shut_down();
        }
    }

    /// A callback which asserts that it's invoked with `expected` (or, if
    /// `None`, with no link), raising `called`.
    fn expect_link(expected: Option<Arc<NodeLink>>, called: &Arc<AtomicBool>) -> EstablishLinkCallback {
        let called = called.clone();
        Box::new(move |link| {
            match (&expected, &link) {
                (Some(expected), Some(link)) => assert!(Arc::ptr_eq(expected, link)),
                (None, None) => {}
                _ => panic!("unexpected link result"),
            }
            called.store(true, Ordering::SeqCst);
        })
    }

    fn take_called(called: &Arc<AtomicBool>) -> bool {
        called.swap(false, Ordering::SeqCst)
    }

    #[test]
    fn establish_existing_links() {
        // When the requested node is already known, `establish_link()` responds
        // immediately with the existing NodeLink.
        let t = NodeTest::new();
        let called = Arc::new(AtomicBool::new(false));

        let broker_to_a = t.broker.get_link(&NODE_A_NAME).expect("link");
        t.broker.establish_link(&NODE_A_NAME, expect_link(Some(broker_to_a), &called));
        assert!(take_called(&called));

        let broker_to_b = t.broker.get_link(&NODE_B_NAME).expect("link");
        t.broker.establish_link(&NODE_B_NAME, expect_link(Some(broker_to_b), &called));
        assert!(take_called(&called));

        let a_to_broker = t.node_a.get_link(&t.broker_name()).expect("link");
        t.node_a.establish_link(&t.broker_name(), expect_link(Some(a_to_broker), &called));
        assert!(take_called(&called));

        let b_to_broker = t.node_b.get_link(&t.broker_name()).expect("link");
        t.node_b.establish_link(&t.broker_name(), expect_link(Some(b_to_broker), &called));
        assert!(take_called(&called));
    }

    #[test]
    fn establish_new_links() {
        // When the requested node is not yet known to the caller but is known to
        // their broker, `establish_link()` coordinates with the broker to get
        // the two nodes introduced to each other.
        let t = NodeTest::new();

        let established_link = Arc::new(Mutex::new(None));
        assert!(t.node_a.get_link(&NODE_B_NAME).is_none());
        assert!(t.node_b.get_link(&NODE_A_NAME).is_none());
        let slot = established_link.clone();
        t.node_a
            .establish_link(&NODE_B_NAME, Box::new(move |link| *slot.lock().unwrap() = link));
        let established_link = established_link.lock().unwrap().take().expect("established link");

        let a_to_b = t.node_a.get_link(&NODE_B_NAME).expect("link");
        let b_to_a = t.node_b.get_link(&NODE_A_NAME).expect("link");
        assert!(Arc::ptr_eq(&a_to_b, &established_link));

        // A redundant `establish_link()` changes nothing, even from the other
        // side.
        let called = Arc::new(AtomicBool::new(false));
        t.node_a
            .establish_link(&NODE_B_NAME, expect_link(Some(a_to_b.clone()), &called));
        t.node_b
            .establish_link(&NODE_A_NAME, expect_link(Some(b_to_a.clone()), &called));
        assert!(Arc::ptr_eq(&a_to_b, &t.node_a.get_link(&NODE_B_NAME).unwrap()));
        assert!(Arc::ptr_eq(&b_to_a, &t.node_b.get_link(&NODE_A_NAME).unwrap()));

        // Verify that the new links are managing a common buffer pool.
        const MAGIC: u64 = 0x1123581321345589;
        let a_fragment = a_to_b.memory().allocate_fragment(8);
        assert!(a_fragment.is_addressable());
        // SAFETY: the fragment is addressable and at least 8 bytes, 8-aligned.
        unsafe { a_fragment.address().cast::<u64>().write(MAGIC) };

        let b_fragment = b_to_a.memory().get_fragment(a_fragment.descriptor());
        assert!(b_fragment.is_addressable());
        // SAFETY: as above, for the peer's mapping of the same fragment.
        unsafe {
            assert_eq!(MAGIC, b_fragment.address().cast::<u64>().read());
            b_fragment.address().cast::<u64>().write(0);
            assert_eq!(0, a_fragment.address().cast::<u64>().read());
        }
    }

    #[test]
    fn establish_link_failure_from_non_broker() {
        // If the named node is unknown to the broker, a link can't be
        // established by a non-broker.
        let t = NodeTest::new();
        let failed = Arc::new(AtomicBool::new(false));
        assert!(t.broker.get_link(&NODE_C_NAME).is_none());
        assert!(t.node_a.get_link(&NODE_C_NAME).is_none());
        t.node_a.establish_link(&NODE_C_NAME, expect_link(None, &failed));
        assert!(take_called(&failed));
    }

    #[test]
    fn establish_link_failure_from_broker() {
        // New links can't be automatically established by the broker.
        let t = NodeTest::new();
        let failed = Arc::new(AtomicBool::new(false));
        assert!(t.broker.get_link(&NODE_C_NAME).is_none());
        t.broker.establish_link(&NODE_C_NAME, expect_link(None, &failed));
        assert!(take_called(&failed));
    }

    #[test]
    fn establish_link_failure_without_broker_link() {
        // A node with no broker link can't be introduced to anyone.
        let t = NodeTest::new();
        let failed = Arc::new(AtomicBool::new(false));
        let node_c = Node::new(NodeType::Normal, SyncDriver::new(), None);
        assert!(t.broker.get_link(&NODE_A_NAME).is_some());
        node_c.establish_link(&NODE_A_NAME, expect_link(None, &failed));
        assert!(take_called(&failed));
    }
}
