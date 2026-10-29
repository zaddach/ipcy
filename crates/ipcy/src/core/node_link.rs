//! Port of `ipcz/node_link.{h,cc}`.
//!
//! A NodeLink represents the local end of a link between the local node and a
//! remote node. A NodeLink is used to transmit messages, shared memory and
//! other resources between the two nodes, and is the owner of all the
//! RemoteRouterLinks (sublinks) which run over it.
//!
//! DEVIATIONS: callbacks are boxed `FnOnce` closures, message handlers receive
//! messages by value, and handlers which need a strong reference to the link
//! recover one from a weak self-reference.

use std::collections::{HashMap, HashSet};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::time::{Duration, Instant};

use super::api_object::ApiObject;
use super::box_object::{ApiBox, BoxType};
use super::buffer_id::BufferId;
use super::driver::{DriverHandle, RawMessage};
use super::driver_memory::DriverMemory;
use super::driver_object::DriverObject;
use super::driver_transport::{DriverTransport, TransportListener};
use super::features::Features;
use super::fragment_descriptor::FragmentDescriptor;
use super::fragment_ref::FragmentRef;
use super::handle_type::HandleType;
use super::link_side::LinkSide;
use super::link_type::LinkType;
use super::message::internal::MessageHeaderV0;
use super::message::{Message, WireEnum};
use super::node::Node;
use super::node_connector::NodeConnector;
use super::node_link_memory::{MemoryLink, MemoryNode, NodeLinkMemory};
use super::node_messages::{self as msg, NodeMessageListener};
use super::node_name::NodeName;
use super::node_type::NodeType;
use super::parcel::{Parcel, MAX_SUBPARCELS_PER_PARCEL};
use super::parcel_wrapper::ParcelWrapper;
use super::remote_router_link::RemoteRouterLink;
use super::router::{FlushBehavior, Router};
use super::router_descriptor::RouterDescriptor;
use super::router_link::RouterLink;
use super::router_link_state::RouterLinkState;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;

/// A callback invoked with the link to a referred node and its number of
/// initial portals, or `None` on failure.
pub type ReferralCallback = Box<dyn FnOnce(Option<Arc<NodeLink>>, u32) + Send>;

/// A callback invoked with memory provided by the remote node.
pub type RequestMemoryCallback = Box<dyn FnOnce(DriverMemory) + Send>;

/// A RemoteRouterLink and the Router it delivers to.
#[derive(Clone)]
pub struct Sublink {
    pub router_link: Arc<RemoteRouterLink>,
    pub receiver: Arc<Router>,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ActivationState {
    NeverActivated,
    Active,
    Deactivated,
}

/// A set of recently deleted sublinks, which forgets IDs after a while so that
/// it can't grow without bound. IDs live for between 5 and 10 minutes.
struct AutoClearedSublinkSet {
    previous: HashSet<SublinkId>,
    current: HashSet<SublinkId>,
    last_clear_time: Instant,
}

impl AutoClearedSublinkSet {
    const CLEAR_DURATION: Duration = Duration::from_secs(5 * 60);

    fn new() -> Self {
        Self {
            previous: HashSet::new(),
            current: HashSet::new(),
            last_clear_time: Instant::now(),
        }
    }

    fn clear_expired_ids_if_needed(&mut self) {
        if self.current.is_empty() && self.previous.is_empty() {
            // Fast path to avoid measuring time ticks.
            return;
        }

        let now = Instant::now();
        if now - self.last_clear_time > Self::CLEAR_DURATION {
            self.previous.clear();
            std::mem::swap(&mut self.previous, &mut self.current);
            self.last_clear_time = now;
        }
    }

    fn insert(&mut self, id: SublinkId) {
        self.clear_expired_ids_if_needed();
        self.current.insert(id);
    }

    fn contains(&mut self, id: SublinkId) -> bool {
        self.clear_expired_ids_if_needed();
        self.current.contains(&id) || self.previous.contains(&id)
    }

    fn size(&self) -> usize {
        self.previous.len() + self.current.len()
    }

    fn clear(&mut self) {
        self.previous.clear();
        self.current.clear();
        self.last_clear_time = Instant::now();
    }
}

#[derive(Default)]
struct SubparcelTracker {
    num_subparcels_received: usize,
    subparcels: Vec<Option<Arc<ParcelWrapper>>>,
}

struct NodeLinkState {
    activation_state: ActivationState,
    sublinks: HashMap<SublinkId, Sublink>,
    deleted_sublinks: AutoClearedSublinkSet,

    /// Callbacks for pending memory requests, by requested size.
    pending_memory_requests: HashMap<u32, Vec<RequestMemoryCallback>>,

    /// Parcels which have arrived without their driver objects (or vice versa),
    /// waiting for the other half.
    partial_parcels: HashMap<(SublinkId, SequenceNumber), Box<Parcel>>,

    subparcel_trackers: HashMap<(SublinkId, SequenceNumber), SubparcelTracker>,

    /// Parcels which arrived for sublinks we don't know yet.
    #[allow(clippy::vec_box)]
    early_parcels_for_sublink: HashMap<SublinkId, Vec<Box<Parcel>>>,

    next_referral_id: u64,
    pending_referrals: HashMap<u64, ReferralCallback>,
}

pub struct NodeLink {
    weak_self: Weak<NodeLink>,
    node: Arc<Node>,
    link_side: LinkSide,
    local_node_name: NodeName,
    remote_node_name: NodeName,
    remote_node_type: NodeType,
    remote_protocol_version: u32,
    remote_features: Features,
    available_features: Features,
    transport: Arc<DriverTransport>,
    memory: Arc<NodeLinkMemory>,
    next_outgoing_sequence_number: AtomicU64,
    state: Mutex<NodeLinkState>,
}

impl NodeLink {
    /// Creates a new NodeLink over `transport` and `memory`, which is
    /// immediately active.
    #[allow(clippy::too_many_arguments)]
    pub fn create_active(
        node: Arc<Node>,
        link_side: LinkSide,
        local_node_name: NodeName,
        remote_node_name: NodeName,
        remote_node_type: NodeType,
        remote_protocol_version: u32,
        remote_features: Features,
        transport: Arc<DriverTransport>,
        memory: Arc<NodeLinkMemory>,
    ) -> Arc<NodeLink> {
        let link = Self::new(
            node,
            link_side,
            local_node_name,
            remote_node_name,
            remote_node_type,
            remote_protocol_version,
            remote_features,
            transport,
            memory,
            ActivationState::Active,
        );
        link.transport.set_listener(Some(link.clone()));
        link.memory.set_node_link(Some(link.clone()));
        link
    }

    /// Like [`Self::create_active`], but the link doesn't start receiving
    /// messages until `activate()` is called.
    #[allow(clippy::too_many_arguments)]
    pub fn create_inactive(
        node: Arc<Node>,
        link_side: LinkSide,
        local_node_name: NodeName,
        remote_node_name: NodeName,
        remote_node_type: NodeType,
        remote_protocol_version: u32,
        remote_features: Features,
        transport: Arc<DriverTransport>,
        memory: Arc<NodeLinkMemory>,
    ) -> Arc<NodeLink> {
        Self::new(
            node,
            link_side,
            local_node_name,
            remote_node_name,
            remote_node_type,
            remote_protocol_version,
            remote_features,
            transport,
            memory,
            ActivationState::NeverActivated,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new(
        node: Arc<Node>,
        link_side: LinkSide,
        local_node_name: NodeName,
        remote_node_name: NodeName,
        remote_node_type: NodeType,
        remote_protocol_version: u32,
        remote_features: Features,
        transport: Arc<DriverTransport>,
        memory: Arc<NodeLinkMemory>,
        initial_activation_state: ActivationState,
    ) -> Arc<NodeLink> {
        let available_features = node.features().intersect(&remote_features);
        Arc::new_cyclic(|weak_self| NodeLink {
            weak_self: weak_self.clone(),
            node,
            link_side,
            local_node_name,
            remote_node_name,
            remote_node_type,
            remote_protocol_version,
            remote_features,
            available_features,
            transport,
            memory,
            next_outgoing_sequence_number: AtomicU64::new(0),
            state: Mutex::new(NodeLinkState {
                activation_state: initial_activation_state,
                sublinks: HashMap::new(),
                deleted_sublinks: AutoClearedSublinkSet::new(),
                pending_memory_requests: HashMap::new(),
                partial_parcels: HashMap::new(),
                subparcel_trackers: HashMap::new(),
                early_parcels_for_sublink: HashMap::new(),
                next_referral_id: 0,
                pending_referrals: HashMap::new(),
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, NodeLinkState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    // A strong reference to this link. Links are always created behind an Arc,
    // and callers can't be running a method on a link which has been dropped.
    fn arc(&self) -> Arc<NodeLink> {
        self.weak_self.upgrade().expect("NodeLink is alive while in use")
    }

    pub fn node(&self) -> &Arc<Node> {
        &self.node
    }

    pub fn link_side(&self) -> LinkSide {
        self.link_side
    }

    pub fn local_node_name(&self) -> &NodeName {
        &self.local_node_name
    }

    pub fn remote_node_name(&self) -> &NodeName {
        &self.remote_node_name
    }

    pub fn remote_node_type(&self) -> NodeType {
        self.remote_node_type
    }

    pub fn remote_protocol_version(&self) -> u32 {
        self.remote_protocol_version
    }

    pub fn remote_features(&self) -> &Features {
        &self.remote_features
    }

    pub fn available_features(&self) -> &Features {
        &self.available_features
    }

    pub fn transport(&self) -> &Arc<DriverTransport> {
        &self.transport
    }

    pub fn memory(&self) -> &Arc<NodeLinkMemory> {
        &self.memory
    }

    /// Begins receiving messages on this link. Must only be called once, and
    /// only on a link created inactive.
    pub fn activate(&self) {
        let this = self.arc();
        self.transport.set_listener(Some(this.clone()));
        self.memory.set_node_link(Some(this));

        {
            let mut state = self.lock();
            assert_eq!(state.activation_state, ActivationState::NeverActivated);
            state.activation_state = ActivationState::Active;
        }

        self.transport.activate();
    }

    /// Binds `sublink` on this NodeLink to `router`, using a new
    /// RemoteRouterLink. Returns `None` if the link was already deactivated or
    /// the sublink ID is already in use.
    pub fn add_remote_router_link(
        &self,
        sublink: SublinkId,
        link_state: FragmentRef<RouterLinkState>,
        link_type: LinkType,
        side: LinkSide,
        router: Arc<Router>,
    ) -> Option<Arc<RemoteRouterLink>> {
        let link = RemoteRouterLink::create(self.arc(), sublink, link_state, link_type, side);

        let mut state = self.lock();
        if state.activation_state == ActivationState::Deactivated {
            // We don't bind new RemoteRouterLinks once we've been deactivated,
            // lest we incur leaky NodeLink references.
            return None;
        }

        log::trace!("Adding sublink {sublink}");
        match state.sublinks.entry(sublink) {
            std::collections::hash_map::Entry::Occupied(_) => {
                // The SublinkId provided here may have been received from
                // another node and may already be in use if the node is
                // misbehaving.
                None
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Sublink {
                    router_link: link.clone(),
                    receiver: router,
                });
                Some(link)
            }
        }
    }

    /// Removes the RemoteRouterLink bound to `sublink`.
    pub fn remove_remote_router_link(&self, sublink: SublinkId) {
        let removed = {
            let mut state = self.lock();
            log::trace!("Removing RemoteRouterLink for sublink {sublink}");
            state.deleted_sublinks.insert(sublink);
            state.sublinks.remove(&sublink)
        };
        // Release the sublink outside of the lock, since it may hold the last
        // reference to a Router.
        drop(removed);
    }

    pub fn get_sublink(&self, sublink: SublinkId) -> Option<Sublink> {
        self.lock().sublinks.get(&sublink).cloned()
    }

    pub fn get_router(&self, sublink: SublinkId) -> Option<Arc<Router>> {
        self.lock().sublinks.get(&sublink).map(|s| s.receiver.clone())
    }

    /// Shares a new block buffer with the remote node.
    pub fn add_block_buffer(&self, id: BufferId, block_size: u32, mut memory: DriverMemory) {
        let mut add = msg::AddBlockBuffer::new();
        let buffer = add.append_driver_object(memory.take_driver_object());
        let v0 = add.v0_mut().expect("v0");
        v0.id = id;
        v0.block_size = block_size;
        v0.buffer = buffer;
        self.transmit(&mut add);
    }

    /// Asks the remote broker to introduce us to the node named `name`.
    pub fn request_introduction(&self, name: &NodeName) {
        assert_eq!(self.remote_node_type, NodeType::Broker);

        let mut request = msg::RequestIntroduction::new();
        request.v0_mut().expect("v0").name = *name;
        self.transmit(&mut request);
    }

    /// Introduces the remote node to the node named `name`, using `transport`
    /// and `memory` for their link. Only brokers do this.
    #[allow(clippy::too_many_arguments)]
    pub fn accept_introduction(
        &self,
        name: &NodeName,
        side: LinkSide,
        remote_node_type: NodeType,
        remote_protocol_version: u32,
        remote_features: &Features,
        transport: Arc<DriverTransport>,
        mut memory: DriverMemory,
    ) {
        assert_eq!(self.node.node_type(), NodeType::Broker);

        let mut accept = msg::AcceptIntroduction::new();
        let transport_object = accept.append_driver_object(transport.take_driver_object());
        let memory_object = accept.append_driver_object(memory.take_driver_object());
        let features = remote_features.serialize(&mut accept);
        let v0 = accept.v0_mut().expect("v0");
        v0.name = *name;
        v0.link_side = side.to_raw();
        v0.remote_node_type = remote_node_type.to_raw();
        v0.padding = 0;
        v0.remote_protocol_version = remote_protocol_version;
        v0.transport = transport_object;
        v0.memory = memory_object;
        accept.v1_mut().expect("v1").remote_features = features;
        self.transmit(&mut accept);
    }

    /// Tells the remote node that its introduction request for `name` failed.
    pub fn reject_introduction(&self, name: &NodeName) {
        assert_eq!(self.node.node_type(), NodeType::Broker);

        let mut reject = msg::RejectIntroduction::new();
        reject.v0_mut().expect("v0").name = *name;
        self.transmit(&mut reject);
    }

    /// Asks the remote broker to accept a new non-broker node on the other end
    /// of `transport`.
    pub fn refer_non_broker(&self, transport: Arc<DriverTransport>, num_initial_portals: u32, callback: ReferralCallback) {
        assert!(self.node.node_type() == NodeType::Normal && self.remote_node_type == NodeType::Broker);

        let referral_id = {
            let mut state = self.lock();
            let id = state.next_referral_id;
            state.next_referral_id += 1;
            state.pending_referrals.insert(id, callback);
            id
        };

        let mut refer = msg::ReferNonBroker::new();
        let object = refer.append_driver_object(transport.take_driver_object());
        let v0 = refer.v0_mut().expect("v0");
        v0.referral_id = referral_id;
        v0.num_initial_portals = num_initial_portals;
        v0.transport = object;
        self.transmit(&mut refer);
    }

    /// Informs the remote node of a new link which bypasses a proxy.
    pub fn accept_bypass_link(
        &self,
        current_peer_node: &NodeName,
        current_peer_sublink: SublinkId,
        inbound_sequence_length_from_bypassed_link: SequenceNumber,
        new_sublink: SublinkId,
        mut link_state: FragmentRef<RouterLinkState>,
    ) {
        let mut accept = msg::AcceptBypassLink::new();
        let v0 = accept.v0_mut().expect("v0");
        v0.current_peer_node = *current_peer_node;
        v0.current_peer_sublink = current_peer_sublink;
        v0.inbound_sequence_length_from_bypassed_link = inbound_sequence_length_from_bypassed_link;
        v0.new_sublink = new_sublink;
        v0.new_link_state_fragment = *link_state.release().descriptor();
        self.transmit(&mut accept);
    }

    /// Requests a new shared memory region of `size` bytes from the remote node.
    pub fn request_memory(&self, size: usize, callback: RequestMemoryCallback) {
        let size32 = u32::try_from(size).expect("memory request size fits in 32 bits");
        self.lock().pending_memory_requests.entry(size32).or_default().push(callback);

        let mut request = msg::RequestMemory::new();
        let v0 = request.v0_mut().expect("v0");
        v0.size = size32;
        v0.padding = 0;
        self.transmit(&mut request);
    }

    /// Relays `message` to `to_node` through this link's broker.
    pub fn relay_message(&self, to_node: &NodeName, message: &mut Message) {
        assert_eq!(self.remote_node_type, NodeType::Broker);

        let mut relay = msg::RelayMessage::new();
        let data_offset = relay.allocate_and_set_array(message.data_view());
        let objects = relay.append_driver_objects(message.driver_objects());
        let v0 = relay.v0_mut().expect("v0");
        v0.destination = *to_node;
        v0.data = data_offset;
        v0.padding = 0;
        v0.driver_objects = objects;
        self.transmit(&mut relay);
    }

    /// Dispatches a message which was relayed to us by a broker.
    pub fn dispatch_relayed_message(&self, accept: &mut msg::AcceptRelayedMessage) -> bool {
        // We only allow a limited subset of messages to be relayed through a
        // broker. Namely, any message which might carry driver objects between
        // two non-brokers needs to be relayable.
        //
        // If an unknown or unsupported message type is relayed it's silently
        // discarded, rather than being rejected as a validation failure. This
        // leaves open the possibility for newer versions of a message to
        // introduce driver objects and support relaying.
        let data_offset = accept.v0().expect("v0").data;
        let data = accept.get_array_view::<u8>(data_offset as usize).to_vec();
        let driver_objects_data = accept.v0().expect("v0").driver_objects;
        let objects = accept.get_driver_object_array_view(&driver_objects_data);
        if data.len() < size_of::<MessageHeaderV0>() {
            return false;
        }

        // The message id is the third byte of the header.
        let message_id = data[2];
        match message_id {
            msg::AcceptParcelDriverObjects::ID => {
                let mut accept_parcel = msg::AcceptParcelDriverObjects::new_incoming();
                accept_parcel.deserialize_relayed(&data, objects) && self.on_accept_parcel_driver_objects(accept_parcel)
            }

            msg::AddBlockBuffer::ID => {
                let mut add = msg::AddBlockBuffer::new_incoming();
                add.deserialize_relayed(&data, objects) && self.on_add_block_buffer(add)
            }

            _ => {
                log::trace!("Ignoring relayed message with ID {message_id}");
                true
            }
        }
    }

    /// Permanently stops this link, dropping all of its sublinks.
    pub fn deactivate(&self) {
        {
            let mut state = self.lock();
            if state.activation_state != ActivationState::Active {
                return;
            }
            state.activation_state = ActivationState::Deactivated;
        }

        self.handle_transport_error();
        self.transport.deactivate();
        self.memory.set_node_link(None);
    }

    /// Transmits `message` to the remote node, relaying it through the broker
    /// if the transport cannot carry it directly.
    pub fn transmit(&self, message: &mut Message) {
        if !message.can_transmit_on(&self.transport) {
            // The driver has indicated that it can't transmit this message
            // through our transport, so the message must instead be relayed
            // through a broker.
            let Some(broker) = self.node.get_broker_link() else {
                log::error!("Cannot relay message without a broker link");
                return;
            };

            broker.relay_message(&self.remote_node_name, message);
            return;
        }

        message.header_mut().node_sequence_number = self.generate_outgoing_sequence_number();
        self.transport.transmit(message);
    }

    pub fn deleted_sublink_count_for_testing(&self) -> usize {
        self.lock().deleted_sublinks.size()
    }

    pub fn early_parcel_count_for_testing(&self) -> usize {
        self.lock().early_parcels_for_sublink.len()
    }

    /// Accepts any parcels which arrived for `sublink_id` before the sublink
    /// was bound.
    pub fn accept_early_parcels_for_sublink(&self, sublink_id: SublinkId) {
        let early_parcels = self.lock().early_parcels_for_sublink.remove(&sublink_id).unwrap_or_default();
        for early_parcel in early_parcels {
            self.accept_complete_parcel(sublink_id, early_parcel);
        }
    }

    fn generate_outgoing_sequence_number(&self) -> SequenceNumber {
        SequenceNumber::new(self.next_outgoing_sequence_number.fetch_add(1, Ordering::Relaxed))
    }

    fn handle_transport_error(&self) {
        let sublinks;
        let subparcel_trackers;
        let pending_referrals;
        let early_parcels_for_sublink;
        {
            let mut state = self.lock();
            sublinks = std::mem::take(&mut state.sublinks);
            state.deleted_sublinks.clear();
            early_parcels_for_sublink = std::mem::take(&mut state.early_parcels_for_sublink);
            subparcel_trackers = std::mem::take(&mut state.subparcel_trackers);
            pending_referrals = std::mem::take(&mut state.pending_referrals);
        }

        for sublink in sublinks.values() {
            log::trace!("NodeLink disconnection dropping {}", sublink.router_link.describe());
            sublink.receiver.notify_link_disconnected(&sublink.router_link);
        }

        pending_referrals.into_values().for_each(|callback| callback(None, 0));

        let this = self.arc();
        self.node.drop_connection(&this);
        self.memory.notify_link_disconnected();
        drop((sublinks, subparcel_trackers, early_parcels_for_sublink));
    }

    fn wait_for_parcel_fragment_to_resolve(
        &self,
        for_sublink: SublinkId,
        parcel: Box<Parcel>,
        descriptor: FragmentDescriptor,
        is_split_parcel: bool,
    ) {
        let this_link = self.arc();
        self.memory.wait_for_buffer_async(
            descriptor.buffer_id(),
            Box::new(move || {
                let memory = this_link.memory().clone();
                let fragment = memory.get_fragment(&descriptor);
                let mut parcel = parcel;
                if !fragment.is_addressable() || !parcel.adopt_data_fragment(memory, fragment) {
                    // The fragment is out of bounds or had an invalid header.
                    // Either way it doesn't look good for the remote node.
                    this_link.handle_transport_error();
                    return;
                }

                if is_split_parcel {
                    this_link.accept_parcel_without_driver_objects(for_sublink, parcel);
                } else {
                    this_link.accept_complete_parcel(for_sublink, parcel);
                }
            }),
        );
    }

    fn accept_parcel_without_driver_objects(&self, for_sublink: SublinkId, parcel: Box<Parcel>) -> bool {
        let key = (for_sublink, parcel.sequence_number());
        let parcel_with_driver_objects = {
            let mut state = self.lock();
            match state.partial_parcels.remove(&key) {
                Some(other_half) => other_half,
                None => {
                    state.partial_parcels.insert(key, parcel);
                    return true;
                }
            }
        };

        self.accept_split_parcel(for_sublink, parcel, parcel_with_driver_objects)
    }

    fn accept_parcel_driver_objects(&self, for_sublink: SublinkId, parcel: Box<Parcel>) -> bool {
        let key = (for_sublink, parcel.sequence_number());
        let parcel_without_driver_objects = {
            let mut state = self.lock();
            match state.partial_parcels.remove(&key) {
                Some(other_half) => other_half,
                None => {
                    state.partial_parcels.insert(key, parcel);
                    return true;
                }
            }
        };

        self.accept_split_parcel(for_sublink, parcel_without_driver_objects, parcel)
    }

    #[allow(clippy::boxed_local)]
    fn accept_split_parcel(
        &self,
        for_sublink: SublinkId,
        mut parcel_without_driver_objects: Box<Parcel>,
        mut parcel_with_driver_objects: Box<Parcel>,
    ) -> bool {
        // The parcel with no driver objects should still have an object
        // attachment slot reserved for every relayed driver object.
        if parcel_without_driver_objects.num_objects() < parcel_with_driver_objects.num_objects() {
            return false;
        }

        // Fill in all the object gaps in the data-only parcel with the boxed
        // objects from the driver objects parcel.
        let complete_parcel = &mut parcel_without_driver_objects;
        let mut remaining_driver_objects = parcel_with_driver_objects.objects_view_mut().iter_mut();
        for object in complete_parcel.objects_view_mut().iter_mut().filter(|object| object.is_none()) {
            match remaining_driver_objects.next() {
                Some(driver_object) => *object = driver_object.take(),
                None => return false,
            }
        }

        // At least one driver object was unclaimed by the data half of the
        // parcel. That's not right.
        if remaining_driver_objects.any(|object| object.is_some()) {
            return false;
        }

        self.accept_complete_parcel(for_sublink, parcel_without_driver_objects)
    }

    fn accept_complete_parcel(&self, for_sublink: SublinkId, mut parcel: Box<Parcel>) -> bool {
        let Some(sublink) = self.get_sublink(for_sublink) else {
            let mut state = self.lock();
            if !state.deleted_sublinks.contains(for_sublink) {
                log::trace!(
                    "Queuing {} at {}, arriving from {} via unknown sublink {}",
                    parcel.describe(),
                    self.local_node_name,
                    self.remote_node_name,
                    for_sublink
                );
                state.early_parcels_for_sublink.entry(for_sublink).or_default().push(parcel);
            }
            return true;
        };

        // Note that the common case is a standalone complete parcel, where the
        // number of subparcels is 1. In that case no additional tracking is
        // necessary and we immediately accept the parcel below.
        let num_subparcels = parcel.num_subparcels();
        if num_subparcels > 1 {
            let key = (for_sublink, parcel.sequence_number());
            let mut state = self.lock();
            let tracker = state.subparcel_trackers.entry(key).or_insert_with(|| SubparcelTracker {
                num_subparcels_received: 0,
                subparcels: vec![None; num_subparcels],
            });
            if tracker.subparcels.len() != num_subparcels {
                // Inconsistent subparcel count expectations across subparcels.
                // This is a validation failure.
                return false;
            }

            // Note that `index` has already been validated against the expected
            // number of subparcels in `on_accept_parcel()`.
            let index = parcel.subparcel_index();
            assert!(index < tracker.subparcels.len());
            if tracker.subparcels[index].is_some() {
                // Multiple subparcels claim the same index for this
                // SequenceNumber. Bad.
                return false;
            }
            tracker.subparcels[index] = Some(ParcelWrapper::new(Some(parcel)));
            tracker.num_subparcels_received += 1;
            if tracker.num_subparcels_received < num_subparcels {
                // Still waiting for more subparcels.
                return true;
            }

            // We have all subparcels for this SequenceNumber. Join them and
            // proceed. We do this by iterating over the object attachments in
            // subparcel 0, replacing any placeholder ParcelWrapper objects with
            // our own real ones.
            let subparcels = state.subparcel_trackers.remove(&key).expect("tracker exists").subparcels;
            drop(state);
            let mut subparcels = subparcels.into_iter().flatten();
            parcel = subparcels
                .next()
                .and_then(|first| first.take_parcel())
                .expect("subparcel 0 is present");
            for object in parcel.objects_view().iter().flatten() {
                let Some(api_box) = object.clone().downcast::<ApiBox>() else {
                    continue;
                };
                if api_box.box_type() != BoxType::Subparcel {
                    continue;
                }

                let Some(next_subparcel) = subparcels.next() else {
                    // Too many placeholder objects in the main parcel. Bad.
                    return false;
                };
                let wrapper = api_box.subparcel().expect("subparcel box has a wrapper");
                wrapper.set_parcel(next_subparcel.take_parcel());
            }

            if subparcels.next().is_some() {
                // One or more subparcels unclaimed by the main parcel. Bad.
                return false;
            }
        }

        // At this point we've collected all expected subparcels and can pass the
        // full parcel along to its receiver.
        parcel.set_remote_source(Some(self.arc()));
        let link_type = sublink.router_link.get_type();
        if link_type.is_outward() {
            log::trace!("Accepting inbound {} at {}", parcel.describe(), sublink.router_link.describe());
            return sublink.receiver.accept_inbound_parcel(parcel);
        }

        assert!(link_type.is_peripheral_inward());
        log::trace!("Accepting outbound {} at {}", parcel.describe(), sublink.router_link.describe());
        sublink.receiver.accept_outbound_parcel(parcel)
    }
}

// Protocol version negotiation is kept for parity with upstream, though there is
// only one protocol version for now.
#[allow(clippy::unnecessary_min_or_max)]
impl NodeMessageListener for NodeLink {
    fn on_refer_non_broker(&self, mut refer: msg::ReferNonBroker) -> bool {
        if self.remote_node_type != NodeType::Normal || self.node.node_type() != NodeType::Broker {
            return false;
        }

        let v0 = *refer.v0().expect("v0");
        let transport = refer.take_driver_object(v0.transport);
        if !transport.is_valid() {
            return false;
        }

        let link_memory = NodeLinkMemory::allocate_memory(self.node.driver());
        let client_link_memory = NodeLinkMemory::allocate_memory(self.node.driver());
        if !link_memory.mapping.is_valid() || !client_link_memory.mapping.is_valid() {
            // Not a validation failure, but we can't accept the referral because
            // we can't allocate link memory for one side or the other.
            let mut rejected = msg::NonBrokerReferralRejected::new();
            rejected.v0_mut().expect("v0").referral_id = v0.referral_id;
            self.transmit(&mut rejected);
            return true;
        }

        NodeConnector::handle_non_broker_referral(
            self.node.clone(),
            v0.referral_id,
            v0.num_initial_portals,
            self.arc(),
            DriverTransport::new(transport),
            link_memory,
            client_link_memory,
        )
    }

    fn on_non_broker_referral_accepted(&self, mut accepted: msg::NonBrokerReferralAccepted) -> bool {
        if self.remote_node_type != NodeType::Broker {
            return false;
        }

        let v0 = *accepted.v0().expect("v0");
        let callback = match self.lock().pending_referrals.remove(&{ v0.referral_id }) {
            Some(callback) => callback,
            None => return false,
        };

        let protocol_version = msg::PROTOCOL_VERSION.min(v0.protocol_version);
        let transport = DriverTransport::new(accepted.take_driver_object(v0.transport));
        let mapping = DriverMemory::from_object(accepted.take_driver_object(v0.buffer)).map();
        if !transport.driver_handle().is_valid() || !mapping.is_valid() {
            // Not quite a validation failure if the broker simply failed to
            // allocate resources for this link. Treat it like a connection
            // failure.
            callback(None, 0);
            return true;
        }

        let remote_features = match accepted.v1().map(|v1| v1.features) {
            Some(offset) => Features::deserialize(&accepted, offset),
            None => Features::default(),
        };
        let node: Arc<dyn MemoryNode> = self.node.clone();
        let link_to_referree = NodeLink::create_inactive(
            self.node.clone(),
            LinkSide::A,
            self.local_node_name,
            v0.name,
            NodeType::Normal,
            protocol_version,
            remote_features,
            transport,
            NodeLinkMemory::create(node, LinkSide::A, &remote_features, mapping),
        );
        callback(Some(link_to_referree.clone()), v0.num_initial_portals);
        link_to_referree.activate();
        true
    }

    fn on_non_broker_referral_rejected(&self, rejected: msg::NonBrokerReferralRejected) -> bool {
        if self.remote_node_type != NodeType::Broker {
            return false;
        }

        let referral_id = rejected.v0().expect("v0").referral_id;
        let callback = match self.lock().pending_referrals.remove(&referral_id) {
            Some(callback) => callback,
            None => return false,
        };

        callback(None, 0);
        true
    }

    fn on_request_introduction(&self, request: msg::RequestIntroduction) -> bool {
        if self.node.node_type() != NodeType::Broker {
            return false;
        }

        let name = request.v0().expect("v0").name;
        self.node.handle_introduction_request(&self.arc(), &name);
        true
    }

    fn on_accept_introduction(&self, mut accept: msg::AcceptIntroduction) -> bool {
        if self.remote_node_type != NodeType::Broker {
            return false;
        }

        let v0 = *accept.v0().expect("v0");
        if self.node.node_type() == NodeType::Broker && NodeType::from_raw(v0.remote_node_type) == Some(NodeType::Broker)
        {
            // Brokers are never introduced to other brokers; broker-to-broker
            // links are always established directly via connect_node().
            return false;
        }

        let mapping = DriverMemory::from_object(accept.take_driver_object(v0.memory)).map();
        if !mapping.is_valid() {
            return false;
        }

        let remote_features = match accept.v1().map(|v1| v1.remote_features) {
            Some(offset) => Features::deserialize(&accept, offset),
            None => Features::default(),
        };
        let transport_object = accept.take_driver_object(v0.transport);
        if !transport_object.is_valid() {
            return false;
        }

        let (Some(link_side), Some(remote_node_type)) =
            (LinkSide::from_raw(v0.link_side), NodeType::from_raw(v0.remote_node_type))
        else {
            return false;
        };
        let node: Arc<dyn MemoryNode> = self.node.clone();
        self.node.accept_introduction(
            &self.arc(),
            &{ v0.name },
            link_side,
            remote_node_type,
            v0.remote_protocol_version,
            &remote_features,
            DriverTransport::new(transport_object),
            NodeLinkMemory::create(node, link_side, &remote_features, mapping),
        );
        true
    }

    fn on_reject_introduction(&self, reject: msg::RejectIntroduction) -> bool {
        if self.remote_node_type != NodeType::Broker {
            return false;
        }

        let name = reject.v0().expect("v0").name;
        self.node.notify_introduction_failed(self, &name);
        true
    }

    fn on_request_indirect_introduction(&self, request: msg::RequestIndirectIntroduction) -> bool {
        // By convention only a broker on side B of a broker-to-broker link will
        // send this message, and so only side-A broker-to-broker links can
        // receive it.
        if self.remote_node_type != NodeType::Broker
            || self.node.node_type() != NodeType::Broker
            || !self.link_side.is_side_a()
        {
            return false;
        }

        let v0 = *request.v0().expect("v0");
        self.node
            .handle_indirect_introduction_request(&self.arc(), &{ v0.target_node }, &{ v0.source_node })
    }

    fn on_add_block_buffer(&self, mut add: msg::AddBlockBuffer) -> bool {
        let v0 = *add.v0().expect("v0");
        let mapping = DriverMemory::from_object(add.take_driver_object(v0.buffer)).map();
        if !mapping.is_valid() {
            return false;
        }
        self.memory.add_block_buffer(v0.id, v0.block_size as usize, mapping)
    }

    fn on_accept_parcel(&self, mut accept: msg::AcceptParcel) -> bool {
        let v0 = *accept.v0().expect("v0");
        let handle_types: Vec<u32> = accept.get_array_view::<u32>(v0.handle_types as usize).to_vec();
        let mut new_routers = accept
            .get_array_view::<RouterDescriptor>(v0.new_routers as usize)
            .to_vec()
            .into_iter();
        let for_sublink = v0.sublink;
        let driver_objects_data = v0.driver_objects;
        let mut driver_objects: Vec<DriverObject> =
            accept.get_driver_object_array_view(&driver_objects_data).iter_mut().map(std::mem::take).collect();
        let mut driver_objects = driver_objects.drain(..);

        // Note that on any validation failure below, we defer rejection at least
        // until any deserialized objects are stored in a new Parcel object. This
        // ensures that they're properly cleaned up before we return.
        let mut parcel_valid = true;
        let mut is_split_parcel = false;
        let mut objects: Vec<Option<Arc<dyn ApiObject>>> = vec![None; handle_types.len()];
        for (slot, handle_type) in objects.iter_mut().zip(handle_types) {
            match HandleType::try_from(handle_type) {
                Ok(HandleType::Portal) => {
                    let Some(descriptor) = new_routers.next() else {
                        parcel_valid = false;
                        continue;
                    };

                    match Router::deserialize(&descriptor, &self.arc(), for_sublink) {
                        Some(new_router) => *slot = Some(new_router),
                        None => parcel_valid = false,
                    }
                }

                Ok(HandleType::BoxedDriverObject) => {
                    let Some(object) = driver_objects.next() else {
                        return false;
                    };
                    *slot = Some(ApiBox::new_driver_object(object));
                }

                Ok(HandleType::RelayedBoxedDriverObject) => is_split_parcel = true,

                // Store a placeholder object for each expected subparcel. These
                // will be filled in by `accept_complete_parcel()` once the last
                // complete subparcel is accepted.
                Ok(HandleType::BoxedSubparcel) => *slot = Some(ApiBox::new_subparcel(ParcelWrapper::new(None))),

                Err(_) => parcel_valid = false,
            }
        }

        if new_routers.next().is_some() || driver_objects.next().is_some() {
            // There should be no unclaimed routers. If there are, it's a
            // validation failure.
            parcel_valid = false;
        }

        let num_subparcels = v0.num_subparcels as usize;
        let subparcel_index = v0.subparcel_index as usize;
        if num_subparcels > MAX_SUBPARCELS_PER_PARCEL || subparcel_index >= num_subparcels {
            return false;
        }

        let mut parcel = Box::new(Parcel::new(v0.sequence_number));
        parcel.set_num_subparcels(num_subparcels);
        parcel.set_subparcel_index(subparcel_index);
        parcel.set_objects(objects);
        parcel.set_envelope(accept.take_envelope());
        if !parcel_valid {
            return false;
        }

        let descriptor = v0.parcel_fragment;
        if !descriptor.is_null() {
            // The parcel's data resides in a shared memory fragment.
            let fragment = self.memory.get_fragment(&descriptor);
            if fragment.is_pending() {
                // We don't have this buffer yet, but we expect to receive it
                // ASAP. Defer acceptance until then.
                self.wait_for_parcel_fragment_to_resolve(for_sublink, parcel, descriptor, is_split_parcel);
                return true;
            }

            if fragment.is_null() || !parcel.adopt_data_fragment(self.memory.clone(), fragment) {
                return false;
            }
        } else {
            // The parcel's data was inlined within the AcceptParcel message.
            // Adopt the Message contents so our local Parcel doesn't need to
            // copy any data.
            let parcel_data = accept.get_array_view::<u8>(v0.parcel_data as usize);
            let range = if parcel_data.is_empty() {
                0..0
            } else {
                let start = parcel_data.as_ptr() as usize - accept.data_view().as_ptr() as usize;
                start..start + parcel_data.len()
            };
            parcel.set_data_from_message(accept.take_received_data(), range);
        }

        if is_split_parcel {
            return self.accept_parcel_without_driver_objects(for_sublink, parcel);
        }
        self.accept_complete_parcel(for_sublink, parcel)
    }

    fn on_accept_parcel_driver_objects(&self, mut accept: msg::AcceptParcelDriverObjects) -> bool {
        let v0 = *accept.v0().expect("v0");
        let mut parcel = Box::new(Parcel::new(v0.sequence_number));
        let driver_objects_data = v0.driver_objects;
        let objects = accept
            .get_driver_object_array_view(&driver_objects_data)
            .iter_mut()
            .map(|object| Some(ApiBox::new_driver_object(std::mem::take(object)) as Arc<dyn ApiObject>))
            .collect();
        parcel.set_objects(objects);
        self.accept_parcel_driver_objects(v0.sublink, parcel)
    }

    fn on_route_closed(&self, route_closed: msg::RouteClosed) -> bool {
        let v0 = *route_closed.v0().expect("v0");
        let Some(sublink) = self.get_sublink(v0.sublink) else {
            // The sublink may have already been removed, for example if the
            // application has already closed the associated router. It is
            // therefore not considered an error to receive a RouteClosed message
            // for an unknown sublink.
            return true;
        };

        sublink
            .receiver
            .accept_route_closure_from(sublink.router_link.get_type(), v0.sequence_length)
    }

    fn on_route_disconnected(&self, route_disconnected: msg::RouteDisconnected) -> bool {
        let Some(sublink) = self.get_sublink(route_disconnected.v0().expect("v0").sublink) else {
            return true;
        };

        log::trace!("Accepting RouteDisconnected at {}", sublink.router_link.describe());
        sublink.receiver.accept_route_disconnected_from(sublink.router_link.get_type())
    }

    fn on_bypass_peer(&self, bypass: msg::BypassPeer) -> bool {
        let v0 = *bypass.v0().expect("v0");
        let Some(sublink) = self.get_sublink(v0.sublink) else {
            return true;
        };

        // NOTE: This request is authenticated by the receiving Router, within
        // `bypass_peer()`.
        sublink
            .receiver
            .bypass_peer(&sublink.router_link, &{ v0.bypass_target_node }, v0.bypass_target_sublink)
    }

    fn on_accept_bypass_link(&self, accept: msg::AcceptBypassLink) -> bool {
        let v0 = *accept.v0().expect("v0");
        let Some(node_link_to_peer) = self.node.get_link(&{ v0.current_peer_node }) else {
            // If the link to the peer has been severed for whatever reason, the
            // relevant route will be torn down anyway. It's safe to ignore this
            // request in that case.
            return true;
        };

        let Some(receiver) = node_link_to_peer.get_router(v0.current_peer_sublink) else {
            // Similar to above, if the targeted Router cannot be resolved from
            // the given sublink, this implies that the route has already been at
            // least partially torn down. It's safe to ignore this request.
            return true;
        };

        let link_state = self
            .memory
            .adopt_fragment_ref_if_valid::<RouterLinkState>(&{ v0.new_link_state_fragment });
        if link_state.is_null() {
            // Bypass links must always come with a valid fragment for their
            // RouterLinkState. If one has not been provided, that's a validation
            // failure.
            return false;
        }

        receiver.accept_bypass_link(
            &self.arc(),
            v0.new_sublink,
            link_state,
            v0.inbound_sequence_length_from_bypassed_link,
        )
    }

    fn on_stop_proxying(&self, stop: msg::StopProxying) -> bool {
        let v0 = *stop.v0().expect("v0");
        match self.get_router(v0.sublink) {
            Some(router) => router.stop_proxying(v0.inbound_sequence_length, v0.outbound_sequence_length),
            None => true,
        }
    }

    fn on_proxy_will_stop(&self, will_stop: msg::ProxyWillStop) -> bool {
        let v0 = *will_stop.v0().expect("v0");
        match self.get_router(v0.sublink) {
            Some(router) => router.notify_proxy_will_stop(v0.inbound_sequence_length),
            None => true,
        }
    }

    fn on_bypass_peer_with_link(&self, bypass: msg::BypassPeerWithLink) -> bool {
        let v0 = *bypass.v0().expect("v0");
        let Some(router) = self.get_router(v0.sublink) else {
            return true;
        };

        let link_state = self
            .memory
            .adopt_fragment_ref_if_valid::<RouterLinkState>(&{ v0.new_link_state_fragment });
        if link_state.is_null() {
            return false;
        }

        router.accept_bypass_link(&self.arc(), v0.new_sublink, link_state, v0.inbound_sequence_length)
    }

    fn on_stop_proxying_to_local_peer(&self, stop: msg::StopProxyingToLocalPeer) -> bool {
        let v0 = *stop.v0().expect("v0");
        match self.get_router(v0.sublink) {
            Some(router) => router.stop_proxying_to_local_peer(v0.outbound_sequence_length),
            None => true,
        }
    }

    fn on_flush_router(&self, flush: msg::FlushRouter) -> bool {
        if let Some(router) = self.get_router(flush.v0().expect("v0").sublink) {
            router.flush(FlushBehavior::ForceProxyBypassAttempt);
        }
        true
    }

    fn on_request_memory(&self, request: msg::RequestMemory) -> bool {
        let size = request.v0().expect("v0").size;
        if size == 0 {
            return false;
        }

        let mut memory = DriverMemory::allocate(self.node.driver(), size as usize);
        let mut provide = msg::ProvideMemory::new();
        let buffer = provide.append_driver_object(memory.take_driver_object());
        let v0 = provide.v0_mut().expect("v0");
        v0.size = size;
        v0.buffer = buffer;
        self.transmit(&mut provide);
        true
    }

    fn on_provide_memory(&self, mut provide: msg::ProvideMemory) -> bool {
        let v0 = *provide.v0().expect("v0");
        let memory = DriverMemory::from_object(provide.take_driver_object(v0.buffer));
        let callback = {
            let mut state = self.lock();
            let Some(callbacks) = state.pending_memory_requests.get_mut(&{ v0.size }) else {
                return false;
            };

            assert!(!callbacks.is_empty());
            let callback = callbacks.remove(0);
            if callbacks.is_empty() {
                state.pending_memory_requests.remove(&{ v0.size });
            }
            callback
        };

        callback(memory);
        true
    }

    fn on_relay_message(&self, mut relay: msg::RelayMessage) -> bool {
        // Relay requests are only sent by non-brokers to their own broker.
        if self.node.node_type() != NodeType::Broker || self.remote_node_type != NodeType::Normal {
            return false;
        }

        self.node.relay_message(&self.remote_node_name, &mut relay)
    }

    fn on_accept_relayed_message(&self, mut accept: msg::AcceptRelayedMessage) -> bool {
        if self.remote_node_type != NodeType::Broker {
            return false;
        }

        self.node.accept_relayed_message(&mut accept)
    }
}

impl TransportListener for NodeLink {
    fn on_transport_message(&self, message: &RawMessage<'_>, transport: &DriverTransport, envelope: DriverHandle) -> bool {
        self.handle_transport_message(message, transport, envelope)
    }

    fn on_transport_error(&self) {
        self.handle_transport_error();
    }
}

impl MemoryLink for NodeLink {
    fn add_block_buffer(&self, id: BufferId, block_size: usize, memory: DriverMemory) {
        NodeLink::add_block_buffer(
            self,
            id,
            u32::try_from(block_size).expect("block size fits in 32 bits"),
            memory,
        );
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::Driver;
    use crate::core::driver_object::DriverObject;
    use crate::core::features::IPCZ_FEATURE_MEM_V2;
    use crate::core::ipcz_result::IpczResult;
    use crate::core::ipcz_types::{CreateNodeOptions, GetFlags};
    use crate::core::node::Connection;
    use crate::core::node_type::NodeType;
    use crate::core::reference_drivers::sync::SyncDriver;
    use crate::core::router_descriptor::RouterDescriptor;
    use std::sync::atomic::Ordering;

    fn link_nodes_without_activation(
        driver: &Arc<SyncDriver>,
        broker: &Arc<Node>,
        non_broker: &Arc<Node>,
    ) -> (Arc<NodeLink>, Arc<NodeLink>) {
        let dyn_driver: Arc<dyn Driver> = driver.clone();
        let (handle0, handle1) = driver.create_transports(Default::default(), Default::default()).unwrap();
        let transport0 = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle0));
        let transport1 = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle1));

        let buffer = NodeLinkMemory::allocate_memory(&dyn_driver);
        assert!(buffer.mapping.is_valid());

        let non_broker_name = broker.generate_random_name();
        let broker_memory_node: Arc<dyn MemoryNode> = broker.clone();
        let non_broker_memory_node: Arc<dyn MemoryNode> = non_broker.clone();
        let link0 = NodeLink::create_inactive(
            broker.clone(),
            LinkSide::A,
            broker.get_assigned_name(),
            non_broker_name,
            NodeType::Normal,
            0,
            *non_broker.features(),
            transport0,
            NodeLinkMemory::create(broker_memory_node, LinkSide::A, non_broker.features(), buffer.mapping),
        );
        let link1 = NodeLink::create_inactive(
            non_broker.clone(),
            LinkSide::B,
            non_broker_name,
            broker.get_assigned_name(),
            NodeType::Normal,
            0,
            *broker.features(),
            transport1,
            NodeLinkMemory::create(non_broker_memory_node, LinkSide::B, broker.features(), buffer.memory.map()),
        );
        (link0, link1)
    }

    fn link_nodes(driver: &Arc<SyncDriver>, broker: &Arc<Node>, non_broker: &Arc<Node>) -> (Arc<NodeLink>, Arc<NodeLink>) {
        let (link0, link1) = link_nodes_without_activation(driver, broker, non_broker);
        link0.activate();
        link1.activate();
        (link0, link1)
    }

    fn link_brokers(driver: &Arc<SyncDriver>, broker0: &Arc<Node>, broker1: &Arc<Node>) -> (Arc<NodeLink>, Arc<NodeLink>) {
        let dyn_driver: Arc<dyn Driver> = driver.clone();
        let (handle0, handle1) = driver.create_transports(Default::default(), Default::default()).unwrap();
        let transport0 = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle0));
        let transport1 = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle1));

        let buffer = NodeLinkMemory::allocate_memory(&dyn_driver);
        assert!(buffer.mapping.is_valid());

        let memory_node0: Arc<dyn MemoryNode> = broker0.clone();
        let memory_node1: Arc<dyn MemoryNode> = broker1.clone();
        let link0 = NodeLink::create_inactive(
            broker0.clone(),
            LinkSide::A,
            broker0.get_assigned_name(),
            broker1.get_assigned_name(),
            NodeType::Broker,
            0,
            *broker1.features(),
            transport0,
            NodeLinkMemory::create(memory_node0, LinkSide::A, broker1.features(), buffer.mapping),
        );
        let link1 = NodeLink::create_inactive(
            broker1.clone(),
            LinkSide::B,
            broker1.get_assigned_name(),
            broker0.get_assigned_name(),
            NodeType::Broker,
            0,
            *broker0.features(),
            transport1,
            NodeLinkMemory::create(memory_node1, LinkSide::B, broker0.features(), buffer.memory.map()),
        );
        broker0.add_connection(
            broker1.get_assigned_name(),
            Connection {
                link: link0.clone(),
                broker: Some(link0.clone()),
            },
        );
        broker1.add_connection(
            broker0.get_assigned_name(),
            Connection {
                link: link1.clone(),
                broker: Some(link1.clone()),
            },
        );
        link0.activate();
        link1.activate();
        (link0, link1)
    }

    fn attach_routers(link0: &Arc<NodeLink>, link1: &Arc<NodeLink>) -> (Arc<Router>, Arc<Router>) {
        let router0 = Router::new();
        let router1 = Router::new();
        let link_state = link0.memory().get_initial_router_link_state(0);
        router0.set_outward_link(
            link0
                .add_remote_router_link(SublinkId::new(0), link_state.clone(), LinkType::Central, LinkSide::A, router0.clone())
                .unwrap(),
        );
        router1.set_outward_link(
            link1
                .add_remote_router_link(SublinkId::new(0), link_state.clone(), LinkType::Central, LinkSide::B, router1.clone())
                .unwrap(),
        );
        link_state.status.store(RouterLinkState::STABLE, Ordering::SeqCst);
        assert!(!router1.is_peer_closed());
        assert!(!router0.is_peer_closed());
        (router0, router1)
    }

    fn broker_and_non_broker(driver: &Arc<SyncDriver>) -> (Arc<Node>, Arc<Node>) {
        (
            Node::new(NodeType::Broker, driver.clone(), None),
            Node::new(NodeType::Normal, driver.clone(), None),
        )
    }

    const MESSAGE: &[u8] = b"hello\0";

    #[test]
    fn basic_transmission() {
        let driver = SyncDriver::new();
        let (node0, node1) = broker_and_non_broker(&driver);

        let (link0, link1) = link_nodes(&driver, &node0, &node1);
        let (router0, router1) = attach_routers(&link0, &link1);

        assert!(!router1.is_peer_closed());
        router0.close_route();
        assert!(router1.is_peer_closed());
        router1.close_route();

        link0.deactivate();
        link1.deactivate();
    }

    #[test]
    fn basic_transmission_with_message() {
        let driver = SyncDriver::new();
        let (node0, node1) = broker_and_non_broker(&driver);

        let (link0, link1) = link_nodes(&driver, &node0, &node1);
        let (router0, router1) = attach_routers(&link0, &link1);

        // Send message.
        assert_eq!(IpczResult::Ok, router0.put(MESSAGE, &[]));

        // Verify that one parcel arrived.
        let status = router1.query_status();
        assert_eq!(1, status.num_local_parcels);
        assert_eq!(MESSAGE.len(), status.num_local_bytes);

        // Verify message contents.
        let mut received_data = [0u8; 100];
        let out = router1.get(GetFlags::empty(), &mut received_data, &mut [], false);
        assert_eq!(IpczResult::Ok, out.result);
        assert_eq!(MESSAGE, &received_data[..out.num_bytes]);

        router0.close_route();
        router1.close_route();

        link0.deactivate();
        link1.deactivate();
    }

    #[test]
    fn accept_early_parcel() {
        let driver = SyncDriver::new();
        let (node0, node1) = broker_and_non_broker(&driver);
        let (link0, link1) = link_nodes(&driver, &node0, &node1);

        // Set up the central link, but delay attaching the Router on the
        // receiving end.
        let router0 = Router::new();
        let link_state = link0.memory().get_initial_router_link_state(0);
        router0.set_outward_link(
            link0
                .add_remote_router_link(SublinkId::new(0), link_state.clone(), LinkType::Central, LinkSide::A, router0.clone())
                .unwrap(),
        );
        link_state.status.store(RouterLinkState::STABLE, Ordering::SeqCst);

        // Send message. The parcel is sent immediately, but should be queued as
        // 'early' because there is no Router+RouterLink for it.
        assert_eq!(IpczResult::Ok, router0.put(MESSAGE, &[]));

        // Attach the Router.
        let router1 = Router::new();
        router1.set_outward_link(
            link1
                .add_remote_router_link(SublinkId::new(0), link_state, LinkType::Central, LinkSide::B, router1.clone())
                .unwrap(),
        );

        // Verify that no parcels arrived to the receiving Router.
        assert_eq!(0, router1.query_status().num_local_parcels);

        // Take the early parcels. Normally it is done during Router
        // deserialization. Here, to avoid deserialization verbosity simulate it
        // for a Router that already exists (router1).
        link1.accept_early_parcels_for_sublink(SublinkId::new(0));

        // Verify that the parcel is accepted after the simulated
        // deserialization.
        assert_eq!(1, router1.query_status().num_local_parcels);

        // Verify message contents.
        let mut received_data = [0u8; 100];
        let out = router1.get(GetFlags::empty(), &mut received_data, &mut [], false);
        assert_eq!(IpczResult::Ok, out.result);
        assert_eq!(MESSAGE, &received_data[..out.num_bytes]);

        link0.deactivate();
        link1.deactivate();
    }

    #[test]
    fn reject_parcel_for_closed_router() {
        let driver = SyncDriver::new();
        let (node0, node1) = broker_and_non_broker(&driver);

        // Link nodes and avoid activating link1 to let the incoming message wait
        // until transport activation.
        let (link0, link1) = link_nodes_without_activation(&driver, &node0, &node1);
        link0.activate();
        let (router0, router1) = attach_routers(&link0, &link1);

        // Send message. The parcel should get queued in router0 because the
        // transport is not activated.
        assert_eq!(IpczResult::Ok, router0.put(MESSAGE, &[]));

        // Simulate early closing of the Router while the parcel is in flight.
        link1.remove_remote_router_link(SublinkId::new(0));
        assert_eq!(1, link1.deleted_sublink_count_for_testing());

        // Activate transport.
        link1.activate();

        // Verify that the early parcels are gone.
        assert_eq!(0, link1.early_parcel_count_for_testing());
        let mut received_data = [0u8; 100];
        let out = router1.get(GetFlags::empty(), &mut received_data, &mut [], false);
        assert_eq!(IpczResult::Unavailable, out.result);

        link0.deactivate();
        link1.deactivate();
    }

    #[test]
    fn available_features() {
        let driver = SyncDriver::new();
        let options_with_features = CreateNodeOptions {
            enabled_features: vec![IPCZ_FEATURE_MEM_V2],
            ..Default::default()
        };
        let node0 = Node::new(NodeType::Broker, driver.clone(), Some(&options_with_features));
        let node1 = Node::new(NodeType::Normal, driver.clone(), Some(&options_with_features));
        let node2 = Node::new(NodeType::Normal, driver.clone(), None);

        let (link01, link10) = link_nodes(&driver, &node0, &node1);
        let (link02, link20) = link_nodes(&driver, &node0, &node2);

        // The node0-node1 link supports mem_v2 because both nodes enabled it.
        assert!(link01.available_features().mem_v2());
        assert!(link01.memory().available_features().mem_v2());
        assert!(link10.available_features().mem_v2());
        assert!(link10.memory().available_features().mem_v2());

        // The node0-node2 link doesn't support mem_v2 because node2 didn't
        // enable it.
        assert!(!link02.available_features().mem_v2());
        assert!(!link02.memory().available_features().mem_v2());
        assert!(!link20.available_features().mem_v2());
        assert!(!link20.memory().available_features().mem_v2());

        for link in [link01, link10, link02, link20] {
            link.deactivate();
        }
    }

    #[test]
    fn stop_proxying_redundant() {
        let driver = SyncDriver::new();
        let (node0, node1) = broker_and_non_broker(&driver);

        let (link0, link1) = link_nodes(&driver, &node0, &node1);
        let (router0, router1) = attach_routers(&link0, &link1);

        // Configure router0 as a proxy.
        let mut descriptor = RouterDescriptor::default();
        router0.serialize_new_router(&link0, &mut descriptor);

        // Calling `stop_proxying()` multiple times used to trigger an assertion
        // failure. With the fix, the second call is safely ignored.
        assert!(router0.stop_proxying(SequenceNumber::new(100), SequenceNumber::new(100)));
        assert!(!router0.stop_proxying(SequenceNumber::new(100), SequenceNumber::new(100)));

        router0.close_route();
        router1.close_route();
        link0.deactivate();
        link1.deactivate();
    }

    #[test]
    fn notify_proxy_will_stop_redundant() {
        let driver = SyncDriver::new();
        let (node0, node1) = broker_and_non_broker(&driver);

        let (link0, link1) = link_nodes(&driver, &node0, &node1);
        let (router0, router1) = attach_routers(&link0, &link1);

        // Configure router0 as a proxy.
        let mut descriptor = RouterDescriptor::default();
        router0.serialize_new_router(&link0, &mut descriptor);

        // Calling `notify_proxy_will_stop()` multiple times used to trigger an
        // assertion failure. With the fix, the second call is safely ignored.
        assert!(router0.notify_proxy_will_stop(SequenceNumber::new(100)));
        assert!(!router0.notify_proxy_will_stop(SequenceNumber::new(100)));

        router0.close_route();
        router1.close_route();
        link0.deactivate();
        link1.deactivate();
    }

    #[test]
    fn broker_rejects_introduction_to_broker() {
        // Two brokers connected to one another, as in a broker-to-broker
        // handshake. A broker should never be the target of an introduction to
        // another broker: broker-to-broker links are only ever established
        // directly via connect_node().
        let driver = SyncDriver::new();
        let node0 = Node::new(NodeType::Broker, driver.clone(), None);
        let node1 = Node::new(NodeType::Broker, driver.clone(), None);
        let (link0, link1) = link_brokers(&driver, &node0, &node1);
        let (router0, router1) = attach_routers(&link0, &link1);

        let dyn_driver: Arc<dyn Driver> = driver.clone();
        let (new_transport, _other_transport) = DriverTransport::create_pair(&dyn_driver, None, None);
        let new_buffer = NodeLinkMemory::allocate_memory(&dyn_driver);
        let introduced_name = node0.generate_random_name();
        assert!(node1.get_link(&introduced_name).is_none());
        link0.accept_introduction(
            &introduced_name,
            LinkSide::A,
            NodeType::Broker,
            0,
            &Features::default(),
            new_transport,
            new_buffer.memory,
        );

        // The introduction is rejected and no new connection is established.
        assert!(node1.get_link(&introduced_name).is_none());
        assert!(router1.is_peer_closed());

        router0.close_route();
        router1.close_route();
    }

    #[test]
    fn broker_rejects_relay_from_broker() {
        // Relay requests are only ever sent by non-brokers to their own
        // broker, so a broker must reject any relay request arriving over a
        // broker-to-broker link.
        let driver = SyncDriver::new();
        let node0 = Node::new(NodeType::Broker, driver.clone(), None);
        let node1 = Node::new(NodeType::Broker, driver.clone(), None);
        let (link0, link1) = link_brokers(&driver, &node0, &node1);
        let (router0, router1) = attach_routers(&link0, &link1);

        let mut payload = msg::AddBlockBuffer::new();
        link0.relay_message(&node0.get_assigned_name(), &mut payload);

        // The relay request is rejected and the link is torn down.
        assert!(router1.is_peer_closed());

        router0.close_route();
        router1.close_route();
    }
}
