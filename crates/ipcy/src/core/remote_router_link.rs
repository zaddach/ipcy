//! Port of `ipcz/remote_router_link.{h,cc}`.

use std::sync::atomic::{AtomicBool, AtomicPtr, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use super::api_object::ApiObjectType;
use super::application_object::serialize_application_object;
use super::box_object::{ApiBox, BoxType};
use super::driver_object::DriverObject;
use super::fragment_ref::FragmentRef;
use super::handle_type::HandleType;
use super::link_side::LinkSide;
use super::link_type::LinkType;
use super::node_link::NodeLink;
use super::node_messages as msg;
use super::node_name::NodeName;
use super::parcel::{Parcel, MAX_SUBPARCELS_PER_PARCEL};
use super::parcel_wrapper::ParcelWrapper;
use super::router::{FlushBehavior, Router};
use super::router_descriptor::RouterDescriptor;
use super::router_link::RouterLink;
use super::router_link_state::RouterLinkState;
use super::sequence_number::SequenceNumber;
use super::sublink_id::SublinkId;

type LinkStateCallback = Box<dyn FnOnce() + Send>;

/// How an object attached to a parcel will be transmitted.
enum Attachment {
    Portal(Arc<Router>),
    DriverObject,
    Subparcel,
}

/// A link to a Router on another node, running over a sublink of a NodeLink.
pub struct RemoteRouterLink {
    sublink: SublinkId,
    link_type: LinkType,
    side: LinkSide,
    side_is_stable: AtomicBool,

    /// Keeps the RouterLinkState memory alive; set exactly once, before
    /// `link_state` is published.
    link_state_fragment: OnceLock<FragmentRef<RouterLinkState>>,
    link_state: AtomicPtr<RouterLinkState>,

    link_state_callbacks: Mutex<Vec<LinkStateCallback>>,

    // NOTE: Declared last so it's dropped last: `link_state_fragment` may refer
    // to memory (e.g. the primary buffer) which is only kept alive by the
    // NodeLink's NodeLinkMemory.
    node_link: Arc<NodeLink>,
}

impl RemoteRouterLink {
    pub fn create(
        node_link: Arc<NodeLink>,
        sublink: SublinkId,
        link_state: FragmentRef<RouterLinkState>,
        link_type: LinkType,
        side: LinkSide,
    ) -> Arc<RemoteRouterLink> {
        // Central links must be constructed with a valid RouterLinkState
        // fragment. Other links must not.
        assert_eq!(link_type.is_central(), !link_state.is_null());

        let link = Arc::new(RemoteRouterLink {
            node_link,
            sublink,
            link_type,
            side,
            side_is_stable: AtomicBool::new(false),
            link_state_fragment: OnceLock::new(),
            link_state: AtomicPtr::new(std::ptr::null_mut()),
            link_state_callbacks: Mutex::new(Vec::new()),
        });
        if link_type.is_central() {
            link.set_link_state(link_state);
        }
        link
    }

    pub fn node_link(&self) -> &Arc<NodeLink> {
        &self.node_link
    }

    pub fn sublink(&self) -> SublinkId {
        self.sublink
    }

    fn lock_callbacks(&self) -> MutexGuard<'_, Vec<LinkStateCallback>> {
        self.link_state_callbacks.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn set_link_state(self: &Arc<Self>, state: FragmentRef<RouterLinkState>) {
        assert!(self.link_type.is_central());
        assert!(!state.is_null());

        if state.is_pending() {
            // Wait for the fragment's buffer to be mapped locally.
            let memory = self.node_link.memory().clone();
            let descriptor = *state.fragment().descriptor();
            let link = self.clone();
            let memory_for_callback = memory.clone();
            memory.wait_for_buffer_async(
                descriptor.buffer_id(),
                Box::new(move || {
                    let state = memory_for_callback.adopt_fragment_ref_if_valid::<RouterLinkState>(&descriptor);
                    if state.is_addressable() {
                        link.set_link_state(state);
                        link.flush_router();
                    }
                }),
            );
            return;
        }

        assert!(state.is_addressable());

        // `set_link_state()` must be called with an addressable fragment only
        // once.
        assert!(self.link_state.load(Ordering::Acquire).is_null());
        let state_ptr = state.get();
        assert!(self.link_state_fragment.set(state).is_ok());

        let callbacks = {
            let mut callbacks = self.lock_callbacks();
            // This store-release is balanced by a load-acquire in
            // `get_link_state()`.
            self.link_state.store(state_ptr, Ordering::Release);
            std::mem::take(&mut *callbacks)
        };
        callbacks.into_iter().for_each(|callback| callback());

        // If this side of the link was already marked stable before the
        // RouterLinkState was available, `side_is_stable` will be true. In that
        // case, set the stable bit in RouterLinkState immediately. This may
        // unblock some routing work. The acquire here is balanced by a release in
        // `mark_side_stable()`.
        if self.side_is_stable.load(Ordering::Acquire) {
            self.mark_side_stable();
        }
    }

    fn flush_router(&self) {
        if let Some(router) = self.node_link.get_router(self.sublink) {
            router.flush(FlushBehavior::ForceProxyBypassAttempt);
        }
    }
}

impl RouterLink for RemoteRouterLink {
    fn get_type(&self) -> LinkType {
        self.link_type
    }

    fn get_link_state(&self) -> Option<&RouterLinkState> {
        // SAFETY: the pointer, once published, targets memory kept alive by
        // `link_state_fragment` for this link's lifetime.
        unsafe { self.link_state.load(Ordering::Acquire).as_ref() }
    }

    fn wait_for_link_state_async(&self, callback: Box<dyn FnOnce() + Send>) {
        {
            let mut callbacks = self.lock_callbacks();
            if self.link_state.load(Ordering::Relaxed).is_null() {
                callbacks.push(callback);
                return;
            }
        }
        callback();
    }

    fn get_local_peer(&self) -> Option<Arc<Router>> {
        None
    }

    fn as_remote_router_link(&self) -> Option<&RemoteRouterLink> {
        Some(self)
    }

    fn into_remote_router_link(self: Arc<Self>) -> Option<Arc<RemoteRouterLink>> {
        Some(self)
    }

    fn allocate_parcel_data(&self, num_bytes: usize, allow_partial: bool, parcel: &mut Parcel) {
        parcel.allocate_data(num_bytes, allow_partial, Some(self.node_link.memory()));
    }

    fn accept_parcel(&self, mut parcel: Box<Parcel>) {
        let node_link = &self.node_link;
        let mut accept = msg::AcceptParcel::new();
        {
            let v0 = accept.v0_mut().expect("v0");
            v0.sublink = self.sublink;
            v0.sequence_number = parcel.sequence_number();
            v0.padding = 0;
        }

        let mut driver_objects: Vec<DriverObject> = Vec::new();
        let mut subparcels: Vec<Arc<ParcelWrapper>> = Vec::new();
        let mut must_relay_driver_objects = false;
        let mut attachments = Vec::with_capacity(parcel.num_objects());
        for object in parcel.objects_view() {
            let object = object.as_ref().expect("attached object present");
            match object.object_type() {
                ApiObjectType::Portal => {
                    let router = object.clone().downcast::<Router>().expect("portal is a router");
                    attachments.push(Attachment::Portal(router));
                }
                ApiObjectType::Box => {
                    let boxed = object.clone().downcast::<ApiBox>().expect("box is a box");
                    match boxed.box_type() {
                        BoxType::DriverObject => {
                            if !boxed.driver_object_can_transmit_on(node_link.transport()) {
                                must_relay_driver_objects = true;
                            }
                            driver_objects.push(boxed.take_driver_object());
                            attachments.push(Attachment::DriverObject);
                        }
                        BoxType::ApplicationObject => {
                            // Application objects must be serialized into
                            // subparcels.
                            let application_object = boxed.take_application_object();
                            if !application_object.is_serializable() {
                                log::error!("Cannot transmit unserializable object");
                                return;
                            }
                            let Some(subparcel) = serialize_application_object(&*application_object, node_link)
                            else {
                                log::error!("Failed to serialize application object");
                                return;
                            };
                            subparcels.push(subparcel);
                            attachments.push(Attachment::Subparcel);
                        }
                        BoxType::Subparcel => {
                            subparcels.push(boxed.take_subparcel());
                            attachments.push(Attachment::Subparcel);
                        }
                        BoxType::Empty => {
                            log::error!("Attempted to transmit an invalid object");
                            return;
                        }
                    }
                }
                _ => {
                    log::error!("Attempted to transmit an invalid object");
                    return;
                }
            }
        }
        let num_portals = attachments.iter().filter(|a| matches!(a, Attachment::Portal(_))).count();

        // Subparcels cannot contain other subparcels.
        assert!(parcel.subparcel_index() == 0 || subparcels.is_empty());

        // Receivers will reject parcels which contain more than this maximum
        // number of subparcels.
        assert!(subparcels.len() < MAX_SUBPARCELS_PER_PARCEL);

        let num_subparcels = if parcel.subparcel_index() == 0 {
            // The total subparcel count includes this (the main) parcel.
            let num_subparcels = subparcels.len() + 1;

            // Send the other subparcels separately before sending this main one.
            // All will be collected on the receiving end and reconstituted into
            // a single parcel.
            for (i, wrapper) in subparcels.iter().enumerate() {
                let mut subparcel = wrapper.take_parcel().expect("subparcel has a parcel");
                subparcel.set_sequence_number(parcel.sequence_number());
                subparcel.set_num_subparcels(num_subparcels);
                subparcel.set_subparcel_index(i + 1);
                self.accept_parcel(subparcel);
            }
            num_subparcels
        } else {
            // This is not the main parcel, so the number of subparcels has
            // already been set correctly by the main parcel.
            parcel.num_subparcels()
        };
        {
            let v0 = accept.v0_mut().expect("v0");
            v0.num_subparcels = num_subparcels as u32;
            v0.subparcel_index = parcel.subparcel_index() as u32;
        }

        // If driver objects will require relaying through the broker, then the
        // parcel must be split into two separate messages: one for the driver
        // objects (which will be relayed), and one for the rest of the message
        // (which will transmit directly).
        //
        // This ensures that many side effects of message receipt are well-
        // ordered with other transmissions on the same link from the same
        // thread. Namely, since a thread may send a message which introduces a
        // new remote Router on a new sublink, followed immediately by a message
        // which targets that Router, it is critical that both messages arrive in
        // the order they were sent. If one of the messages is relayed while the
        // other is not, ordering could not be guaranteed.
        let must_split_parcel = must_relay_driver_objects;

        // Allocate all the arrays in the message. Note that each allocation may
        // relocate the parcel data in memory, so views into these arrays should
        // not be acquired until all allocations are complete.
        if !parcel.has_data_fragment() || !Arc::ptr_eq(parcel.data_fragment_memory(), node_link.memory()) {
            // Only inline parcel data within the message when we don't have a
            // separate data fragment allocated already, or if the allocated
            // fragment is on the wrong link. The latter case is possible if the
            // transmitting Router switched links since the Parcel's data was
            // allocated.
            let offset = accept.allocate_array::<u8>(parcel.data_size());
            accept.v0_mut().expect("v0").parcel_data = offset;
        } else {
            // The data for this parcel already exists in this link's memory, so
            // we only stash a reference to it in the message. This relinquishes
            // ownership of the fragment, effectively passing it to the
            // recipient.
            accept.v0_mut().expect("v0").parcel_fragment = *parcel.data_fragment().descriptor();
            parcel.release_data_fragment();
        }
        let handle_types_offset = accept.allocate_array::<u32>(attachments.len());
        let new_routers_offset = accept.allocate_array::<RouterDescriptor>(num_portals);
        {
            let v0 = accept.v0_mut().expect("v0");
            v0.handle_types = handle_types_offset;
            v0.new_routers = new_routers_offset;
        }

        let parcel_data_offset = accept.v0().expect("v0").parcel_data;
        let inline_parcel_data = accept.get_array_view_mut::<u8>(parcel_data_offset as usize);
        if !inline_parcel_data.is_empty() {
            inline_parcel_data.copy_from_slice(parcel.data_view());
        }

        // Serialize attached objects. We accumulate the Routers of all attached
        // portals, because we need to reference them again after transmission,
        // with a 1:1 correspondence to the serialized RouterDescriptors.
        //
        // The descriptors are zeroed since there may be padding bits within and
        // we'll be copying the full contents into message data below.
        let mut routers_to_proxy = Vec::with_capacity(num_portals);
        let mut descriptors = Vec::with_capacity(num_portals);
        let handle_types = accept.get_array_view_mut::<u32>(handle_types_offset as usize);
        for (handle_type, attachment) in handle_types.iter_mut().zip(attachments) {
            match attachment {
                Attachment::Portal(router) => {
                    *handle_type = HandleType::Portal as u32;
                    let mut descriptor = RouterDescriptor::default();
                    router.serialize_new_router(node_link, &mut descriptor);
                    descriptors.push(descriptor);
                    routers_to_proxy.push(router);
                }
                Attachment::DriverObject => {
                    *handle_type = if must_split_parcel {
                        HandleType::RelayedBoxedDriverObject as u32
                    } else {
                        HandleType::BoxedDriverObject as u32
                    };
                }
                // Subparcels and application objects are both serialized as
                // subparcels.
                Attachment::Subparcel => *handle_type = HandleType::BoxedSubparcel as u32,
            }
        }

        // Copy all the serialized router descriptors into the message. Our local
        // copy will supply inputs for `begin_proxying_to_new_router()` calls
        // below.
        if !descriptors.is_empty() {
            accept
                .get_array_view_mut::<RouterDescriptor>(new_routers_offset as usize)
                .copy_from_slice(&descriptors);
        }

        if must_split_parcel {
            let mut accept_objects = msg::AcceptParcelDriverObjects::new();
            {
                let objects = accept_objects.append_driver_objects(&mut driver_objects);
                let v0 = accept_objects.v0_mut().expect("v0");
                v0.sublink = self.sublink;
                v0.sequence_number = parcel.sequence_number();
                v0.driver_objects = objects;
            }
            log::trace!("Transmitting objects for {} over {}", parcel.describe(), self.describe());
            node_link.transmit(&mut accept_objects);
        } else {
            let objects = accept.append_driver_objects(&mut driver_objects);
            accept.v0_mut().expect("v0").driver_objects = objects;
        }

        log::trace!("Transmitting {} over {}", parcel.describe(), self.describe());
        node_link.transmit(&mut accept);

        // Now that the parcel has been transmitted, it's safe to start proxying
        // from any routers whose routes have just been extended to the
        // destination.
        assert_eq!(routers_to_proxy.len(), descriptors.len());
        for (router, descriptor) in routers_to_proxy.iter().zip(&descriptors) {
            router.begin_proxying_to_new_router(node_link, descriptor);
        }

        // Finally, a Parcel will normally close all attached objects when
        // destroyed. Since we've successfully transmitted this parcel and all
        // its objects, we prevent that behavior by taking away all its object
        // references.
        parcel.objects_view_mut().iter_mut().for_each(|object| drop(object.take()));
    }

    fn accept_route_closure(&self, sequence_length: SequenceNumber) {
        let mut route_closed = msg::RouteClosed::new();
        let v0 = route_closed.v0_mut().expect("v0");
        v0.sublink = self.sublink;
        v0.sequence_length = sequence_length;
        self.node_link.transmit(&mut route_closed);
    }

    fn accept_route_disconnected(&self) {
        let mut route_disconnected = msg::RouteDisconnected::new();
        route_disconnected.v0_mut().expect("v0").sublink = self.sublink;
        self.node_link.transmit(&mut route_disconnected);
    }

    fn mark_side_stable(&self) {
        self.side_is_stable.store(true, Ordering::Release);
        if let Some(state) = self.get_link_state() {
            state.set_side_stable(self.side);
        }
    }

    fn try_lock_for_bypass(&self, bypass_request_source: NodeName) -> bool {
        let Some(state) = self.get_link_state() else {
            return false;
        };
        if !state.try_lock(self.side) {
            return false;
        }

        state.store_allowed_bypass_request_source(bypass_request_source);
        true
    }

    fn try_lock_for_closure(&self) -> bool {
        self.get_link_state().is_some_and(|state| state.try_lock(self.side))
    }

    fn unlock(&self) {
        if let Some(state) = self.get_link_state() {
            state.unlock(self.side);
        }
    }

    fn flush_other_side_if_waiting(&self) -> bool {
        let Some(state) = self.get_link_state() else {
            return false;
        };
        if !state.reset_waiting_bit(self.side.opposite()) {
            return false;
        }

        let mut flush = msg::FlushRouter::new();
        flush.v0_mut().expect("v0").sublink = self.sublink;
        self.node_link.transmit(&mut flush);
        true
    }

    fn can_node_request_bypass(&self, bypass_request_source: &NodeName) -> bool {
        let Some(state) = self.get_link_state() else {
            return false;
        };
        let allowed_source = state.load_allowed_bypass_request_source();
        state.is_locked_by(self.side.opposite()) && allowed_source == *bypass_request_source
    }

    fn deactivate(&self) {
        self.node_link.remove_remote_router_link(self.sublink);
    }

    fn bypass_peer(&self, bypass_target_node: &NodeName, bypass_target_sublink: SublinkId) {
        let mut bypass = msg::BypassPeer::new();
        let v0 = bypass.v0_mut().expect("v0");
        v0.sublink = self.sublink;
        v0.reserved0 = 0;
        v0.bypass_target_node = *bypass_target_node;
        v0.bypass_target_sublink = bypass_target_sublink;
        self.node_link.transmit(&mut bypass);
    }

    fn stop_proxying(&self, inbound_sequence_length: SequenceNumber, outbound_sequence_length: SequenceNumber) {
        let mut stop = msg::StopProxying::new();
        let v0 = stop.v0_mut().expect("v0");
        v0.sublink = self.sublink;
        v0.inbound_sequence_length = inbound_sequence_length;
        v0.outbound_sequence_length = outbound_sequence_length;
        self.node_link.transmit(&mut stop);
    }

    fn proxy_will_stop(&self, inbound_sequence_length: SequenceNumber) {
        let mut will_stop = msg::ProxyWillStop::new();
        let v0 = will_stop.v0_mut().expect("v0");
        v0.sublink = self.sublink;
        v0.inbound_sequence_length = inbound_sequence_length;
        self.node_link.transmit(&mut will_stop);
    }

    fn bypass_peer_with_link(
        &self,
        new_sublink: SublinkId,
        mut new_link_state: FragmentRef<RouterLinkState>,
        inbound_sequence_length: SequenceNumber,
    ) {
        let mut bypass = msg::BypassPeerWithLink::new();
        let v0 = bypass.v0_mut().expect("v0");
        v0.sublink = self.sublink;
        v0.new_sublink = new_sublink;
        v0.new_link_state_fragment = *new_link_state.release().descriptor();
        v0.inbound_sequence_length = inbound_sequence_length;
        self.node_link.transmit(&mut bypass);
    }

    fn stop_proxying_to_local_peer(&self, outbound_sequence_length: SequenceNumber) {
        let mut stop = msg::StopProxyingToLocalPeer::new();
        let v0 = stop.v0_mut().expect("v0");
        v0.sublink = self.sublink;
        v0.outbound_sequence_length = outbound_sequence_length;
        self.node_link.transmit(&mut stop);
    }

    fn describe(&self) -> String {
        format!(
            "{} link from {} to {} via sublink {}",
            self.link_type,
            self.node_link.local_node_name(),
            self.node_link.remote_node_name(),
            self.sublink
        )
    }
}
