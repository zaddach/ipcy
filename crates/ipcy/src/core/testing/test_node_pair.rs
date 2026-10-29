//! Port of the `TestNodePair` helper from `router_link_test.cc` (also used by
//! the node link and connector tests): two nodes connected by a pair of
//! NodeLinks over the sync reference driver.

use std::sync::Arc;

use crate::core::buffer_id::BufferId;
use crate::core::driver::Driver;
use crate::core::driver_transport::DriverTransport;
use crate::core::features::Features;
use crate::core::fragment_ref::FragmentRef;
use crate::core::link_side::LinkSide;
use crate::core::link_type::LinkType;
use crate::core::node::{Connection, Node};
use crate::core::node_link::NodeLink;
use crate::core::node_link_memory::{MemoryNode, NodeLinkMemory};
use crate::core::node_name::NodeName;
use crate::core::node_type::NodeType;
use crate::core::reference_drivers::sync::SyncDriver;
use crate::core::remote_router_link::RemoteRouterLink;
use crate::core::router::Router;
use crate::core::router_link_state::RouterLinkState;
use crate::core::driver_object::DriverObject;

pub const TEST_BROKER_NAME: NodeName = NodeName::new(1, 2);
pub const TEST_NON_BROKER_NAME: NodeName = NodeName::new(2, 3);
pub const TEST_PEER1_NAME: NodeName = NodeName::new(3, 4);
pub const TEST_PEER2_NAME: NodeName = NodeName::new(4, 5);

#[allow(dead_code)]
pub struct TestNodePair {
    pub driver: Arc<SyncDriver>,
    pub node_a: Arc<Node>,
    pub node_b: Arc<Node>,
    pub node_link_a: Arc<NodeLink>,
    pub node_link_b: Arc<NodeLink>,
}

impl TestNodePair {
    pub fn new() -> Self {
        let driver = SyncDriver::new();
        let dyn_driver: Arc<dyn Driver> = driver.clone();
        let node_a = Node::new(NodeType::Broker, dyn_driver.clone(), None);
        let node_b = Node::new(NodeType::Normal, dyn_driver.clone(), None);

        let (handle_a, handle_b) = driver
            .create_transports(Default::default(), Default::default())
            .expect("create transports");
        let transport_a = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle_a));
        let transport_b = DriverTransport::new(DriverObject::new(dyn_driver.clone(), handle_b));

        let buffer = NodeLinkMemory::allocate_memory(&dyn_driver);
        let memory_node_a: Arc<dyn MemoryNode> = node_a.clone();
        let memory_node_b: Arc<dyn MemoryNode> = node_b.clone();
        let node_link_a = NodeLink::create_inactive(
            node_a.clone(),
            LinkSide::A,
            TEST_BROKER_NAME,
            TEST_NON_BROKER_NAME,
            NodeType::Normal,
            0,
            Features::default(),
            transport_a,
            NodeLinkMemory::create(memory_node_a, LinkSide::A, &Features::default(), buffer.mapping),
        );
        let node_link_b = NodeLink::create_inactive(
            node_b.clone(),
            LinkSide::B,
            TEST_NON_BROKER_NAME,
            TEST_BROKER_NAME,
            NodeType::Broker,
            0,
            Features::default(),
            transport_b,
            NodeLinkMemory::create(memory_node_b, LinkSide::B, &Features::default(), buffer.memory.map()),
        );
        node_a.add_connection(
            TEST_NON_BROKER_NAME,
            Connection {
                link: node_link_a.clone(),
                broker: None,
            },
        );
        node_b.add_connection(
            TEST_BROKER_NAME,
            Connection {
                link: node_link_b.clone(),
                broker: Some(node_link_b.clone()),
            },
        );

        Self {
            driver,
            node_a,
            node_b,
            node_link_a,
            node_link_b,
        }
    }

    pub fn memory_a(&self) -> &Arc<NodeLinkMemory> {
        self.node_link_a.memory()
    }

    pub fn memory_b(&self) -> &Arc<NodeLinkMemory> {
        self.node_link_b.memory()
    }

    /// Activates both of the test nodes' NodeLink transports. Tests can defer
    /// this activation as a means of deferring NodeLink communications in
    /// general.
    pub fn activate_transports(&self) {
        self.node_link_a.activate();
        self.node_link_b.activate();
    }

    /// Establishes new RemoteRouterLinks between `a` and `b`. Different initial
    /// RouterLinkState references may be provided for the link on either side in
    /// order to mimic various production scenarios.
    pub fn link_remote_routers(
        &self,
        a: &Arc<Router>,
        a_state: FragmentRef<RouterLinkState>,
        b: &Arc<Router>,
        b_state: FragmentRef<RouterLinkState>,
    ) -> (Arc<RemoteRouterLink>, Arc<RemoteRouterLink>) {
        let sublink = self.memory_a().allocate_sublink_ids(1);
        let a_link = self
            .node_link_a
            .add_remote_router_link(sublink, a_state, LinkType::Central, LinkSide::A, a.clone())
            .expect("sublink is new");
        let b_link = self
            .node_link_b
            .add_remote_router_link(sublink, b_state, LinkType::Central, LinkSide::B, b.clone())
            .expect("sublink is new");
        a.set_outward_link(a_link.clone());
        b.set_outward_link(b_link.clone());
        (a_link, b_link)
    }

    /// Depletes the available supply of RouterLinkState fragments and returns
    /// references to all of them. Note that one side effect of this call is that
    /// `memory_a()` will expand its RouterLinkState fragment capacity, so
    /// subsequent allocation requests will still succeed.
    pub fn allocate_all_router_link_states(&self) -> Vec<FragmentRef<RouterLinkState>> {
        std::iter::from_fn(|| {
            let fragment = self.memory_a().try_allocate_router_link_state();
            (!fragment.is_null()).then_some(fragment)
        })
        .collect()
    }

    #[allow(dead_code)]
    pub fn generate_buffer_id(&self) -> BufferId {
        self.memory_a().allocate_new_buffer_id()
    }
}

impl Drop for TestNodePair {
    fn drop(&mut self) {
        self.node_b.shut_down();
        self.node_a.shut_down();
    }
}
