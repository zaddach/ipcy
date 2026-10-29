//! Test doubles for the node and link a NodeLinkMemory talks to, standing in
//! for `Node` and `NodeLink` until the node layer exists.

use std::sync::{Arc, Mutex};

use crate::core::buffer_id::BufferId;
use crate::core::driver::Driver;
use crate::core::driver_memory::DriverMemory;
use crate::core::features::Features;
use crate::core::node_link_memory::{MemoryLink, MemoryNode, NodeLinkMemory};

/// A node which allocates shared memory synchronously through its driver.
pub struct TestMemoryNode {
    driver: Arc<dyn Driver>,
    features: Features,
    memory_flags: u32,
}

impl TestMemoryNode {
    pub fn new(driver: &Arc<dyn Driver>, features: Features, memory_flags: u32) -> Arc<TestMemoryNode> {
        Arc::new(TestMemoryNode {
            driver: driver.clone(),
            features,
            memory_flags,
        })
    }
}

impl MemoryNode for TestMemoryNode {
    fn features(&self) -> Features {
        self.features
    }

    fn memory_flags(&self) -> u32 {
        self.memory_flags
    }

    fn allocate_shared_memory(&self, size: usize, callback: Box<dyn FnOnce(DriverMemory) + Send>) {
        callback(DriverMemory::allocate(&self.driver, size));
    }
}

/// A link which delivers new block buffers synchronously to the peer
/// NodeLinkMemory, as a NodeLink's AddBlockBuffer message would on arrival.
#[derive(Default)]
pub struct TestMemoryLink {
    peer: Mutex<Option<Arc<NodeLinkMemory>>>,
}

impl TestMemoryLink {
    pub fn new(peer: &Arc<NodeLinkMemory>) -> Arc<TestMemoryLink> {
        Arc::new(TestMemoryLink {
            peer: Mutex::new(Some(peer.clone())),
        })
    }

    /// Drops the reference to the peer, breaking the memory/link cycle.
    pub fn disconnect(&self) {
        self.peer.lock().unwrap().take();
    }
}

impl MemoryLink for TestMemoryLink {
    fn add_block_buffer(&self, id: BufferId, block_size: usize, memory: DriverMemory) {
        let peer = self.peer.lock().unwrap().clone().expect("link connected");
        assert!(peer.add_block_buffer(id, block_size, memory.map()));
    }
}
