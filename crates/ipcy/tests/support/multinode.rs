use std::ops::{Deref, DerefMut};
#[cfg(target_os = "linux")]
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;
use std::process::Child;
#[cfg(target_os = "linux")]
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use ipcy::core::api;
use ipcy::core::box_object::BoxContents;
use ipcy::core::driver::{Driver, DriverHandle};
use ipcy::core::features::{IPCZ_FEATURE_MEM_V2, IpczFeature};
use ipcy::core::ipcz_types::*;
use ipcy::core::reference_drivers::async_driver::AsyncDriver;
#[cfg(target_os = "linux")]
use ipcy::drivers::unix::MultiprocessDriver;
use ipcy::core::reference_drivers::sync::SyncDriver;

use super::base::TestBase;

#[derive(Clone, Copy, Debug)]
pub enum DriverKind {
    Sync,
    Async,
    AsyncDelegatedAlloc,
    AsyncForcedBrokering,
    AsyncDelegatedAllocAndForcedBrokering,
    Multiprocess,
}

impl DriverKind {
    pub fn name(self) -> &'static str {
        match self {
            DriverKind::Sync => "Sync",
            DriverKind::Async => "Async",
            DriverKind::AsyncDelegatedAlloc => "AsyncDelegatedAlloc",
            DriverKind::AsyncForcedBrokering => "AsyncForcedBrokering",
            DriverKind::AsyncDelegatedAllocAndForcedBrokering => "AsyncDelegatedAllocAndForcedBrokering",
            DriverKind::Multiprocess => "Multiprocess",
        }
    }

    pub fn from_name(name: &str) -> Option<DriverKind> {
        [
            DriverKind::Sync,
            DriverKind::Async,
            DriverKind::AsyncDelegatedAlloc,
            DriverKind::AsyncForcedBrokering,
            DriverKind::AsyncDelegatedAllocAndForcedBrokering,
            DriverKind::Multiprocess,
        ]
        .into_iter()
        .find(|kind| kind.name() == name)
    }
}

pub fn feature_set(name: &str) -> Vec<IpczFeature> {
    match name {
        "MemV2" => vec![IPCZ_FEATURE_MEM_V2],
        _ => Vec::new(),
    }
}

pub struct TransportPair {
    pub ours: DriverHandle,
    pub theirs: DriverHandle,
}

/// A driver instance shared by all nodes of one test.
pub struct TestDriver {
    pub kind: DriverKind,
    pub driver: Arc<dyn Driver>,
    sync: Option<Arc<SyncDriver>>,
    asynchronous: Option<Arc<AsyncDriver>>,
    #[cfg(target_os = "linux")]
    multiprocess: Option<Arc<MultiprocessDriver>>,
}

impl TestDriver {
    pub fn new(kind: DriverKind) -> Arc<TestDriver> {
        match kind {
            DriverKind::Sync => {
                let driver = SyncDriver::new();
                Arc::new(TestDriver {
                    kind,
                    driver: driver.clone(),
                    sync: Some(driver),
                    asynchronous: None,
                    #[cfg(target_os = "linux")]
                    multiprocess: None,
                })
            }
            #[cfg(target_os = "linux")]
            DriverKind::Multiprocess => {
                let driver = MultiprocessDriver::new();
                Arc::new(TestDriver {
                    kind,
                    driver: driver.clone(),
                    sync: None,
                    asynchronous: None,
                    multiprocess: Some(driver),
                })
            }
            #[cfg(not(target_os = "linux"))]
            DriverKind::Multiprocess => unimplemented!("the multiprocess test driver is Linux-only"),
            _ => {
                let forced = matches!(
                    kind,
                    DriverKind::AsyncForcedBrokering | DriverKind::AsyncDelegatedAllocAndForcedBrokering
                );
                let driver = if forced {
                    AsyncDriver::new_with_forced_brokering()
                } else {
                    AsyncDriver::new()
                };
                Arc::new(TestDriver {
                    kind,
                    driver: driver.clone(),
                    sync: None,
                    asynchronous: Some(driver),
                    #[cfg(target_os = "linux")]
                    multiprocess: None,
                })
            }
        }
    }

    #[cfg(target_os = "linux")]
    fn create_multiprocess_transports(&self) -> (DriverHandle, DriverHandle) {
        self.multiprocess
            .as_ref()
            .expect("multiprocess driver")
            .create_transports(Default::default(), Default::default())
            .expect("create transports")
    }

    #[cfg(not(target_os = "linux"))]
    fn create_multiprocess_transports(&self) -> (DriverHandle, DriverHandle) {
        unreachable!("the multiprocess test driver is Linux-only")
    }

    pub fn create_transports(&self, for_broker_target: bool) -> TransportPair {
        let (ours, theirs) = match (&self.sync, &self.asynchronous) {
            (Some(driver), _) => driver
                .create_transports(Default::default(), Default::default())
                .expect("create transports"),
            (_, Some(driver)) if for_broker_target => driver.create_transport_pair_for_brokers(),
            (_, Some(driver)) => driver.create_transport_pair(),
            _ => self.create_multiprocess_transports(),
        };
        TransportPair { ours, theirs }
    }

    /// Extra flags for ConnectNode() when connecting to the main test node from
    /// a node spawned by the test.
    pub fn extra_client_connect_node_flags(&self) -> ConnectNodeFlags {
        match self.kind {
            DriverKind::AsyncDelegatedAlloc | DriverKind::AsyncDelegatedAllocAndForcedBrokering => {
                ConnectNodeFlags::TO_ALLOCATION_DELEGATE
            }
            _ => ConnectNodeFlags::empty(),
        }
    }
}

/// Describes a node body which can be spawned by a test.
#[derive(Clone, Copy)]
pub struct NodeDef {
    pub name: &'static str,
    pub body: fn(&mut TestNode),
    pub is_broker: bool,
    /// The path of the hidden test which runs this node in a child process,
    /// including the test crate name (as given by `module_path!()`).
    pub child_test_path: &'static str,
}

pub struct Controller {
    driver: Arc<TestDriver>,
    thread: Option<JoinHandle<()>>,
    process: Option<Child>,
}

impl Controller {
    /// Blocks until the spawned node has terminated. Returns true if it
    /// terminated cleanly, or false if it panicked (e.g. on a failed
    /// expectation).
    pub fn wait_for_shutdown(&mut self) -> bool {
        if let Some(mut process) = self.process.take() {
            return process.wait().is_ok_and(|status| status.success());
        }
        self.thread.take().is_none_or(|thread| thread.join().is_ok())
    }

    /// Creates a new pair of transports suitable for connecting the calling node
    /// with the node controlled by this object.
    pub fn create_new_transports(&self) -> TransportPair {
        self.driver.create_transports(false)
    }
}

pub struct TestNode {
    pub test_driver: Arc<TestDriver>,
    feature_set: String,
    is_broker: bool,
    node: IpczHandle,
    transport: DriverHandle,
    spawned_nodes: Vec<Controller>,
    base: TestBase,
}

impl Deref for TestNode {
    type Target = TestBase;
    fn deref(&self) -> &TestBase {
        &self.base
    }
}

impl DerefMut for TestNode {
    fn deref_mut(&mut self) -> &mut TestBase {
        &mut self.base
    }
}

impl TestNode {
    pub fn new(test_driver: Arc<TestDriver>, feature_set_name: &str, is_broker: bool) -> TestNode {
        let options = CreateNodeOptions {
            enabled_features: feature_set(feature_set_name),
            ..Default::default()
        };
        let flags = if is_broker {
            CreateNodeFlags::AS_BROKER
        } else {
            CreateNodeFlags::empty()
        };
        let node = api::create_node(test_driver.driver.clone(), flags, Some(&options)).expect("create node");
        TestNode {
            test_driver,
            feature_set: feature_set_name.to_string(),
            is_broker,
            node,
            transport: Default::default(),
            spawned_nodes: Vec::new(),
            base: TestBase,
        }
    }

    /// Handle to this node.
    pub fn node(&self) -> IpczHandle {
        self.node
    }

    pub fn driver(&self) -> &Arc<dyn Driver> {
        &self.test_driver.driver
    }

    /// Handle to this node's broker-facing transport, if and only if
    /// `connect_to_broker()` hasn't been called yet.
    pub fn transport(&self) -> DriverHandle {
        self.transport
    }

    /// Releases `transport()` to the caller.
    pub fn release_transport(&mut self) -> DriverHandle {
        std::mem::take(&mut self.transport)
    }

    /// Sets the transport to use when connecting to a broker.
    pub fn set_transport(&mut self, transport: DriverHandle) {
        assert!(!self.transport.is_valid());
        self.transport = transport;
    }

    /// Connects initial portals to the node that spawned this one.
    pub fn connect_to_parent_n(&mut self, num_portals: usize, flags: ConnectNodeFlags) -> Vec<IpczHandle> {
        let flags = flags | self.test_driver.extra_client_connect_node_flags();
        let transport = self.release_transport();
        assert!(transport.is_valid());
        api::connect_node(self.node, transport, num_portals, flags).expect("connect to parent")
    }

    pub fn connect_to_broker_n(&mut self, num_portals: usize) -> Vec<IpczHandle> {
        self.connect_to_parent_n(num_portals, ConnectNodeFlags::TO_BROKER)
    }

    pub fn connect_to_parent(&mut self, flags: ConnectNodeFlags) -> IpczHandle {
        self.connect_to_parent_n(1, flags)[0]
    }

    pub fn connect_to_broker(&mut self) -> IpczHandle {
        self.connect_to_parent(ConnectNodeFlags::TO_BROKER)
    }

    pub fn open_portals(&self) -> (IpczHandle, IpczHandle) {
        api::open_portals(self.node).expect("open portals")
    }

    /// Creates a new driver memory object populated with `contents`, boxes it,
    /// and returns a handle to the new box.
    pub fn box_blob(&self, contents: &str) -> IpczHandle {
        let driver = self.driver();
        let memory = driver.allocate_shared_memory(contents.len()).expect("allocate");
        let mapped = driver.map_shared_memory(memory).expect("map");
        // SAFETY: the mapping is at least `contents.len()` bytes.
        unsafe { std::ptr::copy_nonoverlapping(contents.as_ptr(), mapped.address, contents.len()) };
        driver.close(mapped.mapping);
        api::box_driver_object(self.node, memory).expect("box")
    }

    /// Extracts the string contents of a boxed driver memory object.
    pub fn unbox_blob(&self, handle: IpczHandle) -> String {
        let Ok(api::UnboxedContents::Unboxed(BoxContents::DriverObject(mut object))) =
            api::unbox(handle, UnboxFlags::empty())
        else {
            panic!("expected a boxed driver object");
        };
        let driver = self.driver();
        let memory = object.release();
        let info = driver.get_shared_memory_info(memory).expect("info");
        let mapped = driver.map_shared_memory(memory).expect("map");
        // SAFETY: the mapping is `region_num_bytes` bytes.
        let bytes = unsafe { std::slice::from_raw_parts(mapped.address, info.region_num_bytes) }.to_vec();
        driver.close(mapped.mapping);
        driver.close(memory);
        String::from_utf8(bytes).expect("utf8 blob")
    }

    /// Spawns a new test node and returns its controller, along with
    /// `num_portals` initial portals connected to it via a new transport.
    pub fn spawn_test_node_n(
        &mut self,
        def: NodeDef,
        num_portals: usize,
        mut flags: ConnectNodeFlags,
    ) -> (Vec<IpczHandle>, usize) {
        let our_transport = self.spawn_test_node_impl(def);
        if def.is_broker {
            flags |= ConnectNodeFlags::TO_BROKER;
        }
        let portals = api::connect_node(self.node, our_transport, num_portals, flags).expect("connect to spawned node");
        (portals, self.spawned_nodes.len() - 1)
    }

    /// Shorthand for the above, for the common case with one initial portal.
    pub fn spawn_test_node(&mut self, def: NodeDef) -> IpczHandle {
        self.spawn_test_node_with_flags(def, ConnectNodeFlags::empty())
    }

    pub fn spawn_test_node_with_flags(&mut self, def: NodeDef, flags: ConnectNodeFlags) -> IpczHandle {
        self.spawn_test_node_n(def, 1, flags).0[0]
    }

    /// Spawns a node giving it a transport to connect to its broker; the caller
    /// is responsible for the other end of that connection.
    pub fn spawn_test_node_no_connect(&mut self, def: NodeDef) -> (DriverHandle, usize) {
        let ours = self.spawn_test_node_impl(def);
        (ours, self.spawned_nodes.len() - 1)
    }

    pub fn controller(&mut self, index: usize) -> &mut Controller {
        &mut self.spawned_nodes[index]
    }

    fn spawn_test_node_impl(&mut self, def: NodeDef) -> DriverHandle {
        let transports = self.test_driver.create_transports(def.is_broker);
        #[cfg(target_os = "linux")]
        if let Some(driver) = &self.test_driver.multiprocess {
            let socket = driver
                .take_transport_fd(transports.theirs)
                .expect("an unactivated transport");
            let process = spawn_child_process(&def, &self.feature_set, socket);
            self.spawned_nodes.push(Controller {
                driver: self.test_driver.clone(),
                thread: None,
                process: Some(process),
            });
            return transports.ours;
        }
        let driver = self.test_driver.clone();
        let feature_set = self.feature_set.clone();
        let their_transport = transports.theirs;
        let thread = thread::Builder::new()
            .name(def.name.to_string())
            .spawn(move || {
                let mut node = TestNode::new(driver, &feature_set, def.is_broker);
                node.set_transport(their_transport);
                (def.body)(&mut node);
            })
            .expect("spawn node thread");
        self.spawned_nodes.push(Controller {
            driver: self.test_driver.clone(),
            thread: Some(thread),
            process: None,
        });
        transports.ours
    }

    /// Forcibly closes this node, severing all links to other nodes and
    /// implicitly disconnecting any portals which relied on those links.
    pub fn close_this_node(&mut self) {
        if self.node != 0 {
            let node = std::mem::take(&mut self.node);
            api::close(node);
        }
    }

    pub fn create_transports(&self) -> TransportPair {
        self.test_driver.create_transports(false)
    }

    pub fn create_broker_to_broker_transports(&self) -> TransportPair {
        self.test_driver.create_transports(true)
    }

    pub fn is_broker(&self) -> bool {
        self.is_broker
    }
}

impl Drop for TestNode {
    fn drop(&mut self) {
        let panicking = thread::panicking();
        if panicking {
            // Sever connections first so spawned nodes waiting on this one
            // observe disconnection instead of hanging the failed test.
            self.close_this_node();
        }
        for spawned in &mut self.spawned_nodes {
            let ok = spawned.wait_for_shutdown();
            if !panicking {
                assert!(ok, "a spawned test node failed");
            }
        }

        // If we never connected to the broker, make sure we don't leak our
        // transport.
        if self.transport.is_valid() {
            self.test_driver.driver.close(self.transport);
        }

        self.close_this_node();
    }
}

#[cfg(target_os = "linux")]
const CHILD_NODE_ENV: &str = "IPCY_TEST_CHILD_NODE";
#[cfg(target_os = "linux")]
const CHILD_FD_ENV: &str = "IPCY_TEST_CHILD_FD";
#[cfg(target_os = "linux")]
const CHILD_FEATURES_ENV: &str = "IPCY_TEST_CHILD_FEATURES";

/// Launches the test binary again to run the node described by `def`, handing
/// it `socket` as its transport to the spawning node.
#[cfg(target_os = "linux")]
fn spawn_child_process(def: &NodeDef, feature_set: &str, socket: OwnedFd) -> Child {
    // Test names are relative to the test crate's root module.
    let test_name = def
        .child_test_path
        .split_once("::")
        .map_or(def.child_test_path, |(_, path)| path);
    let fd = socket.as_raw_fd();
    let mut command = Command::new(std::env::current_exe().expect("current exe"));
    command
        .args(["--exact", test_name, "--test-threads=1", "--nocapture"])
        .env(CHILD_NODE_ENV, def.name)
        .env(CHILD_FD_ENV, fd.to_string())
        .env(CHILD_FEATURES_ENV, feature_set)
        .stdout(Stdio::null());
    // SAFETY: the closure only calls async-signal-safe `fcntl()`.
    unsafe {
        command.pre_exec(move || {
            // Sockets are created close-on-exec so unrelated children don't
            // inherit them. Only the child's own transport is inherited.
            if nix::libc::fcntl(fd, nix::libc::F_SETFD, 0) < 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().expect("spawn child test process");
    drop(socket);
    child
}

/// Runs `def` as a node in a child process spawned by a test, if this process
/// was launched for that purpose; otherwise does nothing.
#[cfg(not(target_os = "linux"))]
pub fn run_as_child(_def: NodeDef) {}

#[cfg(target_os = "linux")]
pub fn run_as_child(def: NodeDef) {
    if std::env::var(CHILD_NODE_ENV).as_deref() != Ok(def.name) {
        return;
    }

    let fd: i32 = std::env::var(CHILD_FD_ENV).expect("child fd").parse().expect("fd number");
    let feature_set = std::env::var(CHILD_FEATURES_ENV).unwrap_or_default();
    let test_driver = TestDriver::new(DriverKind::Multiprocess);
    let mut node = TestNode::new(test_driver.clone(), &feature_set, def.is_broker);
    // SAFETY: the parent passed us ownership of this descriptor.
    let socket = unsafe { OwnedFd::from_raw_fd(fd) };
    let transport = test_driver
        .multiprocess
        .as_ref()
        .expect("multiprocess driver")
        .transport_from_owned_fd(socket);
    node.set_transport(transport);
    (def.body)(&mut node);
}

/// Runs `body` as the main node of a multinode test.
pub fn run_multinode_test(kind: DriverKind, feature_set: &str, body: fn(&mut TestNode)) {
    let _ = env_logger::builder().is_test(true).try_init();
    let driver = TestDriver::new(kind);
    let mut node = TestNode::new(driver, feature_set, true);
    body(&mut node);
}

/// Defines a multinode test, run once for every combination of test driver and
/// feature set.
#[macro_export]
macro_rules! multinode_test {
    ($name:ident, | $t:ident | $body:block) => {
        #[allow(non_snake_case)]
        mod $name {
            #[allow(unused_imports)]
            use super::*;
            use $crate::support::{DriverKind, TestNode, run_multinode_test};

            fn body($t: &mut TestNode) $body

            $crate::multinode_test_variants!(
                body;
                sync_default(Sync, "Default"),
                sync_memv2(Sync, "MemV2"),
                async_default(Async, "Default"),
                async_memv2(Async, "MemV2"),
                async_delegated_alloc_default(AsyncDelegatedAlloc, "Default"),
                async_delegated_alloc_memv2(AsyncDelegatedAlloc, "MemV2"),
                async_forced_brokering_default(AsyncForcedBrokering, "Default"),
                async_forced_brokering_memv2(AsyncForcedBrokering, "MemV2"),
                async_delegated_alloc_and_forced_brokering_default(AsyncDelegatedAllocAndForcedBrokering, "Default"),
                async_delegated_alloc_and_forced_brokering_memv2(AsyncDelegatedAllocAndForcedBrokering, "MemV2")
            );

            #[cfg(target_os = "linux")]
            $crate::multinode_test_variants!(
                body;
                multiprocess_default(Multiprocess, "Default"),
                multiprocess_memv2(Multiprocess, "MemV2")
            );
        }
    };
}

#[macro_export]
macro_rules! multinode_test_variants {
    ($body:ident; $($variant:ident($kind:ident, $features:literal)),*) => {
        $(
            #[test]
            fn $variant() {
                run_multinode_test(DriverKind::$kind, $features, $body);
            }
        )*
    };
}

/// Defines a non-broker node body that tests can spawn.
#[macro_export]
macro_rules! multinode_test_node {
    ($name:ident, | $t:ident | $body:block) => {
        #[allow(non_upper_case_globals)]
        const $name: $crate::support::NodeDef = {
            fn body($t: &mut $crate::support::TestNode) $body
            $crate::support::NodeDef {
                name: stringify!($name),
                body,
                is_broker: false,
                child_test_path: concat!(module_path!(), "::child_", stringify!($name)),
            }
        };

        // Runs the node when this test binary is launched as a child process.
        ::paste::paste! {
            #[test]
            #[allow(non_snake_case)]
            fn [<child_ $name>]() {
                $crate::support::run_as_child($name);
            }
        }
    };
}

/// Defines a broker node body that tests can spawn.
#[macro_export]
macro_rules! multinode_test_broker_node {
    ($name:ident, | $t:ident | $body:block) => {
        #[allow(non_upper_case_globals)]
        const $name: $crate::support::NodeDef = {
            fn body($t: &mut $crate::support::TestNode) $body
            $crate::support::NodeDef {
                name: stringify!($name),
                body,
                is_broker: true,
                child_test_path: concat!(module_path!(), "::child_", stringify!($name)),
            }
        };

        // Runs the node when this test binary is launched as a child process.
        ::paste::paste! {
            #[test]
            #[allow(non_snake_case)]
            fn [<child_ $name>]() {
                $crate::support::run_as_child($name);
            }
        }
    };
}
