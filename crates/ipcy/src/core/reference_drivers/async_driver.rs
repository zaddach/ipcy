//! Port of `reference_drivers/async_reference_driver.{h,cc}`: a single-process
//! driver whose transports each deliver messages on their own thread.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, Weak};
use std::thread::{self, JoinHandle};

use super::base::{DriverBase, Object};
use crate::core::driver::{
    Driver, DriverHandle, INVALID_DRIVER_HANDLE, MappedMemory, RawMessage, SerializeBuffers, SerializeOutcome,
    SharedMemoryInfo, TransportActivityHandler,
};
use crate::core::ipcz_result::IpczResult;

fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeKind {
    Broker,
    NonBroker,
}

type Base = DriverBase<AsyncTransport>;

fn close_handle(base: &Base, handle: DriverHandle) -> IpczResult {
    match base.take(handle) {
        Some(Object::Transport(transport)) => transport.close(),
        Some(_) => IpczResult::Ok,
        None => IpczResult::InvalidArgument,
    }
}

enum Task {
    Message { data: Vec<u8>, handles: Vec<DriverHandle> },
    Error,
}

/// A task together with the means to close any handles it still owns when it's
/// dropped without having been run.
struct PostedTask {
    task: Task,
    base: Weak<Base>,
}

impl Drop for PostedTask {
    fn drop(&mut self) {
        if let (Task::Message { handles, .. }, Some(base)) = (&mut self.task, self.base.upgrade()) {
            handles.drain(..).for_each(|handle| {
                close_handle(&base, handle);
            });
        }
    }
}

#[derive(Default)]
struct State {
    active: bool,
    handler: Option<Arc<dyn TransportActivityHandler>>,
    task_thread: Option<JoinHandle<()>>,
    tasks: Vec<PostedTask>,
    peer: Option<Arc<AsyncTransport>>,
}

pub struct AsyncTransport {
    local: NodeKind,
    remote: NodeKind,
    state: Mutex<State>,
    wake: Condvar,
}

impl AsyncTransport {
    fn create_pair(node0: NodeKind, node1: NodeKind) -> (Arc<AsyncTransport>, Arc<AsyncTransport>) {
        let make = |local, remote| {
            Arc::new(AsyncTransport {
                local,
                remote,
                state: Mutex::new(State::default()),
                wake: Condvar::new(),
            })
        };
        let first = make(node0, node1);
        let second = make(node1, node0);
        guard(&first.state).peer = Some(second.clone());
        guard(&second.state).peer = Some(first.clone());
        (first, second)
    }

    fn activate(self: &Arc<Self>, handler: Arc<dyn TransportActivityHandler>) {
        let mut state = guard(&self.state);
        state.handler = Some(handler);
        state.active = true;
        let transport = self.clone();
        state.task_thread = Some(thread::spawn(move || {
            transport.run_tasks_until_deactivation();
            // The driver drops its reference to the handler on deactivation.
            let handler = guard(&transport.state).handler.take();
            if let Some(handler) = handler {
                handler.on_deactivated();
            }
        }));
    }

    fn deactivate(&self) {
        let thread_to_join = {
            let mut state = guard(&self.state);
            // Join the task thread if we're not on it; otherwise detach it.
            // Detachment is safe: the thread owns a ref to `self` as long as
            // it's running, and it will terminate very soon after deactivation.
            let handle = state.task_thread.take().expect("transport was activated");
            state.active = false;
            self.wake.notify_all();
            (handle.thread().id() != thread::current().id()).then_some(handle)
        };
        if let Some(handle) = thread_to_join {
            let _ = handle.join();
        }
    }

    fn post_task(&self, task: PostedTask) {
        guard(&self.state).tasks.push(task);
        self.wake.notify_all();
    }

    fn transmit(&self, base: &Arc<Base>, data: &[u8], handles: &[DriverHandle]) -> IpczResult {
        let peer = guard(&self.state).peer.clone();
        let task = Task::Message {
            data: data.to_vec(),
            handles: handles.to_vec(),
        };
        match peer {
            Some(peer) => peer.post_task(PostedTask {
                task,
                base: Arc::downgrade(base),
            }),
            // The peer is gone; the message is dropped along with its handles.
            None => drop(PostedTask {
                task,
                base: Arc::downgrade(base),
            }),
        }
        IpczResult::Ok
    }

    fn close(&self) -> IpczResult {
        let peer = guard(&self.state).peer.take();
        if let Some(peer) = peer {
            peer.post_task(PostedTask {
                task: Task::Error,
                base: Weak::new(),
            });
        }
        IpczResult::Ok
    }

    fn run_tasks_until_deactivation(&self) {
        loop {
            let tasks = {
                let mut state = guard(&self.state);
                loop {
                    if !state.active {
                        return;
                    }
                    if !state.tasks.is_empty() {
                        break std::mem::take(&mut state.tasks);
                    }
                    state = self.wake.wait(state).unwrap_or_else(|e| e.into_inner());
                }
            };

            let handler = guard(&self.state).handler.clone().expect("active transport has a handler");
            for mut posted in tasks {
                match &mut posted.task {
                    Task::Message { data, handles } => {
                        let handles = std::mem::take(handles);
                        let result = handler.on_message(RawMessage { data, handles: &handles }, INVALID_DRIVER_HANDLE);
                        if result != IpczResult::Ok && result != IpczResult::Unimplemented {
                            handler.on_error();
                            return;
                        }
                    }
                    Task::Error => handler.on_error(),
                }
            }
        }
    }
}

/// A single-process driver delivering transport messages from per-transport
/// threads.
pub struct AsyncDriver {
    base: Arc<Base>,
    force_brokering: bool,
}

impl AsyncDriver {
    pub fn new() -> Arc<AsyncDriver> {
        Arc::new(AsyncDriver {
            base: Arc::new(Base::default()),
            force_brokering: false,
        })
    }

    /// Like [`Self::new`], but driver objects are never transmissible directly
    /// between two non-brokers, forcing ipcz to relay them through a broker.
    pub fn new_with_forced_brokering() -> Arc<AsyncDriver> {
        Arc::new(AsyncDriver {
            base: Arc::new(Base::default()),
            force_brokering: true,
        })
    }

    pub fn base(&self) -> &Arc<Base> {
        &self.base
    }

    /// Creates a connected pair of transports between a broker and a non-broker.
    pub fn create_transport_pair(&self) -> (DriverHandle, DriverHandle) {
        self.create_pair(NodeKind::Broker, NodeKind::NonBroker)
    }

    /// Creates a connected pair of transports between two brokers.
    pub fn create_transport_pair_for_brokers(&self) -> (DriverHandle, DriverHandle) {
        self.create_pair(NodeKind::Broker, NodeKind::Broker)
    }

    fn create_pair(&self, node0: NodeKind, node1: NodeKind) -> (DriverHandle, DriverHandle) {
        let (first, second) = AsyncTransport::create_pair(node0, node1);
        (
            self.base.insert(Object::Transport(first)),
            self.base.insert(Object::Transport(second)),
        )
    }
}

impl Driver for AsyncDriver {
    fn close(&self, handle: DriverHandle) -> IpczResult {
        close_handle(&self.base, handle)
    }

    fn serialize(
        &self,
        handle: DriverHandle,
        transport: DriverHandle,
        buffers: Option<SerializeBuffers<'_>>,
    ) -> SerializeOutcome {
        if self.force_brokering {
            let Some(target) = self.base.transport(transport) else {
                return SerializeOutcome::NeedsTransport;
            };
            if target.local == NodeKind::NonBroker && target.remote == NodeKind::NonBroker {
                // Force ipcz to relay driver objects through a broker.
                return SerializeOutcome::NotTransmissible;
            }
        }
        self.base.serialize(handle, buffers)
    }

    fn deserialize(
        &self,
        data: &[u8],
        driver_handles: &[DriverHandle],
        _transport: DriverHandle,
    ) -> Result<DriverHandle, IpczResult> {
        self.base.deserialize(data, driver_handles)
    }

    fn create_transports(
        &self,
        transport0: DriverHandle,
        transport1: DriverHandle,
    ) -> Result<(DriverHandle, DriverHandle), IpczResult> {
        let remote_kind = |handle| {
            self.base
                .transport(handle)
                .map_or(NodeKind::NonBroker, |transport| transport.remote)
        };
        Ok(self.create_pair(remote_kind(transport0), remote_kind(transport1)))
    }

    fn activate_transport(&self, transport: DriverHandle, handler: Arc<dyn TransportActivityHandler>) -> IpczResult {
        match self.base.transport(transport) {
            Some(transport) => {
                transport.activate(handler);
                IpczResult::Ok
            }
            None => IpczResult::InvalidArgument,
        }
    }

    fn deactivate_transport(&self, transport: DriverHandle) -> IpczResult {
        match self.base.transport(transport) {
            Some(transport) => {
                transport.deactivate();
                IpczResult::Ok
            }
            None => IpczResult::InvalidArgument,
        }
    }

    fn transmit(&self, transport: DriverHandle, data: &[u8], driver_handles: &[DriverHandle]) -> IpczResult {
        match self.base.transport(transport) {
            Some(transport) => transport.transmit(&self.base, data, driver_handles),
            None => IpczResult::InvalidArgument,
        }
    }

    fn report_bad_transport_activity(&self, transport: DriverHandle, context: usize) -> IpczResult {
        self.base.report_bad_transport_activity(transport, context)
    }

    fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult> {
        self.base.allocate_shared_memory(num_bytes)
    }

    fn get_shared_memory_info(&self, driver_memory: DriverHandle) -> Result<SharedMemoryInfo, IpczResult> {
        self.base.get_shared_memory_info(driver_memory)
    }

    fn duplicate_shared_memory(&self, driver_memory: DriverHandle) -> Result<DriverHandle, IpczResult> {
        self.base.duplicate_shared_memory(driver_memory)
    }

    fn map_shared_memory(&self, driver_memory: DriverHandle) -> Result<MappedMemory, IpczResult> {
        self.base.map_shared_memory(driver_memory)
    }

    fn generate_random_bytes(&self, buffer: &mut [u8]) -> IpczResult {
        self.base.generate_random_bytes(buffer)
    }
}
