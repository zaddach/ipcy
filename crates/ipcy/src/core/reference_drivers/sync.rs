//! Port of `reference_drivers/sync_reference_driver.{h,cc}`: a single-process
//! driver whose transports deliver messages synchronously on the calling thread.

use std::sync::{Arc, Mutex, MutexGuard};

use super::base::{DriverBase, Object};
use crate::core::driver::{
    Driver, DriverHandle, INVALID_DRIVER_HANDLE, MappedMemory, RawMessage, SerializeBuffers, SerializeOutcome,
    SharedMemoryInfo, TransportActivityHandler,
};
use crate::core::ipcz_result::IpczResult;

fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

struct DeferredNotification {
    error: bool,
    data: Vec<u8>,
    handles: Vec<DriverHandle>,
}

#[derive(Default)]
struct WrapperState {
    in_notification: bool,
    deferred_notifications: Vec<DeferredNotification>,
}

/// Serializes the invocation of potentially overlapping or reentrant
/// notifications from a transport.
struct TransportWrapper {
    handler: Arc<dyn TransportActivityHandler>,
    state: Mutex<WrapperState>,
}

impl TransportWrapper {
    fn new(handler: Arc<dyn TransportActivityHandler>) -> Arc<Self> {
        Arc::new(Self {
            handler,
            state: Mutex::new(WrapperState::default()),
        })
    }

    fn notify(&self, data: &[u8], handles: &[DriverHandle]) {
        self.do_notify(false, data, handles);
    }

    fn notify_error(&self) {
        self.do_notify(true, &[], &[]);
    }

    fn do_notify(&self, error: bool, data: &[u8], handles: &[DriverHandle]) {
        {
            let mut state = guard(&self.state);
            if state.in_notification {
                state.deferred_notifications.push(DeferredNotification {
                    error,
                    data: data.to_vec(),
                    handles: handles.to_vec(),
                });
                return;
            }
            state.in_notification = true;
        }

        self.do_notify_exclusive(error, data, handles);

        // Now flush any notifications that queued while this one was in
        // progress. This continues until we complete an iteration with no new
        // notifications being queued.
        loop {
            let notifications = {
                let mut state = guard(&self.state);
                if state.deferred_notifications.is_empty() {
                    state.in_notification = false;
                    return;
                }
                std::mem::take(&mut state.deferred_notifications)
            };

            for n in &notifications {
                self.do_notify_exclusive(n.error, &n.data, &n.handles);
            }
        }
    }

    fn do_notify_exclusive(&self, error: bool, data: &[u8], handles: &[DriverHandle]) {
        if error {
            self.handler.on_error();
            return;
        }

        let result = self.handler.on_message(RawMessage { data, handles }, INVALID_DRIVER_HANDLE);
        if result != IpczResult::Ok && result != IpczResult::Unimplemented {
            self.handler.on_error();
        }
    }
}

impl Drop for TransportWrapper {
    fn drop(&mut self) {
        // Since this is destruction, the invocation is necessarily exclusive.
        self.handler.on_deactivated();
    }
}

struct SavedMessage {
    data: Vec<u8>,
    handles: Vec<DriverHandle>,
}

#[derive(Default)]
struct TransportState {
    peer: Option<Arc<InProcessTransport>>,
    transport: Option<Arc<TransportWrapper>>,
    peer_active: bool,
    peer_closed: bool,
    saved_messages: Vec<SavedMessage>,
}

#[derive(Default)]
pub struct InProcessTransport {
    state: Mutex<TransportState>,
}

impl InProcessTransport {
    fn close(&self, driver: &SyncDriver) {
        self.deactivate();

        let (peer, saved_messages) = {
            let mut state = guard(&self.state);
            (state.peer.take(), std::mem::take(&mut state.saved_messages))
        };

        if let Some(peer) = peer {
            for message in saved_messages {
                driver.close_all_handles(&message.handles);
            }

            // NOTE: Although nothing should ever call back into `self` after
            // close, for consistency with other methods we still take care not
            // to call into the peer while holding the lock.
            peer.on_peer_closed();
        }
    }

    fn activate(&self, handler: Arc<dyn TransportActivityHandler>) -> IpczResult {
        let new_transport = TransportWrapper::new(handler);
        let peer;
        {
            let mut state = guard(&self.state);
            assert!(state.transport.is_none());
            if state.peer_closed {
                // Our peer has already been closed. Signal a transport error, as
                // peer closure would have done.
                drop(state);
                new_transport.notify_error();
                return IpczResult::Ok;
            }
            state.transport = Some(new_transport);
            peer = state.peer.clone();
        }

        // Let the peer know that it can now call into us directly. This may
        // re-enter this transport, as the peer will synchronously flush any
        // queued transmissions before returning, and the logic handling the
        // receipt of those transmissions may perform additional operations on
        // this transport; all before `on_peer_activated()` returns. We must
        // therefore ensure the lock is not held while calling this.
        if let Some(peer) = peer {
            peer.on_peer_activated();
        }
        IpczResult::Ok
    }

    fn deactivate(&self) {
        // NOTE: Dropping this reference may in turn lead to ipcz dropping its
        // own last reference to `self`, so we must be careful not hold the lock
        // when that happens.
        let transport = guard(&self.state).transport.take();
        drop(transport);
    }

    fn peer_transport(&self) -> Option<Arc<TransportWrapper>> {
        guard(&self.state).transport.clone()
    }

    fn transmit(&self, driver: &SyncDriver, data: &[u8], handles: &[DriverHandle]) -> IpczResult {
        let peer = {
            let mut state = guard(&self.state);
            if !state.peer_active {
                state.saved_messages.push(SavedMessage {
                    data: data.to_vec(),
                    handles: handles.to_vec(),
                });
                return IpczResult::Ok;
            }
            state.peer.clone().expect("active peer")
        };

        // NOTE: Notifying the peer of anything may re-enter `self`, so we must
        // be careful not hold the lock while doing that.
        match peer.peer_transport() {
            Some(peer_transport) => peer_transport.notify(data, handles),
            None => driver.close_all_handles(handles),
        }
        IpczResult::Ok
    }

    fn on_peer_activated(&self) {
        loop {
            let (peer, saved_messages) = {
                let mut state = guard(&self.state);
                assert!(!state.peer_active);
                if state.saved_messages.is_empty() {
                    state.peer_active = true;
                    return;
                }
                (state.peer.clone(), std::mem::take(&mut state.saved_messages))
            };

            let Some(peer_transport) = peer.and_then(|peer| peer.peer_transport()) else {
                // NOTE: We could have close retain the peer reference if there
                // are queued messages, and only drop the reference after
                // flushing them. This is not necessary in practice.
                return;
            };

            for message in &saved_messages {
                peer_transport.notify(&message.data, &message.handles);
            }
        }
    }

    fn on_peer_closed(&self) {
        let transport = {
            let mut state = guard(&self.state);
            state.peer_closed = true;
            state.transport.take()
        };

        // NOTE: Notifying ipcz of an error here may re-enter this transport
        // (e.g. to close it), so we must be careful not to hold the lock while
        // doing that.
        if let Some(transport) = transport {
            transport.notify_error();
        }
    }
}

/// A single-process driver delivering transport messages synchronously.
#[derive(Default)]
pub struct SyncDriver {
    base: DriverBase<InProcessTransport>,
}

impl SyncDriver {
    pub fn new() -> Arc<SyncDriver> {
        Arc::new(SyncDriver::default())
    }

    pub fn base(&self) -> &DriverBase<InProcessTransport> {
        &self.base
    }

    fn close_all_handles(&self, handles: &[DriverHandle]) {
        handles.iter().for_each(|&handle| {
            self.close(handle);
        });
    }
}

impl Driver for SyncDriver {
    fn close(&self, handle: DriverHandle) -> IpczResult {
        match self.base.take(handle) {
            Some(Object::Transport(transport)) => {
                transport.close(self);
                IpczResult::Ok
            }
            Some(_) => IpczResult::Ok,
            None => IpczResult::InvalidArgument,
        }
    }

    fn serialize(
        &self,
        handle: DriverHandle,
        _transport: DriverHandle,
        buffers: Option<SerializeBuffers<'_>>,
    ) -> SerializeOutcome {
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
        _transport0: DriverHandle,
        _transport1: DriverHandle,
    ) -> Result<(DriverHandle, DriverHandle), IpczResult> {
        let first = Arc::new(InProcessTransport::default());
        let second = Arc::new(InProcessTransport::default());
        guard(&first.state).peer = Some(second.clone());
        guard(&second.state).peer = Some(first.clone());
        Ok((
            self.base.insert(Object::Transport(first)),
            self.base.insert(Object::Transport(second)),
        ))
    }

    fn activate_transport(&self, transport: DriverHandle, handler: Arc<dyn TransportActivityHandler>) -> IpczResult {
        match self.base.transport(transport) {
            Some(transport) => transport.activate(handler),
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
            Some(transport) => transport.transmit(self, data, driver_handles),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::INVALID_DRIVER_HANDLE;
    use crate::core::testing::transport_receiver::TransportReceiver;
    use std::sync::atomic::{AtomicBool, Ordering};

    const MESSAGE: &[u8] = b"hello, world?";

    fn flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    fn is_set(flag: &AtomicBool) -> bool {
        flag.load(Ordering::SeqCst)
    }

    fn transports(driver: &SyncDriver) -> (DriverHandle, DriverHandle) {
        driver
            .create_transports(INVALID_DRIVER_HANDLE, INVALID_DRIVER_HANDLE)
            .unwrap()
    }

    fn close_all(driver: &SyncDriver, handles: &[DriverHandle]) {
        handles
            .iter()
            .for_each(|&handle| assert_eq!(IpczResult::Ok, driver.close(handle)));
    }

    #[test]
    fn create_transports() {
        let driver = SyncDriver::new();
        let (a, b) = transports(&driver);
        close_all(&driver, &[a, b]);
    }

    #[test]
    fn transmit_before_active() {
        let driver = SyncDriver::new();
        let (a, b) = transports(&driver);
        assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));

        let received = flag();
        let receiver = {
            let received = received.clone();
            TransportReceiver::default()
                .on_message(move |message| {
                    assert_eq!(MESSAGE, message.data);
                    received.store(true, Ordering::SeqCst);
                    IpczResult::Ok
                })
                .build()
        };

        // Activation should immediately flush out the already transmitted message.
        assert!(!is_set(&received));
        assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));
        assert!(is_set(&received));

        assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
        close_all(&driver, &[a, b]);
    }

    #[test]
    fn transmit_while_active() {
        let driver = SyncDriver::new();
        let (a, b) = transports(&driver);

        let received = flag();
        let receiver = {
            let received = received.clone();
            TransportReceiver::default()
                .on_message(move |message| {
                    assert_eq!(MESSAGE, message.data);
                    received.store(true, Ordering::SeqCst);
                    IpczResult::Ok
                })
                .build()
        };
        assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

        // Transmission must result in synchronous receipt of the message.
        assert!(!is_set(&received));
        assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));
        assert!(is_set(&received));

        assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
        close_all(&driver, &[a, b]);
    }

    #[test]
    fn deactivate() {
        let driver = SyncDriver::new();
        let (a, b) = transports(&driver);

        let deactivated = flag();
        let receiver = {
            let deactivated = deactivated.clone();
            TransportReceiver::default()
                .on_deactivate(move || deactivated.store(true, Ordering::SeqCst))
                .build()
        };
        assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

        assert!(!is_set(&deactivated));
        assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
        assert!(is_set(&deactivated));

        close_all(&driver, &[a, b]);
    }

    #[test]
    fn transmit_after_deactivated() {
        let driver = SyncDriver::new();
        let (a, b) = transports(&driver);

        let message_received = flag();
        let deactivated = flag();
        let receiver = {
            let (message_received, deactivated) = (message_received.clone(), deactivated.clone());
            TransportReceiver::default()
                .on_message(move |_| {
                    message_received.store(true, Ordering::SeqCst);
                    IpczResult::Ok
                })
                .on_deactivate(move || deactivated.store(true, Ordering::SeqCst))
                .build()
        };
        assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

        assert!(!is_set(&deactivated));
        assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
        assert!(is_set(&deactivated));

        assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));
        assert!(!is_set(&message_received));

        close_all(&driver, &[a, b]);
    }

    #[test]
    fn notify_error() {
        let driver = SyncDriver::new();
        let (a, b) = transports(&driver);

        let observed_error = flag();
        let deactivated = flag();
        let receiver = {
            let (observed_error, deactivated) = (observed_error.clone(), deactivated.clone());
            TransportReceiver::default()
                // Simulate ipcz rejecting an incoming message from the driver.
                .on_message(|_| IpczResult::InvalidArgument)
                .on_deactivate(move || deactivated.store(true, Ordering::SeqCst))
                .on_error(move || observed_error.store(true, Ordering::SeqCst))
                .build()
        };
        assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

        assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));
        assert!(is_set(&observed_error));

        // Errors imply deactivation, so no separate notification happens for
        // deactivation.
        assert!(!is_set(&deactivated));

        close_all(&driver, &[a, b]);
    }

    #[test]
    fn shared_memory() {
        let driver = SyncDriver::new();

        const SIZE: usize = 64;
        let memory = driver.allocate_shared_memory(SIZE).unwrap();
        assert_eq!(SIZE, driver.get_shared_memory_info(memory).unwrap().region_num_bytes);

        let dupe = driver.duplicate_shared_memory(memory).unwrap();
        assert!(dupe.is_valid());

        let mapped1 = driver.map_shared_memory(memory).unwrap();
        assert!(mapped1.mapping.is_valid());
        assert!(!mapped1.address.is_null());

        let mapped2 = driver.map_shared_memory(dupe).unwrap();
        assert!(mapped2.mapping.is_valid());
        assert!(!mapped2.address.is_null());

        // Two different mappings of the same memory object from the
        // single-process driver will always map to the same base address.
        assert_eq!(mapped1.address, mapped2.address);

        close_all(&driver, &[mapped1.mapping, mapped2.mapping, dupe, memory]);
    }

    #[test]
    fn transmit_handles() {
        let driver = SyncDriver::new();
        let (a, b) = transports(&driver);

        let received_handle = Arc::new(Mutex::new(INVALID_DRIVER_HANDLE));
        let receiver = {
            let received_handle = received_handle.clone();
            TransportReceiver::default()
                .on_message(move |message| {
                    if let [handle] = message.handles[..] {
                        *received_handle.lock().unwrap() = handle;
                    }
                    IpczResult::Ok
                })
                .build()
        };
        assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

        let memory = driver.allocate_shared_memory(64).unwrap();
        assert!(memory.is_valid());
        assert_eq!(IpczResult::Ok, driver.transmit(a, &[], &[memory]));

        // Driver handles transmitted through the single-process driver should
        // retain their value across transmission.
        assert_eq!(memory, *received_handle.lock().unwrap());

        assert_eq!(IpczResult::Ok, driver.close(memory));
        assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
        close_all(&driver, &[a, b]);
    }
}
