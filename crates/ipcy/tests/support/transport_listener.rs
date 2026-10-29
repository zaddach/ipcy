//! Port of `test/test_transport_listener.{h,cc}`: a helper for tests to
//! conveniently listen for incoming messages on a driver transport.

use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, Mutex};

use ipcy::core::driver::{Driver, DriverHandle, RawMessage};
use ipcy::core::driver_object::DriverObject;
use ipcy::core::driver_transport::{DriverTransport, TransportListener};
use ipcy::core::message::Message;

type MessageHandler = Box<dyn FnMut(&RawMessage<'_>) -> bool + Send>;
type ErrorHandler = Box<dyn FnMut() + Send>;

struct ListenerImpl {
    message_handler: Mutex<Option<MessageHandler>>,
    error_handler: Mutex<Option<ErrorHandler>>,
    deactivated: SyncSender<()>,
}

impl TransportListener for ListenerImpl {
    fn on_transport_message(&self, message: &RawMessage<'_>, _: &DriverTransport, _: DriverHandle) -> bool {
        match self.message_handler.lock().unwrap().as_mut() {
            Some(handler) => handler(message),
            None => false,
        }
    }

    fn on_transport_error(&self) {
        if let Some(handler) = self.error_handler.lock().unwrap().as_mut() {
            handler();
        }
    }

    fn on_transport_deactivated(&self) {
        let _ = self.deactivated.try_send(());
    }
}

/// Owns `transport` for the duration of the test. Dropping this deactivates the
/// transport (if activated), which in turn closes it.
pub struct TestTransportListener {
    transport: Arc<DriverTransport>,
    listener: Arc<ListenerImpl>,
    deactivated: Receiver<()>,
    activated: Mutex<bool>,
}

impl TestTransportListener {
    pub fn new(driver: &Arc<dyn Driver>, transport: DriverHandle) -> Self {
        let transport = DriverTransport::new(DriverObject::new(driver.clone(), transport));
        let (deactivated_tx, deactivated) = sync_channel(1);
        let listener = Arc::new(ListenerImpl {
            message_handler: Mutex::new(None),
            error_handler: Mutex::new(None),
            deactivated: deactivated_tx,
        });
        transport.set_listener(Some(listener.clone()));
        Self {
            transport,
            listener,
            deactivated,
            activated: Mutex::new(false),
        }
    }

    fn activate_if_necessary(&self) {
        let mut activated = self.activated.lock().unwrap();
        if !*activated {
            *activated = true;
            self.transport.activate();
        }
    }

    /// Invokes `handler` every time a message is received on the transport.
    /// Activates the transport.
    pub fn on_raw_message(&self, handler: impl FnMut(&RawMessage<'_>) -> bool + Send + 'static) {
        *self.listener.message_handler.lock().unwrap() = Some(Box::new(handler));
        self.activate_if_necessary();
    }

    /// Invokes `handler` when the transport encounters an error. Activates the
    /// transport.
    pub fn on_error(&self, handler: impl FnMut() + Send + 'static) {
        *self.listener.error_handler.lock().unwrap() = Some(Box::new(handler));

        // Since the caller only cares about handling errors, ensure all valid
        // messages are cleanly discarded. This also activates the transport.
        let transport = self.transport.clone();
        self.on_raw_message(move |message| Message::new_incoming().deserialize_unknown_type(message, &transport));
    }

    /// Stops listening, deactivating the transport and waiting for the driver
    /// to finish deactivating it.
    pub fn stop_listening(&self) {
        let mut activated = self.activated.lock().unwrap();
        if !*activated {
            return;
        }
        self.transport.deactivate();
        *activated = false;
        let _ = self.deactivated.recv();
    }
}

impl Drop for TestTransportListener {
    fn drop(&mut self) {
        self.stop_listening();
    }
}
