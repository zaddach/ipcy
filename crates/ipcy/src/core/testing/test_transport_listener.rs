//! Port of `test/test_transport_listener.{h,cc}` (the parts not needing the
//! message layer): a helper for tests to conveniently listen for incoming
//! messages on a driver transport.

use std::sync::{Arc, Mutex};

use crate::core::driver::RawMessage;
use crate::core::driver::DriverHandle;
use crate::core::driver_transport::{DriverTransport, TransportListener};

type MessageHandler = Box<dyn FnMut(&RawMessage<'_>) -> bool + Send>;
type ErrorHandler = Box<dyn FnMut() + Send>;

#[derive(Default)]
struct ListenerImpl {
    message_handler: Mutex<Option<MessageHandler>>,
    error_handler: Mutex<Option<ErrorHandler>>,
}

impl TransportListener for ListenerImpl {
    fn on_transport_message(&self, message: &RawMessage<'_>, _: &DriverTransport, _: DriverHandle) -> bool {
        let mut handler = self.message_handler.lock().unwrap();
        (handler.as_mut().expect("message handler set"))(message)
    }

    fn on_transport_error(&self) {
        if let Some(handler) = self.error_handler.lock().unwrap().as_mut() {
            handler();
        }
    }
}

pub struct TestTransportListener {
    transport: Arc<DriverTransport>,
    listener: Arc<ListenerImpl>,
}

impl TestTransportListener {
    pub fn new(transport: Arc<DriverTransport>) -> Self {
        let listener = Arc::new(ListenerImpl::default());
        transport.set_listener(Some(listener.clone()));
        Self { transport, listener }
    }

    /// Invokes `handler` every time a message is received on the transport.
    /// Activates the transport.
    pub fn on_raw_message(&self, handler: impl FnMut(&RawMessage<'_>) -> bool + Send + 'static) {
        *self.listener.message_handler.lock().unwrap() = Some(Box::new(handler));
        self.transport.activate();
    }

    /// Like `on_raw_message()`, but messages are expected to have no driver
    /// handles attached, and message data is exposed as string data.
    pub fn on_string_message(&self, mut handler: impl FnMut(&str) + Send + 'static) {
        self.on_raw_message(move |message| {
            assert!(message.handles.is_empty());
            handler(std::str::from_utf8(message.data).unwrap());
            true
        });
    }

    /// Invokes `handler` when the transport encounters an error. Activates the
    /// transport.
    pub fn on_error(&self, handler: impl FnMut() + Send + 'static) {
        *self.listener.error_handler.lock().unwrap() = Some(Box::new(handler));
        // Errors alone don't need a message handler, but a message arriving
        // without one is a test bug.
        self.listener
            .message_handler
            .lock()
            .unwrap()
            .get_or_insert_with(|| Box::new(|_| panic!("unexpected message")));
        self.transport.activate();
    }
}
