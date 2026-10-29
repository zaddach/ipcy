//! A [`TransportActivityHandler`] built from closures, for driver tests.
//! Mirrors the `TransportReceiver` helper of upstream's driver tests.

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use crate::core::driver::{DriverHandle, RawMessage, TransportActivityHandler};
use crate::core::ipcz_result::IpczResult;

/// An owned copy of a message delivered to a transport.
pub struct TransportMessage {
    pub data: Vec<u8>,
    pub handles: Vec<DriverHandle>,
}

type MessageHandler = Box<dyn Fn(TransportMessage) -> IpczResult + Send + Sync>;
type EventHandler = Box<dyn Fn() + Send + Sync>;

pub struct TransportReceiver {
    on_message: MessageHandler,
    on_deactivate: EventHandler,
    on_error: EventHandler,
}

impl Default for TransportReceiver {
    fn default() -> Self {
        Self {
            on_message: Box::new(|_| IpczResult::Ok),
            on_deactivate: Box::new(|| {}),
            on_error: Box::new(|| {}),
        }
    }
}

impl TransportReceiver {
    pub fn on_message(mut self, handler: impl Fn(TransportMessage) -> IpczResult + Send + Sync + 'static) -> Self {
        self.on_message = Box::new(handler);
        self
    }

    pub fn on_deactivate(mut self, handler: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_deactivate = Box::new(handler);
        self
    }

    pub fn on_error(mut self, handler: impl Fn() + Send + Sync + 'static) -> Self {
        self.on_error = Box::new(handler);
        self
    }

    pub fn build(self) -> Arc<dyn TransportActivityHandler> {
        Arc::new(self)
    }
}

impl TransportActivityHandler for TransportReceiver {
    fn on_message(&self, message: RawMessage<'_>, _envelope: DriverHandle) -> IpczResult {
        (self.on_message)(TransportMessage {
            data: message.data.to_vec(),
            handles: message.handles.to_vec(),
        })
    }

    fn on_error(&self) {
        (self.on_error)();
    }

    fn on_deactivated(&self) {
        (self.on_deactivate)();
    }
}

/// A one-shot event a test thread can wait on, like `absl::Notification`.
#[derive(Clone, Default)]
pub struct Notification(Arc<(Mutex<bool>, Condvar)>);

impl Notification {
    pub fn notify(&self) {
        *self.0.0.lock().unwrap() = true;
        self.0.1.notify_all();
    }

    /// Waits for `notify()`, failing the test if it takes implausibly long.
    pub fn wait(&self) {
        let (mutex, condvar) = &*self.0;
        let (_guard, timeout) = condvar
            .wait_timeout_while(mutex.lock().unwrap(), Duration::from_secs(30), |notified| !*notified)
            .unwrap();
        assert!(!timeout.timed_out(), "timed out waiting for notification");
    }
}
