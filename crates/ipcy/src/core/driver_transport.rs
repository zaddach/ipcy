//! Port of `ipcz/driver_transport.{h,cc}`.
//!
//! DEVIATION: the `APIObject` aspect (i.e. `Close()` rejecting application
//! closes of transport handles) arrives with the API-object layer. Since drivers hold an `Arc<dyn TransportActivityHandler>` instead
//! of an `IpczHandle`, no handle round-trip through the API object table is
//! needed to dispatch driver activity back to a transport.

use std::sync::{Arc, Mutex, MutexGuard};

use super::driver::{
    Driver, DriverHandle, INVALID_DRIVER_HANDLE, RawMessage, TransportActivityHandler,
};
use super::driver_object::DriverObject;
use super::ipcz_result::IpczResult;
use super::message::Message;

/// A listener to receive message and error events from the driver.
pub trait TransportListener: Send + Sync {
    /// Accepts a raw message from the transport. Note that this is called
    /// without *any* validation of the size or content of `message`.
    fn on_transport_message(
        &self,
        message: &RawMessage<'_>,
        transport: &DriverTransport,
        envelope: DriverHandle,
    ) -> bool;

    /// Indicates that some unrecoverable error has occurred with the transport.
    fn on_transport_error(&self);

    /// Indicates that deactivation has been completed by the driver, meaning
    /// that no further methods will be invoked on this listener.
    fn on_transport_deactivated(&self) {}
}

/// Encapsulates shared ownership of a transport endpoint created by an ipcz
/// driver. The driver calls into this object to notify ipcz of incoming messages
/// on the transport, and ipcz calls into this object to submit outgoing messages
/// for transmission by the driver.
pub struct DriverTransport {
    // DEVIATION: guarded because `take_driver_object()` is called on shared
    // references in upstream.
    transport: Mutex<DriverObject>,
    listener: Mutex<Option<Arc<dyn TransportListener>>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

impl DriverTransport {
    /// Constructs a new DriverTransport over the driver-created transport handle
    /// in `transport`.
    pub fn new(transport: DriverObject) -> Arc<DriverTransport> {
        Arc::new(DriverTransport {
            transport: Mutex::new(transport),
            listener: Mutex::new(None),
        })
    }

    /// Creates a new pair of connected DriverTransports, one to send over
    /// `transport0`, and one to send over `transport1`, in order to establish a
    /// direct link between their respective remote nodes. Both may be `None` if
    /// the new transport pair won't be sent anywhere.
    pub fn create_pair(
        driver: &Arc<dyn Driver>,
        transport0: Option<&DriverTransport>,
        transport1: Option<&DriverTransport>,
    ) -> (Arc<DriverTransport>, Arc<DriverTransport>) {
        let (target0, target1) = match transport0 {
            Some(t0) => (
                t0.driver_handle(),
                transport1.expect("both or neither transport").driver_handle(),
            ),
            None => (INVALID_DRIVER_HANDLE, INVALID_DRIVER_HANDLE),
        };
        let (new0, new1) = driver
            .create_transports(target0, target1)
            .expect("driver failed to create transports");
        (
            DriverTransport::new(DriverObject::new(driver.clone(), new0)),
            DriverTransport::new(DriverObject::new(driver.clone(), new1)),
        )
    }

    /// Sets the object handling any incoming message or error notifications.
    /// This is only safe to set before `activate()` is called, or from within
    /// one of the listener methods when invoked by this DriverTransport (because
    /// invocations are mutually exclusive).
    pub fn set_listener(&self, listener: Option<Arc<dyn TransportListener>>) {
        *lock(&self.listener) = listener;
    }

    /// The underlying driver handle for this transport.
    pub fn driver_handle(&self) -> DriverHandle {
        lock(&self.transport).handle()
    }

    /// The driver which created this transport, if the transport is valid.
    pub fn driver(&self) -> Option<Arc<dyn Driver>> {
        lock(&self.transport).driver().cloned()
    }

    fn driver_and_handle(&self) -> Option<(Arc<dyn Driver>, DriverHandle)> {
        let transport = lock(&self.transport);
        transport.driver().cloned().map(|d| (d, transport.handle()))
    }

    /// Releases ownership of the underlying driver transport, returning it to
    /// the caller. After this call, the DriverTransport is reset and has an
    /// invalid driver handle.
    pub fn take_driver_object(&self) -> DriverObject {
        assert!(lock(&self.listener).is_none());
        std::mem::take(&mut *lock(&self.transport))
    }

    /// Begins listening on the transport for incoming data and driver objects.
    /// Once this is called, the transport's listener may be invoked by the
    /// driver at any time from arbitrary threads, as determined by the driver
    /// implementation itself. The driver continues listening until
    /// `deactivate()` is called or an unrecoverable error is encountered.
    pub fn activate(self: &Arc<Self>) -> IpczResult {
        let Some((driver, handle)) = self.driver_and_handle() else {
            return IpczResult::FailedPrecondition;
        };
        // The driver's reference to this transport is the self-reference that
        // upstream acquires here; it's balanced when the driver reports
        // deactivation.
        driver.activate_transport(handle, self.clone())
    }

    /// Requests that the driver cease listening for incoming data and driver
    /// objects on this transport. Once a transport is deactivated, it can never
    /// be reactivated.
    pub fn deactivate(&self) -> IpczResult {
        match self.driver_and_handle() {
            // The transport is already deactivated.
            None => IpczResult::FailedPrecondition,
            Some((driver, handle)) => driver.deactivate_transport(handle),
        }
    }

    /// Helper for transmitting macro-generated ipcz messages. This performs any
    /// necessary in-place serialization of driver objects before transmitting,
    /// hence it takes a mutable reference to `message`.
    pub fn transmit(&self, message: &mut Message) -> IpczResult {
        assert!(message.can_transmit_on(self));
        if !message.serialize(self) {
            // If serialization fails despite the object appearing to be
            // serializable, we have to assume the transport is in a
            // dysfunctional state and will be torn down by the driver soon.
            // Discard the transmission.
            return IpczResult::FailedPrecondition;
        }

        self.transmit_raw(message.data_view(), message.transmissible_driver_handles())
    }

    /// Hands `data` and `handles` to the driver for transmission.
    pub fn transmit_raw(&self, data: &[u8], handles: &[DriverHandle]) -> IpczResult {
        match self.driver_and_handle() {
            None => IpczResult::FailedPrecondition,
            Some((driver, handle)) => driver.transmit(handle, data, handles),
        }
    }

    /// Invoked by the driver any time this transport receives data and driver
    /// handles to be passed back into ipcz.
    pub fn notify(&self, message: &RawMessage<'_>, envelope: DriverHandle) -> bool {
        // Listener methods may set a new listener on this DriverTransport, and
        // that may drop their own last reference. Keep a reference here to
        // ensure this listener remains alive through its notification.
        let listener = lock(&self.listener).clone().expect("transport has a listener");
        listener.on_transport_message(message, self, envelope)
    }

    pub fn notify_error(&self) {
        let listener = lock(&self.listener).clone().expect("transport has a listener");
        listener.on_transport_error();
    }

    /// Invoked once the driver has finalized deactivation of this transport, as
    /// previously requested by a call to `deactivate()`.
    pub fn notify_deactivated(&self) {
        let listener = lock(&self.listener).take().expect("transport has a listener");
        listener.on_transport_deactivated();
    }
}

impl TransportActivityHandler for DriverTransport {
    fn on_message(&self, message: RawMessage<'_>, envelope: DriverHandle) -> IpczResult {
        if self.notify(&message, envelope) {
            IpczResult::Ok
        } else {
            IpczResult::InvalidArgument
        }
    }

    fn on_error(&self) {
        self.notify_error();
    }

    fn on_deactivated(&self) {
        self.notify_deactivated();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::testing::mock_driver::MockDriver;
    use crate::core::testing::test_transport_listener::TestTransportListener;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn h(value: usize) -> DriverHandle {
        DriverHandle::new(value)
    }

    fn create_transport_pair(
        driver: &Arc<MockDriver>,
        transport0: usize,
        transport1: usize,
    ) -> (Arc<DriverTransport>, Arc<DriverTransport>) {
        (
            DriverTransport::new(DriverObject::new(driver.as_driver(), h(transport0))),
            DriverTransport::new(DriverObject::new(driver.as_driver(), h(transport1))),
        )
    }

    // Captures the handler a transport was activated with.
    fn capture_activation(driver: &MockDriver, transport: usize) -> Arc<Mutex<Option<Arc<dyn TransportActivityHandler>>>> {
        let captured = Arc::new(Mutex::new(None));
        let sink = captured.clone();
        driver.expect_activate(h(transport), move |_, handler| {
            *sink.lock().unwrap() = Some(handler);
            IpczResult::Ok
        });
        captured
    }

    #[test]
    fn activation() {
        let driver = MockDriver::new();
        let (a, b) = create_transport_pair(&driver, 5, 42);
        let handler = capture_activation(&driver, 42);

        // And verify that the activity handler actually invokes the transport's
        // listener.
        const TEST_MESSAGE: &str = "hihihihi";
        let received = Arc::new(AtomicBool::new(false));
        let listener = TestTransportListener::new(b.clone());
        let flag = received.clone();
        listener.on_string_message(move |message| {
            assert_eq!(message, TEST_MESSAGE);
            flag.store(true, Ordering::SeqCst);
        });

        // Verify that activation of a DriverTransport feeds the driver a handler
        // to use when notifying ipcz of incoming communications.
        let handler = handler.lock().unwrap().take().expect("activated");
        assert!(!received.load(Ordering::SeqCst));
        assert_eq!(
            handler.on_message(
                RawMessage { data: TEST_MESSAGE.as_bytes(), handles: &[] },
                INVALID_DRIVER_HANDLE
            ),
            IpczResult::Ok
        );
        assert!(received.load(Ordering::SeqCst));

        // Normal shutdown involves ipcz calling deactivate() on the
        // DriverTransport. This should result in a call to
        // deactivate_transport() on the driver.
        driver.expect_deactivate(h(42), IpczResult::Ok);
        assert_eq!(b.deactivate(), IpczResult::Ok);

        // The driver must also release its reference to ipcz' DriverTransport,
        // which it does by signaling deactivation like this.
        handler.on_deactivated();
        drop(handler);

        driver.expect_close(h(42));
        driver.expect_close(h(5));
        drop((a, b, listener));
    }

    #[test]
    fn error() {
        let driver = MockDriver::new();
        let (a, b) = create_transport_pair(&driver, 5, 42);
        let handler = capture_activation(&driver, 42);

        let observed_error = Arc::new(AtomicBool::new(false));
        let listener = TestTransportListener::new(b.clone());
        let flag = observed_error.clone();
        listener.on_error(move || flag.store(true, Ordering::SeqCst));

        // Verify that a driver signaling an error results in an error
        // notification on the DriverTransport's listener. This implies
        // deactivation on the ipcz side, so no call to deactivate() is
        // necessary.
        let handler = handler.lock().unwrap().take().expect("activated");
        assert!(!observed_error.load(Ordering::SeqCst));
        handler.on_error();
        assert!(observed_error.load(Ordering::SeqCst));

        // Even after signaling an error, the driver must also signal
        // deactivation on its side, to release its reference.
        handler.on_deactivated();
        drop(handler);

        driver.expect_close(h(42));
        driver.expect_close(h(5));
        drop((a, b, listener));
    }

    #[test]
    fn rejected_message_reports_invalid_argument() {
        let driver = MockDriver::new();
        let (a, b) = create_transport_pair(&driver, 5, 42);
        let handler = capture_activation(&driver, 42);

        let listener = TestTransportListener::new(b.clone());
        listener.on_raw_message(|_| false);
        let handler = handler.lock().unwrap().take().unwrap();
        assert_eq!(
            handler.on_message(RawMessage { data: &[], handles: &[] }, INVALID_DRIVER_HANDLE),
            IpczResult::InvalidArgument
        );
        handler.on_deactivated();

        driver.expect_close(h(42));
        driver.expect_close(h(5));
        drop((a, b, listener, handler));
    }

    #[test]
    fn transmit_raw() {
        let driver = MockDriver::new();
        let (a, b) = create_transport_pair(&driver, 5, 42);

        driver.expect_transmit(h(5), |_, data, handles| {
            assert_eq!(data, b"hello");
            assert_eq!(handles, [DriverHandle::new(9)]);
            IpczResult::Ok
        });
        assert_eq!(a.transmit_raw(b"hello", &[h(9)]), IpczResult::Ok);

        driver.expect_close(h(42));
        driver.expect_close(h(5));
        drop((a, b));
    }

    #[test]
    fn taken_transport_is_inert() {
        let driver = MockDriver::new();
        let (a, b) = create_transport_pair(&driver, 5, 42);

        let mut object = a.take_driver_object();
        assert_eq!(object.handle(), h(5));
        assert_eq!(a.driver_handle(), INVALID_DRIVER_HANDLE);
        assert_eq!(a.deactivate(), IpczResult::FailedPrecondition);
        assert_eq!(a.transmit_raw(&[], &[]), IpczResult::FailedPrecondition);

        driver.expect_close(h(5));
        object.reset();
        driver.expect_close(h(42));
        drop((a, b));
    }
}
