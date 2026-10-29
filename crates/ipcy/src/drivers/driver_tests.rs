//! A driver test suite shared by the real platform drivers. It is instantiated
//! for the Mach driver on macOS, the named-pipe driver on Windows, and — which
//! is what validates the suite itself in environments without those systems —
//! the Unix-socket driver on Linux.
//!
//! Everything runs in one process: a driver's `create_transports()` ends talk
//! to each other exactly as ends in two processes would, and handles are moved
//! with the same serialize/transmit/deserialize sequence ipcz uses.

use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::core::api;
use crate::core::driver::{Driver, DriverHandle, INVALID_DRIVER_HANDLE, SerializeBuffers, SerializeOutcome};
use crate::core::ipcz_result::IpczResult;
use crate::core::ipcz_types::{ConnectNodeFlags, CreateNodeFlags, GetFlags, IpczHandle, PortalStatusFlags};
use crate::core::testing::transport_receiver::{Notification, TransportMessage, TransportReceiver};

const MESSAGE: &[u8] = b"Hello, world!";
const TIMEOUT: Duration = Duration::from_secs(30);

/// Describes a platform driver to the shared tests.
pub trait Platform {
    type Driver: Driver;

    fn new_driver() -> Arc<Self::Driver>;

    /// Lets the two transports of a pair move handles to each other, as they
    /// could between two cooperating processes.
    fn allow_handle_transfer(_driver: &Self::Driver, _a: DriverHandle, _b: DriverHandle) {}
}

pub(super) fn transports<P: Platform>(driver: &P::Driver) -> (DriverHandle, DriverHandle) {
    let (a, b) = driver
        .create_transports(INVALID_DRIVER_HANDLE, INVALID_DRIVER_HANDLE)
        .unwrap();
    P::allow_handle_transfer(driver, a, b);
    (a, b)
}

/// Collects the messages delivered to an activated transport.
pub(super) struct Inbox {
    messages: mpsc::Receiver<TransportMessage>,
    errored: Notification,
    deactivated: Notification,
}

impl Inbox {
    pub(super) fn activate<D: Driver>(driver: &D, transport: DriverHandle) -> Inbox {
        let (sender, messages) = mpsc::channel();
        let sender = Mutex::new(sender);
        let (errored, deactivated) = (Notification::default(), Notification::default());
        let receiver = {
            let (errored, deactivated) = (errored.clone(), deactivated.clone());
            TransportReceiver::default()
                .on_message(move |message| {
                    let _ = sender.lock().unwrap().send(message);
                    IpczResult::Ok
                })
                .on_error(move || errored.notify())
                .on_deactivate(move || deactivated.notify())
                .build()
        };
        assert_eq!(IpczResult::Ok, driver.activate_transport(transport, receiver));
        Inbox {
            messages,
            errored,
            deactivated,
        }
    }

    fn next(&self) -> TransportMessage {
        self.messages
            .recv_timeout(TIMEOUT)
            .expect("timed out waiting for a message")
    }

    pub(super) fn deactivate<D: Driver>(&self, driver: &D, transport: DriverHandle) {
        assert_eq!(IpczResult::Ok, driver.deactivate_transport(transport));
        self.deactivated.wait();
    }
}

/// Serializes `object` for transmission over `carrier`, returning the
/// serialized bytes and the driver handles to transmit.
fn serialize_for<D: Driver>(driver: &D, object: DriverHandle, carrier: DriverHandle) -> (Vec<u8>, Vec<DriverHandle>) {
    let SerializeOutcome::Dimensions(dimensions) = driver.serialize(object, carrier, None) else {
        panic!("object is not serializable over the carrier");
    };
    let mut data = vec![0u8; dimensions.num_bytes];
    let mut handles = vec![INVALID_DRIVER_HANDLE; dimensions.num_driver_handles];
    assert_eq!(
        SerializeOutcome::Serialized,
        driver.serialize(
            object,
            carrier,
            Some(SerializeBuffers {
                data: &mut data,
                handles: &mut handles,
            }),
        )
    );
    (data, handles)
}

/// Sends `object` from `sender` to `receiver`, which are two ends of a pair,
/// and deserializes it on arrival.
pub(super) fn send_object<D: Driver>(
    driver: &D,
    object: DriverHandle,
    sender: DriverHandle,
    receiver_inbox: &Inbox,
    receiver: DriverHandle,
) -> DriverHandle {
    let (serialized, handles) = serialize_for(driver, object, sender);
    assert_eq!(IpczResult::Ok, driver.transmit(sender, &serialized, &handles));
    let message = receiver_inbox.next();
    assert_eq!(serialized, message.data);
    assert_eq!(handles.len(), message.handles.len());
    driver.deserialize(&message.data, &message.handles, receiver).unwrap()
}

pub fn read_write<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);
    let inbox = Inbox::activate(&*driver, b);

    assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));
    let message = inbox.next();
    assert_eq!(MESSAGE, message.data);
    assert!(message.handles.is_empty());

    inbox.deactivate(&*driver, b);
}

pub fn empty_message<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);
    let inbox = Inbox::activate(&*driver, b);

    // Every transmission results in a notification, even an empty one.
    assert_eq!(IpczResult::Ok, driver.transmit(a, &[], &[]));
    let message = inbox.next();
    assert!(message.data.is_empty() && message.handles.is_empty());

    inbox.deactivate(&*driver, b);
}

pub fn transmit_before_activation<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);

    assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));
    let inbox = Inbox::activate(&*driver, b);
    assert_eq!(MESSAGE, inbox.next().data);

    inbox.deactivate(&*driver, b);
}

pub fn disconnect<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);
    let inbox = Inbox::activate(&*driver, b);

    // Closing one end tells the other.
    assert_eq!(IpczResult::Ok, driver.close(a));
    inbox.errored.wait();
    assert!(inbox.messages.try_recv().is_err());

    inbox.deactivate(&*driver, b);
}

pub fn ordered_flood<P: Platform>() {
    const NUM_MESSAGES: u32 = 25_000;
    const MESSAGE_NUM_VALUES: usize = 256;
    let message_bytes = |value: u32| -> Vec<u8> {
        [value; MESSAGE_NUM_VALUES]
            .iter()
            .flat_map(|v| v.to_ne_bytes())
            .collect()
    };

    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);

    let finished = Notification::default();
    let next_expected = Arc::new(AtomicU32::new(0));
    let deactivated = Notification::default();
    let receiver = {
        let (finished, next_expected, deactivated) = (finished.clone(), next_expected.clone(), deactivated.clone());
        TransportReceiver::default()
            .on_message(move |message| {
                // Messages arrive in the order they were sent.
                let expected = next_expected.fetch_add(1, Ordering::SeqCst);
                assert_eq!(message_bytes(expected), message.data);
                if expected + 1 == NUM_MESSAGES {
                    finished.notify();
                }
                IpczResult::Ok
            })
            .on_deactivate(move || deactivated.notify())
            .build()
    };
    assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

    (0..NUM_MESSAGES).for_each(|i| assert_eq!(IpczResult::Ok, driver.transmit(a, &message_bytes(i), &[])));

    finished.wait();
    assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
    deactivated.wait();
}

pub fn large_message<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);
    let inbox = Inbox::activate(&*driver, b);

    // Big enough for out-of-line transfer and for many pipe reads.
    let data: Vec<u8> = (0..3 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
    assert_eq!(IpczResult::Ok, driver.transmit(a, &data, &[]));
    assert!(inbox.next().data == data);

    inbox.deactivate(&*driver, b);
}

pub fn destroy_from_io_thread<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);

    // Closing `b` from within its own error notification, which runs on its
    // I/O thread, must not deadlock or try to join that thread.
    let destruction_done = Notification::default();
    let receiver = {
        let (driver, destruction_done) = (driver.clone(), destruction_done.clone());
        TransportReceiver::default()
            .on_error(move || {
                assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
                assert_eq!(IpczResult::Ok, driver.close(b));
            })
            .on_deactivate(move || destruction_done.notify())
            .build()
    };
    assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

    assert_eq!(IpczResult::Ok, driver.close(a));
    destruction_done.wait();
}

pub fn transmit_after_deactivation_is_discarded<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);

    let inbox_a = Inbox::activate(&*driver, a);
    inbox_a.deactivate(&*driver, a);

    let received = Arc::new(AtomicBool::new(false));
    let receiver = {
        let received = received.clone();
        TransportReceiver::default()
            .on_message(move |_| {
                received.store(true, Ordering::SeqCst);
                IpczResult::Ok
            })
            .build()
    };
    assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

    assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));
    std::thread::sleep(Duration::from_millis(200));
    assert!(!received.load(Ordering::SeqCst));
}

pub fn shared_memory<P: Platform>() {
    let driver = P::new_driver();

    const SIZE: usize = 5000;
    let memory = driver.allocate_shared_memory(SIZE).unwrap();
    assert_eq!(SIZE, driver.get_shared_memory_info(memory).unwrap().region_num_bytes);
    let dupe = driver.duplicate_shared_memory(memory).unwrap();
    assert_eq!(SIZE, driver.get_shared_memory_info(dupe).unwrap().region_num_bytes);

    // Mappings of the same memory, even through a duplicate, are backed by the
    // same pages; they may or may not have the same address.
    let first = driver.map_shared_memory(memory).unwrap();
    let second = driver.map_shared_memory(dupe).unwrap();
    assert!(!first.address.is_null() && !second.address.is_null());
    // SAFETY: both are live mappings of at least `SIZE` bytes.
    unsafe {
        first.address.write_volatile(0);
        first.address.add(SIZE - 1).write_volatile(0);
        second.address.write_volatile(42);
        second.address.add(SIZE - 1).write_volatile(43);
        assert_eq!(42, first.address.read_volatile());
        assert_eq!(43, first.address.add(SIZE - 1).read_volatile());
    }

    // The mappings outlive the memory handles.
    assert_eq!(IpczResult::Ok, driver.close(memory));
    assert_eq!(IpczResult::Ok, driver.close(dupe));
    // SAFETY: the mappings are still live.
    unsafe { assert_eq!(42, first.address.read_volatile()) };
    assert_eq!(IpczResult::Ok, driver.close(first.mapping));
    assert_eq!(IpczResult::Ok, driver.close(second.mapping));
}

pub fn send_memory<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);
    let inbox = Inbox::activate(&*driver, b);

    const MEMORY_MESSAGE: &[u8] = b"heckin memory chonk here";
    let memory = driver.allocate_shared_memory(MEMORY_MESSAGE.len()).unwrap();
    let mapped = driver.map_shared_memory(memory).unwrap();
    // SAFETY: the mapping is at least `MEMORY_MESSAGE.len()` writable bytes.
    unsafe { std::ptr::copy_nonoverlapping(MEMORY_MESSAGE.as_ptr(), mapped.address, MEMORY_MESSAGE.len()) };

    let received = send_object(&*driver, memory, a, &inbox, b);
    assert_eq!(
        MEMORY_MESSAGE.len(),
        driver.get_shared_memory_info(received).unwrap().region_num_bytes
    );
    let remapped = driver.map_shared_memory(received).unwrap();
    // SAFETY: the mapping is at least `MEMORY_MESSAGE.len()` readable bytes.
    let contents = unsafe { std::slice::from_raw_parts(remapped.address, MEMORY_MESSAGE.len()) };
    assert_eq!(MEMORY_MESSAGE, contents);

    inbox.deactivate(&*driver, b);
}

pub fn send_transport<P: Platform>() {
    let driver = P::new_driver();
    let (a, b) = transports::<P>(&driver);
    let inbox = Inbox::activate(&*driver, b);

    // A new pair of transports: send one end across the carrier and talk
    // through it from the other side.
    let (x, y) = transports::<P>(&driver);
    let y_received = send_object(&*driver, y, a, &inbox, b);

    let inbox_y = Inbox::activate(&*driver, y_received);
    assert_eq!(IpczResult::Ok, driver.transmit(x, MESSAGE, &[]));
    assert_eq!(MESSAGE, inbox_y.next().data);

    inbox_y.deactivate(&*driver, y_received);
    inbox.deactivate(&*driver, b);
}

pub fn serialization_needs_a_transport<P: Platform>() {
    let driver = P::new_driver();
    let memory = driver.allocate_shared_memory(64).unwrap();
    assert_eq!(
        SerializeOutcome::NeedsTransport,
        driver.serialize(memory, INVALID_DRIVER_HANDLE, None)
    );
    assert_eq!(
        SerializeOutcome::Error(IpczResult::InvalidArgument),
        driver.serialize(INVALID_DRIVER_HANDLE, INVALID_DRIVER_HANDLE, None)
    );
}

fn get_message(portal: IpczHandle) -> Option<Vec<u8>> {
    let mut data = [0u8; 64];
    let out = api::get(portal, GetFlags::empty(), &mut data, &mut [], false);
    (out.result == IpczResult::Ok).then(|| data[..out.num_bytes].to_vec())
}

fn wait_for_message(portal: IpczHandle) -> Vec<u8> {
    for _ in 0..3000 {
        if let Some(message) = get_message(portal) {
            return message;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("timed out waiting for a parcel");
}

/// Two nodes connected over the driver exchange parcels, a portal, and see
/// closure.
pub fn nodes_exchange_parcels<P: Platform>() {
    let driver = P::new_driver();
    let broker = api::create_node(driver.clone(), CreateNodeFlags::AS_BROKER, None).unwrap();
    let client = api::create_node(driver.clone(), CreateNodeFlags::empty(), None).unwrap();
    let (t0, t1) = transports::<P>(&driver);
    let p0 = api::connect_node(broker, t0, 1, ConnectNodeFlags::empty()).unwrap();
    let p1 = api::connect_node(client, t1, 1, ConnectNodeFlags::TO_BROKER).unwrap();

    assert_eq!(IpczResult::Ok, api::put(p0[0], b"ping", &[]));
    assert_eq!(b"ping", wait_for_message(p1[0]).as_slice());
    assert_eq!(IpczResult::Ok, api::put(p1[0], b"pong", &[]));
    assert_eq!(b"pong", wait_for_message(p0[0]).as_slice());

    // Send a portal across and use it end to end.
    let (kept, sent) = api::open_portals(client).unwrap();
    assert_eq!(IpczResult::Ok, api::put(p1[0], b"portal", &[sent]));
    let mut handles = [0u64; 1];
    let received = (0..3000)
        .find_map(|_| {
            let mut data = [0u8; 16];
            let out = api::get(p0[0], GetFlags::empty(), &mut data, &mut handles, false);
            if out.result != IpczResult::Ok {
                std::thread::sleep(Duration::from_millis(10));
            }
            (out.result == IpczResult::Ok).then_some(handles[0])
        })
        .expect("timed out waiting for the portal");
    assert_eq!(IpczResult::Ok, api::put(kept, b"through the portal", &[]));
    assert_eq!(b"through the portal", wait_for_message(received).as_slice());

    // Closure propagates.
    api::close(kept);
    for _ in 0..3000 {
        if api::query_portal_status(received)
            .unwrap()
            .flags
            .contains(PortalStatusFlags::PEER_CLOSED)
        {
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(
        api::query_portal_status(received)
            .unwrap()
            .flags
            .contains(PortalStatusFlags::PEER_CLOSED)
    );

    [received, p0[0], p1[0], broker, client].into_iter().for_each(|h| {
        api::close(h);
    });
}

/// Instantiates every shared test for the platform `$platform`.
macro_rules! instantiate_driver_tests {
    ($platform:ty) => {
        instantiate_driver_tests!(
            $platform;
            read_write,
            empty_message,
            transmit_before_activation,
            disconnect,
            ordered_flood,
            large_message,
            destroy_from_io_thread,
            transmit_after_deactivation_is_discarded,
            shared_memory,
            send_memory,
            send_transport,
            serialization_needs_a_transport,
            nodes_exchange_parcels
        );
    };
    ($platform:ty; $($name:ident),*) => {
        $(
            #[test]
            fn $name() {
                $crate::drivers::driver_tests::$name::<$platform>();
            }
        )*
    };
}
pub(crate) use instantiate_driver_tests;
