//! Port of `api_test.cc`.
//!
//! Cases which only exercise the C ABI's argument validation (null pointers,
//! `size` fields, `IpczGetAPI`) have no equivalent in the typed Rust API; the
//! C API they test was removed upstream.

mod support;

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

use ipcy::core::api::{self, TrapError, UnboxedContents};
use ipcy::core::application_object::ApplicationObject;
use ipcy::core::box_object::BoxContents;
use ipcy::core::driver::{Driver, DriverHandle, INVALID_DRIVER_HANDLE};
use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;
use ipcy::core::reference_drivers::sync::SyncDriver;
use support::TestBase;

struct ApiTest {
    base: TestBase,
    driver: Arc<SyncDriver>,
}

impl ApiTest {
    fn new() -> Self {
        Self {
            base: TestBase,
            driver: SyncDriver::new(),
        }
    }

    fn create_node(&self, flags: CreateNodeFlags) -> IpczHandle {
        api::create_node(self.driver.clone(), flags, None).unwrap()
    }

    fn create_transports(&self) -> (DriverHandle, DriverHandle) {
        self.driver
            .create_transports(INVALID_DRIVER_HANDLE, INVALID_DRIVER_HANDLE)
            .unwrap()
    }
}

fn string_from_data(data: *const u8, size: usize) -> String {
    // SAFETY: `data` is valid for `size` bytes for the duration of the
    // transaction that produced it.
    String::from_utf8(unsafe { std::slice::from_raw_parts(data, size) }.to_vec()).unwrap()
}

#[test]
fn close_invalid() {
    assert_eq!(IpczResult::InvalidArgument, api::close(0));
    assert_eq!(IpczResult::InvalidArgument, api::close(0xdead_beef));
}

#[test]
fn create_node() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    assert_ne!(0, node);
    let broker = t.create_node(CreateNodeFlags::AS_BROKER);
    assert_ne!(node, broker);
    t.base.close_all(&[node, broker]);
}

#[test]
fn connect_node_invalid() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (transport0, transport1) = t.create_transports();

    // Invalid node handle.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::connect_node(0, transport0, 1, ConnectNodeFlags::empty()).unwrap_err()
    );

    // Non-node handle.
    let (a, b) = api::open_portals(node).unwrap();
    assert_eq!(
        IpczResult::InvalidArgument,
        api::connect_node(a, transport0, 1, ConnectNodeFlags::empty()).unwrap_err()
    );
    t.base.close_all(&[a, b]);

    // Invalid transport.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::connect_node(node, INVALID_DRIVER_HANDLE, 1, ConnectNodeFlags::empty()).unwrap_err()
    );

    // No initial portals.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::connect_node(node, transport0, 0, ConnectNodeFlags::empty()).unwrap_err()
    );

    // Too many initial portals.
    assert_eq!(
        IpczResult::OutOfRange,
        api::connect_node(node, transport0, 1000, ConnectNodeFlags::empty()).unwrap_err()
    );

    assert_eq!(IpczResult::Ok, t.driver.close(transport0));
    assert_eq!(IpczResult::Ok, t.driver.close(transport1));

    t.base.close(node);
}

#[test]
fn open_portals_invalid() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());

    // Invalid node.
    assert_eq!(IpczResult::InvalidArgument, api::open_portals(0).unwrap_err());

    // Not a node.
    let (a, b) = api::open_portals(node).unwrap();
    assert_eq!(IpczResult::InvalidArgument, api::open_portals(a).unwrap_err());
    t.base.close_all(&[a, b, node]);
}

#[test]
fn open_portals() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());

    let (a, b) = api::open_portals(node).unwrap();
    t.base.close_all(&[a, b, node]);
}

#[test]
fn query_portal_status_invalid() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    // Null portal.
    assert_eq!(IpczResult::InvalidArgument, api::query_portal_status(0).unwrap_err());

    // Not a portal.
    assert_eq!(IpczResult::InvalidArgument, api::query_portal_status(node).unwrap_err());

    t.base.close_all(&[a, b, node]);
}

#[test]
fn query_portal_status() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    let status = api::query_portal_status(a).unwrap();
    assert!(!status.flags.contains(PortalStatusFlags::PEER_CLOSED));
    assert!(!status.flags.contains(PortalStatusFlags::DEAD));
    assert_eq!(0, status.num_local_parcels);
    assert_eq!(0, status.num_local_bytes);

    t.base.close(b);
    let status = api::query_portal_status(a).unwrap();
    assert!(status.flags.contains(PortalStatusFlags::PEER_CLOSED));
    assert!(status.flags.contains(PortalStatusFlags::DEAD));

    t.base.close_all(&[a, node]);
}

#[test]
fn merge_portals_failure() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    // Invalid portal handles.
    assert_eq!(IpczResult::InvalidArgument, api::merge_portals(a, 0));
    assert_eq!(IpczResult::InvalidArgument, api::merge_portals(0, a));
    assert_eq!(IpczResult::InvalidArgument, api::merge_portals(0, 0));

    // Can't merge into own peer.
    assert_eq!(IpczResult::InvalidArgument, api::merge_portals(a, b));

    // Can't merge into self.
    assert_eq!(IpczResult::InvalidArgument, api::merge_portals(a, a));

    let (c, d) = api::open_portals(node).unwrap();

    // Can't merge a portal that's had parcels put into it.
    assert_eq!(IpczResult::Ok, t.base.put(c, "!", &[]));
    assert_eq!(IpczResult::FailedPrecondition, api::merge_portals(a, c));

    // Can't merge a portal that's had parcels retrieved from it.
    assert_eq!(IpczResult::Ok, t.base.get_message(d).0);
    assert_eq!(IpczResult::FailedPrecondition, api::merge_portals(a, d));

    t.base.close_all(&[a, b, c, d, node]);
}

#[test]
fn merge_portals() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();
    let (c, d) = api::open_portals(node).unwrap();

    assert_eq!(IpczResult::Ok, t.base.put(a, "!", &[]));
    assert_eq!(IpczResult::Ok, api::merge_portals(b, c));

    // The message from `a` should be routed to `d`, since `b` and `c` have been
    // merged.
    let (result, message) = t.base.get_message(d);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!("!", message);

    t.base.close_all(&[a, d, node]);
}

#[test]
fn put_get() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    // Get from an empty portal.
    let mut data = [0u8; 4];
    assert_eq!(
        IpczResult::Unavailable,
        api::get(b, GetFlags::empty(), &mut data, &mut [], false).result
    );

    // A portal can't transfer itself or its peer.
    assert_eq!(IpczResult::InvalidArgument, api::put(a, &[], &[a]));
    assert_eq!(IpczResult::InvalidArgument, api::put(a, &[], &[b]));

    assert_eq!(IpczResult::Ok, api::put(a, b"hi", &[]));
    assert_eq!(IpczResult::Ok, api::put(a, b"bye", &[]));
    assert_eq!(IpczResult::Ok, api::put(a, &[], &[]));

    let (c, d) = api::open_portals(node).unwrap();
    assert_eq!(IpczResult::Ok, api::put(a, &[], &[d]));

    let status = api::query_portal_status(b).unwrap();
    assert_eq!(4, status.num_local_parcels);
    assert_eq!(5, status.num_local_bytes);

    // Insufficient data storage.
    let out = api::get(b, GetFlags::empty(), &mut [], &mut [], false);
    assert_eq!(IpczResult::ResourceExhausted, out.result);
    assert_eq!(2, out.num_bytes);

    let out = api::get(b, GetFlags::empty(), &mut data, &mut [], false);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(b"hi", &data[..out.num_bytes]);

    let out = api::get(b, GetFlags::empty(), &mut data, &mut [], false);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(b"bye", &data[..out.num_bytes]);

    let status = api::query_portal_status(b).unwrap();
    assert_eq!(2, status.num_local_parcels);
    assert_eq!(0, status.num_local_bytes);

    // Getting an empty parcel requires no storage.
    assert_eq!(
        IpczResult::Ok,
        api::get(b, GetFlags::empty(), &mut [], &mut [], false).result
    );
    let status = api::query_portal_status(b).unwrap();
    assert_eq!(1, status.num_local_parcels);
    assert_eq!(0, status.num_local_bytes);

    // Insufficient handle storage.
    let out = api::get(b, GetFlags::empty(), &mut [], &mut [], false);
    assert_eq!(IpczResult::ResourceExhausted, out.result);
    assert_eq!(1, out.num_handles);

    let mut handles = [0u64; 1];
    let out = api::get(b, GetFlags::empty(), &mut [], &mut handles, false);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(1, out.num_handles);
    t.base.close(handles[0]);

    let status = api::query_portal_status(c).unwrap();
    assert!(status.flags.contains(PortalStatusFlags::PEER_CLOSED));
    assert!(status.flags.contains(PortalStatusFlags::DEAD));

    t.base.close_all(&[a, b, c, node]);
}

#[test]
fn begin_end_put_failure() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    // Invalid portal.
    const PUT_SIZE: usize = 64;
    assert_eq!(
        IpczResult::InvalidArgument,
        api::begin_put(0, BeginPutFlags::empty(), PUT_SIZE).result
    );

    // Start a put transaction to test end_put().
    let out = api::begin_put(a, BeginPutFlags::empty(), PUT_SIZE);
    assert_eq!(IpczResult::Ok, out.result);
    let transaction = out.transaction;

    // Invalid portal.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_put(0, transaction, 0, &[], EndPutFlags::empty())
    );

    // Invalid transaction.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_put(a, 0, 0, &[], EndPutFlags::empty())
    );

    // Oversized data.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_put(a, transaction, PUT_SIZE * 2, &[], EndPutFlags::empty())
    );

    // Invalid handle attachment.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_put(a, transaction, 0, &[0], EndPutFlags::empty())
    );

    // Commit it.
    assert_eq!(
        IpczResult::Ok,
        api::end_put(a, transaction, 0, &[], EndPutFlags::empty())
    );

    t.base.close_all(&[a, b, node]);
}

#[test]
fn begin_end_get_failure() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    // Invalid portal.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::begin_get(0, BeginGetFlags::empty(), &mut []).result
    );

    // No parcel yet.
    assert_eq!(
        IpczResult::Unavailable,
        api::begin_get(a, BeginGetFlags::empty(), &mut []).result
    );

    assert_eq!(IpczResult::Ok, t.base.put(b, "ipcz", &[]));

    // Successful begin_get() to exercise end_get() below.
    let out = api::begin_get(a, BeginGetFlags::empty(), &mut []);
    assert_eq!(IpczResult::Ok, out.result);
    let transaction = out.transaction;

    // Invalid handle.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_get(0, transaction, EndGetFlags::empty(), false).0
    );

    // Invalid transaction.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_get(a, 0, EndGetFlags::empty(), false).0
    );

    // Terminate the get.
    assert_eq!(
        IpczResult::Ok,
        api::end_get(a, transaction, EndGetFlags::empty(), false).0
    );

    // No transaction in progress. `transaction` no longer valid.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_get(a, transaction, EndGetFlags::empty(), false).0
    );

    t.base.close_all(&[a, b, node]);
}

#[test]
fn two_phase_put_get() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    const MESSAGE: &str = "ipcz!";
    let out = api::begin_put(a, BeginPutFlags::empty(), MESSAGE.len());
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(MESSAGE.len(), out.num_bytes);
    // SAFETY: the transaction grants `num_bytes` writable bytes.
    unsafe { std::ptr::copy_nonoverlapping(MESSAGE.as_ptr(), out.data, MESSAGE.len()) };
    assert_eq!(
        IpczResult::Ok,
        api::end_put(a, out.transaction, out.num_bytes, &[], EndPutFlags::empty())
    );

    let get = api::begin_get(b, BeginGetFlags::empty(), &mut []);
    assert_eq!(IpczResult::Ok, get.result);
    assert!(!get.data.is_null());
    assert_eq!(MESSAGE, string_from_data(get.data, get.num_bytes));

    // Aborting the get leaves its parcel queued for another get.
    assert_eq!(
        IpczResult::Ok,
        api::end_get(b, get.transaction, EndGetFlags::ABORT, false).0
    );

    // Beginning a new get exposes the same data.
    let get = api::begin_get(b, BeginGetFlags::empty(), &mut []);
    assert_eq!(IpczResult::Ok, get.result);
    assert_eq!(MESSAGE, string_from_data(get.data, get.num_bytes));
    assert_eq!(
        IpczResult::Ok,
        api::end_get(b, get.transaction, EndGetFlags::empty(), false).0
    );

    // The parcel has been consumed. Nothing else to get from `b` yet.
    assert_eq!(
        IpczResult::Unavailable,
        api::begin_get(b, BeginGetFlags::empty(), &mut []).result
    );

    t.base.close_all(&[a, b, node]);
}

#[test]
fn overlapped_two_phase_puts() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    const MESSAGE1: &str = "Hello.";
    const MESSAGE2: &str = "World?";
    const MESSAGE3: &str = "OK!";

    // Set up three concurrent transactions.
    let begin = |message: &str| {
        let out = api::begin_put(a, BeginPutFlags::empty(), message.len());
        assert_eq!(IpczResult::Ok, out.result);
        assert_eq!(message.len(), out.num_bytes);
        // SAFETY: the transaction grants `num_bytes` writable bytes.
        unsafe { std::ptr::copy_nonoverlapping(message.as_ptr(), out.data, message.len()) };
        out
    };
    let (put1, put2, put3) = (begin(MESSAGE1), begin(MESSAGE2), begin(MESSAGE3));

    // Complete them out-of-order. They should arrive in the order in which they
    // were completed rather than the order in which they were started.
    for put in [&put3, &put1, &put2] {
        assert_eq!(
            IpczResult::Ok,
            api::end_put(a, put.transaction, put.num_bytes, &[], EndPutFlags::empty())
        );
    }

    // Also for good measure attempt to terminate a transaction twice.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::end_put(a, put1.transaction, put1.num_bytes, &[], EndPutFlags::empty())
    );

    for expected in [MESSAGE3, MESSAGE1, MESSAGE2] {
        let mut message = [0u8; 16];
        let out = api::get(b, GetFlags::empty(), &mut message, &mut [], false);
        assert_eq!(IpczResult::Ok, out.result);
        assert_eq!(expected.as_bytes(), &message[..out.num_bytes]);
    }

    t.base.close_all(&[a, b, node]);
}

#[test]
fn trap_invalid() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();

    let handler: TrapEventHandler = Arc::new(|_| {});

    // Invalid or non-portal handle.
    assert_eq!(
        Err(TrapError::Failed(IpczResult::InvalidArgument)),
        api::trap(0, TrapConditions::default(), handler.clone(), 0)
    );
    assert_eq!(
        Err(TrapError::Failed(IpczResult::InvalidArgument)),
        api::trap(node, TrapConditions::default(), handler, 0)
    );

    t.base.close_all(&[a, b, node]);
}

#[test]
fn reject_invalid() {
    let t = ApiTest::new();
    assert_eq!(IpczResult::InvalidArgument, api::reject(0, 0));

    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();
    assert_eq!(IpczResult::InvalidArgument, api::reject(a, 0));
    t.base.close_all(&[a, b, node]);
}

#[test]
fn reject_local() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());
    let (a, b) = api::open_portals(node).unwrap();
    t.base.put(a, "!", &[]);

    let mut byte = [0u8; 1];
    let out = api::get(b, GetFlags::empty(), &mut byte, &mut [], true);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(b'!', byte[0]);
    let parcel = out.parcel.unwrap();

    assert_eq!(IpczResult::FailedPrecondition, api::reject(parcel, 0));

    t.base.close_all(&[a, b, node, parcel]);
}

#[test]
fn reject_remote() {
    let t = ApiTest::new();
    let node_a = t.create_node(CreateNodeFlags::AS_BROKER);
    let node_b = t.create_node(CreateNodeFlags::empty());
    let (transport0, transport1) = t.create_transports();

    let a = api::connect_node(node_a, transport0, 1, ConnectNodeFlags::empty()).unwrap()[0];
    let b = api::connect_node(node_b, transport1, 1, ConnectNodeFlags::TO_BROKER).unwrap()[0];

    t.base.put(a, "!", &[]);
    let mut byte = [0u8; 1];
    let out = api::get(b, GetFlags::empty(), &mut byte, &mut [], true);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(b'!', byte[0]);
    let parcel = out.parcel.unwrap();

    const TEST_CONTEXT: usize = 42;
    let (tx, rx) = mpsc::channel();
    t.driver
        .base()
        .set_bad_transport_activity_callback(Some(Box::new(move |transport, context| {
            tx.send((transport, context)).unwrap();
        })));
    assert_eq!(IpczResult::Ok, api::reject(parcel, TEST_CONTEXT));
    let (error_transport, error_context) = rx.try_recv().unwrap();
    assert_eq!(transport1, error_transport);
    assert_eq!(TEST_CONTEXT, error_context);
    t.driver.base().set_bad_transport_activity_callback(None);

    t.base.close_all(&[a, b, node_b, node_a, parcel]);
}

#[test]
fn box_invalid() {
    let t = ApiTest::new();
    let (transport0, transport1) = t.create_transports();
    assert_eq!(IpczResult::Ok, t.driver.close(transport1));

    let node = t.create_node(CreateNodeFlags::empty());

    // Invalid node handle.
    assert_eq!(IpczResult::InvalidArgument, api::box_driver_object(0, transport0).unwrap_err());

    // Invalid driver object.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::box_driver_object(node, INVALID_DRIVER_HANDLE).unwrap_err()
    );

    assert_eq!(IpczResult::Ok, t.driver.close(transport0));
    t.base.close(node);
}

#[test]
fn unbox_invalid() {
    let t = ApiTest::new();
    let (transport0, transport1) = t.create_transports();
    assert_eq!(IpczResult::Ok, t.driver.close(transport1));

    let node = t.create_node(CreateNodeFlags::empty());
    let boxed = api::box_driver_object(node, transport0).unwrap();
    assert_ne!(0, boxed);

    // Null box handle.
    assert_eq!(
        IpczResult::InvalidArgument,
        api::unbox(0, UnboxFlags::empty()).err().unwrap()
    );

    // Invalid box handle type (node instead of box).
    assert_eq!(
        IpczResult::InvalidArgument,
        api::unbox(node, UnboxFlags::empty()).err().unwrap()
    );

    // Don't close `transport0` because `boxed` owns it.
    t.base.close_all(&[boxed, node]);
}

struct MockApplicationObject {
    id: usize,
    destroyed: Arc<AtomicUsize>,
}

impl Drop for MockApplicationObject {
    fn drop(&mut self) {
        self.destroyed.store(self.id, Ordering::SeqCst);
    }
}

impl ApplicationObject for MockApplicationObject {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[test]
fn box_and_unbox() {
    let t = ApiTest::new();
    let (transport0, transport1) = t.create_transports();
    assert_eq!(IpczResult::Ok, t.driver.close(transport1));

    let node = t.create_node(CreateNodeFlags::empty());

    // Driver object.
    let driver_box = api::box_driver_object(node, transport0).unwrap();
    assert_ne!(0, driver_box);
    let Ok(UnboxedContents::Unboxed(BoxContents::DriverObject(mut object))) =
        api::unbox(driver_box, UnboxFlags::empty())
    else {
        panic!("expected a driver object");
    };
    // Successfully unboxed which dropped ownership of `driver_box`.
    assert_eq!(IpczResult::InvalidArgument, api::close(driver_box));
    assert_eq!(transport0, object.release());
    assert_eq!(IpczResult::Ok, t.driver.close(transport0));

    // Application object.
    let destroyed = Arc::new(AtomicUsize::new(0));
    let app_box = api::box_application_object(
        node,
        Arc::new(MockApplicationObject {
            id: 1,
            destroyed: destroyed.clone(),
        }),
    )
    .unwrap();
    let Ok(UnboxedContents::Unboxed(BoxContents::ApplicationObject(object))) =
        api::unbox(app_box, UnboxFlags::empty())
    else {
        panic!("expected an application object");
    };
    assert_eq!(IpczResult::InvalidArgument, api::close(app_box));
    assert_eq!(1, object.as_any().downcast_ref::<MockApplicationObject>().unwrap().id);
    assert_eq!(0, destroyed.load(Ordering::SeqCst));
    drop(object);
    assert_eq!(1, destroyed.load(Ordering::SeqCst));

    t.base.close(node);
}

#[test]
fn box_destructor() {
    let t = ApiTest::new();
    let node = t.create_node(CreateNodeFlags::empty());

    const OBJECT_ID: usize = 123;
    let destroyed = Arc::new(AtomicUsize::new(0));
    let boxed = api::box_application_object(
        node,
        Arc::new(MockApplicationObject {
            id: OBJECT_ID,
            destroyed: destroyed.clone(),
        }),
    )
    .unwrap();
    assert_ne!(0, boxed);

    // Closing the box should invoke the destructor.
    assert_eq!(0, destroyed.load(Ordering::SeqCst));
    assert_eq!(IpczResult::Ok, api::close(boxed));
    assert_eq!(OBJECT_ID, destroyed.load(Ordering::SeqCst));

    t.base.close(node);
}
