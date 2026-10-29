//! Tests of the high-level RAII API (`ipcy::{Node, Portal, ...}`) over the native
//! core, using the sync reference driver.

use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

use ipcy::core::driver::Driver;
use ipcy::core::ipcz_types::*;
use ipcy::core::reference_drivers::sync::SyncDriver;
use ipcy::{BoxContents, IpczError, Node, Portal, TrapInstallError};

fn node(driver: &Arc<SyncDriver>, broker: bool) -> Node {
    let flags = if broker { CreateNodeFlags::AS_BROKER } else { CreateNodeFlags::empty() };
    Node::create(driver.clone(), flags, None).unwrap()
}

fn connected_nodes(driver: &Arc<SyncDriver>) -> ((Node, Portal), (Node, Portal)) {
    let broker = node(driver, true);
    let client = node(driver, false);
    let (t0, t1) = driver.create_transports(Default::default(), Default::default()).unwrap();
    let p0 = broker.connect_node(t0, 1, ConnectNodeFlags::empty()).unwrap().remove(0);
    let p1 = client.connect_node(t1, 1, ConnectNodeFlags::TO_BROKER).unwrap().remove(0);
    ((broker, p0), (client, p1))
}

#[test]
fn put_and_get_across_nodes() {
    let driver = SyncDriver::new();
    let ((_broker, p0), (_client, p1)) = connected_nodes(&driver);

    p0.put(b"hello", vec![]).unwrap();
    let out = p1.get().unwrap();
    assert_eq!(b"hello", &out.data[..]);
    assert!(out.handles.is_empty());
    assert_eq!(IpczError::Unavailable, p1.get().unwrap_err());
}

#[test]
fn handles_transfer_with_parcel() {
    let driver = SyncDriver::new();
    let ((client_node, p0), (_other, p1)) = connected_nodes(&driver);
    let (a, b) = client_node.open_portals().unwrap();

    p0.put(b"portal", vec![b.into()]).unwrap();
    let mut out = p1.get().unwrap();
    assert_eq!(1, out.handles.len());
    let moved = unsafe { Portal::from_raw(out.handles.remove(0).into_raw()) }.unwrap();

    a.put(b"through", vec![]).unwrap();
    assert_eq!(b"through", &moved.get().unwrap().data[..]);
}

#[test]
fn failed_put_returns_handles() {
    let driver = SyncDriver::new();
    let n = node(&driver, true);
    let (a, b) = n.open_portals().unwrap();
    // A portal can't be sent through itself.
    let err = a.put(b"", vec![a_handle(&a)]).unwrap_err();
    assert_eq!(IpczError::InvalidArgument, err.error);
    assert_eq!(1, err.handles.len());
    // Don't close `a` twice: the returned handle is a second owner.
    err.handles.into_iter().for_each(|h| {
        h.into_raw();
    });
    drop(b);
}

fn a_handle(portal: &Portal) -> ipcy::Handle {
    // SAFETY: only used to provoke an error; callers leak it afterwards.
    unsafe { ipcy::Handle::from_raw(portal.raw()) }.unwrap()
}

#[test]
fn two_phase_put_and_get() {
    let driver = SyncDriver::new();
    let n = node(&driver, true);
    let (a, b) = n.open_portals().unwrap();

    let mut put = a.begin_put(5, BeginPutFlags::empty()).unwrap();
    assert_eq!(5, put.capacity());
    put.buffer().copy_from_slice(b"ipcz!");
    put.commit(5, vec![]).unwrap();

    let get = b.begin_get(BeginGetFlags::empty()).unwrap();
    assert_eq!(b"ipcz!", get.data());
    // Dropping aborts nothing for a full get: it commits and dequeues.
    drop(get);
    assert_eq!(IpczError::Unavailable, b.begin_get(BeginGetFlags::empty()).unwrap_err());
}

#[test]
fn partial_get_returns_parcel() {
    let driver = SyncDriver::new();
    let n = node(&driver, true);
    let (a, b) = n.open_portals().unwrap();
    a.put(b"abcdef", vec![]).unwrap();

    let out = b.get_partial(0, 0).unwrap();
    let parcel = out.parcel.expect("a parcel object");
    let full = parcel.get().unwrap();
    assert_eq!(b"abcdef", &full.data[..]);
}

#[test]
fn trap_fires_once_and_reports_satisfied() {
    let driver = SyncDriver::new();
    let n = node(&driver, true);
    let (a, b) = n.open_portals().unwrap();

    let conditions = TrapConditions {
        flags: TrapConditionFlags::PEER_CLOSED,
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel();
    let _trap = b.trap(conditions, move |event| tx.send(event).unwrap()).unwrap();
    drop(a);
    let event = rx.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(event.condition_flags.contains(TrapConditionFlags::PEER_CLOSED));

    match b.trap(conditions, |_| {}) {
        Err(TrapInstallError::AlreadySatisfied { satisfied_condition_flags, status }) => {
            assert_eq!(TrapConditionFlags::PEER_CLOSED, satisfied_condition_flags);
            assert!(status.flags.contains(PortalStatusFlags::PEER_CLOSED));
        }
        other => panic!("unexpected result {:?}", other.map(|_| ())),
    }
}

#[test]
fn merge_portals() {
    let driver = SyncDriver::new();
    let n = node(&driver, true);
    let (a, b) = n.open_portals().unwrap();
    let (c, d) = n.open_portals().unwrap();

    a.put(b"!", vec![]).unwrap();
    b.merge_with(c).unwrap();
    assert_eq!(b"!", &d.get().unwrap().data[..]);

    // Can't merge a portal with its own peer.
    let (e, f) = n.open_portals().unwrap();
    let err = e.merge_with(f).unwrap_err();
    assert_eq!(IpczError::InvalidArgument, err.error);
}

#[test]
fn box_and_unbox_driver_object() {
    let driver = SyncDriver::new();
    let n = node(&driver, true);
    let memory = driver.allocate_shared_memory(16).unwrap();

    let boxed = n.box_contents(BoxContents::DriverObject(memory)).unwrap();
    assert!(matches!(boxed.peek_unbox().unwrap(), ipcy::PeekedBoxContents::DriverObject(h) if h == memory));
    match boxed.unbox().unwrap() {
        BoxContents::DriverObject(handle) => assert_eq!(memory, handle),
        other => panic!("unexpected contents {other:?}"),
    }
    driver.close(memory);
}

#[test]
fn unboxing_a_non_box_fails() {
    let driver = SyncDriver::new();
    let n = node(&driver, true);
    let (a, _b) = n.open_portals().unwrap();
    let fake = unsafe { ipcy::BoxHandle::from_raw(a.raw()) }.unwrap();
    let err = fake.unbox().unwrap_err();
    assert_eq!(IpczError::InvalidArgument, err.error);
    // `fake` aliases `a`; leak it to avoid a double close.
    err.handle.into_raw();
}
