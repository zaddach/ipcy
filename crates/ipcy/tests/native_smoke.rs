//! Smoke tests for the native core over the sync reference driver.

use ipcy::core::api;
use ipcy::core::driver::Driver;
use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;
use ipcy::core::reference_drivers::sync::SyncDriver;

fn get_message(portal: IpczHandle) -> Option<Vec<u8>> {
    let mut data = [0u8; 64];
    let out = api::get(portal, GetFlags::empty(), &mut data, &mut [], false);
    (out.result == IpczResult::Ok).then(|| data[..out.num_bytes].to_vec())
}

#[test]
fn local_portals() {
    let driver = SyncDriver::new();
    let node = api::create_node(driver, CreateNodeFlags::AS_BROKER, None).unwrap();
    let (a, b) = api::open_portals(node).unwrap();
    assert_eq!(api::put(a, b"hello", &[]), IpczResult::Ok);
    assert_eq!(get_message(b).as_deref(), Some(&b"hello"[..]));
    assert_eq!(get_message(b), None);
    assert_eq!(api::close(a), IpczResult::Ok);
    assert!(api::query_portal_status(b).unwrap().flags.contains(PortalStatusFlags::PEER_CLOSED));
    assert_eq!(api::close(b), IpczResult::Ok);
    assert_eq!(api::close(node), IpczResult::Ok);
}

#[test]
fn two_nodes() {
    let driver = SyncDriver::new();
    let broker = api::create_node(driver.clone(), CreateNodeFlags::AS_BROKER, None).unwrap();
    let client = api::create_node(driver.clone(), CreateNodeFlags::empty(), None).unwrap();
    let (t0, t1) = driver.create_transports(Default::default(), Default::default()).unwrap();
    let p0 = api::connect_node(broker, t0, 1, ConnectNodeFlags::empty()).unwrap();
    let p1 = api::connect_node(client, t1, 1, ConnectNodeFlags::TO_BROKER).unwrap();

    assert_eq!(api::put(p0[0], b"ping", &[]), IpczResult::Ok);
    assert_eq!(get_message(p1[0]).as_deref(), Some(&b"ping"[..]));
    assert_eq!(api::put(p1[0], b"pong", &[]), IpczResult::Ok);
    assert_eq!(get_message(p0[0]).as_deref(), Some(&b"pong"[..]));

    // Send a portal across the connection.
    let (a, b) = api::open_portals(client).unwrap();
    assert_eq!(api::put(p1[0], b"portal", &[b]), IpczResult::Ok);
    let mut data = [0u8; 16];
    let mut handles = [0u64; 1];
    let out = api::get(p0[0], GetFlags::empty(), &mut data, &mut handles, false);
    assert_eq!(out.result, IpczResult::Ok);
    assert_eq!(out.num_handles, 1);
    assert_eq!(api::put(a, b"via", &[]), IpczResult::Ok);
    assert_eq!(get_message(handles[0]).as_deref(), Some(&b"via"[..]));

    for h in [a, handles[0], p0[0], p1[0], broker, client] {
        api::close(h);
    }
}
