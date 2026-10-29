//! Port of `merge_portals_test.cc`.

#[macro_use]
mod support;

use std::time::Duration;

use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;

const MESSAGE1: &str = "bork bork";
const MESSAGE2: &str = "aw heck";

multinode_test_node!(MergeWithInitialPortalClient, |t| {
    let b = t.connect_to_broker();

    let (result, message) = t.wait_to_get_message(b);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(MESSAGE1, message);
    assert_eq!(IpczResult::Ok, t.put(b, MESSAGE2, &[]));
    t.wait_for_direct_remote_link(b);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test!(merge_with_initial_portal, |t| {
    let c = t.spawn_test_node(MergeWithInitialPortalClient);
    let (q, p) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.merge(c, p));
    assert_eq!(IpczResult::Ok, t.put(q, MESSAGE1, &[]));

    let (result, message) = t.wait_to_get_message(q);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(MESSAGE2, message);
    t.wait_for_direct_remote_link(q);
    t.close(q);
});

multinode_test!(merge_with_closed_local_peer, |t| {
    let (q, p) = t.open_portals();
    let (d, b) = t.open_portals();

    t.close(d);
    assert_eq!(IpczResult::Ok, t.merge(b, p));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(q, TrapConditionFlags::PEER_CLOSED));
    t.close(q);
});

multinode_test_node!(MergeWithClosedRemotePeerClient, |t| {
    let b = t.connect_to_broker();
    let (q, p) = t.open_portals();
    t.close(q);
    let r = t.wait_to_get_handle(b);
    t.merge(p, r);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test!(merge_with_closed_remote_peer, |t| {
    let c = t.spawn_test_node(MergeWithClosedRemotePeerClient);
    let (r, s) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.put(c, "", &[r]));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(s, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&[c, s]);
});

const MERGE_COMPLEX_ROUTES_NUM_ITERATIONS: usize = 1000;

multinode_test_node!(MergeComplexRoutesClient, |t| {
    let b = t.connect_to_broker();
    let other_client = t.wait_to_get_handle(b);
    let mut portal = t.wait_to_get_handle(b);

    for _ in 0..MERGE_COMPLEX_ROUTES_NUM_ITERATIONS {
        assert_eq!(IpczResult::Ok, t.put(other_client, "", &[portal]));
        portal = t.wait_to_get_handle(other_client);
        assert_eq!(IpczResult::Ok, t.put(b, "", &[portal]));
        portal = t.wait_to_get_handle(b);
    }

    t.wait_for_direct_remote_link(portal);
    t.ping_pong(portal);
    assert_eq!(IpczResult::Ok, t.put(b, "done", &[]));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&[b, portal, other_client]);
});

multinode_test!(merge_complex_routes, |t| {
    let c1 = t.spawn_test_node(MergeComplexRoutesClient);
    let c2 = t.spawn_test_node(MergeComplexRoutesClient);

    let (q, p) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.put(c1, "", &[q]));
    assert_eq!(IpczResult::Ok, t.put(c2, "", &[p]));

    let (s, mut tt) = t.open_portals();
    let (u, mut v) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.put(c1, "", &[tt]));
    assert_eq!(IpczResult::Ok, t.put(c2, "", &[v]));

    for _ in 0..MERGE_COMPLEX_ROUTES_NUM_ITERATIONS {
        tt = t.wait_to_get_handle(c1);
        assert_eq!(IpczResult::Ok, t.put(c1, "", &[tt]));
        v = t.wait_to_get_handle(c2);
        assert_eq!(IpczResult::Ok, t.put(c2, "", &[v]));
    }

    t.merge(s, u);

    assert_eq!(IpczResult::Ok, t.wait_to_get_message(c1).0);
    assert_eq!(IpczResult::Ok, t.wait_to_get_message(c2).0);
    t.close_all(&[c1, c2]);
});

multinode_test_node!(RaceWithDisconnectClient, |t| {
    let b = t.connect_to_broker();
    t.put(b, "ping", &[]);

    // Small delay on this thread which gives some test drivers enough time to
    // wake up the broker node with the above ping and have them initiate a
    // portal merge. The race condition is extremely sensitive to timing, so
    // this still does not guarantee that the bug would be triggered if present.
    std::thread::sleep(Duration::from_micros(300));

    // Forcibly disconnect, simulating sudden client termination from the
    // broker's perspective.
    t.close_this_node();

    t.close(b);
});

multinode_test!(race_with_disconnect, |t| {
    // Regression test for https://crbug.com/1495461. If the bug is present this
    // test becomes flaky.
    let c = t.spawn_test_node(RaceWithDisconnectClient);

    // Wait for a message on `c`, indicating that handshake is done and there is
    // a direct connection between `c` and the client.
    let conditions = TrapConditions {
        flags: TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS | TrapConditionFlags::DEAD,
        min_local_parcels: 0,
        min_local_bytes: 0,
    };
    assert_eq!(IpczResult::Ok, t.wait_for_conditions(c, conditions));

    // Initiate a merge with `c` and wait for `p` to observe peer closure, which
    // will be triggered by the client node's forced disconnection. Disconnection
    // propagation can race with the merge.
    let (q, p) = t.open_portals();
    t.merge(c, q);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(p, TrapConditionFlags::PEER_CLOSED));
});
