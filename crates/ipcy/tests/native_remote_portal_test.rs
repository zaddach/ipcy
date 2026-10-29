//! Port of `remote_portal_test.cc`.

#[macro_use]
mod support;

use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;

const TEST_MESSAGE1: &str = "hello world";
const TEST_MESSAGE2: &str = "hola mundo";

multinode_test_node!(BasicConnectionClient, |t| {
    let b = t.connect_to_broker();
    assert_eq!(IpczResult::Ok, t.put(b, TEST_MESSAGE1, &[]));

    let (result, message) = t.wait_to_get_message(b);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(TEST_MESSAGE2, message);
    t.close(b);
});

multinode_test!(basic_connection, |t| {
    let c = t.spawn_test_node(BasicConnectionClient);

    let (result, message) = t.wait_to_get_message(c);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(TEST_MESSAGE1, message);
    assert_eq!(IpczResult::Ok, t.put(c, TEST_MESSAGE2, &[]));
    t.close(c);
});

multinode_test_node!(PortalTransferClient, |t| {
    let b = t.connect_to_broker();

    let mut p = [0; 1];
    let (result, message) = t.wait_to_get_full(b, true, &mut p);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(TEST_MESSAGE1, message);
    assert_ne!(0, p[0]);

    t.verify_end_to_end(p[0]);
    t.wait_for_direct_remote_link(p[0]);
    t.close_all(&[p[0], b]);
});

multinode_test!(portal_transfer, |t| {
    let c = t.spawn_test_node(PortalTransferClient);

    let (q, p) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.put(c, TEST_MESSAGE1, &[p]));

    t.verify_end_to_end(q);
    t.wait_for_direct_remote_link(q);
    t.close_all(&[q, c]);
});

const MULTIPLE_HOPS_NUM_ITERATIONS: usize = 100;

multinode_test_node!(MultipleHopsClient1, |t| {
    let b = t.connect_to_broker();

    let (q, p) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.put(b, "", &[p]));

    for _ in 0..MULTIPLE_HOPS_NUM_ITERATIONS {
        assert_eq!(IpczResult::Ok, t.put(q, TEST_MESSAGE2, &[]));
    }

    for _ in 0..MULTIPLE_HOPS_NUM_ITERATIONS {
        let (result, message) = t.wait_to_get_message(q);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(TEST_MESSAGE1, message);
    }

    t.wait_for_direct_remote_link(q);
    t.ping_pong(b);
    t.close_all(&[q, b]);
});

multinode_test_node!(MultipleHopsClient2, |t| {
    let b = t.connect_to_broker();

    let p = t.wait_to_get_handle(b);

    for _ in 0..MULTIPLE_HOPS_NUM_ITERATIONS {
        assert_eq!(IpczResult::Ok, t.put(p, TEST_MESSAGE1, &[]));
    }

    for _ in 0..MULTIPLE_HOPS_NUM_ITERATIONS {
        let (result, message) = t.wait_to_get_message(p);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(TEST_MESSAGE2, message);
    }

    t.wait_for_direct_remote_link(p);
    t.ping_pong(b);
    t.close_all(&[p, b]);
});

multinode_test!(multiple_hops, |t| {
    let c1 = t.spawn_test_node(MultipleHopsClient1);
    let c2 = t.spawn_test_node(MultipleHopsClient2);

    let p = t.wait_to_get_handle(c1);
    assert_eq!(IpczResult::Ok, t.put(c2, "", &[p]));

    t.wait_for_ping_and_reply(c1);
    t.wait_for_ping_and_reply(c2);
    t.close_all(&[c1, c2]);
});

const TRANSFER_BACK_AND_FORTH_NUM_ITERATIONS: usize = 100;

multinode_test_node!(TransferBackAndForthClient, |t| {
    let b = t.connect_to_broker();

    for _ in 0..TRANSFER_BACK_AND_FORTH_NUM_ITERATIONS {
        let p = t.wait_to_get_handle(b);
        assert_eq!(IpczResult::Ok, t.put(b, "", &[p]));
    }

    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test!(transfer_back_and_forth, |t| {
    let c = t.spawn_test_node(TransferBackAndForthClient);

    const MESSAGE: &str = "hihihihi";
    let (q, mut p) = t.open_portals();
    for _ in 0..TRANSFER_BACK_AND_FORTH_NUM_ITERATIONS {
        assert_eq!(IpczResult::Ok, t.put(q, MESSAGE, &[]));
        assert_eq!(IpczResult::Ok, t.put(c, "", &[p]));
        p = t.wait_to_get_handle(c);
        let (result, message) = t.wait_to_get_message(p);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(MESSAGE, message);
    }

    t.verify_end_to_end_local(q, p);
    t.wait_for_direct_local_link(q, p);
    t.close_all(&[q, p, c]);
});

const HUGE_NUMBER_OF_PORTALS_COUNT: usize = 1000;

multinode_test_node!(HugeNumberOfPortalsClient, |t| {
    let b = t.connect_to_broker();
    let other_client = t.wait_to_get_handle(b);

    let mut portals = vec![0; HUGE_NUMBER_OF_PORTALS_COUNT];
    for portal in &mut portals {
        *portal = t.wait_to_get_handle(b);
        assert_eq!(IpczResult::Ok, t.put(other_client, "", &[*portal]));
    }

    for (i, portal) in portals.iter_mut().enumerate() {
        *portal = t.wait_to_get_handle(other_client);

        let boxed = t.box_blob(&i.to_string());
        assert_eq!(IpczResult::Ok, t.put(*portal, "", &[boxed]));

        let boxed = t.wait_to_get_handle(*portal);
        assert_eq!(i.to_string(), t.unbox_blob(boxed));
    }

    assert_eq!(IpczResult::Ok, t.put(b, "", &[other_client]));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&portals);
    t.close(b);
});

multinode_test!(huge_number_of_portals, |t| {
    // Opens a very large number of portals, and sends them all to client nodes.
    // The client nodes exchange these portals with each other and transmit
    // parcels over them, with and without driver objects. This exercises
    // NodeLinkMemory allocations and related synchronization around e.g.
    // delegated allocation and driver object relaying.
    let c1 = t.spawn_test_node(HugeNumberOfPortalsClient);
    let c2 = t.spawn_test_node(HugeNumberOfPortalsClient);
    let (q, p) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.put(c1, "", &[q]));
    assert_eq!(IpczResult::Ok, t.put(c2, "", &[p]));
    for _ in 0..HUGE_NUMBER_OF_PORTALS_COUNT {
        let (a, b) = t.open_portals();
        assert_eq!(IpczResult::Ok, t.put(c1, "", &[a]));
        assert_eq!(IpczResult::Ok, t.put(c2, "", &[b]));
    }

    let q = t.wait_to_get_handle(c1);
    let p = t.wait_to_get_handle(c2);
    t.wait_for_direct_local_link(q, p);
    t.close_all(&[c1, c2, q, p]);
});

const ROUTE_EXPANSION_STRESS_TEST_NUM_ITERATIONS: usize = 100;

multinode_test_node!(RoutingStressTestClient, |t| {
    // Each client only expects portals from the broker and bounces them back.
    let b = t.connect_to_broker();
    for _ in 0..ROUTE_EXPANSION_STRESS_TEST_NUM_ITERATIONS {
        let p = t.wait_to_get_handle(b);
        assert_eq!(IpczResult::Ok, t.put(b, "", &[p]));
    }
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test!(routing_stress_test, |t| {
    // This test spawns a bunch of nodes and bounces two portals back and forth
    // among them over a large number of iterations, then waits for all the
    // intermediate routers to be removed. Every iteration also sends a message
    // from each end of the pipe to exercise parcel routing, including with
    // driver objects attached.
    const NUM_CLIENT_PAIRS: usize = 3;
    let client_pairs: Vec<(IpczHandle, IpczHandle)> = (0..NUM_CLIENT_PAIRS)
        .map(|_| {
            (
                t.spawn_test_node(RoutingStressTestClient),
                t.spawn_test_node(RoutingStressTestClient),
            )
        })
        .collect();

    let (mut a, mut b) = t.open_portals();
    for j in 0..ROUTE_EXPANSION_STRESS_TEST_NUM_ITERATIONS {
        let boxed = t.box_blob(&j.to_string());
        assert_eq!(IpczResult::Ok, t.put(a, &format!("a{j}"), &[boxed]));
        assert_eq!(IpczResult::Ok, t.put(b, &format!("b{j}"), &[]));
        for &(first, second) in &client_pairs {
            assert_eq!(IpczResult::Ok, t.put(first, "", &[a]));
            assert_eq!(IpczResult::Ok, t.put(second, "", &[b]));
            a = t.wait_to_get_handle(first);
            b = t.wait_to_get_handle(second);
        }
    }

    t.wait_for_direct_local_link(a, b);

    for i in 0..ROUTE_EXPANSION_STRESS_TEST_NUM_ITERATIONS {
        let (result, message) = t.wait_to_get_message(a);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(format!("b{i}"), message);
        let mut boxed = [0; 1];
        let (result, message) = t.wait_to_get_full(b, true, &mut boxed);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(format!("a{i}"), message);
        assert_eq!(i.to_string(), t.unbox_blob(boxed[0]));
    }

    for &(first, second) in &client_pairs {
        t.close_all(&[first, second]);
    }
    t.close_all(&[a, b]);
});

const NUM_PORTALS_FOR_CLOSURE_PROPAGATION: usize = 3000;

multinode_test_node!(ClosurePropagationClient1, |t| {
    let b = t.connect_to_broker();
    t.wait_for_direct_remote_link(b);

    let wait_for_c2 = t.wait_to_get_handle(b);
    t.wait_for_direct_remote_link(wait_for_c2);
    t.put(b, "ok", &[]);

    let qs: Vec<IpczHandle> = (0..NUM_PORTALS_FOR_CLOSURE_PROPAGATION)
        .map(|_| t.wait_to_get_handle(b))
        .collect();

    for &q in &qs {
        t.wait_for_condition_flags(q, TrapConditionFlags::PEER_CLOSED);
    }

    t.put(b, "done", &[]);
    assert_eq!("bye", t.wait_to_get_string(b));
    t.close(b);
});

multinode_test_node!(ClosurePropagationClient2, |t| {
    let b = t.connect_to_broker();
    t.wait_for_direct_remote_link(b);

    let wait_for_c1 = t.wait_to_get_handle(b);
    t.wait_for_direct_remote_link(wait_for_c1);
    t.put(b, "ok", &[]);

    let ps: Vec<IpczHandle> = (0..NUM_PORTALS_FOR_CLOSURE_PROPAGATION)
        .map(|_| t.wait_to_get_handle(b))
        .collect();

    t.close_all(&ps);

    assert_eq!("bye", t.wait_to_get_string(b));
    t.close(b);
});

multinode_test!(closure_propagation, |t| {
    // Regression test for https://crbug.com/1426471. The goal of this test is
    // to trigger a large number of parallel proxy bypasses to put pressure on
    // RouterLinkState allocation. Then we verify that we can still propagate
    // closure from end-to-end on each route, implying that the route didn't get
    // stuck halfway through a bypass.
    let c1 = t.spawn_test_node(ClosurePropagationClient1);
    let c2 = t.spawn_test_node(ClosurePropagationClient2);
    t.wait_for_direct_remote_link(c1);
    t.wait_for_direct_remote_link(c2);

    // Wait for the clients to establish a direct link so we start the real test
    // work in a consistent state.
    let (wait_for_c1, wait_for_c2) = t.open_portals();
    t.put(c1, "", &[wait_for_c2]);
    t.put(c2, "", &[wait_for_c1]);
    assert_eq!("ok", t.wait_to_get_string(c1));
    assert_eq!("ok", t.wait_to_get_string(c2));

    for _ in 0..NUM_PORTALS_FOR_CLOSURE_PROPAGATION {
        let (q, p) = t.open_portals();
        t.put(c1, "", &[q]);
        t.put(c2, "", &[p]);
    }

    assert_eq!("done", t.wait_to_get_string(c1));
    t.put(c1, "bye", &[]);
    t.put(c2, "bye", &[]);
    t.close_all(&[c1, c2]);
});

multinode_test_node!(DisconnectThroughProxyClient1, |t| {
    let b = t.connect_to_broker();

    let q = t.wait_to_get_handle(b);

    // Should eventually be observed by virtue of the forced disconnection of
    // client 3 in the main test body.
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(q, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&[q, b]);
});

multinode_test_node!(DisconnectThroughProxyClient2, |t| {
    let b = t.connect_to_broker();

    // Receive a portal p from the broker and bounce it back.
    let p = t.wait_to_get_handle(b);
    assert_eq!(IpczResult::Ok, t.put(b, "", &[p]));

    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test_node!(DisconnectThroughProxyClient3, |t| {
    let b = t.connect_to_broker();

    // Receive a portal p from the broker and then immediately terminate this
    // node.
    let p = t.wait_to_get_handle(b);

    // Forcibly shut this node down, severing all connections to other nodes. We
    // do this *before* closing `p` to ensure that we are't just exercising
    // normal portal closure, which other tests already cover. From the
    // perspective of other nodes, we are effectively simulating a crash of this
    // node.
    t.close_this_node();

    // It's still necessary to explicitly close local portals after the node
    // itself has been shut down. Otherwise they'd leak.
    t.close_all(&[p, b]);
});

multinode_test!(disconnect_through_proxy, |t| {
    // Exercises node disconnection. Namely if portals on nodes 1 and 3 are
    // connected via proxy on node 2, and node 3 disappears, node 1's portal
    // should observe peer closure.
    let (c1s, c1_control) = t.spawn_test_node_n(DisconnectThroughProxyClient1, 1, ConnectNodeFlags::empty());
    let c2 = t.spawn_test_node(DisconnectThroughProxyClient2);
    let (c3s, c3_control) = t.spawn_test_node_n(DisconnectThroughProxyClient3, 1, ConnectNodeFlags::empty());
    let (c1, c3) = (c1s[0], c3s[0]);

    let (q, p) = t.open_portals();

    // We send q to client 1, and p to client 2.
    assert_eq!(IpczResult::Ok, t.put(c1, "", &[q]));
    assert_eq!(IpczResult::Ok, t.put(c2, "", &[p]));

    // Client 2 forwards p back to us, and we forward it now to client 3. This
    // process ensures that client 2 will for some time serve as a proxy between
    // client 1 and client 3.
    let p = t.wait_to_get_handle(c2);
    assert_eq!(IpczResult::Ok, t.put(c3, "", &[p]));

    // Client 3 will terminate on its own. Though not determinstic, this will
    // race with proxy reduction such that client 2 may still be proxying for the
    // q-p portal pair when client 3 (who owns p) goes away.
    assert!(t.controller(c3_control).wait_for_shutdown());

    // Client 1 waits on q observing peer closure before terminating. This will
    // block until that happens.
    assert!(t.controller(c1_control).wait_for_shutdown());
    t.close_all(&[c1, c2, c3]);
});
