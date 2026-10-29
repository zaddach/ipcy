//! Port of `connect_test.cc`.

#[macro_use]
mod support;

use std::sync::Arc;
use std::sync::mpsc;

use ipcy::core::api;
use ipcy::core::driver::{DriverHandle, RawMessage, TransportActivityHandler};
use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;
use support::{TestNode, TestTransportListener};

fn activate_and_close(t: &TestNode, transport: DriverHandle) {
    // Registering any listener callback activates the transport, and listener
    // destruction closes it.
    let listener = TestTransportListener::new(t.driver(), transport);
    listener.on_error(|| {});
}

const BAD_MESSAGE: &[u8] = b"this will never be a valid handshake message!\0";

multinode_test_node!(BrokerToNonBrokerClient, |t| {
    let b = t.connect_to_broker();
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test!(broker_to_non_broker, |t| {
    let c = t.spawn_test_node(BrokerToNonBrokerClient);
    t.close(c);
});

const NUM_BROKER_PORTALS: usize = 2;
const NUM_NON_BROKER_PORTALS: usize = 5;

multinode_test_node!(SurplusPortalsClient, |t| {
    let portals = t.connect_to_broker_n(NUM_NON_BROKER_PORTALS);

    // All of the surplus portals should observe peer closure.
    for &portal in &portals[NUM_BROKER_PORTALS..] {
        assert_eq!(
            IpczResult::Ok,
            t.wait_for_condition_flags(portal, TrapConditionFlags::PEER_CLOSED)
        );
    }
    t.close_all(&portals);
});

multinode_test!(surplus_portals, |t| {
    let (portals, _) = t.spawn_test_node_n(SurplusPortalsClient, NUM_BROKER_PORTALS, ConnectNodeFlags::empty());
    t.close_all(&portals);
});

multinode_test_node!(ExpectDisconnectFromBroker, |t| {
    let b = t.connect_to_broker();
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test!(disconnect_without_broker_handshake, |t| {
    let (our_transport, controller) = t.spawn_test_node_no_connect(ExpectDisconnectFromBroker);
    activate_and_close(t, our_transport);
    t.controller(controller).wait_for_shutdown();
});

multinode_test_node!(DisconnectWithoutNonBrokerHandshakeClient, |_t| {
    // Our transport is automatically closed on exit. No handshake is sent
    // because we never call connect_to_broker(). No action required.
});

multinode_test!(disconnect_without_non_broker_handshake, |t| {
    let c = t.spawn_test_node(DisconnectWithoutNonBrokerHandshakeClient);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c, TrapConditionFlags::PEER_CLOSED));
    t.close(c);
});

multinode_test!(disconnect_on_bad_broker_message, |t| {
    let (our_transport, controller) = t.spawn_test_node_no_connect(ExpectDisconnectFromBroker);

    // Send some garbage to the other node.
    assert_eq!(IpczResult::Ok, t.driver().transmit(our_transport, BAD_MESSAGE, &[]));
    activate_and_close(t, our_transport);

    // The other node will only shut down once it's observed peer closure on its
    // portal to us; which it should, because we just sent it some garbage.
    t.controller(controller).wait_for_shutdown();
});

multinode_test_node!(TransmitSomeGarbage, |t| {
    // Instead of doing the usual connection dance, send some garbage back to
    // the broker. It should disconnect ASAP.
    assert_eq!(IpczResult::Ok, t.driver().transmit(t.transport(), BAD_MESSAGE, &[]));

    let transport = t.release_transport();
    let listener = TestTransportListener::new(t.driver(), transport);
    let (tx, rx) = mpsc::sync_channel(1);
    listener.on_error(move || {
        let _ = tx.try_send(());
    });
    rx.recv().unwrap();
    listener.stop_listening();
});

multinode_test!(disconnect_on_bad_non_broker_message, |t| {
    let (portals, controller) = t.spawn_test_node_n(TransmitSomeGarbage, 1, ConnectNodeFlags::empty());
    let c = portals[0];

    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c, TrapConditionFlags::PEER_CLOSED));
    t.close(c);

    // Make sure the client also observes disconnection of its transport. It
    // won't terminate until that happens.
    t.controller(controller).wait_for_shutdown();
});

const BLOB1_CONTENTS: &str = "from q";
const BLOB2_CONTENTS: &str = "from p";

multinode_test_node!(NonBrokerToNonBrokerClientChild, |t| {
    let parent = t.connect_to_parent(ConnectNodeFlags::INHERIT_BROKER);

    let mut portal = [0u64; 1];
    let (result, expected_contents) = t.wait_to_get_full(parent, true, &mut portal);
    assert_eq!(IpczResult::Ok, result);
    let mut boxed = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(portal[0], false, &mut boxed).0);
    assert_eq!(expected_contents, t.unbox_blob(boxed[0]));

    t.ping_pong(portal[0]);
    t.wait_for_direct_remote_link(portal[0]);
    t.close_all(&[parent, portal[0]]);
});

multinode_test_node!(NonBrokerToNonBrokerClient, |t| {
    let b = t.connect_to_broker();
    let c = t.spawn_test_node_with_flags(NonBrokerToNonBrokerClientChild, ConnectNodeFlags::SHARE_BROKER);

    let mut portal = [0u64; 1];
    let (result, expected_contents) = t.wait_to_get_full(b, true, &mut portal);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(IpczResult::Ok, t.put(c, &expected_contents, &portal));

    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&[c, b]);
});

multinode_test!(non_broker_to_non_broker, |t| {
    let c1 = t.spawn_test_node(NonBrokerToNonBrokerClient);
    let c2 = t.spawn_test_node(NonBrokerToNonBrokerClient);

    let (q, p) = t.open_portals();
    let q_box = t.box_blob(BLOB1_CONTENTS);
    let p_box = t.box_blob(BLOB2_CONTENTS);
    assert_eq!(IpczResult::Ok, t.put(q, "", &[q_box]));
    assert_eq!(IpczResult::Ok, t.put(p, "", &[p_box]));
    assert_eq!(IpczResult::Ok, t.put(c1, BLOB2_CONTENTS, &[q]));
    assert_eq!(IpczResult::Ok, t.put(c2, BLOB1_CONTENTS, &[p]));

    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c1, TrapConditionFlags::PEER_CLOSED));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c2, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&[c1, c2]);
});

struct IgnoreActivity;

impl TransportActivityHandler for IgnoreActivity {
    fn on_message(&self, _: RawMessage<'_>, _: DriverHandle) -> IpczResult {
        IpczResult::Ok
    }
    fn on_error(&self) {}
    fn on_deactivated(&self) {}
}

multinode_test_node!(BadNonBrokerReferralClient, |t| {
    let b = t.connect_to_broker();

    let transports = t.create_transports();

    // Transmit something invalid from the referred node's side of the
    // transport.
    const BAD: &[u8] = b"i am a terrible node plz reject\0";
    assert_eq!(IpczResult::Ok, t.driver().transmit(transports.theirs, BAD, &[]));
    assert_eq!(
        IpczResult::Ok,
        t.driver().activate_transport(transports.theirs, Arc::new(IgnoreActivity))
    );

    // Now refer our imaginary other node using our end of the transport. The
    // broker should reject the referral and we should eventually observe
    // disconnection of our initial portal to the referred node.
    let p = api::connect_node(t.node(), transports.ours, 1, ConnectNodeFlags::SHARE_BROKER).unwrap()[0];
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(p, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&[b, p]);

    assert_eq!(IpczResult::Ok, t.driver().deactivate_transport(transports.theirs));
    assert_eq!(IpczResult::Ok, t.driver().close(transports.theirs));
});

multinode_test!(bad_non_broker_referral, |t| {
    let c = t.spawn_test_node(BadNonBrokerReferralClient);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c, TrapConditionFlags::PEER_CLOSED));
    t.close(c);
});

multinode_test_node!(FailedNonBrokerReferralReferredClient, |t| {
    let transport = t.release_transport();
    let p = api::connect_node(t.node(), transport, 1, ConnectNodeFlags::INHERIT_BROKER).unwrap()[0];
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(p, TrapConditionFlags::PEER_CLOSED));
    t.close(p);
});

multinode_test_node!(FailedNonBrokerReferralClient, |t| {
    let b = t.connect_to_broker();

    let (our_transport, controller) = t.spawn_test_node_no_connect(FailedNonBrokerReferralReferredClient);

    // Activate and immediately disconnect the transport instead of passing to
    // our broker with connect_node(). The referred client should observe
    // disconnection of its initial portals and terminate itself.
    activate_and_close(t, our_transport);
    t.controller(controller).wait_for_shutdown();
    t.close(b);
});

multinode_test!(failed_non_broker_referral, |t| {
    let c = t.spawn_test_node(FailedNonBrokerReferralClient);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c, TrapConditionFlags::PEER_CLOSED));
    t.close(c);
});

multinode_test_broker_node!(AnotherBroker, |t| {
    let b = t.connect_to_broker();
    t.ping_pong(b);
    t.close(b);
});

multinode_test!(broker_to_broker, |t| {
    let b = t.spawn_test_node(AnotherBroker);

    t.ping_pong(b);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::PEER_CLOSED));
    t.close(b);
});

multinode_test_node!(BrokerClient, |t| {
    let b = t.connect_to_broker();
    let mut other_client = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(b, false, &mut other_client).0);

    // Ensure that we end up with a direct connection to the other client, which
    // implies the two non-broker nodes have been properly introduced across the
    // boundary of their respective node networks.
    t.ping_pong(other_client[0]);
    t.wait_for_direct_remote_link(other_client[0]);

    // Synchronize against the main test node. See synchronization comment there.
    t.ping_pong(b);
    t.close_all(&[b, other_client[0]]);
});

multinode_test_broker_node!(BrokerWithClientNode, |t| {
    let b = t.connect_to_broker();
    let client = t.spawn_test_node(BrokerClient);

    let mut other_client = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(b, false, &mut other_client).0);
    t.put(client, "", &other_client);

    // Synchronize against the launched client to ensure that it's done before
    // we join it and terminate.
    t.ping_pong(client);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(client, TrapConditionFlags::PEER_CLOSED));

    // Synchronize against the main test node. See synchronization comment there.
    t.ping_pong(b);
    t.close_all(&[b, client]);
});

multinode_test!(multi_broker_introductions, |t| {
    // This test covers introductions in a multi-broker network. There are four
    // test nodes involved here: the main node (this one, call it A), a
    // secondary broker B launched with the BrokerWithClientNode body defined
    // above; and two client nodes (running BrokerClient above) we will call C
    // and D, with C launched by A and D launched by B.
    //
    // A portal pair is created on A and its portals are passed to node B (our
    // secondary broker) and node C (the singular non-broker client node in A's
    // local network) respectively.
    //
    // Node B in turn passes its end to its own launched non-broker client D.
    // This ultimately elicits a need for node C to be introduced to node D. The
    // test succeeds only once the portal on node C appears to be directly
    // connected to the portal on node D -- and vice versa -- implying
    // successful introduction.
    let other_broker = t.spawn_test_node(BrokerWithClientNode);
    let client = t.spawn_test_node(BrokerClient);

    let (q, p) = t.open_portals();
    t.put(other_broker, "", &[q]);
    t.put(client, "", &[p]);

    // Synchronize against both the launched broker and the launched client node
    // to ensure that they're done before we join them and terminate.
    t.ping_pong(other_broker);
    t.ping_pong(client);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(other_broker, TrapConditionFlags::PEER_CLOSED));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(client, TrapConditionFlags::PEER_CLOSED));

    t.close_all(&[other_broker, client]);
});

fn send_transport(t: &TestNode, portal: IpczHandle, transport: DriverHandle) {
    let boxed = api::box_driver_object(t.node(), transport).unwrap();
    assert_eq!(IpczResult::Ok, api::put(portal, &[], &[boxed]));
}

fn receive_transport(t: &TestNode, portal: IpczHandle) -> DriverHandle {
    let mut boxed = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(portal, false, &mut boxed).0);
    let Ok(api::UnboxedContents::Unboxed(ipcy::core::box_object::BoxContents::DriverObject(mut object))) =
        api::unbox(boxed[0], UnboxFlags::empty())
    else {
        panic!("expected a boxed driver object");
    };
    object.release()
}

fn reconnect(t: &TestNode, transport: DriverHandle, flags: ConnectNodeFlags) -> IpczHandle {
    let flags = flags | t.test_driver.extra_client_connect_node_flags();
    api::connect_node(t.node(), transport, 1, flags).unwrap()[0]
}

multinode_test_node!(ReconnectionTestNonBroker, |t| {
    let broker = t.connect_to_broker();
    t.verify_end_to_end(broker);
    let new_transport = receive_transport(t, broker);
    let new_portal = reconnect(t, new_transport, ConnectNodeFlags::TO_BROKER);
    t.verify_end_to_end(new_portal);
    t.wait_for_condition_flags(broker, TrapConditionFlags::PEER_CLOSED);
    t.close_all(&[broker, new_portal]);
});

multinode_test!(reconnect_broker_non_broker, |t| {
    let (portals, controller) = t.spawn_test_node_n(ReconnectionTestNonBroker, 1, ConnectNodeFlags::empty());
    let non_broker = portals[0];
    let transports = t.controller(controller).create_new_transports();
    t.verify_end_to_end(non_broker);
    send_transport(t, non_broker, transports.theirs);
    let new_portal = reconnect(t, transports.ours, ConnectNodeFlags::empty());
    t.verify_end_to_end(new_portal);
    t.wait_for_condition_flags(non_broker, TrapConditionFlags::PEER_CLOSED);
    t.close_all(&[non_broker, new_portal]);
});

multinode_test_broker_node!(ReconnectionTestBroker, |t| {
    let other_broker = t.connect_to_broker();
    t.verify_end_to_end(other_broker);
    let new_transport = receive_transport(t, other_broker);
    let new_portal = reconnect(t, new_transport, ConnectNodeFlags::TO_BROKER);
    t.verify_end_to_end(new_portal);
    t.wait_for_condition_flags(other_broker, TrapConditionFlags::PEER_CLOSED);
    t.close_all(&[other_broker, new_portal]);
});

multinode_test!(reconnect_broker_broker, |t| {
    let (portals, controller) = t.spawn_test_node_n(ReconnectionTestBroker, 1, ConnectNodeFlags::empty());
    let other_broker = portals[0];
    let transports = t.controller(controller).create_new_transports();
    t.verify_end_to_end(other_broker);
    send_transport(t, other_broker, transports.theirs);
    let new_portal = reconnect(t, transports.ours, ConnectNodeFlags::TO_BROKER);
    t.verify_end_to_end(new_portal);
    t.wait_for_condition_flags(other_broker, TrapConditionFlags::PEER_CLOSED);
    t.close_all(&[other_broker, new_portal]);
});

multinode_test_node!(TransitiveReconnectClientA, |t| {
    let broker = t.connect_to_broker();

    let mut client_b = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(broker, false, &mut client_b).0);
    let client_b = client_b[0];
    t.wait_for_direct_remote_link(client_b);
    t.verify_end_to_end(client_b);

    let new_transport = receive_transport(t, broker);
    let new_broker = reconnect(t, new_transport, ConnectNodeFlags::TO_BROKER);
    t.verify_end_to_end(new_broker);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(broker, TrapConditionFlags::PEER_CLOSED));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(client_b, TrapConditionFlags::PEER_CLOSED));

    // Pass a portal to the broker which it will forward to client B. Then
    // verify that our portal ends up with a working direct link to it, implying
    // that the two client nodes have been automatically re-introduced.
    let (new_client_a, new_client_b) = t.open_portals();
    t.put(new_broker, "", &[new_client_a]);
    t.wait_for_direct_remote_link(new_client_b);
    t.verify_end_to_end(new_client_b);

    t.put(new_broker, "bye", &[]);
    t.close_all(&[broker, new_broker, client_b, new_client_b]);
});

multinode_test_node!(TransitiveReconnectClientB, |t| {
    let broker = t.connect_to_broker();

    let mut client_a = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(broker, false, &mut client_a).0);
    let client_a = client_a[0];
    t.wait_for_direct_remote_link(client_a);
    t.verify_end_to_end(client_a);

    let mut new_client_a = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(broker, false, &mut new_client_a).0);
    let new_client_a = new_client_a[0];
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(client_a, TrapConditionFlags::PEER_CLOSED));
    t.wait_for_direct_remote_link(new_client_a);
    t.verify_end_to_end(new_client_a);

    t.put(broker, "bye", &[]);
    t.close_all(&[broker, client_a, new_client_a]);
});

multinode_test!(reconnect_transitive_reconnection, |t| {
    // Tests that when a non-broker reconnects to a broker, it can also get
    // reconnected to other nodes via that broker.
    let (portals, a_controller) = t.spawn_test_node_n(TransitiveReconnectClientA, 1, ConnectNodeFlags::empty());
    let client_a = portals[0];
    let client_b = t.spawn_test_node(TransitiveReconnectClientB);

    // Establish a pair of portals between clients A and B.
    let (a, b) = t.open_portals();
    t.put(client_a, "", &[a]);
    t.put(client_b, "", &[b]);

    // Send client A a transport with which to re-connect to us. Verify that it's
    // reconnected and wait for its previous portal to be closed.
    let transports = t.controller(a_controller).create_new_transports();
    send_transport(t, client_a, transports.theirs);
    let new_client_a = reconnect(t, transports.ours, ConnectNodeFlags::empty());
    t.verify_end_to_end(new_client_a);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(client_a, TrapConditionFlags::PEER_CLOSED));

    // Accept a new portal from A and forward it to B.
    let mut new_b_to_a = [0u64; 1];
    assert_eq!(IpczResult::Ok, t.wait_to_get_full(new_client_a, false, &mut new_b_to_a).0);
    t.put(client_b, "", &new_b_to_a);

    // Wait for A and B to confirm their reconnection.
    assert_eq!("bye", t.wait_to_get_string(new_client_a));
    assert_eq!("bye", t.wait_to_get_string(client_b));
    t.close_all(&[client_a, client_b, new_client_a]);
});
