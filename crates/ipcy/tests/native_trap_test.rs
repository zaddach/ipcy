//! Port of `trap_test.cc`.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use ipcy::core::api::{self, TrapError};
use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;
use ipcy::core::reference_drivers::sync::SyncDriver;
use support::TestBase;

/// A single-node fixture over the sync driver.
struct TrapTest {
    base: TestBase,
    node: IpczHandle,
}

impl TrapTest {
    fn new() -> Self {
        let node = api::create_node(SyncDriver::new(), CreateNodeFlags::empty(), None).unwrap();
        Self {
            base: TestBase,
            node,
        }
    }

    fn open_portals(&self) -> (IpczHandle, IpczHandle) {
        api::open_portals(self.node).unwrap()
    }
}

impl Drop for TrapTest {
    fn drop(&mut self) {
        api::close(self.node);
    }
}

fn flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

/// Installs a trap that, when fired, checks the event's condition flags and
/// raises `fired`.
fn trap_expecting(
    t: &TrapTest,
    portal: IpczHandle,
    conditions: TrapConditions,
    expected: TrapConditionFlags,
    fired: &Arc<AtomicBool>,
) -> Result<(), TrapError> {
    let fired = fired.clone();
    t.base.trap(portal, conditions, move |e| {
        assert_eq!(expected | TrapConditionFlags::WITHIN_API_CALL, e.condition_flags);
        fired.store(true, Ordering::SeqCst);
    })
}

fn is_set(flag: &AtomicBool) -> bool {
    flag.load(Ordering::SeqCst)
}

fn cond(flags: TrapConditionFlags) -> TrapConditions {
    TrapConditions {
        flags,
        ..Default::default()
    }
}

fn expect_already_satisfied(
    t: &TrapTest,
    portal: IpczHandle,
    conditions: TrapConditions,
    expected_flags: TrapConditionFlags,
) -> PortalStatus {
    match t.base.trap(portal, conditions, |_| {}) {
        Err(TrapError::AlreadySatisfied(satisfied)) => {
            assert_eq!(expected_flags, satisfied.satisfied_condition_flags);
            satisfied.status
        }
        other => panic!("expected AlreadySatisfied, got {other:?}"),
    }
}

#[test]
fn remove_on_close() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();

    // Install a few traps to watch for specific conditions. Both should instead
    // observe REMOVED when the observed portal is closed.
    let parcel_trap_removed = flag();
    let closure_trap_removed = flag();

    assert!(
        trap_expecting(
            &t,
            b,
            cond(TrapConditionFlags::NEW_LOCAL_PARCEL),
            TrapConditionFlags::REMOVED,
            &parcel_trap_removed
        )
        .is_ok()
    );
    assert!(
        trap_expecting(
            &t,
            b,
            cond(TrapConditionFlags::PEER_CLOSED),
            TrapConditionFlags::REMOVED,
            &closure_trap_removed
        )
        .is_ok()
    );

    assert!(!is_set(&parcel_trap_removed));
    assert!(!is_set(&closure_trap_removed));
    t.base.close(b);
    assert!(is_set(&parcel_trap_removed));
    assert!(is_set(&closure_trap_removed));

    t.base.close(a);
}

#[test]
fn remove_on_merge() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();
    let (c, d) = t.open_portals();

    let removed_traps = Arc::new(AtomicU32::new(0));
    for portal in [b, c] {
        let removed_traps = removed_traps.clone();
        t.base
            .trap(portal, cond(TrapConditionFlags::NEW_LOCAL_PARCEL), move |e| {
                assert_eq!(
                    TrapConditionFlags::REMOVED | TrapConditionFlags::WITHIN_API_CALL,
                    e.condition_flags
                );
                removed_traps.fetch_add(1, Ordering::SeqCst);
                // The merged route must be usable from the removal handler.
                TestBase.verify_end_to_end_local(a, d);
            })
            .unwrap();
    }

    assert_eq!(0, removed_traps.load(Ordering::SeqCst));
    assert_eq!(IpczResult::Ok, t.base.merge(b, c));
    assert_eq!(2, removed_traps.load(Ordering::SeqCst));

    t.base.close_all(&[a, d]);
}

#[test]
fn preserve_on_failed_merge() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();
    let (c, d) = t.open_portals();

    let removed_traps = Arc::new(AtomicU32::new(0));
    for portal in [b, c] {
        let removed_traps = removed_traps.clone();
        t.base
            .trap(portal, cond(TrapConditionFlags::REMOVED), move |e| {
                assert_eq!(
                    TrapConditionFlags::REMOVED | TrapConditionFlags::WITHIN_API_CALL,
                    e.condition_flags
                );
                removed_traps.fetch_add(1, Ordering::SeqCst);
            })
            .unwrap();
    }

    assert_eq!(IpczResult::InvalidArgument, t.base.merge(b, b));
    assert_eq!(IpczResult::InvalidArgument, t.base.merge(b, a));
    assert_eq!(IpczResult::Ok, t.base.put(b, "!", &[]));
    assert_eq!(IpczResult::FailedPrecondition, t.base.merge(b, c));
    assert_eq!(0, removed_traps.load(Ordering::SeqCst));

    t.base.verify_end_to_end_local(c, d);
    t.base.close_all(&[a, b, c, d]);
    assert_eq!(2, removed_traps.load(Ordering::SeqCst));
}

#[test]
fn peer_closed() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();

    let conditions = cond(TrapConditionFlags::PEER_CLOSED);
    let received_event = flag();
    assert!(trap_expecting(&t, b, conditions, TrapConditionFlags::PEER_CLOSED, &received_event).is_ok());

    assert!(!is_set(&received_event));
    t.base.close(a);
    assert!(is_set(&received_event));

    let status = expect_already_satisfied(&t, b, conditions, TrapConditionFlags::PEER_CLOSED);
    assert!(status.flags.contains(PortalStatusFlags::PEER_CLOSED));

    t.base.close(b);
}

#[test]
fn min_local_parcels() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();

    let mut conditions = TrapConditions {
        flags: TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS,
        min_local_parcels: 0,
        min_local_bytes: 0,
    };
    let received_event = flag();
    assert!(
        trap_expecting(
            &t,
            b,
            conditions,
            TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS,
            &received_event
        )
        .is_ok()
    );

    assert!(!is_set(&received_event));
    t.base.put(a, "beep beep", &[]);
    assert!(is_set(&received_event));

    // Can't install a new trap with the same conditions, because the minimum
    // local parcel count is already satisifed...
    let status = expect_already_satisfied(&t, b, conditions, TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS);
    assert_eq!(1, status.num_local_parcels);

    // ...but we can still install a trap to watch for a larger number of
    // parcels.
    let received_event = flag();
    conditions.min_local_parcels = 2;
    assert!(
        trap_expecting(
            &t,
            b,
            conditions,
            TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS,
            &received_event
        )
        .is_ok()
    );

    assert!(!is_set(&received_event));
    t.base.put(a, "hihihi", &[]);
    assert!(!is_set(&received_event));
    t.base.put(a, "okokok", &[]);
    assert!(is_set(&received_event));

    // Purge the messages. Should now be able to install another trap for > 0
    // parcels.
    for _ in 0..3 {
        assert_eq!(IpczResult::Ok, t.base.get_message(b).0);
    }
    conditions.min_local_parcels = 0;
    assert!(t.base.trap(b, conditions, |_| {}).is_ok());

    t.base.close_all(&[a, b]);
}

#[test]
fn min_local_bytes() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();

    let conditions = TrapConditions {
        flags: TrapConditionFlags::ABOVE_MIN_LOCAL_BYTES,
        min_local_parcels: 0,
        min_local_bytes: 3,
    };
    let received_event = flag();
    assert!(
        trap_expecting(
            &t,
            b,
            conditions,
            TrapConditionFlags::ABOVE_MIN_LOCAL_BYTES,
            &received_event
        )
        .is_ok()
    );

    for message in ["w", "x", "y"] {
        assert!(!is_set(&received_event));
        t.base.put(a, message, &[]);
    }
    assert!(!is_set(&received_event));
    t.base.put(a, "z", &[]);
    assert!(is_set(&received_event));

    let status = expect_already_satisfied(&t, b, conditions, TrapConditionFlags::ABOVE_MIN_LOCAL_BYTES);
    assert_eq!(4, status.num_local_bytes);

    t.base.close_all(&[a, b]);
}

#[test]
fn new_local_parcel() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();

    let conditions = cond(TrapConditionFlags::NEW_LOCAL_PARCEL);
    let received_event = flag();
    assert!(trap_expecting(&t, b, conditions, TrapConditionFlags::NEW_LOCAL_PARCEL, &received_event).is_ok());

    assert!(!is_set(&received_event));
    t.base.put(a, "beep beep", &[]);
    assert!(is_set(&received_event));

    let received_event = flag();
    assert!(trap_expecting(&t, b, conditions, TrapConditionFlags::NEW_LOCAL_PARCEL, &received_event).is_ok());

    assert!(!is_set(&received_event));
    t.base.put(a, "beep beep beeeeep", &[]);
    assert!(is_set(&received_event));

    t.base.close_all(&[a, b]);
}

#[test]
fn dead_portal() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();

    let conditions = cond(TrapConditionFlags::DEAD);
    let received_event = flag();
    assert!(trap_expecting(&t, b, conditions, TrapConditionFlags::DEAD, &received_event).is_ok());

    // A "dead" signal can't be raised until the peer is closed AND all incoming
    // parcels have been drained.
    assert!(!is_set(&received_event));
    t.base.put(a, "goodbye", &[]);
    t.base.close(a);
    assert!(!is_set(&received_event));

    assert_eq!(IpczResult::Ok, t.base.get_message(b).0);
    assert!(is_set(&received_event));

    let status = expect_already_satisfied(&t, b, conditions, TrapConditionFlags::DEAD);
    assert!(status.flags.contains(PortalStatusFlags::PEER_CLOSED));
    assert!(status.flags.contains(PortalStatusFlags::DEAD));
    assert_eq!(0, status.num_local_parcels);

    t.base.close(b);
}

#[test]
fn multiple_traps() {
    let t = TrapTest::new();
    let (a, b) = t.open_portals();

    // Install 3 separate traps. We'll trigger an event for only one of them
    // first, and then we'll trigger events for the other two simulataneously.
    let observed_parcel = flag();
    let observed_closure = flag();
    let observed_death = flag();
    assert!(
        trap_expecting(
            &t,
            b,
            cond(TrapConditionFlags::NEW_LOCAL_PARCEL),
            TrapConditionFlags::NEW_LOCAL_PARCEL,
            &observed_parcel
        )
        .is_ok()
    );
    assert!(
        trap_expecting(
            &t,
            b,
            cond(TrapConditionFlags::PEER_CLOSED),
            TrapConditionFlags::PEER_CLOSED,
            &observed_closure
        )
        .is_ok()
    );
    assert!(
        trap_expecting(
            &t,
            b,
            cond(TrapConditionFlags::DEAD),
            TrapConditionFlags::DEAD,
            &observed_death
        )
        .is_ok()
    );

    assert!(!is_set(&observed_parcel));
    assert!(!is_set(&observed_closure));
    assert!(!is_set(&observed_death));

    t.base.put(a, "hey", &[]);
    assert!(is_set(&observed_parcel));
    assert!(!is_set(&observed_closure));
    assert!(!is_set(&observed_death));

    assert_eq!(IpczResult::Ok, t.base.get_message(b).0);

    t.base.close(a);
    assert!(is_set(&observed_parcel));
    assert!(is_set(&observed_closure));
    assert!(is_set(&observed_death));

    t.base.close(b);
}

#[test]
fn sync_transport_within_api_call() {
    let driver = SyncDriver::new();
    let node_a = api::create_node(driver.clone(), CreateNodeFlags::AS_BROKER, None).unwrap();
    let node_b = api::create_node(driver.clone(), CreateNodeFlags::empty(), None).unwrap();

    use ipcy::core::driver::Driver;
    let (t0, t1) = driver.create_transports(Default::default(), Default::default()).unwrap();

    let a = api::connect_node(node_a, t0, 1, ConnectNodeFlags::empty()).unwrap()[0];
    let b = api::connect_node(node_b, t1, 1, ConnectNodeFlags::TO_BROKER).unwrap()[0];

    let received_event = flag();
    let event_flags = Arc::new(AtomicU32::new(0));
    {
        let (received_event, event_flags) = (received_event.clone(), event_flags.clone());
        let base = TestBase;
        assert!(
            base.trap(b, cond(TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS), move |e| {
                received_event.store(true, Ordering::SeqCst);
                event_flags.store(e.condition_flags.bits(), Ordering::SeqCst);
            })
            .is_ok()
        );
    }

    TestBase.put(a, "ping!", &[]);
    assert!(is_set(&received_event));

    // Even though the immediate source of the fired trap event was a transport
    // notification, the event should still be flagged as within an API call
    // since it happens while we're still in put() above.
    assert!(TrapConditionFlags::from_bits_truncate(event_flags.load(Ordering::SeqCst))
        .contains(TrapConditionFlags::WITHIN_API_CALL));

    TestBase.close_all(&[a, b, node_a, node_b]);
}
