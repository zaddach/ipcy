//! Port of `parcel_test.cc`.
//!
//! The C API's argument-validation cases for null pointers paired with
//! non-zero capacities have no equivalent with slices and are omitted.

#[macro_use]
mod support;

use ipcy::core::api;
use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;
use support::TestNode;

fn wait_for_parcels(t: &TestNode, portal: IpczHandle, count: usize) -> IpczResult {
    assert!(count > 0);
    t.wait_for_conditions(
        portal,
        TrapConditions {
            flags: TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS,
            min_local_parcels: count - 1,
            min_local_bytes: 0,
        },
    )
}

fn wait_for_parcel(t: &TestNode, portal: IpczHandle) -> IpczResult {
    wait_for_parcels(t, portal, 1)
}

fn say_goodbye(t: &TestNode, portal: IpczHandle) {
    t.put(portal, "goodbye", &[]);
}

fn wait_for_goodbye(t: &TestNode, portal: IpczHandle) {
    let (result, message) = t.wait_to_get_message(portal);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!("goodbye", message);
}

/// Retrieves the next parcel on `portal` as a parcel object, without consuming
/// any of its contents.
fn get_parcel(portal: IpczHandle) -> IpczHandle {
    let out = api::get(portal, GetFlags::PARTIAL, &mut [], &mut [], true);
    assert_eq!(IpczResult::Ok, out.result);
    out.parcel.expect("a parcel handle")
}

fn string_from_data(data: *const u8, size: usize) -> String {
    // SAFETY: `data` is valid for `size` bytes for the duration of the
    // transaction that produced it.
    String::from_utf8(unsafe { std::slice::from_raw_parts(data, size) }.to_vec()).unwrap()
}

const MESSAGE: &str = "here's that box of hornets you wanted";
const HORNETS: &str = "bzzzzz";

multinode_test_node!(GetClient, |t| {
    let b = t.connect_to_broker();
    assert_eq!(IpczResult::Ok, wait_for_parcel(t, b));

    // Retrieving a parcel object removes it from its portal's queue.
    let parcel = get_parcel(b);
    assert_eq!(
        IpczResult::Unavailable,
        api::get(b, GetFlags::PARTIAL, &mut [], &mut [], true).result
    );

    // Short reads behave as with portals, providing parcel dimensions on
    // output.
    let out = api::get(parcel, GetFlags::empty(), &mut [], &mut [], false);
    assert_eq!(IpczResult::ResourceExhausted, out.result);
    assert_eq!(MESSAGE.len(), out.num_bytes);
    assert_eq!(1, out.num_handles);

    // Verify the contents.
    let mut buffer = [0u8; MESSAGE.len()];
    let mut boxed = [0u64; 1];
    let out = api::get(parcel, GetFlags::empty(), &mut buffer, &mut boxed, false);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(MESSAGE.len(), out.num_bytes);
    assert_eq!(MESSAGE.as_bytes(), buffer);
    assert_eq!(1, out.num_handles);

    // Handles are consumed by get(), but data is not. We should no longer see
    // any handles, but should see the same data as before.
    let mut again = [0u64; 1];
    let out = api::get(parcel, GetFlags::empty(), &mut buffer, &mut again, false);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(MESSAGE.len(), out.num_bytes);
    assert_eq!(MESSAGE.as_bytes(), buffer);
    assert_eq!(0, out.num_handles);

    // Send the contents of the box back as another parcel.
    t.put(b, &t.unbox_blob(boxed[0]), &[]);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::DEAD));
    t.close_all(&[b, parcel]);
});

multinode_test!(get, |t| {
    let c = t.spawn_test_node(GetClient);

    let blob = t.box_blob(HORNETS);
    t.put(c, MESSAGE, &[blob]);

    let (result, message) = t.wait_to_get_message(c);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(HORNETS, message);
    t.close(c);
});

multinode_test_node!(TwoPhaseGetClient, |t| {
    let b = t.connect_to_broker();
    assert_eq!(IpczResult::Ok, wait_for_parcel(t, b));

    let parcel = get_parcel(b);

    // First validate feedback behavior for handle capacity requirements.
    let out = api::begin_get(parcel, BeginGetFlags::empty(), &mut []);
    assert_eq!(IpczResult::ResourceExhausted, out.result);
    assert_eq!(1, out.num_handles);

    let mut boxed = [0u64; 1];
    let out = api::begin_get(parcel, BeginGetFlags::empty(), &mut boxed);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(MESSAGE, string_from_data(out.data, out.num_bytes));
    assert_eq!(1, out.num_handles);
    let transaction = out.transaction;

    // We can't start a new get of any kind during the two-phase get.
    let mut other = [0u64; 1];
    assert_eq!(
        IpczResult::AlreadyExists,
        api::begin_get(parcel, BeginGetFlags::empty(), &mut other).result
    );
    let mut buffer = [0u8; MESSAGE.len()];
    assert_eq!(
        IpczResult::AlreadyExists,
        api::get(parcel, GetFlags::empty(), &mut buffer, &mut other, false).result
    );

    // Two-phase gets on parcels can be aborted.
    assert_eq!(
        IpczResult::Ok,
        api::end_get(parcel, transaction, EndGetFlags::ABORT, false).0
    );

    // We can restart a get transaction. The attached handle is now gone since it
    // was taken by the first begin_get().
    let mut none = [0u64; 1];
    let out = api::begin_get(parcel, BeginGetFlags::empty(), &mut none);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(MESSAGE, string_from_data(out.data, out.num_bytes));
    assert_eq!(0, out.num_handles);

    // `boxed` is still valid from the original successful begin_get() further
    // above. Send the contents of the box back as another parcel.
    t.put(b, &t.unbox_blob(boxed[0]), &[]);
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::DEAD));
    t.close_all(&[b, parcel]);
});

multinode_test!(two_phase_get, |t| {
    let c = t.spawn_test_node(TwoPhaseGetClient);

    let blob = t.box_blob(HORNETS);
    t.put(c, MESSAGE, &[blob]);

    let (result, message) = t.wait_to_get_message(c);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(HORNETS, message);
    t.close(c);
});

multinode_test_node!(CloseClient, |t| {
    let b = t.connect_to_broker();
    assert_eq!(IpczResult::Ok, wait_for_parcel(t, b));
    let parcel = get_parcel(b);

    let out = api::get(parcel, GetFlags::empty(), &mut [], &mut [], false);
    assert_eq!(IpczResult::ResourceExhausted, out.result);
    assert_eq!(MESSAGE.len(), out.num_bytes);
    assert_eq!(1, out.num_handles);

    // Closing the parcel should close any attached objects. The broker should
    // observer its `q` portal dying, because that portal's peer was attached to
    // this parcel.
    t.close(parcel);

    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(b, TrapConditionFlags::DEAD));
    t.close(b);
});

multinode_test!(close, |t| {
    let c = t.spawn_test_node(CloseClient);
    let (q, p) = t.open_portals();
    t.put(c, MESSAGE, &[p]);

    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(q, TrapConditionFlags::DEAD));
    t.close_all(&[c, q]);
});

multinode_test_node!(SimpleParcelProvider, |t| {
    let b = t.connect_to_broker();
    let blob = t.box_blob(HORNETS);
    let (q, p) = t.open_portals();
    t.put(b, MESSAGE, &[blob, p]);
    t.verify_end_to_end(q);
    wait_for_goodbye(t, b);
    t.close_all(&[b, q]);
});

/// Runs the partial two-phase get sequence against `source`, which is either a
/// portal or a parcel object.
fn partial_two_phase_get(t: &TestNode, c: IpczHandle, source: IpczHandle, end_flags_for_aborts: EndGetFlags) {
    let out = api::begin_get(source, BeginGetFlags::empty(), &mut []);
    assert_eq!(IpczResult::ResourceExhausted, out.result);
    assert_eq!(2, out.num_handles);

    // Start a partial (empty) get and abort it. Should leave the parcel intact
    // and in queue.
    let out = api::begin_get(source, BeginGetFlags::PARTIAL, &mut []);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(0, out.num_handles);
    assert_eq!(IpczResult::Ok, api::end_get(source, out.transaction, end_flags_for_aborts, false).0);

    // Now do a partial handle retrieval.
    let mut boxed = [0u64; 1];
    let out = api::begin_get(source, BeginGetFlags::PARTIAL, &mut boxed);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(1, out.num_handles);
    assert_eq!(IpczResult::Ok, api::end_get(source, out.transaction, end_flags_for_aborts, false).0);

    // Validate that we actually retrieved a handle, even in spite of the aborted
    // transaction.
    assert_eq!(HORNETS, t.unbox_blob(boxed[0]));

    // Now do another partial retrieval. We should only find one remaining
    // handle, a portal we can ping-pong.
    let mut handles = [0u64; 4];
    let out = api::begin_get(source, BeginGetFlags::PARTIAL, &mut handles);
    assert_eq!(IpczResult::Ok, out.result);
    assert_eq!(1, out.num_handles);
    assert_eq!(MESSAGE, string_from_data(out.data, out.num_bytes));
    assert_eq!(
        IpczResult::Ok,
        api::end_get(source, out.transaction, EndGetFlags::empty(), false).0
    );

    let p = handles[0];
    t.verify_end_to_end(p);

    say_goodbye(t, c);
    t.close_all(&[c, p]);
}

multinode_test!(partial_two_phase_get_on_portal, |t| {
    let c = t.spawn_test_node(SimpleParcelProvider);
    assert_eq!(IpczResult::Ok, wait_for_parcel(t, c));
    partial_two_phase_get(t, c, c, EndGetFlags::ABORT);
});

multinode_test!(partial_two_phase_get_on_parcel, |t| {
    let c = t.spawn_test_node(SimpleParcelProvider);
    assert_eq!(IpczResult::Ok, wait_for_parcel(t, c));
    let parcel = get_parcel(c);
    partial_two_phase_get(t, c, parcel, EndGetFlags::empty());
});

const MESSAGE1: &str = "abcdef";
const MESSAGE2: &str = "12345678";
const MESSAGE3: &str = "whatever i guess";

multinode_test_node!(ProviderOfManyParcels, |t| {
    let b = t.connect_to_broker();
    t.put(b, MESSAGE1, &[]);
    t.put(b, MESSAGE2, &[]);
    t.put(b, MESSAGE3, &[]);
    wait_for_goodbye(t, b);
    t.close(b);
});

multinode_test!(overlapped_two_phase_gets, |t| {
    let c = t.spawn_test_node(ProviderOfManyParcels);
    assert_eq!(IpczResult::Ok, wait_for_parcels(t, c, 3));

    // First begin a non-overlapped transaction and verify that we can't then
    // start another (overlapped or not).
    let first = api::begin_get(c, BeginGetFlags::PARTIAL, &mut []);
    assert_eq!(IpczResult::Ok, first.result);
    assert_eq!(
        IpczResult::AlreadyExists,
        api::begin_get(c, BeginGetFlags::PARTIAL, &mut []).result
    );
    assert_eq!(
        IpczResult::AlreadyExists,
        api::begin_get(c, BeginGetFlags::OVERLAPPED | BeginGetFlags::PARTIAL, &mut []).result
    );
    assert_eq!(
        IpczResult::Ok,
        api::end_get(c, first.transaction, EndGetFlags::ABORT, false).0
    );

    // Now start three concurrent transactions.
    let t1 = api::begin_get(c, BeginGetFlags::OVERLAPPED, &mut []);
    let t2 = api::begin_get(c, BeginGetFlags::OVERLAPPED, &mut []);
    let t3 = api::begin_get(c, BeginGetFlags::OVERLAPPED, &mut []);
    for out in [&t1, &t2, &t3] {
        assert_eq!(IpczResult::Ok, out.result);
    }

    assert_eq!(MESSAGE1, string_from_data(t1.data, t1.num_bytes));
    assert_eq!(MESSAGE2, string_from_data(t2.data, t2.num_bytes));
    assert_eq!(MESSAGE3, string_from_data(t3.data, t3.num_bytes));

    for out in [&t1, &t2, &t3] {
        assert_eq!(
            IpczResult::Ok,
            api::end_get(c, out.transaction, EndGetFlags::empty(), false).0
        );
    }

    say_goodbye(t, c);
    t.close(c);
});
