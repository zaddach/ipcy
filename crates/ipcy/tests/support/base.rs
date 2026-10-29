//! Port of `test/test_base.{h,cc}`: shorthand helpers over the ipcz API.

use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use ipcy::core::api::{self, TrapError};
use ipcy::core::api_object;
use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;
use ipcy::core::router::Router;
use ipcy::core::trap_set::TrapAlreadySatisfied;


#[derive(Default)]
pub struct TestBase;

impl TestBase {
    pub fn close(&self, handle: IpczHandle) {
        assert_eq!(IpczResult::Ok, api::close(handle));
    }

    pub fn close_all(&self, handles: &[IpczHandle]) {
        handles.iter().for_each(|&handle| self.close(handle));
    }

    pub fn merge(&self, a: IpczHandle, b: IpczHandle) -> IpczResult {
        api::merge_portals(a, b)
    }

    pub fn put(&self, portal: IpczHandle, message: &str, handles: &[IpczHandle]) -> IpczResult {
        api::put(portal, message.as_bytes(), handles)
    }

    /// Shorthand for `get()` to retrieve the next available parcel from `portal`.
    /// If no parcel is available, or any other condition prevents the get from
    /// succeeding, this returns the same result as the `get()` API.
    ///
    /// If the parcel has data it's returned as a string iff `want_message`.
    /// Any handles are stored in `handles` if it's large enough to hold all of
    /// them. If the available parcel has data but `want_message` is false, or if
    /// the parcel carries more handles than the capacity of `handles`, the
    /// parcel is not retrieved, and this returns `ResourceExhausted`.
    pub fn get_full(&self, portal: IpczHandle, want_message: bool, handles: &mut [IpczHandle]) -> (IpczResult, String) {
        let first = api::get(portal, GetFlags::empty(), &mut [], handles, false);
        if first.result != IpczResult::ResourceExhausted {
            return (first.result, String::new());
        }

        let mut data = vec![0u8; if want_message { first.num_bytes } else { 0 }];
        let second = api::get(portal, GetFlags::empty(), &mut data, handles, false);
        (second.result, String::from_utf8_lossy(&data).into_owned())
    }

    pub fn get(&self, portal: IpczHandle) -> IpczResult {
        self.get_full(portal, false, &mut []).0
    }

    pub fn get_message(&self, portal: IpczHandle) -> (IpczResult, String) {
        self.get_full(portal, true, &mut [])
    }

    pub fn get_with_handles(&self, portal: IpczHandle, handles: &mut [IpczHandle]) -> (IpczResult, String) {
        self.get_full(portal, true, handles)
    }

    /// Shorthand for the `trap()` API with convenient closure support. Returns
    /// `Ok(())` if the trap was installed.
    pub fn trap(
        &self,
        portal: IpczHandle,
        conditions: TrapConditions,
        handler: impl Fn(&TrapEvent) + Send + Sync + 'static,
    ) -> Result<(), TrapError> {
        api::trap(portal, conditions, Arc::new(handler), 0)
    }

    /// Blocks until one or more conditions indicated by `conditions` are met by
    /// `portal`.
    pub fn wait_for_conditions(&self, portal: IpczHandle, conditions: TrapConditions) -> IpczResult {
        let (tx, rx) = mpsc::sync_channel(1);
        let result = self.trap(portal, conditions, move |_| {
            let _ = tx.try_send(());
        });

        match result {
            Ok(()) => {
                rx.recv().expect("trap handler fired");
                IpczResult::Ok
            }
            Err(TrapError::AlreadySatisfied(TrapAlreadySatisfied { .. })) => IpczResult::Ok,
            Err(TrapError::Failed(result)) => result,
        }
    }

    pub fn wait_for_condition_flags(&self, portal: IpczHandle, flags: TrapConditionFlags) -> IpczResult {
        self.wait_for_conditions(
            portal,
            TrapConditions {
                flags,
                ..Default::default()
            },
        )
    }

    /// Waits to receive any parcel on `portal`.
    pub fn wait_to_get_full(
        &self,
        portal: IpczHandle,
        want_message: bool,
        handles: &mut [IpczHandle],
    ) -> (IpczResult, String) {
        let result = self.wait_for_conditions(
            portal,
            TrapConditions {
                flags: TrapConditionFlags::ABOVE_MIN_LOCAL_PARCELS,
                min_local_parcels: 0,
                min_local_bytes: 0,
            },
        );
        if result != IpczResult::Ok {
            return (result, String::new());
        }
        self.get_full(portal, want_message, handles)
    }

    /// Waits for a parcel with exactly one handle (and no data), expecting
    /// success, and returns the handle.
    pub fn wait_to_get_handle(&self, portal: IpczHandle) -> IpczHandle {
        let mut handles = [0; 1];
        assert_eq!(IpczResult::Ok, self.wait_to_get_full(portal, false, &mut handles).0);
        assert_ne!(0, handles[0]);
        handles[0]
    }

    pub fn wait_to_get(&self, portal: IpczHandle) -> IpczResult {
        self.wait_to_get_full(portal, false, &mut []).0
    }

    pub fn wait_to_get_message(&self, portal: IpczHandle) -> (IpczResult, String) {
        self.wait_to_get_full(portal, true, &mut [])
    }

    /// Like `wait_to_get` but expects success and requires the read parcel to
    /// have no handles. Returns the parcel contents as a string.
    pub fn wait_to_get_string(&self, portal: IpczHandle) -> String {
        let (result, message) = self.wait_to_get_message(portal);
        assert_eq!(IpczResult::Ok, result);
        message
    }

    /// Sends an empty parcel from `portal` and expects an empty parcel in return.
    pub fn ping_pong(&self, portal: IpczHandle) {
        assert_eq!(IpczResult::Ok, self.put(portal, "", &[]));
        assert_eq!(IpczResult::Ok, self.wait_to_get(portal));
    }

    /// Waits for an empty parcel on `portal` and then sends an empty parcel in
    /// return.
    pub fn wait_for_ping_and_reply(&self, portal: IpczHandle) {
        assert_eq!(IpczResult::Ok, self.wait_to_get(portal));
        assert_eq!(IpczResult::Ok, self.put(portal, "", &[]));
    }

    /// Sends a parcel from `portal` and expects to receive a parcel back with
    /// the same contents. Typically called from two different nodes, on a pair
    /// of connected portals, to verify working communication between them.
    pub fn verify_end_to_end(&self, portal: IpczHandle) {
        const MESSAGE: &str = "Ping!!!";
        assert_eq!(IpczResult::Ok, self.put(portal, MESSAGE, &[]));
        let (result, message) = self.wait_to_get_message(portal);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(MESSAGE, message);
    }

    /// Similar to above, but for situations where both portals are local to the
    /// same node.
    pub fn verify_end_to_end_local(&self, a: IpczHandle, b: IpczHandle) {
        const MESSAGE1: &str = "psssst";
        const MESSAGE2: &str = "ssshhh";

        self.put(a, MESSAGE1, &[]);
        self.put(b, MESSAGE2, &[]);

        let (result, message) = self.wait_to_get_message(a);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(MESSAGE2, message);
        let (result, message) = self.wait_to_get_message(b);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(MESSAGE1, message);
    }

    /// Waits until `portal` is backed by a Router which is connected directly to
    /// its peer portal's Router on another node, with no proxies in between.
    pub fn wait_for_direct_remote_link(&self, portal: IpczHandle) {
        let router = api_object::from_handle_typed::<Router>(portal).expect("portal handle");
        while !router.is_on_central_remote_link() {
            std::thread::sleep(Duration::from_millis(8));
        }

        const MESSAGE: &str = "very direct wow";
        assert_eq!(IpczResult::Ok, self.put(portal, MESSAGE, &[]));
        let (result, message) = self.wait_to_get_message(portal);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(MESSAGE, message);
    }

    /// Waits for portals `a` and `b` to become direct local peers, after any
    /// potential proxies in between are eliminated.
    pub fn wait_for_direct_local_link(&self, a: IpczHandle, b: IpczHandle) {
        let router_a = api_object::from_handle_typed::<Router>(a).expect("portal handle");
        let router_b = api_object::from_handle_typed::<Router>(b).expect("portal handle");
        while !router_a.has_local_peer(&router_b) && !router_b.has_local_peer(&router_a) {
            std::thread::sleep(Duration::from_millis(8));
        }
    }
}
