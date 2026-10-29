//! Port of `box_test.cc`.

#[macro_use]
mod support;

use std::any::Any;
use std::sync::{Arc, Mutex};

use ipcy::core::api::{self, UnboxedContents};
use ipcy::core::application_object::{ApplicationObject, SerializedApplicationObject};
use ipcy::core::box_object::{BoxContents, PeekedBoxContents};
use ipcy::core::ipcz_result::IpczResult;
use ipcy::core::ipcz_types::*;

multinode_test!(box_and_unbox, |t| {
    const MESSAGE: &str = "Hello, world?";
    let boxed = t.box_blob(MESSAGE);
    assert_eq!(MESSAGE, t.unbox_blob(boxed));
});

multinode_test!(close_box, |t| {
    // Verifies that box closure releases its underlying driver object.
    let boxed = t.box_blob("meh");
    assert_eq!(IpczResult::Ok, api::close(boxed));
});

multinode_test!(peek, |t| {
    const MESSAGE: &str = "Hello, world?";
    let boxed = t.box_blob(MESSAGE);

    let mut memory = None;
    for _ in 0..3 {
        let Ok(UnboxedContents::Peeked(PeekedBoxContents::DriverObject(handle))) =
            api::unbox(boxed, UnboxFlags::PEEK)
        else {
            panic!("expected a peeked driver object");
        };
        assert!(handle.is_valid());
        memory = Some(handle);
    }

    let memory = memory.unwrap();
    let mapped = t.driver().map_shared_memory(memory).unwrap();
    // SAFETY: the mapping is at least `MESSAGE.len()` bytes.
    let contents = unsafe { std::slice::from_raw_parts(mapped.address, MESSAGE.len()) };
    assert_eq!(MESSAGE.as_bytes(), contents);
    assert_eq!(IpczResult::Ok, t.driver().close(mapped.mapping));

    assert_eq!(MESSAGE, t.unbox_blob(boxed));
});

const MESSAGE1: &str = "Hello, world?";
const MESSAGE2: &str = "Hello, world!";
const MESSAGE3: &str = "Hello. World.";

multinode_test_node!(TransferBoxClient, |t| {
    let b = t.connect_to_broker();

    let mut boxed = [0; 1];
    let (result, message) = t.wait_to_get_full(b, true, &mut boxed);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(MESSAGE2, message);
    assert_eq!(MESSAGE1, t.unbox_blob(boxed[0]));
    t.close(b);
});

multinode_test!(transfer_box, |t| {
    let c = t.spawn_test_node(TransferBoxClient);
    let boxed = t.box_blob(MESSAGE1);
    assert_eq!(IpczResult::Ok, t.put(c, MESSAGE2, &[boxed]));
    t.close(c);
});

multinode_test_node!(TransferBoxAndPortalClient, |t| {
    let b = t.connect_to_broker();

    let mut handles = [0; 2];
    let (result, message) = t.wait_to_get_full(b, true, &mut handles);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(MESSAGE2, message);
    assert_eq!(IpczResult::Ok, t.put(handles[1], MESSAGE3, &[]));
    assert_eq!(MESSAGE1, t.unbox_blob(handles[0]));
    t.close_all(&[b, handles[1]]);
});

multinode_test!(transfer_box_and_portal, |t| {
    let c = t.spawn_test_node(TransferBoxAndPortalClient);

    let (q, p) = t.open_portals();
    let boxed = t.box_blob(MESSAGE1);
    assert_eq!(IpczResult::Ok, t.put(c, MESSAGE2, &[boxed, p]));

    let (result, message) = t.wait_to_get_message(q);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!(MESSAGE3, message);
    t.close_all(&[c, q]);
});

const TRANSFER_BOX_BETWEEN_NON_BROKERS_NUM_ITERATIONS: usize = 50;

multinode_test_node!(TransferBoxBetweenNonBrokersClient1, |t| {
    let b = t.connect_to_broker();
    let q = t.wait_to_get_handle(b);

    for _ in 0..TRANSFER_BOX_BETWEEN_NON_BROKERS_NUM_ITERATIONS {
        let boxed = t.box_blob(MESSAGE1);
        assert_eq!(IpczResult::Ok, t.put(q, MESSAGE2, &[boxed]));

        let mut boxed = [0; 1];
        let (result, message) = t.wait_to_get_full(q, true, &mut boxed);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(MESSAGE1, message);
        assert_eq!(MESSAGE2, t.unbox_blob(boxed[0]));
    }

    t.wait_for_direct_remote_link(q);
    t.close_all(&[q, b]);
});

multinode_test_node!(TransferBoxBetweenNonBrokersClient2, |t| {
    let b = t.connect_to_broker();
    let p = t.wait_to_get_handle(b);

    for _ in 0..TRANSFER_BOX_BETWEEN_NON_BROKERS_NUM_ITERATIONS {
        let mut boxed = [0; 1];
        let (result, message) = t.wait_to_get_full(p, true, &mut boxed);
        assert_eq!(IpczResult::Ok, result);
        assert_eq!(MESSAGE2, message);
        assert_eq!(MESSAGE1, t.unbox_blob(boxed[0]));

        let boxed = t.box_blob(MESSAGE2);
        assert_eq!(IpczResult::Ok, t.put(p, MESSAGE1, &[boxed]));
    }

    t.wait_for_direct_remote_link(p);
    t.close_all(&[p, b]);
});

multinode_test!(transfer_box_between_non_brokers, |t| {
    let c1 = t.spawn_test_node(TransferBoxBetweenNonBrokersClient1);
    let c2 = t.spawn_test_node(TransferBoxBetweenNonBrokersClient2);

    // Create a new portal pair and send each end to one of the two non-brokers
    // so they'll establish a direct link.
    let (q, p) = t.open_portals();
    assert_eq!(IpczResult::Ok, t.put(c1, "", &[q]));
    assert_eq!(IpczResult::Ok, t.put(c2, "", &[p]));

    // Wait for the clients to finish their business and go away.
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c1, TrapConditionFlags::PEER_CLOSED));
    assert_eq!(IpczResult::Ok, t.wait_for_condition_flags(c2, TrapConditionFlags::PEER_CLOSED));
    t.close_all(&[c1, c2]);
});

const HEADER_SIZE: usize = 4;

/// An application object owning a portal and a name, which can be serialized
/// into a subparcel as a `u32` name length, the name, and the portal handle.
struct NamedPortal {
    name: String,
    portal: Mutex<IpczHandle>,
    serializable: bool,
}

impl NamedPortal {
    fn new(name: &str, portal: IpczHandle, serializable: bool) -> Self {
        Self {
            name: name.to_string(),
            portal: Mutex::new(portal),
            serializable,
        }
    }

    fn portal(&self) -> IpczHandle {
        *self.portal.lock().unwrap()
    }

    fn deserialize(bytes: &[u8], handles: &[IpczHandle]) -> NamedPortal {
        assert!(bytes.len() >= HEADER_SIZE);
        let name_length = u32::from_ne_bytes(bytes[..HEADER_SIZE].try_into().unwrap()) as usize;
        assert_eq!(bytes.len(), HEADER_SIZE + name_length);
        assert_eq!(handles.len(), 1);
        NamedPortal::new(std::str::from_utf8(&bytes[HEADER_SIZE..]).unwrap(), handles[0], true)
    }
}

impl Drop for NamedPortal {
    fn drop(&mut self) {
        let portal = std::mem::take(&mut *self.portal.lock().unwrap());
        if portal != 0 {
            api::close(portal);
        }
    }
}

impl ApplicationObject for NamedPortal {
    fn is_serializable(&self) -> bool {
        self.serializable
    }

    fn serialize(&self) -> Result<SerializedApplicationObject, IpczResult> {
        let mut data = (self.name.len() as u32).to_ne_bytes().to_vec();
        data.extend_from_slice(self.name.as_bytes());
        Ok(SerializedApplicationObject {
            data,
            handles: vec![std::mem::take(&mut *self.portal.lock().unwrap())],
        })
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

const PORTAL_NAME: &str = "yahoo?";

multinode_test_node!(SerializedApplicationObjectClient, |t| {
    let b = t.connect_to_broker();
    let mut boxed = [0; 1];
    let (result, message) = t.wait_to_get_full(b, true, &mut boxed);
    assert_eq!(IpczResult::Ok, result);
    assert_eq!("hey", message);

    let Ok(UnboxedContents::Unboxed(BoxContents::Subparcel(subparcel))) = api::unbox(boxed[0], UnboxFlags::empty())
    else {
        panic!("expected a subparcel");
    };
    assert_ne!(0, subparcel);

    let mut data = [0u8; HEADER_SIZE + PORTAL_NAME.len()];
    let mut p = [0; 1];
    let out = api::get(subparcel, GetFlags::empty(), &mut data, &mut p, false);
    assert_eq!(IpczResult::Ok, out.result);
    let portal = NamedPortal::deserialize(&data, &p);
    assert_eq!(PORTAL_NAME, portal.name);
    t.verify_end_to_end(portal.portal());
    t.close_all(&[b, subparcel]);
});

multinode_test!(serialized_application_object, |t| {
    let c = t.spawn_test_node(SerializedApplicationObjectClient);
    let (q, p) = t.open_portals();
    let portal = Arc::new(NamedPortal::new(PORTAL_NAME, p, true));

    let boxed = api::box_application_object(t.node(), portal).unwrap();
    t.put(c, "hey", &[boxed]);
    t.verify_end_to_end(q);
    t.close_all(&[c, q]);
});

multinode_test!(unserialized_application_object, |t| {
    let (a, b) = t.open_portals();
    let (q, p) = t.open_portals();
    let portal = Arc::new(NamedPortal::new(PORTAL_NAME, p, false));

    // We can box an object without a serializer and transfer it between local
    // portals.
    let boxed = api::box_application_object(t.node(), portal).unwrap();
    t.put(a, "", &[boxed]);

    let boxed = t.wait_to_get_handle(b);

    let Ok(UnboxedContents::Unboxed(BoxContents::ApplicationObject(object))) = api::unbox(boxed, UnboxFlags::empty())
    else {
        panic!("expected an application object");
    };
    let portal = object.as_any().downcast_ref::<NamedPortal>().expect("a NamedPortal");
    t.verify_end_to_end_local(q, portal.portal());
    t.close_all(&[a, b, q]);
});
