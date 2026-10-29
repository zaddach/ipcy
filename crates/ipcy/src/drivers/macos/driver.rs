//! A driver for talking to other processes on macOS over Mach ports, like
//! Chromium's Mach channel.
//!
//! A transport is a pair of Mach ports: a receive right for this side and a
//! send right to the peer's receive right (Mach messaging is one-directional).
//! Messages ([`super::wire`]) carry the ipcz bytes inline, or out of line
//! when large, and driver handles as port descriptors, so the kernel moves
//! transports and shared memory between processes as port rights:
//!
//! - A serialized transport is its receive right plus its send right.
//! - Shared memory is a memory-entry send right (`mach_make_memory_entry_64`).
//!
//! Peer death shows up as a no-senders notification on the receive port.
//!
//! Two processes without an inherited transport connect with a handshake: one
//! side [`MachDriver::listen`]s and hands the returned send right to the other
//! through some rendezvous (the bootstrap server, an inherited port); the other
//! calls [`MachDriver::connect_to_send_right`], whose first message carries a
//! send right to its own receive port back to the listener.
//!
//! DEVIATION: none of this has been run on a Mac yet (the development sandbox is
//! Linux); it is type-checked against the `aarch64-apple-darwin` target, its
//! framing is unit tested everywhere, and the shared driver tests (see
//! `drivers/mod.rs`) exercise it on macOS CI.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread;

use mach2::kern_return::{KERN_SUCCESS, kern_return_t};
use mach2::mach_port::{mach_port_construct, mach_port_deallocate, mach_port_insert_right, mach_port_mod_refs};
use mach2::message::{
    MACH_MSG_SUCCESS, MACH_MSG_TIMEOUT_NONE, MACH_MSG_TYPE_COPY_SEND, MACH_MSG_TYPE_MAKE_SEND,
    MACH_MSG_TYPE_MAKE_SEND_ONCE, MACH_MSG_TYPE_MOVE_SEND, MACH_RCV_INTERRUPTED, MACH_RCV_LARGE, MACH_RCV_MSG,
    MACH_RCV_TOO_LARGE, MACH_SEND_INTERRUPTED, MACH_SEND_INVALID_DEST, MACH_SEND_MSG, mach_msg, mach_msg_destroy,
    mach_msg_header_t,
};
use mach2::port::{
    MACH_PORT_NULL, MACH_PORT_QLIMIT_LARGE, MACH_PORT_RIGHT_RECEIVE, MACH_PORT_RIGHT_SEND, MPO_QLIMIT,
    mach_port_options_t, mach_port_t,
};
use mach2::traps::mach_task_self;
use mach2::vm::{mach_make_memory_entry_64, mach_vm_allocate, mach_vm_deallocate, mach_vm_map};
use mach2::vm_inherit::VM_INHERIT_NONE;
use mach2::vm_prot::{VM_PROT_READ, VM_PROT_WRITE};
use mach2::vm_statistics::VM_FLAGS_ANYWHERE;

use super::wire::{self, OutgoingMessage, Payload, PortDescriptor};
use crate::core::driver::{
    Driver, DriverHandle, MappedMemory, RawMessage, SerializeBuffers, SerializeOutcome, SerializedDimensions,
    SharedMemoryInfo, TransportActivityHandler,
};
use crate::core::ipcz_result::IpczResult;
use crate::drivers::{ObjectType, SERIALIZED_OBJECT_BYTES, read_object_header, write_object_header};

/// `MAP_MEM_VM_SHARE`: make the memory entry share the pages with the source
/// mapping rather than snapshot them.
const MAP_MEM_VM_SHARE: i32 = 0x0040_0000;

/// `MACH_NOTIFY_NO_SENDERS`.
const MACH_NOTIFY_NO_SENDERS: i32 = wire::MSG_ID_NO_SENDERS;

/// Room the kernel appends after a received message for its trailer.
const TRAILER_ROOM: usize = 128;

unsafe extern "C" {
    fn mach_port_request_notification(
        task: mach_port_t,
        name: mach_port_t,
        msgid: i32,
        sync: u32,
        notify: mach_port_t,
        notify_poly: u32,
        previous: *mut mach_port_t,
    ) -> kern_return_t;
}

fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn kr_to_result(kr: kern_return_t) -> IpczResult {
    match kr {
        KERN_SUCCESS => IpczResult::Ok,
        // KERN_INVALID_ARGUMENT, KERN_INVALID_NAME, KERN_INVALID_RIGHT.
        4 | 15 | 17 => IpczResult::InvalidArgument,
        // KERN_NO_SPACE, KERN_RESOURCE_SHORTAGE.
        3 | 6 => IpczResult::ResourceExhausted,
        _ => IpczResult::Unknown,
    }
}

/// An owned receive right.
struct ReceiveRight(mach_port_t);

impl ReceiveRight {
    fn allocate() -> Result<Self, kern_return_t> {
        // Messages queue up in the kernel until read, and senders block when
        // the queue is full, so ask for the largest queue.
        let mut options = mach_port_options_t {
            flags: MPO_QLIMIT,
            ..Default::default()
        };
        options.mpl.mpl_qlimit = MACH_PORT_QLIMIT_LARGE;
        let mut name = MACH_PORT_NULL;
        // SAFETY: `options` and `name` are valid for the call.
        match unsafe { mach_port_construct(mach_task_self(), &mut options, 0, &mut name) } {
            KERN_SUCCESS => Ok(Self(name)),
            kr => Err(kr),
        }
    }

    fn name(&self) -> mach_port_t {
        self.0
    }

    /// Gives up ownership without releasing the right.
    fn into_name(self) -> mach_port_t {
        let name = self.0;
        std::mem::forget(self);
        name
    }

    /// Makes a new send right to this port.
    fn make_send(&self) -> Result<SendRight, kern_return_t> {
        SendRight::make_from_receive(self.0)
    }
}

impl Drop for ReceiveRight {
    fn drop(&mut self) {
        // SAFETY: releases the one receive right we own.
        unsafe { mach_port_mod_refs(mach_task_self(), self.0, MACH_PORT_RIGHT_RECEIVE, -1) };
    }
}

/// An owned send right (one user reference).
struct SendRight(mach_port_t);

impl SendRight {
    /// Makes a new send right to the port whose receive right is `name`.
    fn make_from_receive(name: mach_port_t) -> Result<Self, kern_return_t> {
        // SAFETY: `name` is a receive right we own.
        match unsafe { mach_port_insert_right(mach_task_self(), name, name, MACH_MSG_TYPE_MAKE_SEND) } {
            KERN_SUCCESS => Ok(SendRight(name)),
            kr => Err(kr),
        }
    }

    fn name(&self) -> mach_port_t {
        self.0
    }

    fn into_name(self) -> mach_port_t {
        let name = self.0;
        std::mem::forget(self);
        name
    }
}

impl Drop for SendRight {
    fn drop(&mut self) {
        // SAFETY: releases the one send user reference we own.
        unsafe { mach_port_deallocate(mach_task_self(), self.0) };
    }
}

/// A port right in transit as a driver handle: either one we are about to send
/// (it is released if never sent) or one just received.
enum PortRight {
    Receive(ReceiveRight),
    Send(SendRight),
}

impl PortRight {
    fn descriptor(&self) -> PortDescriptor {
        match self {
            PortRight::Receive(right) => PortDescriptor {
                name: right.name(),
                disposition: wire::MSG_TYPE_MOVE_RECEIVE,
            },
            PortRight::Send(right) => PortDescriptor {
                name: right.name(),
                disposition: wire::MSG_TYPE_MOVE_SEND,
            },
        }
    }

    /// Gives up ownership because the kernel took the right with a message.
    fn forget(self) {
        match self {
            PortRight::Receive(right) => drop(right.into_name()),
            PortRight::Send(right) => drop(right.into_name()),
        }
    }

    /// Takes ownership of a right received in a message.
    fn from_received(descriptor: PortDescriptor) -> Self {
        match descriptor.disposition {
            wire::MSG_TYPE_PORT_RECEIVE => PortRight::Receive(ReceiveRight(descriptor.name)),
            _ => PortRight::Send(SendRight(descriptor.name)),
        }
    }
}

struct QueuedMessage {
    data: Vec<u8>,
    ports: Vec<PortRight>,
}

/// The send half of a transport. Until the peer's send right is known (a
/// listening transport before the handshake), outgoing messages queue up.
#[derive(Default)]
struct SendSlot {
    right: Option<SendRight>,
    queued: Vec<QueuedMessage>,
}

struct Endpoint {
    recv_name: mach_port_t,
    /// Taken out when the transport is serialized or its rights extracted.
    recv: Mutex<Option<ReceiveRight>>,
    send: Mutex<SendSlot>,
}

impl Endpoint {
    fn new(recv: ReceiveRight, send: SendSlot) -> Self {
        Self {
            recv_name: recv.name(),
            recv: Mutex::new(Some(recv)),
            send: Mutex::new(send),
        }
    }

    fn take_receive(&self) -> Option<ReceiveRight> {
        guard(&self.recv).take()
    }
}

struct Mapping {
    address: u64,
    size: u64,
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the range came from a successful mach_vm_map().
        unsafe { mach_vm_deallocate(mach_task_self(), self.address, self.size) };
    }
}

/// A shared memory region: a memory-entry send right and the requested size.
struct Memory {
    entry: SendRight,
    size: usize,
}

struct TransportState {
    active: AtomicBool,
    io_thread_done: AtomicBool,
    deactivated_sent: AtomicBool,
    handler: Mutex<Option<Arc<dyn TransportActivityHandler>>>,
}

impl TransportState {
    fn handler(&self) -> Option<Arc<dyn TransportActivityHandler>> {
        guard(&self.handler).clone()
    }

    fn notify_error(&self) {
        if let Some(handler) = self.handler() {
            handler.on_error();
        }
    }

    fn send_deactivated_once(&self) {
        if self
            .deactivated_sent
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return;
        }

        // The driver drops its reference to the handler on deactivation.
        let handler = guard(&self.handler).take();
        if let Some(handler) = handler {
            handler.on_deactivated();
        }
    }
}

struct Transport {
    endpoint: Arc<Endpoint>,
    state: Option<Arc<TransportState>>,
}

enum Object {
    Transport(Transport),
    Memory(Memory),
    Mapping(#[allow(dead_code)] Mapping),
    PortRight(Option<PortRight>),
}

type ObjectRef = Arc<Mutex<Object>>;

/// Sends one message to `remote` (a send right). The rights in `ports` are
/// consumed whatever the outcome: after a failed send the kernel may or may not
/// have taken them, and leaking is safer than releasing a name twice.
fn send_message(
    remote: mach_port_t,
    id: i32,
    remote_disposition: u8,
    data: &[u8],
    ports: Vec<PortRight>,
) -> IpczResult {
    let descriptors: Vec<PortDescriptor> = ports.iter().map(PortRight::descriptor).collect();
    ports.into_iter().for_each(PortRight::forget);

    let mut bytes = wire::encode(&OutgoingMessage {
        remote_disposition,
        remote_port: remote,
        id,
        ports: descriptors,
        data,
    });
    loop {
        // SAFETY: `bytes` holds a complete, well-formed message; `data` (which
        // an out-of-line descriptor may point into) outlives the call.
        let result = unsafe {
            mach_msg(
                bytes.as_mut_ptr().cast::<mach_msg_header_t>(),
                MACH_SEND_MSG,
                bytes.len() as u32,
                0,
                MACH_PORT_NULL,
                MACH_MSG_TIMEOUT_NONE,
                MACH_PORT_NULL,
            )
        };
        match result {
            MACH_MSG_SUCCESS => return IpczResult::Ok,
            MACH_SEND_INTERRUPTED => continue,
            // The peer is gone; the no-senders notification reports that.
            MACH_SEND_INVALID_DEST => return IpczResult::Ok,
            error => {
                log::warn!("mach driver: mach_msg send failed: {error:#x}");
                return IpczResult::Unknown;
            }
        }
    }
}

/// A Mach-port driver. See the module documentation.
pub struct MachDriver {
    weak_self: Weak<MachDriver>,
    next_handle: AtomicUsize,
    objects: Mutex<HashMap<usize, ObjectRef>>,
}

impl MachDriver {
    pub fn new() -> Arc<MachDriver> {
        Arc::new_cyclic(|weak_self| MachDriver {
            weak_self: weak_self.clone(),
            next_handle: AtomicUsize::new(1),
            objects: Mutex::new(HashMap::new()),
        })
    }

    fn shared(&self) -> Arc<MachDriver> {
        self.weak_self.upgrade().expect("driver is alive while in use")
    }

    fn insert(&self, object: Object) -> DriverHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        guard(&self.objects).insert(handle, Arc::new(Mutex::new(object)));
        DriverHandle::new(handle)
    }

    fn get(&self, handle: DriverHandle) -> Option<ObjectRef> {
        guard(&self.objects).get(&handle.value()).cloned()
    }

    fn take(&self, handle: DriverHandle) -> Option<ObjectRef> {
        guard(&self.objects).remove(&handle.value())
    }

    fn insert_transport(&self, endpoint: Endpoint) -> DriverHandle {
        self.insert(Object::Transport(Transport {
            endpoint: Arc::new(endpoint),
            state: None,
        }))
    }

    /// Creates a transport from a receive right for this side and a send right
    /// to the peer, e.g. ones inherited from a parent process. Takes ownership
    /// of both.
    pub fn transport_from_rights(&self, receive: mach_port_t, send: mach_port_t) -> DriverHandle {
        self.insert_transport(Endpoint::new(
            ReceiveRight(receive),
            SendSlot {
                right: Some(SendRight(send)),
                queued: Vec::new(),
            },
        ))
    }

    /// Starts the listening half of a handshake. Returns the transport, which
    /// may be used right away (outgoing messages queue until the peer
    /// connects), and a send right to it for the caller to hand to the peer
    /// process, which then calls [`Self::connect_to_send_right`] with it. The
    /// caller owns the returned right.
    pub fn listen(&self) -> Result<(DriverHandle, mach_port_t), IpczResult> {
        let recv = ReceiveRight::allocate().map_err(kr_to_result)?;
        let for_peer = recv.make_send().map_err(kr_to_result)?;
        let handle = self.insert_transport(Endpoint::new(recv, SendSlot::default()));
        Ok((handle, for_peer.into_name()))
    }

    /// Completes a handshake started by the peer's [`Self::listen`]: sends the
    /// peer a send right to a new receive port as the first message. Takes
    /// ownership of `send`.
    pub fn connect_to_send_right(&self, send: mach_port_t) -> Result<DriverHandle, IpczResult> {
        let recv = ReceiveRight::allocate().map_err(kr_to_result)?;
        let reply = recv.make_send().map_err(kr_to_result)?;
        let to_peer = SendRight(send);
        let result = send_message(
            to_peer.name(),
            wire::MSG_ID_HANDSHAKE,
            MACH_MSG_TYPE_COPY_SEND as u8,
            &[],
            vec![PortRight::Send(reply)],
        );
        result.into_result()?;
        Ok(self.insert_transport(Endpoint::new(
            recv,
            SendSlot {
                right: Some(to_peer),
                queued: Vec::new(),
            },
        )))
    }

    /// Extracts the receive and send rights from a transport that was never
    /// activated, e.g. to pass them to a child process. The caller owns them.
    pub fn take_transport_rights(&self, handle: DriverHandle) -> Option<(mach_port_t, mach_port_t)> {
        let object = self.take(handle)?;
        let Object::Transport(transport) = Arc::into_inner(object)?.into_inner().unwrap_or_else(|e| e.into_inner())
        else {
            return None;
        };
        if transport.state.is_some() {
            return None;
        }
        let endpoint = transport.endpoint;
        let send = guard(&endpoint.send).right.take()?;
        Some((endpoint.take_receive()?.into_name(), send.into_name()))
    }

    fn run_io_thread(driver: Arc<MachDriver>, endpoint: Arc<Endpoint>, state: Arc<TransportState>) {
        let mut buffer: Vec<u64> = vec![0; (64 * 1024 + TRAILER_ROOM) / 8];
        loop {
            if !state.active.load(Ordering::SeqCst) {
                break;
            }

            // SAFETY: `buffer` is writable for the size passed.
            let result = unsafe {
                mach_msg(
                    buffer.as_mut_ptr().cast::<mach_msg_header_t>(),
                    MACH_RCV_MSG | MACH_RCV_LARGE,
                    0,
                    (buffer.len() * 8) as u32,
                    endpoint.recv_name,
                    MACH_MSG_TIMEOUT_NONE,
                    MACH_PORT_NULL,
                )
            };
            match result {
                MACH_MSG_SUCCESS => {}
                MACH_RCV_INTERRUPTED => continue,
                MACH_RCV_TOO_LARGE => {
                    // With MACH_RCV_LARGE the message stays queued and its
                    // header reports the size needed.
                    // SAFETY: the header was written to the start of `buffer`.
                    let needed = unsafe { (*buffer.as_ptr().cast::<mach_msg_header_t>()).msgh_size } as usize;
                    buffer.resize((needed + TRAILER_ROOM).div_ceil(8), 0);
                    continue;
                }
                error => {
                    log::warn!("mach driver: mach_msg receive failed: {error:#x}");
                    state.notify_error();
                    break;
                }
            }

            // SAFETY: the buffer holds `buffer.len() * 8` initialized bytes.
            let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), buffer.len() * 8) };
            match wire::message_id(bytes) {
                Some(MACH_NOTIFY_NO_SENDERS) => {
                    state.notify_error();
                    break;
                }
                Some(wire::MSG_ID_WAKEUP) => continue,
                _ => {}
            }

            let incoming = match wire::decode(bytes) {
                Ok(incoming) => incoming,
                Err(error) => {
                    log::warn!("mach driver: {error}");
                    // SAFETY: releases whatever rights the malformed message carries.
                    unsafe { mach_msg_destroy(buffer.as_mut_ptr().cast::<mach_msg_header_t>()) };
                    state.notify_error();
                    state.active.store(false, Ordering::SeqCst);
                    break;
                }
            };

            match incoming.id {
                wire::MSG_ID_HANDSHAKE => {
                    let [descriptor] = incoming.ports[..] else {
                        state.notify_error();
                        state.active.store(false, Ordering::SeqCst);
                        break;
                    };
                    let PortRight::Send(right) = PortRight::from_received(descriptor) else {
                        state.notify_error();
                        state.active.store(false, Ordering::SeqCst);
                        break;
                    };
                    let mut slot = guard(&endpoint.send);
                    let remote = right.name();
                    slot.right = Some(right);
                    for queued in std::mem::take(&mut slot.queued) {
                        send_message(
                            remote,
                            wire::MSG_ID_MESSAGE,
                            MACH_MSG_TYPE_COPY_SEND as u8,
                            &queued.data,
                            queued.ports,
                        );
                    }
                }
                wire::MSG_ID_MESSAGE => {
                    let data = match incoming.payload {
                        Payload::Inline(data) => data,
                        Payload::OutOfLine { address, size } => {
                            // SAFETY: the kernel mapped `size` bytes at `address`
                            // into this task for us to copy and release.
                            let data =
                                unsafe { std::slice::from_raw_parts(address as *const u8, size as usize) }.to_vec();
                            unsafe { mach_vm_deallocate(mach_task_self(), address, size as u64) };
                            data
                        }
                    };
                    let handles: Vec<DriverHandle> = incoming
                        .ports
                        .into_iter()
                        .map(|descriptor| driver.insert(Object::PortRight(Some(PortRight::from_received(descriptor)))))
                        .collect();

                    let Some(handler) = state.handler() else {
                        break;
                    };
                    let result = handler.on_message(
                        RawMessage {
                            data: &data,
                            handles: &handles,
                        },
                        DriverHandle::default(),
                    );
                    if result != IpczResult::Ok && result != IpczResult::Unimplemented {
                        state.notify_error();
                        state.active.store(false, Ordering::SeqCst);
                        break;
                    }
                }
                other => log::warn!("mach driver: ignoring message with unknown id {other:#x}"),
            }
        }

        // If deactivation was requested while we were exiting (e.g. after an
        // error), `deactivate_transport()` may have missed `io_thread_done`, so
        // make sure the final notification goes out either way.
        state.io_thread_done.store(true, Ordering::SeqCst);
        if !state.active.load(Ordering::SeqCst) {
            state.send_deactivated_once();
        }
    }

    fn page_size() -> usize {
        // SAFETY: sysconf has no preconditions.
        usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(16384)
    }
}

impl Driver for MachDriver {
    fn close(&self, handle: DriverHandle) -> IpczResult {
        match self.take(handle) {
            Some(_) => IpczResult::Ok,
            None => IpczResult::InvalidArgument,
        }
    }

    fn serialize(
        &self,
        handle: DriverHandle,
        transport: DriverHandle,
        buffers: Option<SerializeBuffers<'_>>,
    ) -> SerializeOutcome {
        let Some(object) = self.get(handle) else {
            return SerializeOutcome::Error(IpczResult::InvalidArgument);
        };
        let ty = match &*guard(&object) {
            Object::Transport(_) => ObjectType::Transport,
            Object::Memory(_) => ObjectType::Memory,
            _ => return SerializeOutcome::Error(IpczResult::InvalidArgument),
        };

        // Any Mach port can cross to any process we have a transport to.
        if !transport.is_valid() {
            return SerializeOutcome::NeedsTransport;
        }

        let dimensions = SerializedDimensions {
            num_bytes: SERIALIZED_OBJECT_BYTES,
            num_driver_handles: match ty {
                ObjectType::Transport => 2,
                ObjectType::Memory => 1,
            },
        };
        let Some(SerializeBuffers { data, handles }) = buffers else {
            return SerializeOutcome::Dimensions(dimensions);
        };
        if data.len() < dimensions.num_bytes || handles.len() < dimensions.num_driver_handles {
            return SerializeOutcome::Dimensions(dimensions);
        }

        // Extract the rights atomically with the state checks; the handle is
        // only consumed once serialization is certain to succeed.
        let (size, rights) = {
            let mut inner = guard(&object);
            match &mut *inner {
                Object::Transport(t) => {
                    if t.state.is_some() || Arc::strong_count(&t.endpoint) != 1 {
                        return SerializeOutcome::Error(IpczResult::FailedPrecondition);
                    }
                    let mut slot = guard(&t.endpoint.send);
                    let Some(send) = slot.right.take() else {
                        return SerializeOutcome::Error(IpczResult::FailedPrecondition);
                    };
                    let Some(recv) = t.endpoint.take_receive() else {
                        slot.right = Some(send);
                        return SerializeOutcome::Error(IpczResult::FailedPrecondition);
                    };
                    (0u32, vec![PortRight::Receive(recv), PortRight::Send(send)])
                }
                Object::Memory(m) => {
                    // One user reference of the entry's send right moves to the
                    // peer; the object keeps its own, released when it is closed
                    // below.
                    // SAFETY: adds one user reference to a send right we own.
                    if unsafe { mach_port_mod_refs(mach_task_self(), m.entry.name(), MACH_PORT_RIGHT_SEND, 1) }
                        != KERN_SUCCESS
                    {
                        return SerializeOutcome::Error(IpczResult::Unknown);
                    }
                    (
                        u32::try_from(m.size).unwrap_or(u32::MAX),
                        vec![PortRight::Send(SendRight(m.entry.name()))],
                    )
                }
                _ => return SerializeOutcome::Error(IpczResult::InvalidArgument),
            }
        };

        // On success, ipcz relinquishes `handle` and will no longer refer to it.
        self.take(handle);

        write_object_header(data, ty, size);
        rights.into_iter().zip(handles.iter_mut()).for_each(|(right, slot)| {
            *slot = self.insert(Object::PortRight(Some(right)));
        });
        SerializeOutcome::Serialized
    }

    fn deserialize(
        &self,
        data: &[u8],
        driver_handles: &[DriverHandle],
        _transport: DriverHandle,
    ) -> Result<DriverHandle, IpczResult> {
        let (ty, size) = read_object_header(data).ok_or(IpczResult::InvalidArgument)?;
        let take_right = |handle: DriverHandle| -> Result<PortRight, IpczResult> {
            let object = self.take(handle).ok_or(IpczResult::InvalidArgument)?;
            let right = match &mut *guard(&object) {
                Object::PortRight(right) => right.take(),
                _ => None,
            };
            right.ok_or(IpczResult::InvalidArgument)
        };

        match (ty, driver_handles) {
            (ObjectType::Transport, &[recv, send]) => {
                let (PortRight::Receive(recv), PortRight::Send(send)) = (take_right(recv)?, take_right(send)?) else {
                    return Err(IpczResult::InvalidArgument);
                };
                Ok(self.insert_transport(Endpoint::new(
                    recv,
                    SendSlot {
                        right: Some(send),
                        queued: Vec::new(),
                    },
                )))
            }
            (ObjectType::Memory, &[entry]) => {
                let PortRight::Send(entry) = take_right(entry)? else {
                    return Err(IpczResult::InvalidArgument);
                };
                Ok(self.insert(Object::Memory(Memory {
                    entry,
                    size: size as usize,
                })))
            }
            _ => Err(IpczResult::InvalidArgument),
        }
    }

    fn create_transports(
        &self,
        _transport0: DriverHandle,
        _transport1: DriverHandle,
    ) -> Result<(DriverHandle, DriverHandle), IpczResult> {
        let recv0 = ReceiveRight::allocate().map_err(kr_to_result)?;
        let recv1 = ReceiveRight::allocate().map_err(kr_to_result)?;
        let send0 = recv1.make_send().map_err(kr_to_result)?;
        let send1 = recv0.make_send().map_err(kr_to_result)?;
        let endpoint = |recv, send| {
            Endpoint::new(
                recv,
                SendSlot {
                    right: Some(send),
                    queued: Vec::new(),
                },
            )
        };
        Ok((
            self.insert_transport(endpoint(recv0, send0)),
            self.insert_transport(endpoint(recv1, send1)),
        ))
    }

    fn activate_transport(&self, transport: DriverHandle, handler: Arc<dyn TransportActivityHandler>) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };

        let state = Arc::new(TransportState {
            active: AtomicBool::new(true),
            io_thread_done: AtomicBool::new(false),
            deactivated_sent: AtomicBool::new(false),
            handler: Mutex::new(Some(handler)),
        });
        let endpoint = {
            let mut inner = guard(&object);
            let Object::Transport(t) = &mut *inner else {
                return IpczResult::InvalidArgument;
            };
            if t.state.is_some() {
                return IpczResult::InvalidArgument;
            }
            t.state = Some(state.clone());
            t.endpoint.clone()
        };

        // Ask for a message when the peer's last send right goes away.
        let mut previous = MACH_PORT_NULL;
        let name = endpoint.recv_name;
        // SAFETY: requests a send-once notification to the port itself.
        let kr = unsafe {
            mach_port_request_notification(
                mach_task_self(),
                name,
                MACH_NOTIFY_NO_SENDERS,
                0,
                name,
                MACH_MSG_TYPE_MAKE_SEND_ONCE,
                &mut previous,
            )
        };
        if kr != KERN_SUCCESS {
            return kr_to_result(kr);
        }
        if previous != MACH_PORT_NULL {
            // SAFETY: releases the previously registered notification right.
            unsafe { mach_port_deallocate(mach_task_self(), previous) };
        }

        let driver = self.shared();
        thread::spawn(move || MachDriver::run_io_thread(driver, endpoint, state));
        IpczResult::Ok
    }

    fn deactivate_transport(&self, transport: DriverHandle) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };
        let (state, endpoint) = {
            let inner = guard(&object);
            let Object::Transport(t) = &*inner else {
                return IpczResult::InvalidArgument;
            };
            (t.state.clone(), t.endpoint.clone())
        };
        let Some(state) = state else {
            return IpczResult::Ok;
        };

        state.active.store(false, Ordering::SeqCst);
        // Wake the I/O thread out of its blocking receive with a message from a
        // send right made just for it, which the kernel consumes when sending.
        if let Ok(wakeup) = SendRight::make_from_receive(endpoint.recv_name) {
            send_message(
                wakeup.into_name(),
                wire::MSG_ID_WAKEUP,
                MACH_MSG_TYPE_MOVE_SEND as u8,
                &[],
                Vec::new(),
            );
        }

        // If the I/O thread already exited (e.g. after signaling an error),
        // ensure we still emit a final deactivation notification.
        if state.io_thread_done.load(Ordering::SeqCst) {
            state.send_deactivated_once();
        }
        IpczResult::Ok
    }

    fn transmit(&self, transport: DriverHandle, data: &[u8], driver_handles: &[DriverHandle]) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };
        let (endpoint, state) = {
            let inner = guard(&object);
            let Object::Transport(t) = &*inner else {
                return IpczResult::InvalidArgument;
            };
            (t.endpoint.clone(), t.state.clone())
        };

        let mut ports = Vec::with_capacity(driver_handles.len());
        for &handle in driver_handles {
            let Some(wrapped) = self.get(handle) else {
                return IpczResult::InvalidArgument;
            };
            let right = match &mut *guard(&wrapped) {
                Object::PortRight(right) => right.take(),
                _ => None,
            };
            let Some(right) = right else {
                return IpczResult::InvalidArgument;
            };
            // ipcz relinquishes transmitted driver handles on success.
            self.take(handle);
            ports.push(right);
        }

        // A deactivated transport discards outgoing transmissions, releasing
        // any rights attached to them.
        if state.is_some_and(|state| !state.active.load(Ordering::SeqCst)) {
            return IpczResult::Ok;
        }

        let mut slot = guard(&endpoint.send);
        match &slot.right {
            Some(right) => send_message(
                right.name(),
                wire::MSG_ID_MESSAGE,
                MACH_MSG_TYPE_COPY_SEND as u8,
                data,
                ports,
            ),
            None => {
                slot.queued.push(QueuedMessage {
                    data: data.to_vec(),
                    ports,
                });
                IpczResult::Ok
            }
        }
    }

    fn report_bad_transport_activity(&self, _transport: DriverHandle, _context: usize) -> IpczResult {
        IpczResult::Ok
    }

    fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult> {
        let rounded = num_bytes.max(1).next_multiple_of(Self::page_size()) as u64;

        // Allocate pages, wrap them in a memory entry that shares them, and
        // drop the temporary mapping; the entry keeps the pages alive.
        let mut address = 0u64;
        // SAFETY: allocates anywhere in this task's address space.
        let kr = unsafe { mach_vm_allocate(mach_task_self(), &mut address, rounded, VM_FLAGS_ANYWHERE) };
        if kr != KERN_SUCCESS {
            return Err(kr_to_result(kr));
        }
        let mut entry_size = rounded;
        let mut entry = MACH_PORT_NULL;
        // SAFETY: `address` is a live allocation of `rounded` bytes.
        let kr = unsafe {
            mach_make_memory_entry_64(
                mach_task_self(),
                &mut entry_size,
                address,
                VM_PROT_READ | VM_PROT_WRITE | MAP_MEM_VM_SHARE,
                &mut entry,
                MACH_PORT_NULL,
            )
        };
        // SAFETY: releases the temporary mapping made above.
        unsafe { mach_vm_deallocate(mach_task_self(), address, rounded) };
        if kr != KERN_SUCCESS {
            return Err(kr_to_result(kr));
        }
        Ok(self.insert(Object::Memory(Memory {
            entry: SendRight(entry),
            size: num_bytes,
        })))
    }

    fn get_shared_memory_info(&self, driver_memory: DriverHandle) -> Result<SharedMemoryInfo, IpczResult> {
        let object = self.get(driver_memory).ok_or(IpczResult::InvalidArgument)?;
        match &*guard(&object) {
            Object::Memory(m) => Ok(SharedMemoryInfo {
                region_num_bytes: m.size,
            }),
            _ => Err(IpczResult::InvalidArgument),
        }
    }

    fn duplicate_shared_memory(&self, driver_memory: DriverHandle) -> Result<DriverHandle, IpczResult> {
        let object = self.get(driver_memory).ok_or(IpczResult::InvalidArgument)?;
        let (name, size) = match &*guard(&object) {
            Object::Memory(m) => (m.entry.name(), m.size),
            _ => return Err(IpczResult::InvalidArgument),
        };
        // SAFETY: adds one user reference to a send right we own; the new
        // object releases it.
        let kr = unsafe { mach_port_mod_refs(mach_task_self(), name, MACH_PORT_RIGHT_SEND, 1) };
        if kr != KERN_SUCCESS {
            return Err(kr_to_result(kr));
        }
        Ok(self.insert(Object::Memory(Memory {
            entry: SendRight(name),
            size,
        })))
    }

    fn map_shared_memory(&self, driver_memory: DriverHandle) -> Result<MappedMemory, IpczResult> {
        let object = self.get(driver_memory).ok_or(IpczResult::InvalidArgument)?;
        let (entry, size) = match &*guard(&object) {
            Object::Memory(m) => (m.entry.name(), m.size),
            _ => return Err(IpczResult::InvalidArgument),
        };
        let rounded = size.max(1).next_multiple_of(Self::page_size()) as u64;

        let mut address = 0u64;
        // SAFETY: maps the entry's pages anywhere into this task.
        let kr = unsafe {
            mach_vm_map(
                mach_task_self(),
                &mut address,
                rounded,
                0,
                VM_FLAGS_ANYWHERE,
                entry,
                0,
                0,
                VM_PROT_READ | VM_PROT_WRITE,
                VM_PROT_READ | VM_PROT_WRITE,
                VM_INHERIT_NONE,
            )
        };
        if kr != KERN_SUCCESS {
            return Err(kr_to_result(kr));
        }
        Ok(MappedMemory {
            address: address as *mut u8,
            mapping: self.insert(Object::Mapping(Mapping { address, size: rounded })),
        })
    }

    fn generate_random_bytes(&self, buffer: &mut [u8]) -> IpczResult {
        match getrandom::getrandom(buffer) {
            Ok(()) => IpczResult::Ok,
            Err(_) => IpczResult::Unknown,
        }
    }
}
