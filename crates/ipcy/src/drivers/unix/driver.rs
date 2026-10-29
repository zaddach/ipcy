//! A Unix-socket based multiprocess driver, a port of upstream's
//! `multiprocess_reference_driver`: transports are `SOCK_STREAM` socketpairs
//! carrying framed messages with file descriptors attached via `SCM_RIGHTS`;
//! shared memory is `memfd`.
//!
//! Wire format (little endian):
//! - Each transmission is framed by an 8-byte header holding the total frame
//!   size (header included) and the number of attached descriptors, followed by
//!   the ipcz message bytes.
//! - A serialized driver object is an 8-byte header holding the object type
//!   (0 = transport, 1 = memory) and, for memory, its size, plus one transmitted
//!   descriptor.

use std::collections::{HashMap, VecDeque};
use std::io::{IoSlice, IoSliceMut};
use std::num::NonZeroUsize;
use std::os::fd::{AsRawFd, BorrowedFd, FromRawFd, IntoRawFd, OwnedFd};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread;

use memfd::MemfdOptions;
use nix::errno::Errno;
use nix::fcntl::{FcntlArg, OFlag, fcntl};
use nix::libc;
use nix::sys::mman::{MapFlags, ProtFlags, mmap, munmap};
use nix::sys::socket::{
    AddressFamily, ControlMessage, ControlMessageOwned, MsgFlags, SockFlag, SockType, recvmsg, sendmsg, socketpair,
};
use nix::unistd::{dup, read, write};

use crate::core::driver::{
    Driver, DriverHandle, MappedMemory, RawMessage, SerializeBuffers, SerializeOutcome, SerializedDimensions,
    SharedMemoryInfo, TransportActivityHandler,
};
use crate::core::ipcz_result::IpczResult;

fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

const SERIALIZED_HEADER_BYTES: usize = 8;
const TRANSPORT_HEADER_BYTES: usize = 8;
const MAX_DESCRIPTORS_PER_MESSAGE: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
enum ObjectType {
    Transport = 0,
    Memory = 1,
}

fn write_serialized_header(dst: &mut [u8], ty: ObjectType, size: u32) {
    dst[0..4].copy_from_slice(&(ty as u32).to_le_bytes());
    dst[4..8].copy_from_slice(&size.to_le_bytes());
}

fn read_serialized_header(src: &[u8]) -> (u32, u32) {
    (
        u32::from_le_bytes(src[0..4].try_into().expect("4 bytes")),
        u32::from_le_bytes(src[4..8].try_into().expect("4 bytes")),
    )
}

fn write_transport_header(dst: &mut [u8], num_bytes: u32, num_descriptors: u32) {
    dst[0..4].copy_from_slice(&num_bytes.to_le_bytes());
    dst[4..8].copy_from_slice(&num_descriptors.to_le_bytes());
}

fn read_transport_header(src: &[u8]) -> (u32, u32) {
    read_serialized_header(src)
}

fn set_nonblocking(fd: libc::c_int) -> Result<(), Errno> {
    let current = OFlag::from_bits_truncate(fcntl(fd, FcntlArg::F_GETFL)?);
    fcntl(fd, FcntlArg::F_SETFL(current | OFlag::O_NONBLOCK))?;
    Ok(())
}

fn create_nonblocking_pipe() -> Result<(OwnedFd, OwnedFd), Errno> {
    let mut fds = [0 as libc::c_int; 2];
    // SAFETY: libc call writes two fds on success.
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
    if rc != 0 {
        return Err(Errno::last());
    }
    // SAFETY: returned fds are owned by us.
    Ok(unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) })
}

fn drain_notify(fd: libc::c_int) {
    let mut buf = [0u8; 64];
    while matches!(read(fd, &mut buf), Ok(n) if n > 0) {}
}

fn errno_to_result(errno: Errno) -> IpczResult {
    match errno {
        Errno::EBADF | Errno::EINVAL => IpczResult::InvalidArgument,
        Errno::ENOSYS => IpczResult::Unavailable,
        _ => IpczResult::Unknown,
    }
}

struct Mapping {
    addr: NonNull<libc::c_void>,
    len: NonZeroUsize,
}

// SAFETY: a mapping is just an address range owned by this object.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `addr`/`len` came from a successful mmap().
        let _ = unsafe { munmap(self.addr, self.len.get()) };
    }
}

struct Memory {
    fd: Option<OwnedFd>,
    size: usize,
}

/// An object (a file descriptor) in the process of being transmitted.
struct FileDescriptor {
    fd: Option<OwnedFd>,
}

struct DeferredMessage {
    data: Vec<u8>,
    offset: usize,
    descriptors: Vec<OwnedFd>,
}

struct TransportState {
    active: AtomicBool,
    io_thread_done: AtomicBool,
    deactivated_sent: AtomicBool,
    handler: Mutex<Option<Arc<dyn TransportActivityHandler>>>,
    outgoing: Mutex<VecDeque<DeferredMessage>>,
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
    fd: Option<Arc<OwnedFd>>,
    state: Option<Arc<TransportState>>,
    notify_sender: Option<OwnedFd>,
}

enum Object {
    Transport(Transport),
    Memory(Memory),
    Mapping(#[allow(dead_code)] Mapping),
    FileDescriptor(FileDescriptor),
}

type ObjectRef = Arc<Mutex<Object>>;

/// Unix-socket driver for talking to other processes.
pub struct MultiprocessDriver {
    weak_self: Weak<MultiprocessDriver>,
    next_handle: AtomicUsize,
    objects: Mutex<HashMap<usize, ObjectRef>>,
}

impl MultiprocessDriver {
    pub fn new() -> Arc<MultiprocessDriver> {
        Arc::new_cyclic(|weak_self| MultiprocessDriver {
            weak_self: weak_self.clone(),
            next_handle: AtomicUsize::new(1),
            objects: Mutex::new(HashMap::new()),
        })
    }

    fn shared(&self) -> Arc<MultiprocessDriver> {
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

    /// Creates a transport driver object over `fd`, which must be a connected
    /// stream socket. The driver takes ownership of `fd`.
    pub fn transport_from_owned_fd(&self, fd: OwnedFd) -> DriverHandle {
        self.insert(Object::Transport(Transport {
            fd: Some(Arc::new(fd)),
            state: None,
            notify_sender: None,
        }))
    }

    /// Like [`Self::transport_from_owned_fd`], but duplicates `fd`.
    pub fn transport_from_fd(&self, fd: libc::c_int) -> DriverHandle {
        match dup(fd) {
            // SAFETY: `dup()` returned a new descriptor we own.
            Ok(dup_fd) => self.transport_from_owned_fd(unsafe { OwnedFd::from_raw_fd(dup_fd) }),
            Err(_) => DriverHandle::default(),
        }
    }

    /// Extracts the socket from a transport driver object that was never
    /// activated, e.g. to pass it to a child process.
    pub fn take_transport_fd(&self, handle: DriverHandle) -> Option<OwnedFd> {
        let object = self.take(handle)?;
        let mut object = guard(&object);
        match &mut *object {
            Object::Transport(transport) if transport.state.is_none() => {
                transport.fd.take().and_then(|fd| Arc::try_unwrap(fd).ok())
            }
            _ => None,
        }
    }

    fn make_fd_object(&self, fd: OwnedFd) -> DriverHandle {
        self.insert(Object::FileDescriptor(FileDescriptor { fd: Some(fd) }))
    }

    fn send_all(
        socket: libc::c_int,
        buf: &[u8],
        offset: &mut usize,
        descriptors: &mut Vec<OwnedFd>,
    ) -> Result<bool, Errno> {
        // Sends as much as possible. Returns Ok(true) once everything is sent.
        while *offset < buf.len() {
            let iov = [IoSlice::new(&buf[*offset..])];
            let raw_fds: Vec<libc::c_int> = if *offset == 0 {
                descriptors.iter().map(|fd| fd.as_raw_fd()).collect()
            } else {
                Vec::new()
            };
            let cmsgs_storage;
            let cmsgs: &[ControlMessage] = if raw_fds.is_empty() {
                &[]
            } else {
                cmsgs_storage = [ControlMessage::ScmRights(raw_fds.as_slice())];
                &cmsgs_storage
            };

            match sendmsg::<()>(socket, &iov, cmsgs, MsgFlags::MSG_NOSIGNAL, None) {
                Ok(0) => return Ok(false),
                Ok(n) => {
                    *offset += n;
                    descriptors.clear();
                }
                Err(Errno::EAGAIN) => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }

    fn run_io_thread(
        driver: Arc<MultiprocessDriver>,
        state: Arc<TransportState>,
        socket: Arc<OwnedFd>,
        notify_receiver: OwnedFd,
    ) {
        let fd = socket.as_raw_fd();
        let mut data = Vec::<u8>::with_capacity(64 * 1024);
        let mut data_start = 0usize;
        let mut descriptors = VecDeque::<OwnedFd>::new();

        loop {
            if !state.active.load(Ordering::Relaxed) {
                state.send_deactivated_once();
                break;
            }

            let has_outgoing = !guard(&state.outgoing).is_empty();
            let mut poll_fds = [
                libc::pollfd {
                    fd,
                    events: (libc::POLLIN | if has_outgoing { libc::POLLOUT } else { 0 }) as libc::c_short,
                    revents: 0,
                },
                libc::pollfd {
                    fd: notify_receiver.as_raw_fd(),
                    events: libc::POLLIN as libc::c_short,
                    revents: 0,
                },
            ];

            // SAFETY: poll_fds points to valid pollfd entries.
            let pr = unsafe { libc::poll(poll_fds.as_mut_ptr(), poll_fds.len() as _, -1) };
            if pr < 0 {
                if Errno::last() == Errno::EINTR {
                    continue;
                }
                log::warn!("multiprocess driver: poll failed on fd {fd}: {:?}", Errno::last());
                state.notify_error();
                break;
            }

            if poll_fds[1].revents & libc::POLLIN as libc::c_short != 0 {
                drain_notify(notify_receiver.as_raw_fd());
            }

            if poll_fds[0].revents & (libc::POLLIN | libc::POLLHUP | libc::POLLERR) as libc::c_short != 0 {
                let mut buf = [0u8; 4096];
                match recv_fds_with_payload(fd, &mut buf) {
                    Ok((0, _)) => {
                        state.notify_error();
                        break;
                    }
                    Ok((n, fds)) => {
                        data.extend_from_slice(&buf[..n]);
                        // SAFETY: SCM_RIGHTS descriptors are new and owned by us.
                        descriptors.extend(fds.into_iter().map(|raw| unsafe { OwnedFd::from_raw_fd(raw) }));
                    }
                    Err(Errno::EAGAIN) => {}
                    Err(e) => {
                        log::warn!("multiprocess driver: recvmsg failed on fd {fd}: {e:?}");
                        state.notify_error();
                        break;
                    }
                }

                // Dispatch as many complete framed messages as we can.
                loop {
                    let available = &data[data_start..];
                    if available.len() < TRANSPORT_HEADER_BYTES {
                        break;
                    }
                    let (num_bytes, num_desc) = read_transport_header(&available[..TRANSPORT_HEADER_BYTES]);
                    let (num_bytes, num_desc) = (num_bytes as usize, num_desc as usize);
                    if num_bytes < TRANSPORT_HEADER_BYTES {
                        state.notify_error();
                        state.active.store(false, Ordering::Relaxed);
                        break;
                    }
                    if available.len() < num_bytes || descriptors.len() < num_desc {
                        break;
                    }

                    let payload = available[TRANSPORT_HEADER_BYTES..num_bytes].to_vec();
                    let handles: Vec<DriverHandle> = (0..num_desc)
                        .filter_map(|_| descriptors.pop_front())
                        .map(|fd| driver.make_fd_object(fd))
                        .collect();

                    data_start += num_bytes;
                    if data_start >= 8192 {
                        data.drain(..data_start);
                        data_start = 0;
                    }

                    let Some(handler) = state.handler() else {
                        break;
                    };
                    let result = handler.on_message(
                        RawMessage {
                            data: &payload,
                            handles: &handles,
                        },
                        DriverHandle::default(),
                    );
                    if result != IpczResult::Ok && result != IpczResult::Unimplemented {
                        state.notify_error();
                        state.active.store(false, Ordering::Relaxed);
                        break;
                    }
                }
            }

            if poll_fds[0].revents & libc::POLLOUT as libc::c_short != 0 {
                loop {
                    let mut queue = guard(&state.outgoing);
                    let Some(front) = queue.front_mut() else {
                        break;
                    };
                    match Self::send_all(fd, &front.data, &mut front.offset, &mut front.descriptors) {
                        Ok(true) => {
                            queue.pop_front();
                        }
                        Ok(false) => break,
                        Err(e) => {
                            if e != Errno::EPIPE && e != Errno::ECONNRESET {
                                log::warn!("multiprocess driver: sendmsg failed on fd {fd}: {e:?}");
                            }
                            drop(queue);
                            state.notify_error();
                            break;
                        }
                    }
                }
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
}

fn recv_fds_with_payload(sock_fd: libc::c_int, buffer: &mut [u8]) -> Result<(usize, Vec<libc::c_int>), Errno> {
    let mut iov = [IoSliceMut::new(buffer)];
    let mut cmsgspace = nix::cmsg_space!([libc::c_int; MAX_DESCRIPTORS_PER_MESSAGE]);

    let msg = recvmsg::<()>(sock_fd, &mut iov, Some(&mut cmsgspace), MsgFlags::MSG_CMSG_CLOEXEC)?;

    // If ancillary data is truncated, we may have silently lost descriptors.
    if msg.flags.contains(MsgFlags::MSG_CTRUNC) {
        return Err(Errno::EMSGSIZE);
    }

    let mut fds = Vec::new();
    for cmsg in msg.cmsgs() {
        if let ControlMessageOwned::ScmRights(rights) = cmsg {
            fds.extend(rights);
        }
    }
    Ok((msg.bytes, fds))
}

impl Driver for MultiprocessDriver {
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
        // The serializability of any object depends on the transport.
        let Some(object) = self.get(handle) else {
            return SerializeOutcome::Error(IpczResult::InvalidArgument);
        };
        if !matches!(&*guard(&object), Object::Transport(_) | Object::Memory(_)) {
            return SerializeOutcome::Error(IpczResult::InvalidArgument);
        }

        if !transport.is_valid() {
            return SerializeOutcome::NeedsTransport;
        }

        let dimensions = SerializedDimensions {
            num_bytes: SERIALIZED_HEADER_BYTES,
            num_driver_handles: 1,
        };
        let Some(SerializeBuffers { data, handles }) = buffers else {
            return SerializeOutcome::Dimensions(dimensions);
        };
        if data.len() < SERIALIZED_HEADER_BYTES || handles.is_empty() {
            return SerializeOutcome::Dimensions(dimensions);
        }

        // Extract the descriptor atomically with the state checks. The handle is
        // only consumed once serialization is certain to succeed.
        let (ty, size, fd) = {
            let mut inner = guard(&object);
            match &mut *inner {
                Object::Transport(t) => {
                    if t.state.is_some() {
                        return SerializeOutcome::Error(IpczResult::FailedPrecondition);
                    }
                    match t.fd.take().and_then(|fd| Arc::try_unwrap(fd).ok()) {
                        Some(fd) => (ObjectType::Transport, 0u32, fd),
                        None => return SerializeOutcome::Error(IpczResult::FailedPrecondition),
                    }
                }
                Object::Memory(m) => match m.fd.take() {
                    Some(fd) => (ObjectType::Memory, u32::try_from(m.size).unwrap_or(u32::MAX), fd),
                    None => return SerializeOutcome::Error(IpczResult::FailedPrecondition),
                },
                _ => return SerializeOutcome::Error(IpczResult::InvalidArgument),
            }
        };

        // On success, ipcz relinquishes `handle` and will no longer refer to it.
        self.take(handle);

        write_serialized_header(data, ty, size);
        handles[0] = self.make_fd_object(fd);
        SerializeOutcome::Serialized
    }

    fn deserialize(
        &self,
        data: &[u8],
        driver_handles: &[DriverHandle],
        _transport: DriverHandle,
    ) -> Result<DriverHandle, IpczResult> {
        if driver_handles.len() != 1 || data.len() < SERIALIZED_HEADER_BYTES {
            return Err(IpczResult::InvalidArgument);
        }

        let (ty, size) = read_serialized_header(data);
        let wrapped = self.take(driver_handles[0]).ok_or(IpczResult::InvalidArgument)?;
        let fd = match &mut *guard(&wrapped) {
            Object::FileDescriptor(w) => w.fd.take(),
            _ => None,
        }
        .ok_or(IpczResult::InvalidArgument)?;

        match ty {
            0 => Ok(self.transport_from_owned_fd(fd)),
            1 => Ok(self.insert(Object::Memory(Memory {
                fd: Some(fd),
                size: size as usize,
            }))),
            _ => Err(IpczResult::InvalidArgument),
        }
    }

    fn create_transports(
        &self,
        _transport0: DriverHandle,
        _transport1: DriverHandle,
    ) -> Result<(DriverHandle, DriverHandle), IpczResult> {
        let (fd0, fd1) =
            socketpair(AddressFamily::Unix, SockType::Stream, None, SockFlag::SOCK_CLOEXEC).map_err(errno_to_result)?;
        set_nonblocking(fd0.as_raw_fd()).map_err(errno_to_result)?;
        set_nonblocking(fd1.as_raw_fd()).map_err(errno_to_result)?;
        Ok((self.transport_from_owned_fd(fd0), self.transport_from_owned_fd(fd1)))
    }

    fn activate_transport(&self, transport: DriverHandle, handler: Arc<dyn TransportActivityHandler>) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };

        let (notify_receiver, notify_sender) = match create_nonblocking_pipe() {
            Ok(pipe) => pipe,
            Err(e) => return errno_to_result(e),
        };

        let state = Arc::new(TransportState {
            active: AtomicBool::new(true),
            io_thread_done: AtomicBool::new(false),
            deactivated_sent: AtomicBool::new(false),
            handler: Mutex::new(Some(handler)),
            outgoing: Mutex::new(VecDeque::new()),
        });

        let socket = {
            let mut inner = guard(&object);
            let Object::Transport(t) = &mut *inner else {
                return IpczResult::InvalidArgument;
            };
            if t.state.is_some() {
                return IpczResult::InvalidArgument;
            }
            let Some(socket) = t.fd.clone() else {
                return IpczResult::InvalidArgument;
            };
            t.state = Some(state.clone());
            t.notify_sender = Some(notify_sender);
            socket
        };

        // The I/O thread needs to make new driver objects for incoming
        // descriptors. Rebuild a shared handle to ourselves from the table.
        let driver = self.shared();
        thread::spawn(move || MultiprocessDriver::run_io_thread(driver, state, socket, notify_receiver));
        IpczResult::Ok
    }

    fn deactivate_transport(&self, transport: DriverHandle) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };
        let (state, notify_fd) = {
            let inner = guard(&object);
            let Object::Transport(t) = &*inner else {
                return IpczResult::InvalidArgument;
            };
            (t.state.clone(), t.notify_sender.as_ref().map(|fd| fd.as_raw_fd()))
        };

        let Some(state) = state else {
            return IpczResult::Ok;
        };

        state.active.store(false, Ordering::SeqCst);
        if let Some(notify_fd) = notify_fd {
            // SAFETY: `notify_fd` refers to a live fd owned by the transport.
            let _ = write(unsafe { BorrowedFd::borrow_raw(notify_fd) }, &[1]);
        }

        // If the I/O thread already exited (e.g. after signaling an error),
        // ensure we still emit a final deactivation notification.
        if state.io_thread_done.load(Ordering::SeqCst) {
            state.send_deactivated_once();
        }

        if let Object::Transport(t) = &mut *guard(&object) {
            t.notify_sender = None;
        }
        IpczResult::Ok
    }

    fn transmit(&self, transport: DriverHandle, data: &[u8], driver_handles: &[DriverHandle]) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };
        let (state, socket, notify_fd) = {
            let inner = guard(&object);
            let Object::Transport(t) = &*inner else {
                return IpczResult::InvalidArgument;
            };
            let Some(socket) = t.fd.clone() else {
                return IpczResult::InvalidArgument;
            };
            (
                t.state.clone(),
                socket,
                t.notify_sender.as_ref().map(|fd| fd.as_raw_fd()),
            )
        };

        let mut owned_fds = Vec::with_capacity(driver_handles.len());
        for &handle in driver_handles {
            let Some(wrapped) = self.get(handle) else {
                return IpczResult::InvalidArgument;
            };
            let fd = match &mut *guard(&wrapped) {
                Object::FileDescriptor(w) => w.fd.take(),
                _ => None,
            };
            let Some(fd) = fd else {
                return IpczResult::InvalidArgument;
            };
            // ipcz relinquishes transmitted driver handles on success.
            self.take(handle);
            owned_fds.push(fd);
        }

        // A deactivated transport discards outgoing transmissions, closing any
        // descriptors attached to them.
        if state.as_ref().is_some_and(|state| !state.active.load(Ordering::SeqCst)) {
            return IpczResult::Ok;
        }

        let mut buf = vec![0u8; TRANSPORT_HEADER_BYTES];
        write_transport_header(
            &mut buf,
            u32::try_from(TRANSPORT_HEADER_BYTES + data.len()).unwrap_or(u32::MAX),
            u32::try_from(owned_fds.len()).unwrap_or(u32::MAX),
        );
        buf.extend_from_slice(data);

        match state {
            Some(state) => {
                // Try to send synchronously, as long as nothing is queued ahead
                // of this message, so the data is in the kernel's hands by the
                // time we return (the peer can then still observe it if this
                // process exits right away). Whatever doesn't fit is queued for
                // the I/O thread.
                let mut queue = guard(&state.outgoing);
                let mut offset = 0;
                if queue.is_empty() {
                    match Self::send_all(socket.as_raw_fd(), &buf, &mut offset, &mut owned_fds) {
                        Ok(true) => return IpczResult::Ok,
                        Ok(false) => {}
                        Err(_) => return IpczResult::Ok,
                    }
                }
                queue.push_back(DeferredMessage {
                    data: buf,
                    offset,
                    descriptors: owned_fds,
                });
                drop(queue);
                if let Some(notify_fd) = notify_fd {
                    // SAFETY: `notify_fd` refers to a live fd owned by the
                    // transport.
                    let _ = write(unsafe { BorrowedFd::borrow_raw(notify_fd) }, &[1]);
                }
                IpczResult::Ok
            }
            None => {
                // ipcz may transmit before activating the transport. In that
                // case we must still send reliably.
                let mut offset = 0;
                loop {
                    match Self::send_all(socket.as_raw_fd(), &buf, &mut offset, &mut owned_fds) {
                        Ok(true) => return IpczResult::Ok,
                        Ok(false) => thread::yield_now(),
                        Err(e) => return errno_to_result(e),
                    }
                }
            }
        }
    }

    fn report_bad_transport_activity(&self, _transport: DriverHandle, _context: usize) -> IpczResult {
        IpczResult::Ok
    }

    fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult> {
        let memfd = MemfdOptions::default()
            .create("ipcz")
            .map_err(|_| IpczResult::Unknown)?;
        let file = memfd.into_file();
        file.set_len(num_bytes as u64).map_err(|_| IpczResult::Unknown)?;
        // SAFETY: `into_raw_fd()` relinquishes ownership of a valid descriptor.
        let fd = unsafe { OwnedFd::from_raw_fd(file.into_raw_fd()) };
        Ok(self.insert(Object::Memory(Memory {
            fd: Some(fd),
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
        let (dup_fd, size) = match &*guard(&object) {
            Object::Memory(m) => {
                let fd = m.fd.as_ref().ok_or(IpczResult::FailedPrecondition)?;
                (dup(fd.as_raw_fd()).map_err(errno_to_result)?, m.size)
            }
            _ => return Err(IpczResult::InvalidArgument),
        };
        // SAFETY: `dup()` returned a new descriptor we own.
        let fd = unsafe { OwnedFd::from_raw_fd(dup_fd) };
        Ok(self.insert(Object::Memory(Memory { fd: Some(fd), size })))
    }

    fn map_shared_memory(&self, driver_memory: DriverHandle) -> Result<MappedMemory, IpczResult> {
        let object = self.get(driver_memory).ok_or(IpczResult::InvalidArgument)?;
        let (len, raw_fd) = match &*guard(&object) {
            Object::Memory(m) => {
                let fd = m.fd.as_ref().ok_or(IpczResult::FailedPrecondition)?;
                (m.size, fd.as_raw_fd())
            }
            _ => return Err(IpczResult::InvalidArgument),
        };
        let len = NonZeroUsize::new(len).ok_or(IpczResult::InvalidArgument)?;

        // SAFETY: `raw_fd` is a live memory fd of at least `len` bytes.
        let addr = unsafe {
            mmap(
                None,
                len,
                ProtFlags::PROT_READ | ProtFlags::PROT_WRITE,
                MapFlags::MAP_SHARED,
                BorrowedFd::borrow_raw(raw_fd),
                0,
            )
        }
        .map_err(errno_to_result)?;

        Ok(MappedMemory {
            address: addr.as_ptr().cast(),
            mapping: self.insert(Object::Mapping(Mapping { addr, len })),
        })
    }

    fn generate_random_bytes(&self, buffer: &mut [u8]) -> IpczResult {
        match getrandom::getrandom(buffer) {
            Ok(()) => IpczResult::Ok,
            Err(_) => IpczResult::Unknown,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::INVALID_DRIVER_HANDLE;
    use crate::core::testing::transport_receiver::{Notification, TransportReceiver};
    use std::sync::atomic::AtomicU32;

    const MESSAGE: &[u8] = b"Hello, world!";

    fn transports(driver: &MultiprocessDriver) -> (DriverHandle, DriverHandle) {
        driver
            .create_transports(INVALID_DRIVER_HANDLE, INVALID_DRIVER_HANDLE)
            .unwrap()
    }

    /// Deactivates `transport` and waits for the driver to finalize that.
    fn deactivate_sync(driver: &MultiprocessDriver, transport: DriverHandle, deactivated: &Notification) {
        assert_eq!(IpczResult::Ok, driver.deactivate_transport(transport));
        deactivated.wait();
    }

    /// Activates `transport` with a receiver which signals the returned
    /// notification once the transport is deactivated.
    fn activate(driver: &MultiprocessDriver, transport: DriverHandle, receiver: TransportReceiver) -> Notification {
        let deactivated = Notification::default();
        let signal = deactivated.clone();
        let receiver = receiver.on_deactivate(move || signal.notify()).build();
        assert_eq!(IpczResult::Ok, driver.activate_transport(transport, receiver));
        deactivated
    }

    /// Activates `transport` expecting exactly `MESSAGE`, with no handles, and
    /// returns notifications for its receipt and for deactivation.
    fn activate_expecting_message(
        driver: &MultiprocessDriver,
        transport: DriverHandle,
    ) -> (Notification, Notification) {
        let received = Notification::default();
        let signal = received.clone();
        let deactivated = activate(
            driver,
            transport,
            TransportReceiver::default().on_message(move |message| {
                assert_eq!(MESSAGE, message.data);
                signal.notify();
                IpczResult::Ok
            }),
        );
        (received, deactivated)
    }

    #[test]
    fn read_write() {
        let driver = MultiprocessDriver::new();
        let (a, b) = transports(&driver);

        let (received, deactivated) = activate_expecting_message(&driver, b);
        assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));

        received.wait();
        deactivate_sync(&driver, b, &deactivated);
    }

    #[test]
    fn disconnect() {
        let driver = MultiprocessDriver::new();
        let (a, b) = transports(&driver);

        let received_message = Arc::new(AtomicBool::new(false));
        let errored = Notification::default();
        let deactivated = {
            let (received_message, errored) = (received_message.clone(), errored.clone());
            activate(
                &driver,
                b,
                TransportReceiver::default()
                    .on_message(move |_| {
                        received_message.store(true, Ordering::SeqCst);
                        IpczResult::Ok
                    })
                    .on_error(move || errored.notify()),
            )
        };

        assert_eq!(IpczResult::Ok, driver.close(a));

        errored.wait();
        deactivate_sync(&driver, b, &deactivated);
        assert!(!received_message.load(Ordering::SeqCst));
    }

    #[test]
    fn flood() {
        // Smoke test to throw a very large number of messages at a transport,
        // to exercise any queueing behavior that might be implemented.
        const NUM_MESSAGES: u32 = 25_000;

        // Every message sent is filled with this many u32 values, all reflecting
        // the index of the message within the sequence.
        const MESSAGE_NUM_VALUES: usize = 256;

        let message_bytes = |value: u32| -> Vec<u8> {
            [value; MESSAGE_NUM_VALUES]
                .iter()
                .flat_map(|v| v.to_ne_bytes())
                .collect()
        };

        let driver = MultiprocessDriver::new();
        let (a, b) = transports(&driver);

        let finished = Notification::default();
        let next_expected_value = Arc::new(AtomicU32::new(0));
        let deactivated_b = {
            let (finished, next_expected_value) = (finished.clone(), next_expected_value.clone());
            activate(
                &driver,
                b,
                TransportReceiver::default().on_message(move |message| {
                    // Make sure messages arrive in the order they were sent.
                    let expected = next_expected_value.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(message_bytes(expected), message.data);

                    // Finish only once the last expected message is received.
                    if expected + 1 == NUM_MESSAGES {
                        finished.notify();
                    }
                    IpczResult::Ok
                }),
            )
        };
        let deactivated_a = activate(&driver, a, TransportReceiver::default());

        // Spam, spam, spam, spam, spam.
        (0..NUM_MESSAGES).for_each(|i| assert_eq!(IpczResult::Ok, driver.transmit(a, &message_bytes(i), &[])));

        finished.wait();
        deactivate_sync(&driver, b, &deactivated_b);
        deactivate_sync(&driver, a, &deactivated_a);
    }

    #[test]
    fn destroy_from_io_thread() {
        let driver = MultiprocessDriver::new();
        let (a, b) = transports(&driver);

        // Closing `b` from within its own error notification, which runs on its
        // I/O thread, must not deadlock or try to join that thread.
        let destruction_done = Notification::default();
        let receiver = {
            let (driver, destruction_done) = (driver.clone(), destruction_done.clone());
            TransportReceiver::default()
                .on_error(move || {
                    assert_eq!(IpczResult::Ok, driver.deactivate_transport(b));
                    assert_eq!(IpczResult::Ok, driver.close(b));
                })
                .on_deactivate(move || destruction_done.notify())
                .build()
        };
        assert_eq!(IpczResult::Ok, driver.activate_transport(b, receiver));

        // Closing `a` should elicit the error handler on b's I/O thread.
        assert_eq!(IpczResult::Ok, driver.close(a));

        destruction_done.wait();
    }

    #[test]
    fn serialize_and_deserialize() {
        // Basic smoke test to verify that a transport can be decomposed into its
        // underlying socket descriptor and then reconstructed from that.
        let driver = MultiprocessDriver::new();
        let (a, b) = transports(&driver);

        let fd = driver
            .take_transport_fd(b)
            .expect("an unactivated transport has a descriptor");
        let b = driver.transport_from_owned_fd(fd);

        let (received, deactivated) = activate_expecting_message(&driver, b);
        assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &[]));

        received.wait();
        deactivate_sync(&driver, b, &deactivated);
    }

    #[test]
    fn read_write_with_file_descriptor() {
        let driver = MultiprocessDriver::new();
        let (a, b) = transports(&driver);

        const MEMORY_MESSAGE: &[u8] = b"heckin memory chonk here";
        let memory = driver.allocate_shared_memory(MEMORY_MESSAGE.len()).unwrap();
        let mapped = driver.map_shared_memory(memory).unwrap();
        // SAFETY: the mapping is `MEMORY_MESSAGE.len()` writable bytes.
        unsafe { std::ptr::copy_nonoverlapping(MEMORY_MESSAGE.as_ptr(), mapped.address, MEMORY_MESSAGE.len()) };

        // Serialize the memory into a descriptor to transmit.
        let mut serialized = vec![0u8; SERIALIZED_HEADER_BYTES];
        let mut handles = [INVALID_DRIVER_HANDLE];
        assert_eq!(
            SerializeOutcome::Serialized,
            driver.serialize(
                memory,
                a,
                Some(SerializeBuffers {
                    data: &mut serialized,
                    handles: &mut handles,
                }),
            )
        );

        let received = Notification::default();
        let delivered = Arc::new(Mutex::new(Vec::new()));
        let deactivated = {
            let (received, delivered) = (received.clone(), delivered.clone());
            activate(
                &driver,
                b,
                TransportReceiver::default().on_message(move |message| {
                    assert_eq!(MESSAGE, message.data);
                    assert_eq!(1, message.handles.len());
                    *delivered.lock().unwrap() = message.handles;
                    received.notify();
                    IpczResult::Ok
                }),
            )
        };
        assert_eq!(IpczResult::Ok, driver.transmit(a, MESSAGE, &handles));

        received.wait();
        let delivered = delivered.lock().unwrap().clone();
        let memory = driver.deserialize(&serialized, &delivered, b).unwrap();
        assert_eq!(
            MEMORY_MESSAGE.len(),
            driver.get_shared_memory_info(memory).unwrap().region_num_bytes
        );
        let remapped = driver.map_shared_memory(memory).unwrap();
        // SAFETY: the mapping is `MEMORY_MESSAGE.len()` readable bytes.
        let contents = unsafe { std::slice::from_raw_parts(remapped.address, MEMORY_MESSAGE.len()) };
        assert_eq!(MEMORY_MESSAGE, contents);

        deactivate_sync(&driver, b, &deactivated);
    }

    /// Maps `a` and `b`, which must refer to the same memory, checking that
    /// they have distinct addresses but are backed by the same pages.
    fn check_shared(a: *mut u8, b: *mut u8) {
        assert_ne!(a, b);
        let (a, b) = (a.cast::<i32>(), b.cast::<i32>());
        // SAFETY: both point at live mappings of at least four bytes.
        unsafe {
            b.write_volatile(0);
            a.write_volatile(42);
            assert_eq!(42, b.read_volatile());
        }
    }

    #[test]
    fn memory_create_and_map() {
        let driver = MultiprocessDriver::new();
        let memory = driver.allocate_shared_memory(64).unwrap();

        // Each mapping should have a different base address, but they should be
        // backed by the same physical memory.
        let mapping0 = driver.map_shared_memory(memory).unwrap();
        let mapping1 = driver.map_shared_memory(memory).unwrap();
        check_shared(mapping0.address, mapping1.address);
    }

    #[test]
    fn memory_create_map_close() {
        let driver = MultiprocessDriver::new();
        let memory = driver.allocate_shared_memory(64).unwrap();
        let mapping0 = driver.map_shared_memory(memory).unwrap();
        let mapping1 = driver.map_shared_memory(memory).unwrap();

        // Even with the memory closed, the mappings should persist.
        assert_eq!(IpczResult::Ok, driver.close(memory));
        check_shared(mapping0.address, mapping1.address);
    }

    #[test]
    fn memory_create_clone_map_close() {
        let driver = MultiprocessDriver::new();
        let memory = driver.allocate_shared_memory(64).unwrap();
        let clone = driver.duplicate_shared_memory(memory).unwrap();
        let mapping0 = driver.map_shared_memory(memory).unwrap();
        let mapping1 = driver.map_shared_memory(clone).unwrap();

        assert_eq!(IpczResult::Ok, driver.close(memory));
        assert_eq!(IpczResult::Ok, driver.close(clone));
        check_shared(mapping0.address, mapping1.address);
    }

    #[test]
    fn send_deactivated() {
        let driver = MultiprocessDriver::new();
        let (a, b) = transports(&driver);

        // Activate and immediately deactivate the transport.
        let deactivated = activate(&driver, a, TransportReceiver::default());
        deactivate_sync(&driver, a, &deactivated);

        let (kept, attached) = socketpair(AddressFamily::Unix, SockType::Stream, None, SockFlag::SOCK_CLOEXEC).unwrap();

        // This driver discards outgoing transmissions once the transport has
        // been deactivated. The attached file descriptor must not leak as a
        // result of this transmit(); if it does, the poll below times out.
        let handle = driver.make_fd_object(attached);
        assert_eq!(IpczResult::Ok, driver.transmit(a, &[], &[handle]));

        // Wait for the other side to be closed by the driver: a closed stream
        // socket is readable, and reads zero bytes.
        let mut poll_fd = libc::pollfd {
            fd: kept.as_raw_fd(),
            events: libc::POLLIN as libc::c_short,
            revents: 0,
        };
        // SAFETY: `poll_fd` is a valid pollfd.
        assert_eq!(
            1,
            unsafe { libc::poll(&mut poll_fd, 1, 10_000) },
            "the descriptor leaked"
        );
        let mut byte = [0u8];
        assert_eq!(Ok(0), nix::unistd::read(kept.as_raw_fd(), &mut byte));

        assert_eq!(IpczResult::Ok, driver.close(a));
        assert_eq!(IpczResult::Ok, driver.close(b));
    }
}
