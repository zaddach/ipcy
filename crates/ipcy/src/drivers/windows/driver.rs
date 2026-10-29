//! A driver for talking to other processes on Windows over named pipes, like
//! Chromium's Windows channel.
//!
//! A transport is one end of an overlapped, byte-mode named pipe carrying
//! frames ([`super::wire`]). Pipes cannot carry handles, so handles
//! (transports and shared memory sections) travel as values inside the frame
//! and are moved between processes with `DuplicateHandle`, which needs a handle
//! to the process on the other side:
//!
//! - If we hold the peer's process handle ([`TransportConfig::remote_process`])
//!   we *push*: the handle is duplicated into the peer when transmitted, and
//!   the peer receives a ready-to-use value.
//! - Otherwise, if the peer holds ours ([`TransportConfig::peer_can_pull`]), we
//!   send our own handle value and the peer *pulls* it, duplicating it out of
//!   our process (closing the source) on deserialization.
//! - If neither, serialization fails with `NotTransmissible` and ipcz relays the
//!   object through a broker, which holds a process handle for each node it
//!   introduced. A broker therefore configures `remote_process` for its clients
//!   and clients set `peer_can_pull` for their broker.
//!
//! DEVIATION: none of this has been run on Windows yet (the development sandbox
//! is Linux); it is type-checked against `x86_64-pc-windows-gnu`, its framing is
//! unit tested everywhere, and the shared driver tests (see `drivers/mod.rs`)
//! exercise it on Windows CI.

use std::collections::HashMap;
use std::os::windows::io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle, RawHandle};
use std::ptr::{null, null_mut};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};
use std::thread;

use windows_sys::Win32::Foundation::{
    DUPLICATE_SAME_ACCESS, DuplicateHandle, ERROR_BROKEN_PIPE, ERROR_IO_PENDING, ERROR_NO_DATA,
    ERROR_OPERATION_ABORTED, ERROR_PIPE_CONNECTED, ERROR_PIPE_NOT_CONNECTED, FALSE, GetLastError, INVALID_HANDLE_VALUE,
    TRUE, WAIT_OBJECT_0,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OVERLAPPED, OPEN_EXISTING, PIPE_ACCESS_DUPLEX, ReadFile,
    WriteFile,
};
use windows_sys::Win32::System::IO::{CancelIoEx, GetOverlappedResult, OVERLAPPED};
use windows_sys::Win32::System::Memory::{
    CreateFileMappingW, FILE_MAP_ALL_ACCESS, MEMORY_MAPPED_VIEW_ADDRESS, MapViewOfFile, PAGE_READWRITE, UnmapViewOfFile,
};
use windows_sys::Win32::System::Pipes::{
    ConnectNamedPipe, CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE, PIPE_WAIT,
};
use windows_sys::Win32::System::Threading::{
    CreateEventW, GetCurrentProcess, GetCurrentProcessId, INFINITE, SetEvent, WaitForMultipleObjects,
};

use super::wire::{self, FLAG_PULL, FrameReader, HandleEntry};
use crate::core::driver::{
    Driver, DriverHandle, MappedMemory, RawMessage, SerializeBuffers, SerializeOutcome, SerializedDimensions,
    SharedMemoryInfo, TransportActivityHandler,
};
use crate::core::ipcz_result::IpczResult;
use crate::drivers::{ObjectType, SERIALIZED_OBJECT_BYTES, read_object_header, write_object_header};

const GENERIC_READ: u32 = 0x8000_0000;
const GENERIC_WRITE: u32 = 0x4000_0000;
const PIPE_BUFFER_BYTES: u32 = 64 * 1024;

fn guard<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn last_error_result() -> IpczResult {
    // SAFETY: GetLastError has no preconditions.
    let error = unsafe { GetLastError() };
    log::warn!("windows driver: Win32 error {error}");
    #[cfg(test)]
    eprintln!("Win32 error {error}");
    match error {
        // ERROR_ACCESS_DENIED
        5 => IpczResult::PermissionDenied,
        // ERROR_INVALID_HANDLE, ERROR_INVALID_PARAMETER
        6 | 87 => IpczResult::InvalidArgument,
        // ERROR_NOT_ENOUGH_MEMORY, ERROR_OUTOFMEMORY
        8 | 14 => IpczResult::ResourceExhausted,
        _ => IpczResult::Unknown,
    }
}

fn raw(handle: &OwnedHandle) -> RawHandle {
    handle.as_raw_handle()
}

/// What a transport knows about the process on its far end; see the module
/// documentation.
#[derive(Default)]
pub struct TransportConfig {
    /// A handle to the peer's process with `PROCESS_DUP_HANDLE` access.
    pub remote_process: Option<OwnedHandle>,
    /// Whether the peer holds a handle to *this* process (so it can pull).
    pub peer_can_pull: bool,
}

struct Transport {
    pipe: Arc<OwnedHandle>,
    remote_process: Option<Arc<OwnedHandle>>,
    peer_can_pull: bool,
    /// Serializes writers so frames never interleave.
    write_lock: Arc<Mutex<()>>,
    state: Option<Arc<TransportState>>,
}

struct TransportState {
    active: AtomicBool,
    io_thread_done: AtomicBool,
    deactivated_sent: AtomicBool,
    /// Signaled to wake the I/O thread out of its wait.
    stop_event: OwnedHandle,
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

struct Memory {
    section: OwnedHandle,
    size: usize,
}

struct Mapping(*mut core::ffi::c_void);

// SAFETY: a mapping is just an address owned by this object.
unsafe impl Send for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: the address came from a successful MapViewOfFile().
        unsafe { UnmapViewOfFile(MEMORY_MAPPED_VIEW_ADDRESS { Value: self.0 }) };
    }
}

/// How a serialized handle will reach the peer.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Delivery {
    Push,
    Pull,
}

/// A handle in transit as a driver handle.
enum InTransit {
    /// Produced by `serialize()`, not yet transmitted.
    Outgoing { handle: OwnedHandle, delivery: Delivery },
    /// Received and already valid in this process.
    Pushed(OwnedHandle),
    /// Received, but still in the sender's process.
    Pull(u64),
}

enum Object {
    Transport(Transport),
    Memory(Memory),
    Mapping(#[allow(dead_code)] Mapping),
    InTransit(Option<InTransit>),
}

type ObjectRef = Arc<Mutex<Object>>;

fn duplicate_within_process(handle: RawHandle) -> Result<OwnedHandle, IpczResult> {
    let mut dup = null_mut();
    // SAFETY: duplicates a handle we own into this same process.
    let ok = unsafe {
        DuplicateHandle(
            GetCurrentProcess(),
            handle,
            GetCurrentProcess(),
            &mut dup,
            0,
            FALSE,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if ok == 0 {
        return Err(last_error_result());
    }
    // SAFETY: DuplicateHandle returned a new handle we own.
    Ok(unsafe { OwnedHandle::from_raw_handle(dup) })
}

fn create_event(manual_reset: bool) -> Result<OwnedHandle, IpczResult> {
    // SAFETY: creates an unnamed event.
    let event = unsafe { CreateEventW(null(), i32::from(manual_reset), FALSE, null()) };
    if event.is_null() {
        return Err(last_error_result());
    }
    // SAFETY: CreateEventW returned a new handle we own.
    Ok(unsafe { OwnedHandle::from_raw_handle(event) })
}

/// Waits for an overlapped operation on `pipe` to finish. Returns the number of
/// bytes transferred, or the Win32 error.
fn finish_overlapped(pipe: RawHandle, overlapped: &mut OVERLAPPED) -> Result<u32, u32> {
    let mut transferred = 0u32;
    // SAFETY: `overlapped` belongs to an operation issued on `pipe`.
    if unsafe { GetOverlappedResult(pipe, overlapped, &mut transferred, TRUE) } == 0 {
        // SAFETY: no preconditions.
        return Err(unsafe { GetLastError() });
    }
    Ok(transferred)
}

/// Writes all of `buf` to `pipe`, blocking until it has been accepted.
fn write_all(pipe: RawHandle, mut buf: &[u8]) -> Result<(), u32> {
    let event = create_event(true).map_err(|_| 0u32)?;
    while !buf.is_empty() {
        // SAFETY: an all-zero OVERLAPPED is valid.
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        overlapped.hEvent = raw(&event);
        let mut written = 0u32;
        // SAFETY: `buf` and `overlapped` outlive the (waited-for) operation.
        let ok = unsafe {
            WriteFile(
                pipe,
                buf.as_ptr(),
                buf.len().min(u32::MAX as usize) as u32,
                &mut written,
                &mut overlapped,
            )
        };
        let transferred = if ok != 0 {
            written
        } else {
            // SAFETY: no preconditions.
            match unsafe { GetLastError() } {
                ERROR_IO_PENDING => finish_overlapped(pipe, &mut overlapped)?,
                error => return Err(error),
            }
        };
        buf = &buf[transferred as usize..];
    }
    Ok(())
}

/// A Windows named-pipe driver. See the module documentation.
pub struct WindowsDriver {
    weak_self: Weak<WindowsDriver>,
    next_handle: AtomicUsize,
    objects: Mutex<HashMap<usize, ObjectRef>>,
}

impl WindowsDriver {
    pub fn new() -> Arc<WindowsDriver> {
        Arc::new_cyclic(|weak_self| WindowsDriver {
            weak_self: weak_self.clone(),
            next_handle: AtomicUsize::new(1),
            objects: Mutex::new(HashMap::new()),
        })
    }

    fn shared(&self) -> Arc<WindowsDriver> {
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

    /// Creates a transport over `pipe`, which must be an overlapped, byte-mode
    /// named pipe end. The driver takes ownership of it.
    pub fn transport_from_pipe(&self, pipe: OwnedHandle, config: TransportConfig) -> DriverHandle {
        self.insert(Object::Transport(Transport {
            pipe: Arc::new(pipe),
            remote_process: config.remote_process.map(Arc::new),
            peer_can_pull: config.peer_can_pull,
            write_lock: Arc::new(Mutex::new(())),
            state: None,
        }))
    }

    /// Replaces what `transport` knows about its peer's process, e.g. once the
    /// process that received its other end has been spawned.
    pub fn configure_transport(&self, transport: DriverHandle, config: TransportConfig) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };
        match &mut *guard(&object) {
            Object::Transport(t) => {
                t.remote_process = config.remote_process.map(Arc::new);
                t.peer_can_pull = config.peer_can_pull;
                IpczResult::Ok
            }
            _ => IpczResult::InvalidArgument,
        }
    }

    /// Extracts the pipe from a transport that was never activated, e.g. to
    /// pass it to a child process. Fails if it is active or still shared.
    pub fn take_transport_pipe(&self, handle: DriverHandle) -> Option<OwnedHandle> {
        let object = self.take(handle)?;
        let Object::Transport(transport) = Arc::into_inner(object)?.into_inner().unwrap_or_else(|e| e.into_inner())
        else {
            return None;
        };
        if transport.state.is_some() {
            return None;
        }
        Arc::into_inner(transport.pipe)
    }

    /// Creates a connected pair of overlapped pipe ends in this process.
    fn create_pipe_pair(&self) -> Result<(OwnedHandle, OwnedHandle), IpczResult> {
        // Pipe names are global to the machine, so make them unique across
        // every driver in every process.
        static NEXT_PIPE: AtomicUsize = AtomicUsize::new(0);
        let mut nonce = [0u8; 8];
        getrandom::getrandom(&mut nonce).map_err(|_| IpczResult::Unknown)?;
        let name: Vec<u16> = format!(
            r"\\.\pipe\ipcy.{}.{}.{:016x}",
            // SAFETY: no preconditions.
            unsafe { GetCurrentProcessId() },
            NEXT_PIPE.fetch_add(1, Ordering::Relaxed),
            u64::from_ne_bytes(nonce)
        )
        .encode_utf16()
        .chain(Some(0))
        .collect();

        // SAFETY: `name` is NUL terminated.
        let server = unsafe {
            CreateNamedPipeW(
                name.as_ptr(),
                PIPE_ACCESS_DUPLEX | FILE_FLAG_FIRST_PIPE_INSTANCE | FILE_FLAG_OVERLAPPED,
                PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
                1,
                PIPE_BUFFER_BYTES,
                PIPE_BUFFER_BYTES,
                0,
                null(),
            )
        };
        if server == INVALID_HANDLE_VALUE {
            return Err(last_error_result());
        }
        // SAFETY: CreateNamedPipeW returned a new handle we own.
        let server = unsafe { OwnedHandle::from_raw_handle(server) };

        // A new pipe instance only accepts a client once ConnectNamedPipe() has
        // put it into the listening state, so start that (it completes when the
        // client below connects).
        // SAFETY: an all-zero OVERLAPPED is valid; the server end is overlapped.
        let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
        let connected = create_event(true)?;
        overlapped.hEvent = raw(&connected);
        // SAFETY: `overlapped` outlives the call and the wait below.
        let connect_pending = if unsafe { ConnectNamedPipe(raw(&server), &mut overlapped) } == 0 {
            // SAFETY: no preconditions.
            match unsafe { GetLastError() } {
                ERROR_IO_PENDING => true,
                ERROR_PIPE_CONNECTED => false,
                _ => return Err(last_error_result()),
            }
        } else {
            false
        };

        // SAFETY: `name` is NUL terminated.
        let client = unsafe {
            CreateFileW(
                name.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                0,
                null(),
                OPEN_EXISTING,
                FILE_FLAG_OVERLAPPED,
                null_mut(),
            )
        };
        if client == INVALID_HANDLE_VALUE {
            let error = last_error_result();
            if connect_pending {
                // SAFETY: cancels the connect issued above before `overlapped` goes away.
                unsafe { CancelIoEx(raw(&server), &overlapped) };
                let _ = finish_overlapped(raw(&server), &mut overlapped);
            }
            return Err(error);
        }
        // SAFETY: CreateFileW returned a new handle we own.
        let client = unsafe { OwnedHandle::from_raw_handle(client) };

        if connect_pending {
            finish_overlapped(raw(&server), &mut overlapped).map_err(|_| IpczResult::Unknown)?;
        }
        Ok((server, client))
    }

    fn run_io_thread(driver: Arc<WindowsDriver>, pipe: Arc<OwnedHandle>, state: Arc<TransportState>) {
        let Ok(read_event) = create_event(true) else {
            state.notify_error();
            state.io_thread_done.store(true, Ordering::SeqCst);
            return;
        };
        let mut reader = FrameReader::default();
        let mut buffer = vec![0u8; PIPE_BUFFER_BYTES as usize];

        'io: loop {
            if !state.active.load(Ordering::SeqCst) {
                break;
            }

            // SAFETY: an all-zero OVERLAPPED is valid.
            let mut overlapped: OVERLAPPED = unsafe { std::mem::zeroed() };
            overlapped.hEvent = raw(&read_event);
            let mut read = 0u32;
            // SAFETY: `buffer` and `overlapped` outlive the (waited-for) operation.
            let ok = unsafe {
                ReadFile(
                    raw(&pipe),
                    buffer.as_mut_ptr(),
                    buffer.len() as u32,
                    &mut read,
                    &mut overlapped,
                )
            };
            let received = if ok != 0 {
                read
            } else {
                // SAFETY: no preconditions.
                match unsafe { GetLastError() } {
                    ERROR_IO_PENDING => {
                        let events = [raw(&read_event), raw(&state.stop_event)];
                        // SAFETY: both events are live.
                        let signaled = unsafe { WaitForMultipleObjects(2, events.as_ptr(), FALSE, INFINITE) };
                        if signaled != WAIT_OBJECT_0 {
                            // Deactivated (or the wait failed): abandon the read
                            // and wait for the cancellation to complete so the
                            // buffer is no longer referenced.
                            // SAFETY: cancels the read issued above.
                            unsafe { CancelIoEx(raw(&pipe), &overlapped) };
                            let _ = finish_overlapped(raw(&pipe), &mut overlapped);
                            break 'io;
                        }
                        match finish_overlapped(raw(&pipe), &mut overlapped) {
                            Ok(n) => n,
                            Err(ERROR_BROKEN_PIPE | ERROR_PIPE_NOT_CONNECTED | ERROR_NO_DATA) => {
                                state.notify_error();
                                break 'io;
                            }
                            Err(ERROR_OPERATION_ABORTED) => break 'io,
                            Err(error) => {
                                log::warn!("windows driver: read failed: {error}");
                                state.notify_error();
                                break 'io;
                            }
                        }
                    }
                    ERROR_BROKEN_PIPE | ERROR_PIPE_NOT_CONNECTED | ERROR_NO_DATA => {
                        state.notify_error();
                        break 'io;
                    }
                    error => {
                        log::warn!("windows driver: read failed: {error}");
                        state.notify_error();
                        break 'io;
                    }
                }
            };

            reader.push(&buffer[..received as usize]);
            loop {
                let frame = match reader.next_frame() {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(error) => {
                        log::warn!("windows driver: {error}");
                        state.notify_error();
                        state.active.store(false, Ordering::SeqCst);
                        break 'io;
                    }
                };

                let handles: Vec<DriverHandle> = frame
                    .handles
                    .iter()
                    .map(|entry| {
                        let in_transit = if entry.flags & FLAG_PULL != 0 {
                            InTransit::Pull(entry.value)
                        } else {
                            // SAFETY: a pushed value is a handle the sender
                            // duplicated into this process for us.
                            InTransit::Pushed(unsafe {
                                OwnedHandle::from_raw_handle(entry.value as usize as RawHandle)
                            })
                        };
                        driver.insert(Object::InTransit(Some(in_transit)))
                    })
                    .collect();

                let Some(handler) = state.handler() else {
                    break 'io;
                };
                let result = handler.on_message(
                    RawMessage {
                        data: &frame.data,
                        handles: &handles,
                    },
                    DriverHandle::default(),
                );
                if result != IpczResult::Ok && result != IpczResult::Unimplemented {
                    state.notify_error();
                    state.active.store(false, Ordering::SeqCst);
                    break 'io;
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

    /// Looks up the process handle and pull capability of a carrier transport.
    fn carrier(&self, transport: DriverHandle) -> Option<(Option<Arc<OwnedHandle>>, bool)> {
        let object = self.get(transport)?;
        match &*guard(&object) {
            Object::Transport(t) => Some((t.remote_process.clone(), t.peer_can_pull)),
            _ => None,
        }
    }
}

impl Driver for WindowsDriver {
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

        // Whether the handle can reach the peer depends on the transport.
        if !transport.is_valid() {
            return SerializeOutcome::NeedsTransport;
        }
        let Some((remote_process, peer_can_pull)) = self.carrier(transport) else {
            return SerializeOutcome::Error(IpczResult::InvalidArgument);
        };
        let delivery = match (remote_process.is_some(), peer_can_pull) {
            (true, _) => Delivery::Push,
            (false, true) => Delivery::Pull,
            // Neither side can move handles to the other: have ipcz relay the
            // object through a broker.
            (false, false) => return SerializeOutcome::NotTransmissible,
        };

        let dimensions = SerializedDimensions {
            num_bytes: SERIALIZED_OBJECT_BYTES,
            num_driver_handles: 1,
        };
        let Some(SerializeBuffers { data, handles }) = buffers else {
            return SerializeOutcome::Dimensions(dimensions);
        };
        if data.len() < dimensions.num_bytes || handles.is_empty() {
            return SerializeOutcome::Dimensions(dimensions);
        }

        // Extract the handle atomically with the state checks; the object is
        // only consumed once serialization is certain to succeed.
        let (size, handle_to_send) = {
            let mut inner = guard(&object);
            match &mut *inner {
                Object::Transport(t) => {
                    if t.state.is_some() || Arc::strong_count(&t.pipe) != 1 {
                        return SerializeOutcome::Error(IpczResult::FailedPrecondition);
                    }
                    match duplicate_within_process(raw(&t.pipe)) {
                        Ok(dup) => (0u32, dup),
                        Err(error) => return SerializeOutcome::Error(error),
                    }
                }
                Object::Memory(m) => match duplicate_within_process(raw(&m.section)) {
                    Ok(dup) => (u32::try_from(m.size).unwrap_or(u32::MAX), dup),
                    Err(error) => return SerializeOutcome::Error(error),
                },
                _ => return SerializeOutcome::Error(IpczResult::InvalidArgument),
            }
        };

        // On success, ipcz relinquishes `handle` and will no longer refer to it.
        self.take(handle);

        write_object_header(data, ty, size);
        handles[0] = self.insert(Object::InTransit(Some(InTransit::Outgoing {
            handle: handle_to_send,
            delivery,
        })));
        SerializeOutcome::Serialized
    }

    fn deserialize(
        &self,
        data: &[u8],
        driver_handles: &[DriverHandle],
        transport: DriverHandle,
    ) -> Result<DriverHandle, IpczResult> {
        let (ty, size) = read_object_header(data).ok_or(IpczResult::InvalidArgument)?;
        let [handle] = *driver_handles else {
            return Err(IpczResult::InvalidArgument);
        };
        let object = self.take(handle).ok_or(IpczResult::InvalidArgument)?;
        let in_transit = match &mut *guard(&object) {
            Object::InTransit(in_transit) => in_transit.take(),
            _ => None,
        }
        .ok_or(IpczResult::InvalidArgument)?;

        let local = match in_transit {
            InTransit::Pushed(handle) => handle,
            InTransit::Pull(value) => {
                // Duplicate the handle out of the sender's process, closing it
                // there.
                let (remote_process, _) = self.carrier(transport).ok_or(IpczResult::InvalidArgument)?;
                let remote_process = remote_process.ok_or(IpczResult::PermissionDenied)?;
                let mut local = null_mut();
                // SAFETY: pulls `value` out of the process `remote_process`
                // refers to into this one.
                let ok = unsafe {
                    DuplicateHandle(
                        raw(&remote_process),
                        value as usize as RawHandle,
                        GetCurrentProcess(),
                        &mut local,
                        0,
                        FALSE,
                        DUPLICATE_SAME_ACCESS | windows_sys::Win32::Foundation::DUPLICATE_CLOSE_SOURCE,
                    )
                };
                if ok == 0 {
                    return Err(last_error_result());
                }
                // SAFETY: DuplicateHandle returned a new handle we own.
                unsafe { OwnedHandle::from_raw_handle(local) }
            }
            InTransit::Outgoing { .. } => return Err(IpczResult::InvalidArgument),
        };

        match ty {
            ObjectType::Transport => Ok(self.transport_from_pipe(local, TransportConfig::default())),
            ObjectType::Memory => Ok(self.insert(Object::Memory(Memory {
                section: local,
                size: size as usize,
            }))),
        }
    }

    fn create_transports(
        &self,
        _transport0: DriverHandle,
        _transport1: DriverHandle,
    ) -> Result<(DriverHandle, DriverHandle), IpczResult> {
        let (server, client) = self.create_pipe_pair()?;
        Ok((
            self.transport_from_pipe(server, TransportConfig::default()),
            self.transport_from_pipe(client, TransportConfig::default()),
        ))
    }

    fn activate_transport(&self, transport: DriverHandle, handler: Arc<dyn TransportActivityHandler>) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };
        let stop_event = match create_event(true) {
            Ok(event) => event,
            Err(error) => return error,
        };
        let state = Arc::new(TransportState {
            active: AtomicBool::new(true),
            io_thread_done: AtomicBool::new(false),
            deactivated_sent: AtomicBool::new(false),
            stop_event,
            handler: Mutex::new(Some(handler)),
        });

        let pipe = {
            let mut inner = guard(&object);
            let Object::Transport(t) = &mut *inner else {
                return IpczResult::InvalidArgument;
            };
            if t.state.is_some() {
                return IpczResult::InvalidArgument;
            }
            t.state = Some(state.clone());
            t.pipe.clone()
        };

        let driver = self.shared();
        thread::spawn(move || WindowsDriver::run_io_thread(driver, pipe, state));
        IpczResult::Ok
    }

    fn deactivate_transport(&self, transport: DriverHandle) -> IpczResult {
        let Some(object) = self.get(transport) else {
            return IpczResult::InvalidArgument;
        };
        let state = {
            let inner = guard(&object);
            let Object::Transport(t) = &*inner else {
                return IpczResult::InvalidArgument;
            };
            t.state.clone()
        };
        let Some(state) = state else {
            return IpczResult::Ok;
        };

        state.active.store(false, Ordering::SeqCst);
        // SAFETY: the stop event is live as long as `state`.
        unsafe { SetEvent(raw(&state.stop_event)) };

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
        let (pipe, remote_process, write_lock, state) = {
            let inner = guard(&object);
            let Object::Transport(t) = &*inner else {
                return IpczResult::InvalidArgument;
            };
            (
                t.pipe.clone(),
                t.remote_process.clone(),
                t.write_lock.clone(),
                t.state.clone(),
            )
        };

        let mut entries = Vec::with_capacity(driver_handles.len());
        for &handle in driver_handles {
            let Some(wrapped) = self.get(handle) else {
                return IpczResult::InvalidArgument;
            };
            let in_transit = match &mut *guard(&wrapped) {
                Object::InTransit(in_transit) => in_transit.take(),
                _ => None,
            };
            let Some(InTransit::Outgoing {
                handle: outgoing,
                delivery,
            }) = in_transit
            else {
                return IpczResult::InvalidArgument;
            };
            // ipcz relinquishes transmitted driver handles on success.
            self.take(handle);

            // A deactivated transport discards outgoing transmissions, closing
            // the handles attached to them.
            if state.as_ref().is_some_and(|state| !state.active.load(Ordering::SeqCst)) {
                continue;
            }

            match (delivery, &remote_process) {
                (Delivery::Pull, _) => entries.push(HandleEntry {
                    value: outgoing.into_raw_handle() as usize as u64,
                    flags: FLAG_PULL,
                }),
                (Delivery::Push, Some(remote_process)) => {
                    let mut pushed = null_mut();
                    // SAFETY: duplicates a handle we own into the peer's process.
                    let ok = unsafe {
                        DuplicateHandle(
                            GetCurrentProcess(),
                            raw(&outgoing),
                            raw(remote_process),
                            &mut pushed,
                            0,
                            FALSE,
                            DUPLICATE_SAME_ACCESS,
                        )
                    };
                    if ok == 0 {
                        return last_error_result();
                    }
                    // Our copy closes when `outgoing` drops.
                    entries.push(HandleEntry {
                        value: pushed as usize as u64,
                        flags: 0,
                    });
                }
                (Delivery::Push, None) => return IpczResult::FailedPrecondition,
            }
        }

        if state.as_ref().is_some_and(|state| !state.active.load(Ordering::SeqCst)) {
            return IpczResult::Ok;
        }

        let frame = wire::encode(&entries, data);
        let _writing = guard(&write_lock);
        match write_all(raw(&pipe), &frame) {
            Ok(()) => IpczResult::Ok,
            // The peer is gone; the read side reports that.
            Err(ERROR_BROKEN_PIPE | ERROR_NO_DATA | ERROR_PIPE_NOT_CONNECTED) => IpczResult::Ok,
            Err(error) => {
                log::warn!("windows driver: write failed: {error}");
                IpczResult::Unknown
            }
        }
    }

    fn report_bad_transport_activity(&self, _transport: DriverHandle, _context: usize) -> IpczResult {
        IpczResult::Ok
    }

    fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult> {
        let size = num_bytes as u64;
        // SAFETY: creates an unnamed pagefile-backed section.
        let section = unsafe {
            CreateFileMappingW(
                INVALID_HANDLE_VALUE,
                null(),
                PAGE_READWRITE,
                (size >> 32) as u32,
                size as u32,
                null(),
            )
        };
        if section.is_null() {
            return Err(last_error_result());
        }
        // SAFETY: CreateFileMappingW returned a new handle we own.
        let section = unsafe { OwnedHandle::from_raw_handle(section) };
        Ok(self.insert(Object::Memory(Memory {
            section,
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
        let (section, size) = match &*guard(&object) {
            Object::Memory(m) => (duplicate_within_process(raw(&m.section))?, m.size),
            _ => return Err(IpczResult::InvalidArgument),
        };
        Ok(self.insert(Object::Memory(Memory { section, size })))
    }

    fn map_shared_memory(&self, driver_memory: DriverHandle) -> Result<MappedMemory, IpczResult> {
        let object = self.get(driver_memory).ok_or(IpczResult::InvalidArgument)?;
        let (section, size) = match &*guard(&object) {
            Object::Memory(m) => (duplicate_within_process(raw(&m.section))?, m.size),
            _ => return Err(IpczResult::InvalidArgument),
        };
        // SAFETY: maps the whole section read/write; the view keeps the section
        // alive after `section` closes.
        let view = unsafe { MapViewOfFile(raw(&section), FILE_MAP_ALL_ACCESS, 0, 0, size) };
        if view.Value.is_null() {
            return Err(last_error_result());
        }
        Ok(MappedMemory {
            address: view.Value.cast(),
            mapping: self.insert(Object::Mapping(Mapping(view.Value))),
        })
    }

    fn generate_random_bytes(&self, buffer: &mut [u8]) -> IpczResult {
        match getrandom::getrandom(buffer) {
            Ok(()) => IpczResult::Ok,
            Err(_) => IpczResult::Unknown,
        }
    }
}
