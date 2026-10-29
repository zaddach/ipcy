//! Port of `test/mock_driver.{h,cc}`: a strict, scripted [`Driver`] which lets
//! tests precisely introspect the driver API invocations made by ipcz.
//!
//! Expectations are matched by the primary handle argument (if any) and are
//! "once" unless added with `*_repeatedly`. Unexpected calls panic, and dropping
//! the mock with unsatisfied once-expectations panics, like a gmock
//! `StrictMock`.

use std::sync::{Arc, Mutex, MutexGuard};

use crate::core::driver::*;
use crate::core::ipcz_result::IpczResult;

struct Expect<F: ?Sized> {
    handle: Option<DriverHandle>,
    once: bool,
    f: Box<F>,
}

struct Expectations<F: ?Sized> {
    name: &'static str,
    list: Mutex<Vec<Expect<F>>>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl<F: ?Sized> Expectations<F> {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            list: Mutex::new(Vec::new()),
        }
    }

    fn add(&self, handle: Option<DriverHandle>, once: bool, f: Box<F>) {
        lock(&self.list).push(Expect { handle, once, f });
    }

    // Runs the first matching expectation without holding the list lock, so
    // that expectation bodies may re-enter the mock.
    fn call<R>(&self, handle: Option<DriverHandle>, invoke: impl FnOnce(&mut F) -> R) -> R {
        let mut taken = {
            let mut list = lock(&self.list);
            let index = list
                .iter()
                .position(|e| e.handle.is_none() || e.handle == handle)
                .unwrap_or_else(|| panic!("unexpected MockDriver::{}({handle:?})", self.name));
            list.remove(index)
        };
        let result = invoke(&mut taken.f);
        if !taken.once {
            lock(&self.list).push(taken);
        }
        result
    }

    fn unsatisfied(&self) -> usize {
        lock(&self.list).iter().filter(|e| e.once).count()
    }
}

type CloseFn = dyn FnMut(DriverHandle) -> IpczResult + Send;
type SerializeFn =
    dyn FnMut(DriverHandle, DriverHandle, Option<SerializeBuffers<'_>>) -> SerializeOutcome + Send;
type ActivateFn =
    dyn FnMut(DriverHandle, Arc<dyn TransportActivityHandler>) -> IpczResult + Send;
type DeactivateFn = dyn FnMut(DriverHandle) -> IpczResult + Send;
type TransmitFn = dyn FnMut(DriverHandle, &[u8], &[DriverHandle]) -> IpczResult + Send;
type DeserializeFn =
    dyn FnMut(&[u8], &[DriverHandle], DriverHandle) -> Result<DriverHandle, IpczResult> + Send;
type AllocateFn = dyn FnMut(usize) -> Result<DriverHandle, IpczResult> + Send;
type InfoFn = dyn FnMut(DriverHandle) -> Result<SharedMemoryInfo, IpczResult> + Send;
type HandleFn = dyn FnMut(DriverHandle) -> Result<DriverHandle, IpczResult> + Send;
type MapFn = dyn FnMut(DriverHandle) -> Result<MappedMemory, IpczResult> + Send;

pub struct MockDriver {
    close: Expectations<CloseFn>,
    serialize: Expectations<SerializeFn>,
    deserialize: Expectations<DeserializeFn>,
    activate: Expectations<ActivateFn>,
    deactivate: Expectations<DeactivateFn>,
    transmit: Expectations<TransmitFn>,
    allocate: Expectations<AllocateFn>,
    info: Expectations<InfoFn>,
    duplicate: Expectations<HandleFn>,
    map: Expectations<MapFn>,
}

impl MockDriver {
    pub fn new() -> Arc<MockDriver> {
        Arc::new(MockDriver {
            close: Expectations::new("close"),
            serialize: Expectations::new("serialize"),
            deserialize: Expectations::new("deserialize"),
            activate: Expectations::new("activate_transport"),
            deactivate: Expectations::new("deactivate_transport"),
            transmit: Expectations::new("transmit"),
            allocate: Expectations::new("allocate_shared_memory"),
            info: Expectations::new("get_shared_memory_info"),
            duplicate: Expectations::new("duplicate_shared_memory"),
            map: Expectations::new("map_shared_memory"),
        })
    }

    /// The mock as a `dyn Driver`, suitable for constructing `DriverObject`s.
    pub fn as_driver(self: &Arc<Self>) -> Arc<dyn Driver> {
        self.clone()
    }

    pub fn expect_close(&self, handle: DriverHandle) {
        self.close.add(Some(handle), true, Box::new(|_| IpczResult::Ok));
    }

    pub fn expect_serialize(
        &self,
        handle: DriverHandle,
        f: impl FnMut(DriverHandle, DriverHandle, Option<SerializeBuffers<'_>>) -> SerializeOutcome
        + Send
        + 'static,
    ) {
        self.serialize.add(Some(handle), true, Box::new(f));
    }

    #[allow(dead_code)]
    pub fn expect_serialize_repeatedly(
        &self,
        handle: DriverHandle,
        f: impl FnMut(DriverHandle, DriverHandle, Option<SerializeBuffers<'_>>) -> SerializeOutcome
        + Send
        + 'static,
    ) {
        self.serialize.add(Some(handle), false, Box::new(f));
    }

    /// Like `expect_serialize_repeatedly()`, but matching any object handle.
    pub fn expect_serialize_any_repeatedly(
        &self,
        f: impl FnMut(DriverHandle, DriverHandle, Option<SerializeBuffers<'_>>) -> SerializeOutcome
        + Send
        + 'static,
    ) {
        self.serialize.add(None, false, Box::new(f));
    }

    /// Expects any number of `deserialize()` calls for objects received on
    /// `transport`.
    pub fn expect_deserialize_repeatedly(
        &self,
        transport: DriverHandle,
        f: impl FnMut(&[u8], &[DriverHandle], DriverHandle) -> Result<DriverHandle, IpczResult> + Send + 'static,
    ) {
        self.deserialize.add(Some(transport), false, Box::new(f));
    }

    pub fn expect_transmit_repeatedly(
        &self,
        transport: DriverHandle,
        f: impl FnMut(DriverHandle, &[u8], &[DriverHandle]) -> IpczResult + Send + 'static,
    ) {
        self.transmit.add(Some(transport), false, Box::new(f));
    }

    pub fn expect_activate(
        &self,
        transport: DriverHandle,
        f: impl FnMut(DriverHandle, Arc<dyn TransportActivityHandler>) -> IpczResult + Send + 'static,
    ) {
        self.activate.add(Some(transport), true, Box::new(f));
    }

    pub fn expect_deactivate(&self, transport: DriverHandle, result: IpczResult) {
        self.deactivate.add(Some(transport), true, Box::new(move |_| result));
    }

    pub fn expect_transmit(
        &self,
        transport: DriverHandle,
        f: impl FnMut(DriverHandle, &[u8], &[DriverHandle]) -> IpczResult + Send + 'static,
    ) {
        self.transmit.add(Some(transport), true, Box::new(f));
    }

    pub fn expect_allocate(
        &self,
        f: impl FnMut(usize) -> Result<DriverHandle, IpczResult> + Send + 'static,
    ) {
        self.allocate.add(None, true, Box::new(f));
    }

    pub fn expect_info(&self, memory: DriverHandle, result: Result<SharedMemoryInfo, IpczResult>) {
        self.info.add(Some(memory), true, Box::new(move |_| result));
    }

    pub fn expect_duplicate(&self, memory: DriverHandle, result: Result<DriverHandle, IpczResult>) {
        self.duplicate.add(Some(memory), true, Box::new(move |_| result));
    }

    pub fn expect_map(&self, memory: DriverHandle, result: Result<MappedMemory, IpczResult>) {
        self.map.add(Some(memory), true, Box::new(move |_| result));
    }
}

impl Drop for MockDriver {
    fn drop(&mut self) {
        if std::thread::panicking() {
            return;
        }
        let unsatisfied = [
            ("close", self.close.unsatisfied()),
            ("serialize", self.serialize.unsatisfied()),
            ("deserialize", self.deserialize.unsatisfied()),
            ("activate_transport", self.activate.unsatisfied()),
            ("deactivate_transport", self.deactivate.unsatisfied()),
            ("transmit", self.transmit.unsatisfied()),
            ("allocate_shared_memory", self.allocate.unsatisfied()),
            ("get_shared_memory_info", self.info.unsatisfied()),
            ("duplicate_shared_memory", self.duplicate.unsatisfied()),
            ("map_shared_memory", self.map.unsatisfied()),
        ];
        let missing = unsatisfied
            .iter()
            .filter(|(_, n)| *n > 0)
            .map(|(name, n)| format!("{name} x{n}"))
            .collect::<Vec<_>>();
        assert!(missing.is_empty(), "unsatisfied MockDriver expectations: {missing:?}");
    }
}

impl Driver for MockDriver {
    fn close(&self, handle: DriverHandle) -> IpczResult {
        self.close.call(Some(handle), |f| f(handle))
    }

    fn serialize(
        &self,
        handle: DriverHandle,
        transport: DriverHandle,
        buffers: Option<SerializeBuffers<'_>>,
    ) -> SerializeOutcome {
        self.serialize.call(Some(handle), |f| f(handle, transport, buffers))
    }

    fn deserialize(
        &self,
        data: &[u8],
        handles: &[DriverHandle],
        transport: DriverHandle,
    ) -> Result<DriverHandle, IpczResult> {
        self.deserialize.call(Some(transport), |f| f(data, handles, transport))
    }

    fn create_transports(&self, _: DriverHandle, _: DriverHandle) -> Result<(DriverHandle, DriverHandle), IpczResult> {
        panic!("unexpected MockDriver::create_transports");
    }

    fn activate_transport(&self, transport: DriverHandle, handler: Arc<dyn TransportActivityHandler>) -> IpczResult {
        self.activate.call(Some(transport), |f| f(transport, handler))
    }

    fn deactivate_transport(&self, transport: DriverHandle) -> IpczResult {
        self.deactivate.call(Some(transport), |f| f(transport))
    }

    fn transmit(&self, transport: DriverHandle, data: &[u8], handles: &[DriverHandle]) -> IpczResult {
        self.transmit.call(Some(transport), |f| f(transport, data, handles))
    }

    fn report_bad_transport_activity(&self, _: DriverHandle, _: usize) -> IpczResult {
        panic!("unexpected MockDriver::report_bad_transport_activity");
    }

    fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult> {
        self.allocate.call(None, |f| f(num_bytes))
    }

    fn get_shared_memory_info(&self, memory: DriverHandle) -> Result<SharedMemoryInfo, IpczResult> {
        self.info.call(Some(memory), |f| f(memory))
    }

    fn duplicate_shared_memory(&self, memory: DriverHandle) -> Result<DriverHandle, IpczResult> {
        self.duplicate.call(Some(memory), |f| f(memory))
    }

    fn map_shared_memory(&self, memory: DriverHandle) -> Result<MappedMemory, IpczResult> {
        self.map.call(Some(memory), |f| f(memory))
    }

    fn generate_random_bytes(&self, _: &mut [u8]) -> IpczResult {
        panic!("unexpected MockDriver::generate_random_bytes");
    }
}
