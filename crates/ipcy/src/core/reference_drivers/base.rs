//! Port of `reference_drivers/single_process_reference_driver_base.{h,cc}` and
//! `object.{h,cc}`: driver objects (transports, memory, mappings) living in a
//! handle table, plus the memory-related and serialization operations shared
//! by all single-process reference drivers.
//!
//! DEVIATION: driver handles are keys into a per-driver table rather than
//! leaked pointers, so a stale handle is just an error.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::core::driver::{
    DriverHandle, MappedMemory, SerializeBuffers, SerializeOutcome, SerializedDimensions, SharedMemoryInfo,
};
use crate::core::ipcz_result::IpczResult;

/// Zero-initialized, 8-byte aligned memory with a stable address.
pub struct InProcessMemory {
    words: Box<[UnsafeCell<u64>]>,
    num_bytes: usize,
}

// SAFETY: this models shared memory; users synchronize access themselves.
unsafe impl Send for InProcessMemory {}
unsafe impl Sync for InProcessMemory {}

impl InProcessMemory {
    fn new(num_bytes: usize) -> Self {
        Self {
            words: (0..num_bytes.div_ceil(8)).map(|_| UnsafeCell::new(0)).collect(),
            num_bytes,
        }
    }

    fn address(&self) -> *mut u8 {
        self.words.as_ptr() as *mut u8
    }
}

/// A driver object which lives in the table. `T` is the driver's transport type.
pub enum Object<T> {
    Transport(Arc<T>),
    Memory(Arc<InProcessMemory>),
    // Held only to keep the memory alive while mapped.
    Mapping(#[allow(dead_code)] Arc<InProcessMemory>),
}

pub type BadTransportActivityCallback = Box<dyn Fn(DriverHandle, usize) + Send + Sync>;

/// The handle table and memory operations of a single-process reference driver.
pub struct DriverBase<T> {
    next_handle: AtomicUsize,
    objects: Mutex<HashMap<usize, Object<T>>>,
    bad_transport_activity_callback: Mutex<Option<BadTransportActivityCallback>>,
}

impl<T> Default for DriverBase<T> {
    fn default() -> Self {
        Self {
            next_handle: AtomicUsize::new(1),
            objects: Mutex::new(HashMap::new()),
            bad_transport_activity_callback: Mutex::new(None),
        }
    }
}

impl<T> DriverBase<T> {
    fn lock(&self) -> MutexGuard<'_, HashMap<usize, Object<T>>> {
        self.objects.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// The number of live driver objects, for leak checks.
    pub fn num_live_objects(&self) -> usize {
        self.lock().len()
    }

    pub fn insert(&self, object: Object<T>) -> DriverHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        self.lock().insert(handle, object);
        DriverHandle::new(handle)
    }

    /// Removes the object from the table, transferring its reference.
    pub fn take(&self, handle: DriverHandle) -> Option<Object<T>> {
        self.lock().remove(&handle.value())
    }

    pub fn transport(&self, handle: DriverHandle) -> Option<Arc<T>> {
        match self.lock().get(&handle.value()) {
            Some(Object::Transport(transport)) => Some(transport.clone()),
            _ => None,
        }
    }

    fn memory(&self, handle: DriverHandle) -> Result<Arc<InProcessMemory>, IpczResult> {
        match self.lock().get(&handle.value()) {
            Some(Object::Memory(memory)) => Ok(memory.clone()),
            _ => Err(IpczResult::InvalidArgument),
        }
    }

    /// Sets a callback invoked when ipcz reports bad transport activity.
    pub fn set_bad_transport_activity_callback(&self, callback: Option<BadTransportActivityCallback>) {
        *self
            .bad_transport_activity_callback
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = callback;
    }

    pub fn serialize(
        &self,
        handle: DriverHandle,
        buffers: Option<SerializeBuffers<'_>>,
    ) -> SerializeOutcome {
        if !self.lock().contains_key(&handle.value()) {
            return SerializeOutcome::Error(IpczResult::InvalidArgument);
        }

        // Since this is all in-process, all driver handles can be transmitted
        // as-is.
        match buffers {
            Some(SerializeBuffers { handles, .. }) if !handles.is_empty() => {
                handles[0] = handle;
                SerializeOutcome::Serialized
            }
            _ => SerializeOutcome::Dimensions(SerializedDimensions {
                num_bytes: 0,
                num_driver_handles: 1,
            }),
        }
    }

    pub fn deserialize(&self, data: &[u8], handles: &[DriverHandle]) -> Result<DriverHandle, IpczResult> {
        assert!(data.is_empty());
        assert_eq!(handles.len(), 1);
        Ok(handles[0])
    }

    pub fn report_bad_transport_activity(&self, transport: DriverHandle, context: usize) -> IpczResult {
        if let Some(callback) = self
            .bad_transport_activity_callback
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            callback(transport, context);
        }
        IpczResult::Ok
    }

    pub fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult> {
        Ok(self.insert(Object::Memory(Arc::new(InProcessMemory::new(num_bytes)))))
    }

    pub fn get_shared_memory_info(&self, driver_memory: DriverHandle) -> Result<SharedMemoryInfo, IpczResult> {
        self.memory(driver_memory).map(|memory| SharedMemoryInfo {
            region_num_bytes: memory.num_bytes,
        })
    }

    pub fn duplicate_shared_memory(&self, driver_memory: DriverHandle) -> Result<DriverHandle, IpczResult> {
        self.memory(driver_memory)
            .map(|memory| self.insert(Object::Memory(memory)))
    }

    pub fn map_shared_memory(&self, driver_memory: DriverHandle) -> Result<MappedMemory, IpczResult> {
        let memory = self.memory(driver_memory)?;
        let address = memory.address();
        Ok(MappedMemory {
            address,
            mapping: self.insert(Object::Mapping(memory)),
        })
    }

    pub fn generate_random_bytes(&self, buffer: &mut [u8]) -> IpczResult {
        match getrandom::getrandom(buffer) {
            Ok(()) => IpczResult::Ok,
            Err(_) => IpczResult::Internal,
        }
    }
}
