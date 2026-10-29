//! A minimal in-process [`Driver`] backing shared memory with heap allocations.
//! It supports only the memory operations; anything transport-related reports
//! `Unimplemented`. Tests which need real memory (fragments, buffer pools, ...)
//! use this in place of upstream's sync reference driver, which is ported in
//! Phase 7.

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::driver::*;
use crate::core::ipcz_result::IpczResult;

/// Zero-initialized, 8-byte aligned memory with a stable address.
struct Region {
    words: Box<[UnsafeCell<u64>]>,
    num_bytes: usize,
}

// SAFETY: this models shared memory; users synchronize access themselves.
unsafe impl Send for Region {}
unsafe impl Sync for Region {}

impl Region {
    fn address(&self) -> *mut u8 {
        self.words.as_ptr() as *mut u8
    }
}

enum Entry {
    Memory(Arc<Region>),
    // Held only to keep the region alive while mapped.
    Mapping(#[allow(dead_code)] Arc<Region>),
}

#[derive(Default)]
pub struct HeapMemoryDriver {
    next_handle: AtomicUsize,
    entries: Mutex<HashMap<usize, Entry>>,
}

impl HeapMemoryDriver {
    pub fn new() -> Arc<HeapMemoryDriver> {
        Arc::new(HeapMemoryDriver::default())
    }

    pub fn as_driver(self: &Arc<Self>) -> Arc<dyn Driver> {
        self.clone()
    }

    /// The number of live driver objects, for leak checks.
    pub fn num_live_handles(&self) -> usize {
        self.entries.lock().unwrap().len()
    }

    fn insert(&self, entry: Entry) -> DriverHandle {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed) + 1;
        self.entries.lock().unwrap().insert(handle, entry);
        DriverHandle::new(handle)
    }

    fn memory(&self, handle: DriverHandle) -> Result<Arc<Region>, IpczResult> {
        match self.entries.lock().unwrap().get(&handle.value()) {
            Some(Entry::Memory(region)) => Ok(region.clone()),
            _ => Err(IpczResult::InvalidArgument),
        }
    }
}

impl Driver for HeapMemoryDriver {
    fn close(&self, handle: DriverHandle) -> IpczResult {
        match self.entries.lock().unwrap().remove(&handle.value()) {
            Some(_) => IpczResult::Ok,
            None => IpczResult::InvalidArgument,
        }
    }

    fn serialize(&self, _: DriverHandle, _: DriverHandle, _: Option<SerializeBuffers<'_>>) -> SerializeOutcome {
        SerializeOutcome::Error(IpczResult::InvalidArgument)
    }

    fn deserialize(&self, _: &[u8], _: &[DriverHandle], _: DriverHandle) -> Result<DriverHandle, IpczResult> {
        Err(IpczResult::Unimplemented)
    }

    fn create_transports(&self, _: DriverHandle, _: DriverHandle) -> Result<(DriverHandle, DriverHandle), IpczResult> {
        Err(IpczResult::Unimplemented)
    }

    fn activate_transport(&self, _: DriverHandle, _: Arc<dyn TransportActivityHandler>) -> IpczResult {
        IpczResult::Unimplemented
    }

    fn deactivate_transport(&self, _: DriverHandle) -> IpczResult {
        IpczResult::Unimplemented
    }

    fn transmit(&self, _: DriverHandle, _: &[u8], _: &[DriverHandle]) -> IpczResult {
        IpczResult::Unimplemented
    }

    fn report_bad_transport_activity(&self, _: DriverHandle, _: usize) -> IpczResult {
        IpczResult::Unimplemented
    }

    fn allocate_shared_memory(&self, num_bytes: usize) -> Result<DriverHandle, IpczResult> {
        let words = (0..num_bytes.div_ceil(8)).map(|_| UnsafeCell::new(0)).collect();
        Ok(self.insert(Entry::Memory(Arc::new(Region { words, num_bytes }))))
    }

    fn get_shared_memory_info(&self, driver_memory: DriverHandle) -> Result<SharedMemoryInfo, IpczResult> {
        self.memory(driver_memory).map(|region| SharedMemoryInfo {
            region_num_bytes: region.num_bytes,
        })
    }

    fn duplicate_shared_memory(&self, driver_memory: DriverHandle) -> Result<DriverHandle, IpczResult> {
        self.memory(driver_memory)
            .map(|region| self.insert(Entry::Memory(region)))
    }

    fn map_shared_memory(&self, driver_memory: DriverHandle) -> Result<MappedMemory, IpczResult> {
        let region = self.memory(driver_memory)?;
        let address = region.address();
        // The mapping keeps the region alive independently of the memory object.
        Ok(MappedMemory {
            address,
            mapping: self.insert(Entry::Mapping(region)),
        })
    }

    fn generate_random_bytes(&self, buffer: &mut [u8]) -> IpczResult {
        match getrandom::getrandom(buffer) {
            Ok(()) => IpczResult::Ok,
            Err(_) => IpczResult::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver_memory::DriverMemory;

    #[test]
    fn allocate_map_clone_and_close() {
        let driver = HeapMemoryDriver::new();
        let memory = DriverMemory::allocate(&driver.as_driver(), 20);
        assert!(memory.is_valid());
        assert_eq!(memory.size(), 20);

        let mapping = memory.map();
        assert!(mapping.is_valid());
        assert_eq!(mapping.address() as usize % 8, 0);
        unsafe { mapping.address().write(0x5a) };

        // A clone's mapping aliases the same bytes, and mappings outlive the
        // memory objects they were created from.
        let clone_mapping = memory.clone_memory().map();
        assert_eq!(unsafe { clone_mapping.address().read() }, 0x5a);
        drop(memory);
        assert_eq!(unsafe { mapping.address().read() }, 0x5a);

        drop((mapping, clone_mapping));
        assert_eq!(driver.num_live_handles(), 0);
    }
}
