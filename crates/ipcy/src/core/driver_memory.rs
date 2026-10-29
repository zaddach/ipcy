//! Port of `ipcz/driver_memory.{h,cc}`.

use std::sync::Arc;

use super::driver::Driver;
use super::driver_memory_mapping::DriverMemoryMapping;
use super::driver_object::DriverObject;

/// Scoped wrapper around a shared memory region allocated and manipulated
/// through an ipcz driver.
#[derive(Debug, Default)]
pub struct DriverMemory {
    memory: DriverObject,
    size: usize,
}

impl DriverMemory {
    /// Takes ownership of an existing driver memory object. If the driver can't
    /// describe it as shared memory, the result is invalid.
    pub fn from_object(memory: DriverObject) -> Self {
        let info = memory
            .driver()
            .filter(|_| memory.is_valid())
            .and_then(|driver| driver.get_shared_memory_info(memory.handle()).ok());
        match info {
            Some(info) => Self {
                memory,
                size: info.region_num_bytes,
            },
            None => Self::default(),
        }
    }

    /// Asks the driver to allocate a new shared memory region of at least
    /// `num_bytes` in size. As upstream, `size()` reports `num_bytes` even if
    /// the allocation failed, in which case the result is invalid.
    pub fn allocate(driver: &Arc<dyn Driver>, num_bytes: usize) -> Self {
        assert!(num_bytes > 0);
        let memory = match driver.allocate_shared_memory(num_bytes) {
            Ok(handle) => DriverObject::new(driver.clone(), handle),
            Err(_) => DriverObject::default(),
        };
        Self {
            memory,
            size: num_bytes,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.memory.is_valid()
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn driver_object(&mut self) -> &mut DriverObject {
        &mut self.memory
    }

    pub fn take_driver_object(&mut self) -> DriverObject {
        std::mem::take(&mut self.memory)
    }

    /// Asks the driver to clone this memory object and return a new one which
    /// references the same underlying memory region. Returns an invalid memory
    /// object on failure.
    pub fn clone_memory(&self) -> DriverMemory {
        assert!(self.is_valid());
        let driver = self.memory.driver().expect("valid memory has a driver");
        match driver.duplicate_shared_memory(self.memory.handle()) {
            Ok(handle) => DriverMemory::from_object(DriverObject::new(driver.clone(), handle)),
            Err(_) => DriverMemory::default(),
        }
    }

    /// Asks the driver to map this memory object into the process's address
    /// space and returns a scoper to control the mapping's lifetime. Returns an
    /// invalid mapping if mapping fails or if called on an invalid DriverMemory.
    pub fn map(&self) -> DriverMemoryMapping {
        let Some(driver) = self.memory.driver().filter(|_| self.is_valid()) else {
            return DriverMemoryMapping::default();
        };
        match driver.map_shared_memory(self.memory.handle()) {
            Ok(mapped) => {
                DriverMemoryMapping::new(driver.clone(), mapped.mapping, mapped.address, self.size)
            }
            Err(_) => DriverMemoryMapping::default(),
        }
    }
}

/// This pairs a DriverMemory object with a mapping of that same object, for
/// convenience.
#[derive(Debug, Default)]
pub struct DriverMemoryWithMapping {
    pub memory: DriverMemory,
    pub mapping: DriverMemoryMapping,
}

impl DriverMemoryWithMapping {
    pub fn new(memory: DriverMemory, mapping: DriverMemoryMapping) -> Self {
        Self { memory, mapping }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::{DriverHandle, MappedMemory, SharedMemoryInfo};
    use crate::core::testing::mock_driver::MockDriver;

    fn h(value: usize) -> DriverHandle {
        DriverHandle::new(value)
    }

    #[test]
    fn invalid() {
        assert!(!DriverMemory::default().is_valid());
    }

    #[test]
    fn acquire_from_object() {
        const HANDLE: usize = 1234;
        const SIZE: usize = 64;
        let driver = MockDriver::new();
        let object = DriverObject::new(driver.as_driver(), h(HANDLE));

        // Constructing a new DriverMemory over a generic DriverObject must query
        // the underlying object for its size.
        driver.expect_info(h(HANDLE), Ok(SharedMemoryInfo { region_num_bytes: SIZE }));

        // DriverMemory must reflect the size returned by the driver.
        let memory = DriverMemory::from_object(object);
        assert_eq!(memory.size(), SIZE);
        driver.expect_close(h(HANDLE));
    }

    #[test]
    fn acquire_from_object_without_info_is_invalid() {
        const HANDLE: usize = 1234;
        let driver = MockDriver::new();
        let object = DriverObject::new(driver.as_driver(), h(HANDLE));
        driver.expect_info(h(HANDLE), Err(crate::core::ipcz_result::IpczResult::Unknown));
        driver.expect_close(h(HANDLE));
        assert!(!DriverMemory::from_object(object).is_valid());
    }

    #[test]
    fn allocate() {
        const HANDLE: usize = 54321;
        const SIZE: usize = 256;
        let driver = MockDriver::new();

        // Constructing a new DriverMemory with a size should allocate a new
        // shared memory region through the driver.
        driver.expect_allocate(|num_bytes| {
            assert_eq!(num_bytes, SIZE);
            Ok(h(HANDLE))
        });
        let mut memory = DriverMemory::allocate(&driver.as_driver(), SIZE);
        assert_eq!(memory.driver_object().handle(), h(HANDLE));
        assert_eq!(memory.size(), SIZE);

        driver.expect_close(h(HANDLE));
    }

    #[test]
    fn clone() {
        const HANDLE: usize = 54321;
        const DUPE: usize = 1234;
        const SIZE: usize = 256;
        let driver = MockDriver::new();

        driver.expect_allocate(|_| Ok(h(HANDLE)));
        let memory = DriverMemory::allocate(&driver.as_driver(), SIZE);

        driver.expect_duplicate(h(HANDLE), Ok(h(DUPE)));
        driver.expect_info(h(DUPE), Ok(SharedMemoryInfo { region_num_bytes: SIZE }));
        let mut clone = memory.clone_memory();
        assert!(clone.is_valid());
        assert_eq!(clone.driver_object().handle(), h(DUPE));
        assert_eq!(clone.size(), SIZE);

        driver.expect_close(h(HANDLE));
        driver.expect_close(h(DUPE));
    }

    #[test]
    fn map() {
        const HANDLE: usize = 424242;
        const MAPPING: usize = 777;
        const SIZE: usize = 64;
        let driver = MockDriver::new();
        let mut data = [0u8; SIZE];
        let address = data.as_mut_ptr();

        driver.expect_allocate(|_| Ok(h(HANDLE)));
        let memory = DriverMemory::allocate(&driver.as_driver(), SIZE);

        driver.expect_map(h(HANDLE), Ok(MappedMemory { address, mapping: h(MAPPING) }));
        let mapping = memory.map();
        assert!(mapping.is_valid());
        assert_eq!(mapping.address(), address);
        assert_eq!(mapping.size(), SIZE);

        driver.expect_close(h(MAPPING));
        driver.expect_close(h(HANDLE));
    }

    #[test]
    fn map_failure_is_invalid() {
        let driver = MockDriver::new();
        driver.expect_allocate(|_| Ok(h(1)));
        let memory = DriverMemory::allocate(&driver.as_driver(), 8);
        driver.expect_map(h(1), Err(crate::core::ipcz_result::IpczResult::Internal));
        assert!(!memory.map().is_valid());
        assert!(!DriverMemory::default().map().is_valid());
        driver.expect_close(h(1));
    }
}
