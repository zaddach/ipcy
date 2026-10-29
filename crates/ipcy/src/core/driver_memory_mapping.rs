//! Port of `ipcz/driver_memory_mapping.{h,cc}`.

use std::sync::Arc;

use super::driver::{Driver, DriverHandle, INVALID_DRIVER_HANDLE};

/// Scoped wrapper around a driver-controlled shared memory region mapping.
pub struct DriverMemoryMapping {
    driver: Option<Arc<dyn Driver>>,
    mapping: DriverHandle,
    address: *mut u8,
    size: usize,
}

// SAFETY: the mapping is a plain region of memory owned by the driver, which is
// `Send + Sync`; access to the memory itself is synchronized by its users.
unsafe impl Send for DriverMemoryMapping {}
unsafe impl Sync for DriverMemoryMapping {}

impl Default for DriverMemoryMapping {
    fn default() -> Self {
        Self {
            driver: None,
            mapping: INVALID_DRIVER_HANDLE,
            address: std::ptr::null_mut(),
            size: 0,
        }
    }
}

impl DriverMemoryMapping {
    /// Tracks the driver-produced handle and base address of an active memory
    /// mapping.
    pub fn new(driver: Arc<dyn Driver>, mapping: DriverHandle, address: *mut u8, size: usize) -> Self {
        Self {
            driver: Some(driver),
            mapping,
            address,
            size,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.mapping.is_valid()
    }

    /// The base address of the mapping. Null if invalid.
    pub fn address(&self) -> *mut u8 {
        self.address
    }

    /// The size of the mapping in bytes. Zero if invalid.
    pub fn size(&self) -> usize {
        self.size
    }

    fn unmap(&mut self) {
        if self.is_valid() {
            if let Some(driver) = self.driver.take() {
                driver.close(self.mapping);
            }
            self.mapping = INVALID_DRIVER_HANDLE;
            self.address = std::ptr::null_mut();
            self.size = 0;
        }
    }
}

impl Drop for DriverMemoryMapping {
    fn drop(&mut self) {
        self.unmap();
    }
}

impl std::fmt::Debug for DriverMemoryMapping {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverMemoryMapping")
            .field("mapping", &self.mapping)
            .field("address", &self.address)
            .field("size", &self.size)
            .finish()
    }
}
