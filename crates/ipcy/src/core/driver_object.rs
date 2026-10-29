//! Port of `ipcz/driver_object.{h,cc}`.

use std::sync::Arc;

use super::driver::{
    Driver, DriverHandle, INVALID_DRIVER_HANDLE, SerializeBuffers, SerializeOutcome,
    SerializedDimensions,
};
use super::driver_transport::DriverTransport;

/// Owns a [`DriverHandle`] and exposes a generic interface for serialization
/// and deserialization through the driver.
#[derive(Default)]
pub struct DriverObject {
    driver: Option<Arc<dyn Driver>>,
    handle: DriverHandle,
}

impl DriverObject {
    pub fn new(driver: Arc<dyn Driver>, handle: DriverHandle) -> Self {
        Self {
            driver: Some(driver),
            handle,
        }
    }

    pub fn driver(&self) -> Option<&Arc<dyn Driver>> {
        self.driver.as_ref()
    }

    pub fn handle(&self) -> DriverHandle {
        self.handle
    }

    /// Closes the underlying driver object, if any.
    pub fn reset(&mut self) {
        if self.is_valid() {
            let driver = self.driver.take().expect("valid DriverObject has a driver");
            driver.close(self.handle);
            self.handle = INVALID_DRIVER_HANDLE;
        }
    }

    /// Relinquishes ownership of the handle without closing it.
    pub fn release(&mut self) -> DriverHandle {
        self.driver = None;
        std::mem::replace(&mut self.handle, INVALID_DRIVER_HANDLE)
    }

    pub fn is_valid(&self) -> bool {
        self.handle.is_valid()
    }

    /// Indicates whether this DriverObject can be serialized into a collection
    /// of data and/or transmissible subobjects for transmission over a driver
    /// transport.
    pub fn is_serializable(&self) -> bool {
        let Some(driver) = self.driver.as_ref().filter(|_| self.is_valid()) else {
            return false;
        };
        matches!(
            driver.serialize(self.handle, INVALID_DRIVER_HANDLE, None),
            SerializeOutcome::NeedsTransport | SerializeOutcome::Dimensions(_)
        )
    }

    /// Indicates whether this DriverObject is (either as-is, or after some
    /// serialization) transmissible over the identified transport.
    pub fn can_transmit_on(&self, transport: &DriverTransport) -> bool {
        let Some(driver) = self.driver.as_ref().filter(|_| self.is_valid()) else {
            return false;
        };
        matches!(
            driver.serialize(self.handle, transport.driver_handle(), None),
            SerializeOutcome::Dimensions(_)
        )
    }

    /// Returns the data and transmissible handle capacity required to serialize
    /// this object for transmission over the identified transport. Must only be
    /// called on a valid object which is transmissible over that transport.
    pub fn get_serialized_dimensions(&self, transport: &DriverTransport) -> SerializedDimensions {
        let driver = self.driver.as_ref().expect("valid DriverObject");
        let SerializeOutcome::Dimensions(dimensions) =
            driver.serialize(self.handle, transport.driver_handle(), None)
        else {
            panic!("object is not transmissible on the given transport");
        };
        assert!(dimensions.num_bytes > 0 || dimensions.num_driver_handles > 0);
        dimensions
    }

    /// Serializes this object into `data` and `handles` for imminent
    /// transmission over `transport`. Both buffers must be at least large enough
    /// to support the object's serialized dimensions. Handles placed into
    /// `handles` are transmissible by the driver without further serialization.
    /// Must only be called on valid objects which are known to be serializable
    /// and transmissible over `transport`. On success this object is released.
    pub fn serialize(
        &mut self,
        transport: &DriverTransport,
        data: &mut [u8],
        handles: &mut [DriverHandle],
    ) -> bool {
        let driver = self.driver.as_ref().expect("valid DriverObject");
        let outcome = driver.serialize(
            self.handle,
            transport.driver_handle(),
            Some(SerializeBuffers { data, handles }),
        );
        if outcome == SerializeOutcome::Serialized {
            self.release();
            true
        } else {
            false
        }
    }

    /// Attempts to deserialize a driver object from bytes and transmissible
    /// driver handles produced by a prior call to `serialize()` and received via
    /// `transport`. Returns a valid DriverObject on success, or an invalid one on
    /// failure.
    pub fn deserialize(
        transport: &DriverTransport,
        data: &[u8],
        handles: &[DriverHandle],
    ) -> DriverObject {
        let Some(driver) = transport.driver() else {
            return DriverObject::default();
        };
        match driver.deserialize(data, handles, transport.driver_handle()) {
            Ok(handle) => DriverObject::new(driver, handle),
            Err(_) => DriverObject::default(),
        }
    }
}

impl Drop for DriverObject {
    fn drop(&mut self) {
        self.reset();
    }
}

impl std::fmt::Debug for DriverObject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DriverObject")
            .field("handle", &self.handle)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::driver::{SerializedDimensions, SerializeOutcome};
    use crate::core::driver_transport::DriverTransport;
    use crate::core::testing::mock_driver::MockDriver;

    fn h(value: usize) -> DriverHandle {
        DriverHandle::new(value)
    }

    #[test]
    fn invalid() {
        let mut object = DriverObject::default();
        assert!(!object.is_valid());
        assert!(object.driver().is_none());
        assert_eq!(object.handle(), INVALID_DRIVER_HANDLE);
        assert!(!object.is_serializable());

        let other = std::mem::take(&mut object);
        assert!(!object.is_valid());
        assert!(other.driver().is_none());
        assert!(!other.is_valid());
        assert!(!other.is_serializable());
    }

    #[test]
    fn move_semantics() {
        let driver = MockDriver::new();
        let mut object = DriverObject::new(driver.as_driver(), h(42));
        assert!(Arc::ptr_eq(object.driver().unwrap(), &driver.as_driver()));
        assert!(object.is_valid());
        assert_eq!(object.handle(), h(42));

        let mut other = std::mem::take(&mut object);
        assert!(Arc::ptr_eq(other.driver().unwrap(), &driver.as_driver()));
        assert!(other.is_valid());
        assert_eq!(other.handle(), h(42));
        assert!(!object.is_valid());
        assert!(object.driver().is_none());
        assert_eq!(object.handle(), INVALID_DRIVER_HANDLE);

        // Note that releasing prevents DriverObject from invoking the driver's
        // close() on destruction.
        assert_eq!(other.release(), h(42));
    }

    #[test]
    fn reset() {
        // Both explicit reset and implicit reset (i.e. DriverObject destruction)
        // must invoke close().
        let driver = MockDriver::new();
        driver.expect_close(h(5));
        drop(DriverObject::new(driver.as_driver(), h(5)));

        driver.expect_close(h(5));
        let mut object = DriverObject::new(driver.as_driver(), h(5));
        object.reset();
        assert!(!object.is_valid());
    }

    #[test]
    fn serializable_object() {
        let driver = MockDriver::new();
        let object = DriverObject::new(driver.as_driver(), h(5));

        driver.expect_serialize(h(5), |_, _, _| SerializeOutcome::NeedsTransport);
        assert!(object.is_serializable());

        driver.expect_serialize(h(5), |_, _, _| {
            SerializeOutcome::Dimensions(SerializedDimensions::default())
        });
        assert!(object.is_serializable());

        driver.expect_close(h(5));
    }

    #[test]
    fn unserializable_object() {
        let driver = MockDriver::new();
        let object = DriverObject::new(driver.as_driver(), h(5));

        driver.expect_serialize(h(5), |_, _, _| {
            SerializeOutcome::Error(crate::core::ipcz_result::IpczResult::InvalidArgument)
        });
        assert!(!object.is_serializable());

        driver.expect_close(h(5));
    }

    fn mock_transport(driver: &Arc<MockDriver>, handle: usize) -> Arc<DriverTransport> {
        DriverTransport::new(DriverObject::new(driver.as_driver(), h(handle)))
    }

    #[test]
    fn can_transmit() {
        let driver = MockDriver::new();
        let transport = mock_transport(&driver, 42);
        let object = DriverObject::new(driver.as_driver(), h(5));

        driver.expect_serialize(h(5), |_, transport, _| {
            assert_eq!(transport, h(42));
            SerializeOutcome::Dimensions(SerializedDimensions::default())
        });
        assert!(object.can_transmit_on(&transport));

        driver.expect_close(h(5));
        driver.expect_close(h(42));
    }

    #[test]
    fn cannot_transmit() {
        let driver = MockDriver::new();
        let transport = mock_transport(&driver, 42);
        let object = DriverObject::new(driver.as_driver(), h(5));

        driver.expect_serialize(h(5), |_, transport, _| {
            assert_eq!(transport, h(42));
            SerializeOutcome::NotTransmissible
        });
        assert!(!object.can_transmit_on(&transport));

        driver.expect_close(h(5));
        driver.expect_close(h(42));
    }

    #[test]
    fn get_serialized_dimensions() {
        let driver = MockDriver::new();
        let transport = mock_transport(&driver, 42);
        let object = DriverObject::new(driver.as_driver(), h(5));

        driver.expect_serialize(h(5), |_, transport, buffers| {
            assert_eq!(transport, h(42));
            assert!(buffers.is_none());
            SerializeOutcome::Dimensions(SerializedDimensions {
                num_bytes: 3,
                num_driver_handles: 7,
            })
        });

        let dimensions = object.get_serialized_dimensions(&transport);
        assert_eq!(dimensions.num_bytes, 3);
        assert_eq!(dimensions.num_driver_handles, 7);

        driver.expect_close(h(5));
        driver.expect_close(h(42));
    }

    #[test]
    fn serialize_releases_object_on_success_only() {
        let driver = MockDriver::new();
        let transport = mock_transport(&driver, 42);
        let mut object = DriverObject::new(driver.as_driver(), h(5));

        driver.expect_serialize(h(5), |_, _, _| SerializeOutcome::Error(
            crate::core::ipcz_result::IpczResult::FailedPrecondition,
        ));
        assert!(!object.serialize(&transport, &mut [0; 4], &mut [INVALID_DRIVER_HANDLE; 1]));
        assert!(object.is_valid());

        driver.expect_serialize(h(5), |_, _, buffers| {
            let buffers = buffers.expect("buffers");
            buffers.data[0] = 0xab;
            buffers.handles[0] = DriverHandle::new(99);
            SerializeOutcome::Serialized
        });
        let (mut data, mut handles) = ([0u8; 4], [INVALID_DRIVER_HANDLE; 1]);
        assert!(object.serialize(&transport, &mut data, &mut handles));
        assert!(!object.is_valid());
        assert_eq!((data[0], handles[0]), (0xab, DriverHandle::new(99)));

        // The released handle must not be closed; only the transport is.
        driver.expect_close(h(42));
    }
}
