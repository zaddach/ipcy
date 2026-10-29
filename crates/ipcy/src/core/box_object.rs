//! Port of `ipcz/box.{h,cc}`. (Named `box_object` to avoid the keyword.)

use std::sync::{Arc, Mutex, MutexGuard};

use super::api_object::{self, ApiObject, ApiObjectType};
use super::application_object::ApplicationObject;
use super::driver::DriverHandle;
use super::driver_object::DriverObject;
use super::driver_transport::DriverTransport;
use super::ipcz_result::IpczResult;
use super::ipcz_types::IpczHandle;
use super::parcel_wrapper::ParcelWrapper;
use super::router::Router;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BoxType {
    /// An empty box.
    Empty,
    /// A driver object, potentially serializable by the driver.
    DriverObject,
    /// An opaque application object, optionally serializable.
    ApplicationObject,
    /// A Parcel object which does not stand on its own in any portal's inbound
    /// queue, but which can be embedded within another such Parcel.
    Subparcel,
}

enum Contents {
    Empty,
    DriverObject(DriverObject),
    ApplicationObject(Arc<dyn ApplicationObject>),
    Subparcel(Arc<ParcelWrapper>),
}

/// The contents of a box, handed to the application when unboxing.
pub enum BoxContents {
    DriverObject(DriverObject),
    ApplicationObject(Arc<dyn ApplicationObject>),
    /// An owning handle to the subparcel.
    Subparcel(IpczHandle),
}

/// The contents of a box, as peeked at without unboxing.
pub enum PeekedBoxContents {
    /// The driver handle remains owned by the box.
    DriverObject(DriverHandle),
    ApplicationObject(Arc<dyn ApplicationObject>),
    Subparcel(Arc<ParcelWrapper>),
}

/// Generic handle wrapper around a DriverObject, ApplicationObject, or Parcel,
/// allowing those types to be passed wherever IpczHandles are accepted.
pub struct ApiBox {
    contents: Mutex<Contents>,
}

impl ApiBox {
    fn with(contents: Contents) -> Arc<ApiBox> {
        Arc::new(ApiBox {
            contents: Mutex::new(contents),
        })
    }

    pub fn new_driver_object(object: DriverObject) -> Arc<ApiBox> {
        Self::with(Contents::DriverObject(object))
    }

    pub fn new_application_object(object: Arc<dyn ApplicationObject>) -> Arc<ApiBox> {
        Self::with(Contents::ApplicationObject(object))
    }

    pub fn new_subparcel(parcel: Arc<ParcelWrapper>) -> Arc<ApiBox> {
        Self::with(Contents::Subparcel(parcel))
    }

    fn lock(&self) -> MutexGuard<'_, Contents> {
        self.contents.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn box_type(&self) -> BoxType {
        match &*self.lock() {
            Contents::Empty => BoxType::Empty,
            Contents::DriverObject(_) => BoxType::DriverObject,
            Contents::ApplicationObject(_) => BoxType::ApplicationObject,
            Contents::Subparcel(_) => BoxType::Subparcel,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(&*self.lock(), Contents::Empty)
    }

    /// Whether the boxed driver object can be transmitted on `transport`.
    /// Panics if this isn't a driver object box.
    pub fn driver_object_can_transmit_on(&self, transport: &DriverTransport) -> bool {
        match &*self.lock() {
            Contents::DriverObject(object) => object.can_transmit_on(transport),
            _ => panic!("not a driver object box"),
        }
    }

    /// Moves the boxed driver object out, leaving the box empty.
    pub fn take_driver_object(&self) -> DriverObject {
        match std::mem::replace(&mut *self.lock(), Contents::Empty) {
            Contents::DriverObject(object) => object,
            _ => panic!("not a driver object box"),
        }
    }

    /// Moves the boxed application object out, leaving the box empty.
    pub fn take_application_object(&self) -> Arc<dyn ApplicationObject> {
        match std::mem::replace(&mut *self.lock(), Contents::Empty) {
            Contents::ApplicationObject(object) => object,
            _ => panic!("not an application object box"),
        }
    }

    /// Moves the boxed subparcel out, leaving the box empty.
    pub fn take_subparcel(&self) -> Arc<ParcelWrapper> {
        match std::mem::replace(&mut *self.lock(), Contents::Empty) {
            Contents::Subparcel(wrapper) => wrapper,
            _ => panic!("not a subparcel box"),
        }
    }

    /// The boxed subparcel, if this is a subparcel box.
    pub fn subparcel(&self) -> Option<Arc<ParcelWrapper>> {
        match &*self.lock() {
            Contents::Subparcel(wrapper) => Some(wrapper.clone()),
            _ => None,
        }
    }

    pub fn peek(&self) -> Result<PeekedBoxContents, IpczResult> {
        match &*self.lock() {
            Contents::Empty => Err(IpczResult::InvalidArgument),
            Contents::DriverObject(object) => Ok(PeekedBoxContents::DriverObject(object.handle())),
            Contents::ApplicationObject(object) => Ok(PeekedBoxContents::ApplicationObject(object.clone())),
            Contents::Subparcel(wrapper) => Ok(PeekedBoxContents::Subparcel(wrapper.clone())),
        }
    }

    /// Extracts the contents of the box, leaving it empty.
    pub fn unbox(&self) -> Result<BoxContents, IpczResult> {
        match std::mem::replace(&mut *self.lock(), Contents::Empty) {
            Contents::Empty => Err(IpczResult::InvalidArgument),
            Contents::DriverObject(object) => Ok(BoxContents::DriverObject(object)),
            Contents::ApplicationObject(object) => Ok(BoxContents::ApplicationObject(object)),
            Contents::Subparcel(wrapper) => Ok(BoxContents::Subparcel(api_object::release_as_handle(wrapper))),
        }
    }
}

impl ApiObject for ApiBox {
    fn object_type(&self) -> ApiObjectType {
        ApiObjectType::Box
    }

    fn close(self: Arc<Self>) -> IpczResult {
        *self.lock() = Contents::Empty;
        IpczResult::Ok
    }

    fn can_send_from(&self, sender: &Router) -> bool {
        // Don't hold the lock while consulting the subparcel's objects.
        let wrapper = match &*self.lock() {
            Contents::Empty => return false,
            Contents::DriverObject(object) => return object.is_valid() && object.is_serializable(),
            Contents::ApplicationObject(_) => return true,
            Contents::Subparcel(wrapper) => wrapper.clone(),
        };
        wrapper.with_parcel(|parcel| {
            parcel
                .objects_view()
                .iter()
                .all(|object| object.as_ref().is_some_and(|object| object.can_send_from(sender)))
        })
    }
}
