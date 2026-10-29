//! Port of `ipcz/application_object.{h,cc}`.
//!
//! DEVIATION: upstream's ApplicationObject is an opaque `uintptr_t` accompanied
//! by C serializer and destructor function pointers supplied through `Box()`.
//! Here an application object is any value implementing
//! [`ApplicationObject`]; its destructor is `Drop`, and serialization is the
//! trait's `serialize()` method.

use std::any::Any;
use std::sync::Arc;

use super::api_object::{self, ApiObject};
use super::ipcz_result::IpczResult;
use super::ipcz_types::IpczHandle;
use super::node_link::NodeLink;
use super::parcel::Parcel;
use super::parcel_wrapper::ParcelWrapper;

/// The serialized form of an application object: bytes plus handles to other
/// API objects. Ownership of every handle passes to ipcz.
#[derive(Debug, Default)]
pub struct SerializedApplicationObject {
    pub data: Vec<u8>,
    pub handles: Vec<IpczHandle>,
}

/// An application-defined object which can be boxed and (optionally) sent to
/// other nodes by serializing it into a subparcel.
pub trait ApplicationObject: Any + Send + Sync {
    /// Indicates whether this object can be serialized by ipcz.
    fn is_serializable(&self) -> bool {
        false
    }

    /// Serializes this object. Only called on objects whose
    /// `is_serializable()` returns true. Serialization does *not* consume the
    /// object: it is still destroyed when its box is.
    fn serialize(&self) -> Result<SerializedApplicationObject, IpczResult> {
        Err(IpczResult::FailedPrecondition)
    }

    /// For applications to downcast the object back to its concrete type.
    fn as_any(&self) -> &dyn Any;
}

/// Serializes `object` into a new Parcel (a subparcel) to be transmitted across
/// `link`. Returns the wrapped new parcel, or `None` on failure.
///
/// Must only be called on objects which are known to be serializable.
pub fn serialize_application_object(
    object: &dyn ApplicationObject,
    link: &Arc<NodeLink>,
) -> Option<Arc<ParcelWrapper>> {
    assert!(object.is_serializable());
    let serialized = object.serialize().ok()?;

    let mut parcel = Box::new(Parcel::default());
    parcel.allocate_data(serialized.data.len(), /*allow_partial=*/ false, Some(link.memory()));
    parcel.data_view_mut()[..serialized.data.len()].copy_from_slice(&serialized.data);
    parcel.commit_data(serialized.data.len());

    let objects = serialized
        .handles
        .iter()
        .map(|&handle| api_object::take_from_handle(handle))
        .collect::<Vec<Option<Arc<dyn ApiObject>>>>();
    parcel.set_objects(objects);
    Some(ParcelWrapper::new(Some(parcel)))
}
