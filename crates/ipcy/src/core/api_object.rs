//! Port of `ipcz/api_object.{h,cc}`.
//!
//! DEVIATION: upstream's `IpczHandle` is the address of a leaked, ref-counted
//! `APIObject`. Here handles are keys into a process-global table which owns
//! one strong reference per outstanding handle, so a stale or forged handle
//! simply fails to resolve (`INVALID_ARGUMENT`) instead of being undefined
//! behavior. Releasing an object as a handle inserts it into the table; taking
//! it back out transfers that reference to the caller.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use super::ipcz_result::IpczResult;
use super::ipcz_types::IpczHandle;
use super::router::Router;

/// The kinds of objects which can be referenced by an IpczHandle.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApiObjectType {
    Node,
    Portal,
    Box,
    TransportListener,
    Parcel,
}

/// Base trait for any object which can be referenced by an IpczHandle.
pub trait ApiObject: Any + Send + Sync {
    fn object_type(&self) -> ApiObjectType;

    /// Closes this underlying object, ceasing its operations and freeing its
    /// resources ASAP.
    fn close(self: Arc<Self>) -> IpczResult;

    /// Indicates whether it's possible to send this object from `sender`. By
    /// default the answer is NO.
    fn can_send_from(&self, _sender: &Router) -> bool {
        false
    }
}

impl dyn ApiObject {
    /// Downcasts a shared object to a concrete type, checking its object type.
    pub fn downcast<T: ApiObject>(self: Arc<Self>) -> Option<Arc<T>> {
        let any: Arc<dyn Any + Send + Sync> = self;
        any.downcast::<T>().ok()
    }
}

struct Registry {
    objects: Mutex<HashMap<IpczHandle, Arc<dyn ApiObject>>>,
    next_handle: AtomicU64,
}

fn registry() -> &'static Registry {
    static REGISTRY: OnceLock<Registry> = OnceLock::new();
    REGISTRY.get_or_init(|| Registry {
        objects: Mutex::new(HashMap::new()),
        next_handle: AtomicU64::new(1),
    })
}

fn lock(registry: &Registry) -> MutexGuard<'_, HashMap<IpczHandle, Arc<dyn ApiObject>>> {
    registry.objects.lock().unwrap_or_else(|e| e.into_inner())
}

/// Releases ownership of `object` to produce a new IpczHandle which implicitly
/// owns the released reference.
pub fn release_as_handle(object: Arc<dyn ApiObject>) -> IpczHandle {
    let registry = registry();
    let handle = registry.next_handle.fetch_add(1, Ordering::Relaxed);
    lock(registry).insert(handle, object);
    handle
}

/// Convenience for [`release_as_handle`] with a concrete object type.
pub fn release_as_handle_typed<T: ApiObject>(object: Arc<T>) -> IpczHandle {
    release_as_handle(object)
}

/// Returns the object referenced by `handle` without taking ownership of the
/// handle.
pub fn from_handle(handle: IpczHandle) -> Option<Arc<dyn ApiObject>> {
    lock(registry()).get(&handle).cloned()
}

/// Like [`from_handle`], but additionally requires the object to be a `T`.
pub fn from_handle_typed<T: ApiObject>(handle: IpczHandle) -> Option<Arc<T>> {
    from_handle(handle)?.downcast::<T>()
}

/// Takes ownership of an APIObject from an existing `handle`, invalidating it.
pub fn take_from_handle(handle: IpczHandle) -> Option<Arc<dyn ApiObject>> {
    lock(registry()).remove(&handle)
}

/// Like [`take_from_handle`] but only if the object is a `T`; otherwise the
/// handle remains valid and untouched.
pub fn take_from_handle_typed<T: ApiObject>(handle: IpczHandle) -> Option<Arc<T>> {
    let mut objects = lock(registry());
    let object = objects.get(&handle)?.clone();
    let typed = object.downcast::<T>()?;
    objects.remove(&handle);
    Some(typed)
}

/// The number of outstanding handles, for leak checks in tests.
pub fn num_outstanding_handles() -> usize {
    lock(registry()).len()
}
