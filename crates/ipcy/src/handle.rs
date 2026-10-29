use std::cell::Cell;
use std::num::NonZeroU64;
use std::sync::Arc;

use crate::api::{
    BoxContents, BoxError, ConnectNodeError, MergePortalsError, PeekedBoxContents, PortalStatus, RejectError, Trap,
    TrapEvent, TrapInstallError, UnboxError,
};
use crate::core::api as core_api;
use crate::core::driver::{Driver, DriverHandle};
use crate::core::ipcz_types::{ConnectNodeFlags, CreateNodeFlags, CreateNodeOptions, IpczHandle, TrapConditions};
use crate::error::{IpczError, Result};

/// Owns an ipcz handle and closes it on drop.
///
/// This type is intentionally `!Sync`: applications must avoid concurrent
/// operations on the same handle, especially around closing it.
#[derive(Debug)]
pub struct Handle {
    raw: NonZeroU64,
    _not_sync: Cell<()>,
}

impl Handle {
    pub fn raw(&self) -> IpczHandle {
        self.raw.get()
    }

    /// Leaks this handle without closing it.
    pub fn into_raw(self) -> IpczHandle {
        let raw = self.raw.get();
        std::mem::forget(self);
        raw
    }

    /// Constructs an owned handle wrapper from a raw ipcz handle.
    ///
    /// # Safety
    /// `raw` must be a valid handle and ownership must belong to the caller.
    pub unsafe fn from_raw(raw: IpczHandle) -> Option<Self> {
        Some(Self {
            raw: NonZeroU64::new(raw)?,
            _not_sync: Cell::new(()),
        })
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        let _ = core_api::close(self.raw.get());
    }
}

macro_rules! define_ipcz_handle_type {
    ($name:ident) => {
        #[derive(Debug)]
        pub struct $name(pub(crate) Handle);

        impl $name {
            pub fn raw(&self) -> IpczHandle {
                self.0.raw()
            }

            /// Leaks this handle without closing it.
            pub fn into_raw(self) -> IpczHandle {
                self.0.into_raw()
            }

            /// Constructs an owned handle wrapper from a raw ipcz handle.
            ///
            /// # Safety
            /// `raw` must be a valid handle of the right type and ownership
            /// must belong to the caller.
            pub unsafe fn from_raw(raw: IpczHandle) -> Option<Self> {
                Some(Self(unsafe { Handle::from_raw(raw)? }))
            }
        }

        impl From<$name> for Handle {
            fn from(handle: $name) -> Self {
                handle.0
            }
        }
    };
}

define_ipcz_handle_type!(Node);
define_ipcz_handle_type!(Portal);
define_ipcz_handle_type!(Parcel);
define_ipcz_handle_type!(BoxHandle);

impl Node {
    /// Creates a new node which performs I/O and shared memory management with
    /// `driver`.
    pub fn create(driver: Arc<dyn Driver>, flags: CreateNodeFlags, options: Option<&CreateNodeOptions>) -> Result<Node> {
        let raw = core_api::create_node(driver, flags, options).map_err(|e| IpczError::from_result(e).unwrap_or(IpczError::Internal))?;
        unsafe { Node::from_raw(raw) }.ok_or(IpczError::Internal)
    }

    /// Opens a new pair of directly connected portals on this node.
    pub fn open_portals(&self) -> Result<(Portal, Portal)> {
        let (a, b) = core_api::open_portals(self.raw()).map_err(to_error)?;
        let (a, b) = unsafe { (Portal::from_raw(a), Portal::from_raw(b)) };
        a.zip(b).ok_or(IpczError::Internal)
    }

    /// Connects this node to another over `transport`, a transport created by
    /// this node's driver. On success returns `num_initial_portals` entangled
    /// portals, and `transport` is consumed. On failure it remains owned by
    /// the caller.
    pub fn connect_node(
        &self,
        transport: DriverHandle,
        num_initial_portals: usize,
        flags: ConnectNodeFlags,
    ) -> std::result::Result<Vec<Portal>, ConnectNodeError> {
        let raws = core_api::connect_node(self.raw(), transport, num_initial_portals, flags)
            .map_err(|result| ConnectNodeError {
                error: IpczError::from_result(result).unwrap_or(IpczError::Internal),
                transport,
            })?;
        raws.into_iter()
            .map(|raw| unsafe { Portal::from_raw(raw) }.ok_or(IpczError::Internal))
            .collect::<Result<Vec<_>>>()
            .map_err(|error| ConnectNodeError { error, transport })
    }

    /// Boxes an object into a transferrable box handle. On success ownership of
    /// the contents passes to ipcz; on failure it's returned in the error.
    pub fn box_contents(&self, contents: BoxContents) -> std::result::Result<BoxHandle, BoxError> {
        let result = match &contents {
            BoxContents::DriverObject(object) => core_api::box_driver_object(self.raw(), *object),
            BoxContents::ApplicationObject(object) => core_api::box_application_object(self.raw(), object.clone()),
            // Explicit boxing of subparcels is not supported.
            BoxContents::Subparcel(_) => Err(crate::core::ipcz_result::IpczResult::Unimplemented),
        };
        match result {
            Ok(raw) => unsafe { BoxHandle::from_raw(raw) }.ok_or(BoxError {
                error: IpczError::Internal,
                contents,
            }),
            Err(result) => Err(BoxError {
                error: IpczError::from_result(result).unwrap_or(IpczError::Internal),
                contents,
            }),
        }
    }
}

pub(crate) fn to_error(result: crate::core::ipcz_result::IpczResult) -> IpczError {
    IpczError::from_result(result).unwrap_or(IpczError::Internal)
}

impl Portal {
    /// Returns the current status of this portal.
    pub fn status(&self) -> Result<PortalStatus> {
        core_api::query_portal_status(self.raw()).map(Into::into).map_err(to_error)
    }

    /// Installs a one-shot trap on this portal. See [`Trap`].
    pub fn trap<F>(&self, conditions: TrapConditions, handler: F) -> std::result::Result<Trap, TrapInstallError>
    where
        F: FnOnce(TrapEvent) + Send + 'static,
    {
        Trap::install(self, conditions, handler)
    }

    /// Merges this portal's route with `other`'s, consuming both on success.
    pub fn merge_with(self, other: Portal) -> std::result::Result<(), MergePortalsError> {
        match core_api::merge_portals(self.raw(), other.raw()) {
            crate::core::ipcz_result::IpczResult::Ok => {
                // The merged portals are owned by the route now.
                let _ = (self.into_raw(), other.into_raw());
                Ok(())
            }
            result => Err(MergePortalsError {
                error: to_error(result),
                first: self,
                second: other,
            }),
        }
    }
}

impl Parcel {
    /// Reports application-level rejection of this parcel to the driver.
    pub fn reject(self, context: usize) -> std::result::Result<(), RejectError> {
        match core_api::reject(self.raw(), context) {
            crate::core::ipcz_result::IpczResult::Ok => Ok(()),
            result => Err(RejectError {
                error: to_error(result),
                parcel: self,
            }),
        }
    }
}

impl BoxHandle {
    /// Unboxes the contents, consuming the box on success.
    pub fn unbox(self) -> std::result::Result<BoxContents, UnboxError> {
        match core_api::unbox(self.raw(), crate::core::ipcz_types::UnboxFlags::empty()) {
            Ok(core_api::UnboxedContents::Unboxed(contents)) => {
                // The box is consumed on success.
                let _ = self.into_raw();
                Ok(BoxContents::from_core(contents))
            }
            Ok(core_api::UnboxedContents::Peeked(_)) => unreachable!("not peeking"),
            Err(result) => Err(UnboxError {
                error: to_error(result),
                handle: self,
            }),
        }
    }

    /// Looks at the contents without consuming the box.
    pub fn peek_unbox(&self) -> Result<PeekedBoxContents> {
        match core_api::unbox(self.raw(), crate::core::ipcz_types::UnboxFlags::PEEK) {
            Ok(core_api::UnboxedContents::Peeked(contents)) => Ok(contents),
            Ok(core_api::UnboxedContents::Unboxed(_)) => unreachable!("peeking"),
            Err(result) => Err(to_error(result)),
        }
    }
}
