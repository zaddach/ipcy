//! Port of `ipcz/handle_type.h`.

/// Identifies the type of each IpczHandle attached to a serialized parcel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum HandleType {
    /// A portal handle consumes the next available RouterDescriptor in the
    /// parcel. It does not consume any other data, or any OS handles.
    Portal = 0,

    /// A box handle consumes the next available element in the parcel's
    /// DriverObject array and wraps it as a Box object.
    BoxedDriverObject = 1,

    /// A placeholder for a box handle in a split parcel transmission. This
    /// occurs in the directly transmitted half of the parcel, and it signifies
    /// the existence of a corresponding DriverObject in the relayed half.
    RelayedBoxedDriverObject = 2,

    /// A placeholder for a box handle in a parcel with one or more associated
    /// subparcels. Subparcels are transmitted separately from the main parcel,
    /// and the main parcel is not available to the application until all
    /// subparcels are collected internally.
    BoxedSubparcel = 3,
}

impl TryFrom<u32> for HandleType {
    type Error = u32;

    fn try_from(value: u32) -> Result<Self, u32> {
        match value {
            0 => Ok(HandleType::Portal),
            1 => Ok(HandleType::BoxedDriverObject),
            2 => Ok(HandleType::RelayedBoxedDriverObject),
            3 => Ok(HandleType::BoxedSubparcel),
            other => Err(other),
        }
    }
}
