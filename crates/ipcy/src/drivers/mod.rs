//! Drivers for real inter-process communication, one module per platform
//! family, matching the transports Chromium's ipcz driver uses:
//!
//! - [`unix`]: `AF_UNIX` sockets with `SCM_RIGHTS` and `memfd` (Linux).
//! - [`macos`]: Mach ports.
//! - [`windows`]: named pipes with `DuplicateHandle`.
//!
//! Each platform module always compiles its portable framing code, which is
//! unit tested everywhere; the driver itself only builds on its own platform.
//! (The in-process reference drivers for tests are in
//! `core::reference_drivers`.)

#[cfg(test)]
mod driver_tests;
pub mod macos;
pub mod unix;
pub mod windows;

/// The kinds of driver objects that cross process boundaries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum ObjectType {
    Transport = 0,
    Memory = 1,
}

impl ObjectType {
    #[cfg_attr(not(any(target_os = "macos", windows)), allow(dead_code))]
    fn from_raw(raw: u32) -> Option<Self> {
        match raw {
            0 => Some(Self::Transport),
            1 => Some(Self::Memory),
            _ => None,
        }
    }
}

/// Size in bytes of the data `serialize()` produces for every object: its type
/// and, for memory, its size.
pub const SERIALIZED_OBJECT_BYTES: usize = 8;

#[cfg_attr(not(any(target_os = "macos", windows)), allow(dead_code))]
pub(crate) fn write_object_header(dst: &mut [u8], ty: ObjectType, size: u32) {
    dst[..4].copy_from_slice(&(ty as u32).to_ne_bytes());
    dst[4..8].copy_from_slice(&size.to_ne_bytes());
}

#[cfg_attr(not(any(target_os = "macos", windows)), allow(dead_code))]
pub(crate) fn read_object_header(src: &[u8]) -> Option<(ObjectType, u32)> {
    let header = src.get(..SERIALIZED_OBJECT_BYTES)?;
    let ty = ObjectType::from_raw(u32::from_ne_bytes(header[..4].try_into().ok()?))?;
    Some((ty, u32::from_ne_bytes(header[4..].try_into().ok()?)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_header_round_trips() {
        let mut bytes = [0u8; SERIALIZED_OBJECT_BYTES];
        write_object_header(&mut bytes, ObjectType::Memory, 4096);
        assert_eq!(Some((ObjectType::Memory, 4096)), read_object_header(&bytes));
        write_object_header(&mut bytes, ObjectType::Transport, 0);
        assert_eq!(Some((ObjectType::Transport, 0)), read_object_header(&bytes));
    }

    #[test]
    fn object_header_rejects_garbage() {
        assert_eq!(None, read_object_header(&[0; 4]));
        let mut bytes = [0u8; SERIALIZED_OBJECT_BYTES];
        bytes[..4].copy_from_slice(&7u32.to_ne_bytes());
        assert_eq!(None, read_object_header(&bytes));
    }
}
