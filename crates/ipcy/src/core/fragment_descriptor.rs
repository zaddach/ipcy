//! Port of `ipcz/fragment_descriptor.h`.

use super::buffer_id::{BufferId, INVALID_BUFFER_ID};

/// Represents a span of memory within the shared memory regions owned by a
/// BufferPool. A FragmentDescriptor can be resolved to a concrete Fragment by
/// passing it to `get_fragment()` on an appropriate BufferPool object.
///
/// NOTE: This is a wire structure which must remain stable over time.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
// NOTE: 8-byte alignment comes from `BufferId`; see the note on `NodeName`.
#[repr(C)]
pub struct FragmentDescriptor {
    // Identifies the shared memory buffer in which the memory resides. This ID
    // is scoped to a specific BufferPool (and therefore to a specific NodeLink).
    buffer_id: BufferId,
    // The byte offset from the start of the identified shared memory buffer
    // where this fragment begins.
    offset: u32,
    // The size of this fragment in bytes.
    size: u32,
}

const _: () = assert!(std::mem::size_of::<FragmentDescriptor>() == 16);
const _: () = assert!(std::mem::align_of::<FragmentDescriptor>() == 8);

impl Default for FragmentDescriptor {
    /// A null descriptor. Null descriptors always resolve to null fragments.
    fn default() -> Self {
        Self {
            buffer_id: INVALID_BUFFER_ID,
            offset: 0,
            size: 0,
        }
    }
}

impl FragmentDescriptor {
    /// Constructs a descriptor for a span of memory `size` bytes long, starting
    /// at byte `offset` within the buffer identified by `buffer_id` within some
    /// BufferPool.
    pub const fn new(buffer_id: BufferId, offset: u32, size: u32) -> Self {
        Self {
            buffer_id,
            offset,
            size,
        }
    }

    pub fn is_null(&self) -> bool {
        self.buffer_id == INVALID_BUFFER_ID
    }

    pub fn buffer_id(&self) -> BufferId {
        self.buffer_id
    }

    pub fn offset(&self) -> u32 {
        self.offset
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;

    #[test]
    fn default_is_null() {
        assert!(FragmentDescriptor::default().is_null());
        assert!(!FragmentDescriptor::new(BufferId::new(0), 0, 0).is_null());
    }

    #[test]
    fn wire_layout() {
        assert_eq!(offset_of!(FragmentDescriptor, buffer_id), 0);
        assert_eq!(offset_of!(FragmentDescriptor, offset), 8);
        assert_eq!(offset_of!(FragmentDescriptor, size), 12);
    }
}
