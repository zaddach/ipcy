//! Port of `ipcz/buffer_id.h`.

use std::fmt;

use super::link_side::fmt_side_tagged_id;

/// Identifies a shared memory buffer scoped to a NodeLink and owned by its
/// NodeLinkMemory via a BufferPool. New BufferIds are allocated atomically by
/// either side of the NodeLink.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct BufferId(u64);

pub const INVALID_BUFFER_ID: BufferId = BufferId(u64::MAX);

impl BufferId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for BufferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_side_tagged_id(self.0, f)
    }
}

impl fmt::Debug for BufferId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
