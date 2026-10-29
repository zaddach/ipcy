//! Port of `ipcz/sublink_id.h`.

use std::fmt;

use super::link_side::fmt_side_tagged_id;

/// Identifies a specific subsidiary link along a NodeLink. Each sublink is a
/// path between a unique pair of Router instances, one on each linked node. New
/// SublinkIds are allocated atomically by either side of the NodeLink.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SublinkId(u64);

impl SublinkId {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Display for SublinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_side_tagged_id(self.0, f)
    }
}

impl fmt::Debug for SublinkId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
