//! Port of `ipcz/sequence_number.h`.

use std::fmt;

/// Used to number arbitrary objects in a sequence.
///
/// More specifically this is used by ipcz to maintain relative ordering of
/// parcels against other parcels from the same source portal, or NodeLink
/// messages against other NodeLink messages from the same NodeLink endpoint.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn value(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub const fn next_sequence_number(n: SequenceNumber) -> SequenceNumber {
    SequenceNumber(n.0.wrapping_add(1))
}
