//! Port of `ipcz/node_name.{h,cc}`.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// A 128-bit UUID used to uniquely identify a node within a connected graph of
/// ipcz nodes.
///
/// Names are assigned by a broker node to any connecting client, and clients
/// are introduced to each other by name, exclusively through a broker. This
/// means that the remote node identity on every NodeLink is authoritative and
/// node identities cannot be spoofed without the help of a broker node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
// NOTE: 8-byte alignment comes from the u64 fields. An explicit `align(8)` would
// make it unusable inside the packed message wire structs.
#[repr(C)]
pub struct NodeName {
    high: u64,
    low: u64,
}

const _: () = assert!(std::mem::size_of::<NodeName>() == 16);
const _: () = assert!(std::mem::align_of::<NodeName>() == 8);

impl NodeName {
    pub const fn new(high: u64, low: u64) -> Self {
        Self { high, low }
    }

    pub fn is_valid(&self) -> bool {
        self.high != 0 || self.low != 0
    }

    pub fn high(&self) -> u64 {
        self.high
    }

    pub fn low(&self) -> u64 {
        self.low
    }

    /// Store-release for NodeNames in shared memory.
    ///
    /// # Safety
    /// `dst` must point to a valid, 8-byte aligned NodeName which is only ever
    /// accessed concurrently through these atomic accessors.
    pub unsafe fn store_release(dst: *mut NodeName, name: NodeName) {
        // SAFETY: guaranteed by the caller; `repr(C)` puts `high` first.
        unsafe {
            AtomicU64::from_ptr(&raw mut (*dst).high).store(name.high, Ordering::Relaxed);
            AtomicU64::from_ptr(&raw mut (*dst).low).store(name.low, Ordering::Release);
        }
    }

    /// Load-acquire for NodeNames in shared memory.
    ///
    /// # Safety
    /// As for [`Self::store_release`].
    pub unsafe fn load_acquire(src: *const NodeName) -> NodeName {
        // SAFETY: guaranteed by the caller.
        unsafe {
            let low = AtomicU64::from_ptr((&raw const (*src).low).cast_mut()).load(Ordering::Acquire);
            let high = AtomicU64::from_ptr((&raw const (*src).high).cast_mut()).load(Ordering::Relaxed);
            NodeName { high, low }
        }
    }
}

impl fmt::Display for NodeName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}{:016x}", self.high, self.low)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validity_and_ordering() {
        assert!(!NodeName::default().is_valid());
        assert!(NodeName::new(0, 1).is_valid());
        assert!(NodeName::new(1, 0).is_valid());
        assert!(NodeName::new(1, 9) < NodeName::new(2, 0));
        assert!(NodeName::new(1, 1) < NodeName::new(1, 2));
    }

    #[test]
    fn to_string_is_zero_padded_hex() {
        assert_eq!(
            NodeName::new(0xabc, 0x1).to_string(),
            "0000000000000abc0000000000000001"
        );
    }

    #[test]
    fn store_release_load_acquire() {
        let mut shared = NodeName::default();
        unsafe {
            NodeName::store_release(&mut shared, NodeName::new(3, 4));
            assert_eq!(NodeName::load_acquire(&shared), NodeName::new(3, 4));
        }
    }
}
