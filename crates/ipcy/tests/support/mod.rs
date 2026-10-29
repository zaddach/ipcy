//! Test harness: port of `test/{test_base,multinode_test}`.
//!
//! DEVIATIONS: instead of gtest registration, `multinode_test!` expands to one
//! `#[test]` per (driver, feature set) combination. Spawned nodes run on
//! threads sharing the test's driver instance; the multiprocess driver is not
//! supported by this harness yet.

#![allow(dead_code, unused_imports)]

pub mod base;
pub mod multinode;
pub mod transport_listener;

pub use base::TestBase;
pub use multinode::*;
pub use transport_listener::TestTransportListener;
