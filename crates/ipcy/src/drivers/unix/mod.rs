//! Unix: a driver over `AF_UNIX` sockets with descriptor passing and `memfd`
//! shared memory. See [`MultiprocessDriver`]. Linux only, because of `memfd`,
//! `SOCK_CLOEXEC` and `MSG_NOSIGNAL`.

#[cfg(any(target_os = "linux", target_os = "android"))]
mod driver;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use driver::MultiprocessDriver;

/// The shared driver tests, run against the Unix-socket driver. Besides
/// testing it, this validates the tests themselves for the macOS and Windows
/// drivers, which Linux CI cannot run.
#[cfg(all(test, any(target_os = "linux", target_os = "android")))]
mod tests {
    use std::sync::Arc;

    use super::MultiprocessDriver;
    use crate::drivers::driver_tests::{Platform, instantiate_driver_tests};

    struct Linux;

    impl Platform for Linux {
        type Driver = MultiprocessDriver;

        fn new_driver() -> Arc<MultiprocessDriver> {
            MultiprocessDriver::new()
        }
    }

    instantiate_driver_tests!(Linux);
}
