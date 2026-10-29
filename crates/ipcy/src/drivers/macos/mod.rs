//! macOS: a driver over Mach ports. See [`MachDriver`].

pub mod wire;

#[cfg(target_os = "macos")]
mod driver;
#[cfg(target_os = "macos")]
pub use driver::MachDriver;

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use std::sync::Arc;

    use super::MachDriver;
    use crate::drivers::driver_tests::{Platform, instantiate_driver_tests};

    struct Mac;

    impl Platform for Mac {
        type Driver = MachDriver;

        fn new_driver() -> Arc<MachDriver> {
            MachDriver::new()
        }
    }

    instantiate_driver_tests!(Mac);
}
