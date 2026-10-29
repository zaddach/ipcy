//! Windows: a driver over named pipes with `DuplicateHandle`. See
//! [`WindowsDriver`].

pub mod wire;

#[cfg(windows)]
mod driver;
#[cfg(windows)]
pub use driver::{TransportConfig, WindowsDriver};

#[cfg(all(test, windows))]
mod tests {
    use std::os::windows::io::{FromRawHandle, OwnedHandle};
    use std::sync::Arc;

    use windows_sys::Win32::System::Threading::{GetCurrentProcessId, OpenProcess, PROCESS_DUP_HANDLE};

    use super::{TransportConfig, WindowsDriver};
    use crate::core::driver::{Driver, DriverHandle, INVALID_DRIVER_HANDLE, SerializeOutcome};
    use crate::core::ipcz_result::IpczResult;
    use crate::drivers::driver_tests::{Inbox, Platform, instantiate_driver_tests, send_object, transports};

    /// A handle to this very process, standing in for the process on the other
    /// end of a transport.
    fn this_process() -> OwnedHandle {
        // SAFETY: opens a handle to our own process.
        let process = unsafe { OpenProcess(PROCESS_DUP_HANDLE, 0, GetCurrentProcessId()) };
        assert!(!process.is_null());
        // SAFETY: OpenProcess returned a new handle we own.
        unsafe { OwnedHandle::from_raw_handle(process) }
    }

    struct Windows;

    impl Platform for Windows {
        type Driver = WindowsDriver;

        fn new_driver() -> Arc<WindowsDriver> {
            WindowsDriver::new()
        }

        // Both ends can push handles to the other and pull from it.
        fn allow_handle_transfer(driver: &WindowsDriver, a: DriverHandle, b: DriverHandle) {
            for transport in [a, b] {
                let config = TransportConfig {
                    remote_process: Some(this_process()),
                    peer_can_pull: true,
                };
                assert_eq!(IpczResult::Ok, driver.configure_transport(transport, config));
            }
        }
    }

    instantiate_driver_tests!(Windows);

    /// The sender cannot push, but the peer can pull from it, like a client
    /// sending to its broker.
    #[test]
    fn pulls_handles_from_the_sender() {
        let driver = WindowsDriver::new();
        let (a, b) = transports::<Windows>(&driver);
        let sender = TransportConfig {
            remote_process: None,
            peer_can_pull: true,
        };
        let receiver = TransportConfig {
            remote_process: Some(this_process()),
            peer_can_pull: false,
        };
        assert_eq!(IpczResult::Ok, driver.configure_transport(a, sender));
        assert_eq!(IpczResult::Ok, driver.configure_transport(b, receiver));
        let inbox = Inbox::activate(&*driver, b);

        let memory = driver.allocate_shared_memory(4096).unwrap();
        let mapped = driver.map_shared_memory(memory).unwrap();
        // SAFETY: a live mapping of 4096 bytes.
        unsafe { mapped.address.write_volatile(7) };

        let received = send_object(&*driver, memory, a, &inbox, b);
        let remapped = driver.map_shared_memory(received).unwrap();
        // SAFETY: a live mapping of 4096 bytes.
        assert_eq!(7, unsafe { remapped.address.read_volatile() });

        inbox.deactivate(&*driver, b);
    }

    /// With no process handle either way, objects can't be moved and ipcz must
    /// relay them through a broker.
    #[test]
    fn unprivileged_transports_are_not_transmissible() {
        let driver = WindowsDriver::new();
        let (a, _b) = driver
            .create_transports(INVALID_DRIVER_HANDLE, INVALID_DRIVER_HANDLE)
            .unwrap();
        let memory = driver.allocate_shared_memory(64).unwrap();
        assert_eq!(SerializeOutcome::NotTransmissible, driver.serialize(memory, a, None));
    }
}
