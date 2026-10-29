//! Mirror of the `IpczResult` codes from ipcz.h.

use std::fmt;

/// Result codes returned by ipcz and driver operations. The numeric values
/// match the `IPCZ_RESULT_*` constants of the C API.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(u32)]
pub enum IpczResult {
    Ok = 0,
    Cancelled = 1,
    Unknown = 2,
    InvalidArgument = 3,
    DeadlineExceeded = 4,
    NotFound = 5,
    AlreadyExists = 6,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    FailedPrecondition = 9,
    Aborted = 10,
    OutOfRange = 11,
    Unimplemented = 12,
    Internal = 13,
    Unavailable = 14,
    DataLoss = 15,
}

impl IpczResult {
    pub fn is_ok(self) -> bool {
        self == IpczResult::Ok
    }

    /// Converts `Ok` to `Ok(())` and everything else to `Err(self)`.
    pub fn into_result(self) -> Result<(), IpczResult> {
        if self.is_ok() { Ok(()) } else { Err(self) }
    }
}

impl fmt::Display for IpczResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for IpczResult {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_match_c_api() {
        assert_eq!(IpczResult::Ok as u32, 0);
        assert_eq!(IpczResult::InvalidArgument as u32, 3);
        assert_eq!(IpczResult::PermissionDenied as u32, 7);
        assert_eq!(IpczResult::ResourceExhausted as u32, 8);
        assert_eq!(IpczResult::Aborted as u32, 10);
        assert_eq!(IpczResult::DataLoss as u32, 15);
    }

    #[test]
    fn into_result() {
        assert_eq!(IpczResult::Ok.into_result(), Ok(()));
        assert_eq!(IpczResult::NotFound.into_result(), Err(IpczResult::NotFound));
    }
}
