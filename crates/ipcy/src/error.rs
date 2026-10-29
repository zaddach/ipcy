use crate::core::ipcz_result::IpczResult;

pub type Result<T> = std::result::Result<T, IpczError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum IpczError {
    #[error("cancelled")]
    Cancelled,

    #[error("unknown")]
    Unknown,

    #[error("invalid argument")]
    InvalidArgument,

    #[error("deadline exceeded")]
    DeadlineExceeded,

    #[error("not found")]
    NotFound,

    #[error("already exists")]
    AlreadyExists,

    #[error("permission denied")]
    PermissionDenied,

    #[error("resource exhausted")]
    ResourceExhausted,

    #[error("failed precondition")]
    FailedPrecondition,

    #[error("aborted")]
    Aborted,

    #[error("out of range")]
    OutOfRange,

    #[error("unimplemented")]
    Unimplemented,

    #[error("internal")]
    Internal,

    #[error("unavailable")]
    Unavailable,

    #[error("data loss")]
    DataLoss,
}

impl IpczError {
    /// Converts `result` into `Ok(())` or the corresponding error.
    pub fn check(result: IpczResult) -> Result<()> {
        match Self::from_result(result) {
            None => Ok(()),
            Some(error) => Err(error),
        }
    }

    /// Converts an `IpczResult` into an error, or `None` for `IpczResult::Ok`.
    pub fn from_result(result: IpczResult) -> Option<Self> {
        Some(match result {
            IpczResult::Ok => return None,
            IpczResult::Cancelled => Self::Cancelled,
            IpczResult::Unknown => Self::Unknown,
            IpczResult::InvalidArgument => Self::InvalidArgument,
            IpczResult::DeadlineExceeded => Self::DeadlineExceeded,
            IpczResult::NotFound => Self::NotFound,
            IpczResult::AlreadyExists => Self::AlreadyExists,
            IpczResult::PermissionDenied => Self::PermissionDenied,
            IpczResult::ResourceExhausted => Self::ResourceExhausted,
            IpczResult::FailedPrecondition => Self::FailedPrecondition,
            IpczResult::Aborted => Self::Aborted,
            IpczResult::OutOfRange => Self::OutOfRange,
            IpczResult::Unimplemented => Self::Unimplemented,
            IpczResult::Internal => Self::Internal,
            IpczResult::Unavailable => Self::Unavailable,
            IpczResult::DataLoss => Self::DataLoss,
        })
    }

    /// Returns the underlying `IpczResult` error code.
    pub fn as_result(&self) -> IpczResult {
        match *self {
            Self::Cancelled => IpczResult::Cancelled,
            Self::Unknown => IpczResult::Unknown,
            Self::InvalidArgument => IpczResult::InvalidArgument,
            Self::DeadlineExceeded => IpczResult::DeadlineExceeded,
            Self::NotFound => IpczResult::NotFound,
            Self::AlreadyExists => IpczResult::AlreadyExists,
            Self::PermissionDenied => IpczResult::PermissionDenied,
            Self::ResourceExhausted => IpczResult::ResourceExhausted,
            Self::FailedPrecondition => IpczResult::FailedPrecondition,
            Self::Aborted => IpczResult::Aborted,
            Self::OutOfRange => IpczResult::OutOfRange,
            Self::Unimplemented => IpczResult::Unimplemented,
            Self::Internal => IpczResult::Internal,
            Self::Unavailable => IpczResult::Unavailable,
            Self::DataLoss => IpczResult::DataLoss,
        }
    }
}
