use core::fmt;

/// Error returned by sqlite-provider operations.
#[derive(Clone, Debug)]
pub struct Error {
    /// Primary SQLite-style result code for the failure.
    pub code: ErrorCode,
    /// Optional extended SQLite result code when provided by backend.
    pub extended: Option<i32>,
    /// Optional human-readable error message from backend or wrapper.
    pub message: Option<String>,
}

/// Result alias used across the crate.
pub type Result<T> = core::result::Result<T, Error>;

/// SQLite result codes plus crate-specific conditions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    /// Operation completed successfully.
    Ok,
    /// Generic SQL error or missing database.
    Error,
    /// Internal SQLite logic error.
    Internal,
    /// Access permission denied.
    Perm,
    /// Operation aborted.
    Abort,
    /// Database file is busy.
    Busy,
    /// Database object is locked.
    Locked,
    /// Memory allocation failed.
    NoMem,
    /// Attempt to write a read-only database.
    ReadOnly,
    /// Operation interrupted.
    Interrupt,
    /// Disk I/O error.
    IoErr,
    /// Database image is malformed.
    Corrupt,
    /// Requested operation not found.
    NotFound,
    /// Database or disk is full.
    Full,
    /// Unable to open database file.
    CantOpen,
    /// Locking protocol error.
    Protocol,
    /// Database is empty.
    Empty,
    /// Database schema changed.
    Schema,
    /// String or blob too large.
    TooBig,
    /// Constraint violation.
    Constraint,
    /// Datatype mismatch.
    Mismatch,
    /// API misuse.
    Misuse,
    /// Large-file support unavailable.
    NoLfs,
    /// Authorization denied.
    Auth,
    /// Auxiliary database format error.
    Format,
    /// Parameter index out of range.
    Range,
    /// File opened is not a database.
    NotADb,
    /// Informational notice.
    Notice,
    /// Warning condition.
    Warning,
    /// `sqlite3_step` produced a row.
    Row,
    /// `sqlite3_step` completed without row.
    Done,
    /// Requested optional capability is unavailable.
    FeatureUnavailable,
    /// Unknown or backend-specific result code.
    Unknown(i32),
}

impl ErrorCode {
    /// Decode a raw SQLite result code (including extended codes).
    pub const fn from_code(code: i32) -> ErrorCode {
        let primary = code & 0xff;
        match primary {
            0 => ErrorCode::Ok,
            1 => ErrorCode::Error,
            2 => ErrorCode::Internal,
            3 => ErrorCode::Perm,
            4 => ErrorCode::Abort,
            5 => ErrorCode::Busy,
            6 => ErrorCode::Locked,
            7 => ErrorCode::NoMem,
            8 => ErrorCode::ReadOnly,
            9 => ErrorCode::Interrupt,
            10 => ErrorCode::IoErr,
            11 => ErrorCode::Corrupt,
            12 => ErrorCode::NotFound,
            13 => ErrorCode::Full,
            14 => ErrorCode::CantOpen,
            15 => ErrorCode::Protocol,
            16 => ErrorCode::Empty,
            17 => ErrorCode::Schema,
            18 => ErrorCode::TooBig,
            19 => ErrorCode::Constraint,
            20 => ErrorCode::Mismatch,
            21 => ErrorCode::Misuse,
            22 => ErrorCode::NoLfs,
            23 => ErrorCode::Auth,
            24 => ErrorCode::Format,
            25 => ErrorCode::Range,
            26 => ErrorCode::NotADb,
            27 => ErrorCode::Notice,
            28 => ErrorCode::Warning,
            100 => ErrorCode::Row,
            101 => ErrorCode::Done,
            _ => ErrorCode::Unknown(code),
        }
    }

    /// Encode this error code to the SQLite numeric code when available.
    pub const fn code(self) -> Option<i32> {
        match self {
            ErrorCode::Ok => Some(0),
            ErrorCode::Error => Some(1),
            ErrorCode::Internal => Some(2),
            ErrorCode::Perm => Some(3),
            ErrorCode::Abort => Some(4),
            ErrorCode::Busy => Some(5),
            ErrorCode::Locked => Some(6),
            ErrorCode::NoMem => Some(7),
            ErrorCode::ReadOnly => Some(8),
            ErrorCode::Interrupt => Some(9),
            ErrorCode::IoErr => Some(10),
            ErrorCode::Corrupt => Some(11),
            ErrorCode::NotFound => Some(12),
            ErrorCode::Full => Some(13),
            ErrorCode::CantOpen => Some(14),
            ErrorCode::Protocol => Some(15),
            ErrorCode::Empty => Some(16),
            ErrorCode::Schema => Some(17),
            ErrorCode::TooBig => Some(18),
            ErrorCode::Constraint => Some(19),
            ErrorCode::Mismatch => Some(20),
            ErrorCode::Misuse => Some(21),
            ErrorCode::NoLfs => Some(22),
            ErrorCode::Auth => Some(23),
            ErrorCode::Format => Some(24),
            ErrorCode::Range => Some(25),
            ErrorCode::NotADb => Some(26),
            ErrorCode::Notice => Some(27),
            ErrorCode::Warning => Some(28),
            ErrorCode::Row => Some(100),
            ErrorCode::Done => Some(101),
            ErrorCode::FeatureUnavailable => None,
            ErrorCode::Unknown(code) => Some(code),
        }
    }
}

impl Error {
    /// Create an error with only a primary code.
    pub fn new(code: ErrorCode) -> Self {
        Self {
            code,
            extended: None,
            message: None,
        }
    }

    /// Create an error with a primary code and owned message text.
    pub fn with_message(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            extended: None,
            message: Some(message.into()),
        }
    }

    /// Create an error from raw SQLite result code fields.
    pub fn from_code(code: i32, message: Option<String>, extended: Option<i32>) -> Self {
        Self {
            code: ErrorCode::from_code(code),
            extended,
            message,
        }
    }

    /// Create a capability error when a requested optional feature is unavailable.
    pub fn feature_unavailable(msg: &'static str) -> Self {
        Self {
            code: ErrorCode::FeatureUnavailable,
            extended: None,
            message: Some(msg.into()),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.code, &self.message) {
            (_, Some(msg)) => write!(f, "{}", msg),
            (ErrorCode::Unknown(code), None) => write!(f, "sqlite error code {}", code),
            (ErrorCode::FeatureUnavailable, None) => write!(f, "feature unavailable"),
            (code, None) => write!(f, "sqlite error {:?}", code),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::{Error, ErrorCode};

    #[test]
    fn error_code_mapping() {
        assert_eq!(ErrorCode::from_code(0), ErrorCode::Ok);
        assert_eq!(ErrorCode::from_code(19), ErrorCode::Constraint);
        // Extended result code should map by primary low byte.
        assert_eq!(ErrorCode::from_code((8 << 8) | 19), ErrorCode::Constraint);
        assert_eq!(ErrorCode::from_code(14), ErrorCode::CantOpen);
        assert_eq!(ErrorCode::from_code(999), ErrorCode::Unknown(999));
    }

    #[test]
    fn feature_unavailable_message() {
        let err = Error::feature_unavailable("missing");
        assert_eq!(err.code, ErrorCode::FeatureUnavailable);
        assert_eq!(err.message.as_deref(), Some("missing"));
    }
}
