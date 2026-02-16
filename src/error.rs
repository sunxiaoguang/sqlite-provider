use core::fmt;

/// Error returned by sqlite-provider operations.
#[derive(Clone, Debug)]
pub struct Error {
    pub code: ErrorCode,
    pub extended: Option<i32>,
    pub message: Option<String>,
}

/// Result alias used across the crate.
pub type Result<T> = core::result::Result<T, Error>;

/// SQLite result codes plus crate-specific conditions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    Ok,
    Error,
    Abort,
    Busy,
    NoMem,
    Interrupt,
    NotFound,
    CantOpen,
    Misuse,
    Row,
    Done,
    FeatureUnavailable,
    Unknown(i32),
}

impl ErrorCode {
    pub const fn from_code(code: i32) -> ErrorCode {
        match code {
            0 => ErrorCode::Ok,
            1 => ErrorCode::Error,
            4 => ErrorCode::Abort,
            5 => ErrorCode::Busy,
            7 => ErrorCode::NoMem,
            9 => ErrorCode::Interrupt,
            12 => ErrorCode::NotFound,
            14 => ErrorCode::CantOpen,
            21 => ErrorCode::Misuse,
            100 => ErrorCode::Row,
            101 => ErrorCode::Done,
            other => ErrorCode::Unknown(other),
        }
    }

    pub const fn code(self) -> Option<i32> {
        match self {
            ErrorCode::Ok => Some(0),
            ErrorCode::Error => Some(1),
            ErrorCode::Abort => Some(4),
            ErrorCode::Busy => Some(5),
            ErrorCode::NoMem => Some(7),
            ErrorCode::Interrupt => Some(9),
            ErrorCode::NotFound => Some(12),
            ErrorCode::CantOpen => Some(14),
            ErrorCode::Misuse => Some(21),
            ErrorCode::Row => Some(100),
            ErrorCode::Done => Some(101),
            ErrorCode::FeatureUnavailable => None,
            ErrorCode::Unknown(code) => Some(code),
        }
    }
}

impl Error {
    pub fn new(code: ErrorCode) -> Self {
        Self { code, extended: None, message: None }
    }

    pub fn with_message(code: ErrorCode, message: impl Into<String>) -> Self {
        Self { code, extended: None, message: Some(message.into()) }
    }

    pub fn from_code(code: i32, message: Option<String>, extended: Option<i32>) -> Self {
        Self { code: ErrorCode::from_code(code), extended, message }
    }

    pub fn feature_unavailable(msg: &'static str) -> Self {
        Self { code: ErrorCode::FeatureUnavailable, extended: None, message: Some(msg.into()) }
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
