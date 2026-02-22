mod core;
mod extensions;
mod hooks;

pub use self::core::Connection;
pub use self::extensions::{Backup, Blob, SerializedDb};
pub use self::hooks::{
    AuthorizerAction, AuthorizerEvent, AuthorizerResult, CallbackHandle, TraceEvent, TraceMask,
    authorizer,
};

#[cfg(test)]
mod tests {
    use super::authorizer;
    use super::{AuthorizerAction, AuthorizerResult, TraceMask};

    #[test]
    fn trace_mask_bits() {
        let mask = TraceMask::STMT | TraceMask::PROFILE;
        assert!(mask.contains(TraceMask::STMT));
        assert!(mask.contains(TraceMask::PROFILE));
    }

    #[test]
    fn authorizer_action_from_code() {
        assert_eq!(
            AuthorizerAction::from_code(authorizer::READ),
            AuthorizerAction::Read
        );
        assert_eq!(
            AuthorizerAction::from_code(999),
            AuthorizerAction::Unknown(999)
        );
    }

    #[test]
    fn authorizer_result_codes() {
        assert_eq!(AuthorizerResult::Ok.into_code(), 0);
        assert_eq!(AuthorizerResult::Ignore.into_code(), 2);
        assert_eq!(AuthorizerResult::Deny.into_code(), 1);
    }
}
