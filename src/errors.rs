pub type GenericError = Box<dyn std::error::Error>;

#[derive(Debug, PartialEq, Eq)]
pub struct AuthError;

impl From<crate::jwt::JwtError> for AuthError {
    fn from(_value: crate::jwt::JwtError) -> Self {
        AuthError
    }
}

impl AuthError {
    pub fn new(_: GenericError) -> Self {
        Self
    }
}

#[derive(Debug)]
pub struct SendMessageError;

impl std::fmt::Display for SendMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SendMessageError")
    }
}

impl std::error::Error for SendMessageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug)]
pub struct PublishMessageError;

impl std::fmt::Display for PublishMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PublishMessageError")
    }
}

impl std::error::Error for PublishMessageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug)]
pub struct AckMessageError;

impl std::fmt::Display for AckMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AckMessageError")
    }
}

impl std::error::Error for AckMessageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

#[derive(Debug)]
pub struct FetchMessageError;

impl std::fmt::Display for FetchMessageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FetchMessageError")
    }
}

impl std::error::Error for FetchMessageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}
