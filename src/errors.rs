pub type GenericError = Box<dyn std::error::Error>;

#[derive(Debug, PartialEq, Eq)]
pub struct AuthError;

impl AuthError {
    pub fn new(_: GenericError) -> Self {
        Self
    }
}
