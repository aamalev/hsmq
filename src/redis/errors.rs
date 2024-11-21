use crate::errors::{AckMessageError, PublishMessageError};

impl From<redis::RedisError> for AckMessageError {
    fn from(_value: redis::RedisError) -> Self {
        AckMessageError
    }
}

impl From<redis::RedisError> for PublishMessageError {
    fn from(_value: redis::RedisError) -> Self {
        PublishMessageError
    }
}
