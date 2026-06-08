use thiserror::Error;

/// Classifies a Redis error for logging and gRPC status messages.
pub fn redis_error_kind(err: &redis::RedisError) -> &'static str {
    if err.is_timeout() {
        "timeout"
    } else if err.is_connection_refusal() {
        "connection_refused"
    } else if err.to_string().contains("BUSY") {
        "busy"
    } else {
        "other"
    }
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("response not found: {0}")]
    NotFound(String),
    #[error("invalid request: {0}")]
    InvalidArgument(String),
    #[error("storage unavailable: {0}")]
    Unavailable(String),
    #[error("storage error: {0}")]
    Storage(#[from] redis::RedisError),
    #[error("serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;
