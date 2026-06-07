use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("response not found: {0}")]
    NotFound(String),
    #[error("invalid request: {0}")]
    InvalidArgument(String),
    #[error("storage error: {0}")]
    Storage(#[from] redis::RedisError),
    #[error("serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, StoreError>;
