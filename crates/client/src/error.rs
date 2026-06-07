use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("rpc error: {0}")]
    Rpc(#[from] tonic::Status),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("response not found: {0}")]
    NotFound(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;
