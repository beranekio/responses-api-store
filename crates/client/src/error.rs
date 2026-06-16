use thiserror::Error;
use tonic::Code;

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
    #[error("configuration error: {0}")]
    Configuration(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// Reports whether `err` indicates a missing or deleted response ID.
///
/// `GetResponse` and `DeleteResponse` surface missing records as gRPC `NOT_FOUND`
/// (`ClientError::Rpc`), not as `ClientError::NotFound`. The latter is reserved for
/// successful RPC responses with an empty proto record.
pub fn is_failed_precondition(err: &ClientError) -> bool {
    matches!(err, ClientError::Rpc(status) if status.code() == Code::FailedPrecondition)
}

pub fn is_not_found(err: &ClientError) -> bool {
    match err {
        ClientError::NotFound(_) => true,
        ClientError::Rpc(status) => status.code() == Code::NotFound,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Status;

    #[test]
    fn is_not_found_matches_rpc_status() {
        let err = ClientError::Rpc(Status::not_found("response not found: resp_1"));
        assert!(is_not_found(&err));
    }

    #[test]
    fn is_not_found_matches_client_variant() {
        let err = ClientError::NotFound("resp_1".to_string());
        assert!(is_not_found(&err));
    }

    #[test]
    fn is_not_found_rejects_other_errors() {
        let err = ClientError::Rpc(Status::unavailable("storage unavailable"));
        assert!(!is_not_found(&err));
    }
}
