#![allow(clippy::result_large_err)]

use responses_api_store_core::{
    model::StoredResponse as CoreStoredResponse, redis_error_kind, StoreError,
};
use responses_api_store_proto::v1::{
    BackgroundJob as ProtoBackgroundJob, StoredResponse as ProtoStoredResponse,
};
use serde_json::Value;
use tonic::Status;

pub fn proto_to_core(record: &ProtoStoredResponse) -> Result<CoreStoredResponse, Status> {
    let response: Value = serde_json::from_str(&record.response_json).map_err(|e| {
        Status::invalid_argument(format!(
            "invalid response_json for {}: {e}",
            record.response_id
        ))
    })?;
    let input = record
        .input_json
        .iter()
        .map(|item| {
            serde_json::from_str(item).map_err(|e| {
                Status::invalid_argument(format!(
                    "invalid input_json entry for {}: {e}",
                    record.response_id
                ))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let pending_upstream_request = match &record.pending_upstream_request_json {
        Some(value) if !value.is_empty() => Some(serde_json::from_str(value).map_err(|e| {
            Status::invalid_argument(format!(
                "invalid pending_upstream_request_json for {}: {e}",
                record.response_id
            ))
        })?),
        _ => None,
    };

    Ok(CoreStoredResponse {
        upstream: record.upstream.clone(),
        response,
        input,
        pending_upstream_request,
        upstream_authorization: record.upstream_authorization.clone(),
        enqueued_at: record.enqueued_at,
    })
}

pub fn core_to_proto(
    response_id: &str,
    record: &CoreStoredResponse,
) -> Result<ProtoStoredResponse, Status> {
    let response_json = serde_json::to_string(&record.response).map_err(|e| {
        Status::internal(format!(
            "failed to serialize response for {response_id}: {e}"
        ))
    })?;
    let input_json = record
        .input
        .iter()
        .map(|item| {
            serde_json::to_string(item).map_err(|e| {
                Status::internal(format!("failed to serialize input for {response_id}: {e}"))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let pending_upstream_request_json = match &record.pending_upstream_request {
        Some(value) => Some(serde_json::to_string(value).map_err(|e| {
            Status::internal(format!(
                "failed to serialize pending_upstream_request for {response_id}: {e}"
            ))
        })?),
        None => None,
    };

    Ok(ProtoStoredResponse {
        response_id: response_id.to_string(),
        upstream: record.upstream.clone(),
        response_json,
        input_json,
        pending_upstream_request_json,
        upstream_authorization: record.upstream_authorization.clone(),
        enqueued_at: record.enqueued_at,
    })
}

pub fn core_job_to_proto(
    job: &responses_api_store_core::BackgroundJob,
) -> Result<ProtoBackgroundJob, Status> {
    Ok(ProtoBackgroundJob {
        stream_id: job.stream_id.clone(),
        response_id: job.response_id.clone(),
        record: Some(core_to_proto(&job.response_id, &job.record)?),
        autoclaimed: job.autoclaimed,
        idle_ms: job.idle_ms,
    })
}

pub fn map_store_error(rpc: &'static str, err: StoreError) -> Status {
    if let StoreError::Storage(ref storage_err) = err {
        tracing::warn!(
            rpc,
            storage_error_kind = redis_error_kind(storage_err),
            error = %storage_err,
            "storage operation failed"
        );
    }
    store_error_to_status(err)
}

pub fn map_claim_store_error(err: StoreError, consumer_group: &str, block_ms: u32) -> Status {
    if let StoreError::Storage(ref storage_err) = err {
        tracing::warn!(
            rpc = "ClaimBackgroundJobs",
            consumer_group,
            block_ms,
            storage_error_kind = redis_error_kind(storage_err),
            error = %storage_err,
            "storage operation failed"
        );
    }
    store_error_to_status(err)
}

fn store_error_to_status(err: StoreError) -> Status {
    match err {
        StoreError::NotFound(id) if id.contains("consumer group") => Status::not_found(id),
        StoreError::NotFound(id) => Status::not_found(format!("response not found: {id}")),
        StoreError::InvalidArgument(message) => Status::invalid_argument(message),
        StoreError::Unavailable(message) => Status::unavailable(message),
        StoreError::Storage(err) => {
            let kind = redis_error_kind(&err);
            Status::unavailable(format!("storage unavailable ({kind}): {err}"))
        }
        StoreError::Serialization(message) => Status::internal(message),
    }
}
