#![allow(clippy::result_large_err)]

mod error;

pub use error::{ClientError, Result};
pub use responses_api_store_core::{
    build_cancelled_response, build_queued_response, build_upstream_request, generate_response_id,
    is_in_flight_background, response_id_from_value, stored_response_status, BackgroundJob,
    StoredResponse,
};
pub use responses_api_store_proto::v1::{
    AcknowledgeBackgroundJobRequest, ClaimBackgroundJobsRequest, DeleteResponseRequest,
    EnqueueBackgroundJobRequest, EnsureConsumerGroupRequest, GenerateResponseIdRequest,
    GetResponseRequest, HealthRequest, ReconcileStaleResponseRequest, StoreResponseRequest,
    UpdateResponseRequest,
};

use responses_api_store_proto::ResponsesApiStoreClient;
use tonic::transport::{Channel, Endpoint};

pub struct Client {
    inner: ResponsesApiStoreClient<Channel>,
}

impl Client {
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self> {
        let endpoint = Endpoint::from_shared(endpoint.into())?;
        let channel = endpoint.connect().await?;
        Ok(Self {
            inner: ResponsesApiStoreClient::new(channel),
        })
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: ResponsesApiStoreClient::new(channel),
        }
    }

    pub fn inner_mut(&mut self) -> &mut ResponsesApiStoreClient<Channel> {
        &mut self.inner
    }

    pub async fn health(&mut self) -> Result<responses_api_store_proto::v1::HealthResponse> {
        Ok(self.inner.health(HealthRequest {}).await?.into_inner())
    }

    pub async fn generate_response_id(&mut self) -> Result<String> {
        Ok(self
            .inner
            .generate_response_id(GenerateResponseIdRequest {})
            .await?
            .into_inner()
            .response_id)
    }

    pub async fn store_response(
        &mut self,
        response_id: &str,
        record: &StoredResponse,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        self.inner
            .store_response(StoreResponseRequest {
                record: Some(to_proto_record(response_id, record)?),
                ttl_seconds: ttl_seconds.unwrap_or(0),
            })
            .await?;
        Ok(())
    }

    pub async fn get_response(
        &mut self,
        response_id: &str,
        reconcile_stale: bool,
    ) -> Result<StoredResponse> {
        let response = self
            .inner
            .get_response(GetResponseRequest {
                response_id: response_id.to_string(),
                reconcile_stale,
            })
            .await?
            .into_inner();
        from_proto_record(response.record.as_ref(), response_id)
    }

    pub async fn update_response(
        &mut self,
        response_id: &str,
        record: &StoredResponse,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        self.inner
            .update_response(UpdateResponseRequest {
                record: Some(to_proto_record(response_id, record)?),
                ttl_seconds: ttl_seconds.unwrap_or(0),
            })
            .await?;
        Ok(())
    }

    pub async fn delete_response(&mut self, response_id: &str) -> Result<()> {
        self.inner
            .delete_response(DeleteResponseRequest {
                response_id: response_id.to_string(),
            })
            .await?;
        Ok(())
    }

    pub async fn enqueue_background_job(
        &mut self,
        response_id: &str,
        record: &StoredResponse,
    ) -> Result<()> {
        self.inner
            .enqueue_background_job(EnqueueBackgroundJobRequest {
                record: Some(to_proto_record(response_id, record)?),
            })
            .await?;
        Ok(())
    }

    pub async fn claim_background_jobs(
        &mut self,
        request: ClaimBackgroundJobsRequest,
    ) -> Result<Vec<BackgroundJob>> {
        let response = self
            .inner
            .claim_background_jobs(request)
            .await?
            .into_inner();
        response
            .jobs
            .into_iter()
            .map(|job| {
                let record = from_proto_record(job.record.as_ref(), &job.response_id)?;
                Ok(BackgroundJob {
                    stream_id: job.stream_id,
                    response_id: job.response_id,
                    record,
                    autoclaimed: job.autoclaimed,
                    idle_ms: job.idle_ms,
                })
            })
            .collect()
    }

    pub async fn acknowledge_background_job(
        &mut self,
        consumer_group: &str,
        stream_id: &str,
    ) -> Result<()> {
        self.inner
            .acknowledge_background_job(AcknowledgeBackgroundJobRequest {
                stream_id: stream_id.to_string(),
                consumer_group: consumer_group.to_string(),
            })
            .await?;
        Ok(())
    }

    pub async fn ensure_consumer_group(
        &mut self,
        consumer_group: &str,
        start_id: &str,
    ) -> Result<bool> {
        Ok(self
            .inner
            .ensure_consumer_group(EnsureConsumerGroupRequest {
                consumer_group: consumer_group.to_string(),
                start_id: start_id.to_string(),
            })
            .await?
            .into_inner()
            .created)
    }

    pub async fn reconcile_stale_response(
        &mut self,
        response_id: &str,
        stale_seconds: Option<i64>,
    ) -> Result<(StoredResponse, bool)> {
        let response = self
            .inner
            .reconcile_stale_response(ReconcileStaleResponseRequest {
                response_id: response_id.to_string(),
                stale_seconds: stale_seconds.unwrap_or(0),
            })
            .await?
            .into_inner();
        let record = from_proto_record(response.record.as_ref(), response_id)?;
        Ok((record, response.reconciled))
    }
}

fn to_proto_record(
    response_id: &str,
    record: &StoredResponse,
) -> Result<responses_api_store_proto::v1::StoredResponse> {
    let response_json = serde_json::to_string(&record.response)
        .map_err(|e| ClientError::Serialization(e.to_string()))?;
    let input_json = record
        .input
        .iter()
        .map(|item| {
            serde_json::to_string(item).map_err(|e| ClientError::Serialization(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let pending_upstream_request_json = match &record.pending_upstream_request {
        Some(value) => Some(
            serde_json::to_string(value).map_err(|e| ClientError::Serialization(e.to_string()))?,
        ),
        None => None,
    };

    Ok(responses_api_store_proto::v1::StoredResponse {
        response_id: response_id.to_string(),
        upstream: record.upstream.clone(),
        response_json,
        input_json,
        pending_upstream_request_json,
        upstream_authorization: record.upstream_authorization.clone(),
        enqueued_at: record.enqueued_at,
    })
}

fn from_proto_record(
    record: Option<&responses_api_store_proto::v1::StoredResponse>,
    response_id: &str,
) -> Result<StoredResponse> {
    let Some(record) = record else {
        return Err(ClientError::NotFound(response_id.to_string()));
    };
    let response = serde_json::from_str(&record.response_json)
        .map_err(|e| ClientError::Serialization(e.to_string()))?;
    let input = record
        .input_json
        .iter()
        .map(|item| {
            serde_json::from_str(item).map_err(|e| ClientError::Serialization(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;
    let pending_upstream_request = match &record.pending_upstream_request_json {
        Some(value) if !value.is_empty() => Some(
            serde_json::from_str(value).map_err(|e| ClientError::Serialization(e.to_string()))?,
        ),
        _ => None,
    };

    Ok(StoredResponse {
        upstream: record.upstream.clone(),
        response,
        input,
        pending_upstream_request,
        upstream_authorization: record.upstream_authorization.clone(),
        enqueued_at: record.enqueued_at,
    })
}
