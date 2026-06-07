#![allow(clippy::result_large_err)]

use responses_api_store_core::{
    generate_response_id, is_in_flight_background, unix_seconds_now, BackgroundQueue, ClaimOptions,
    ResponseStore, StoreConfig, StoreError,
};
use responses_api_store_proto::v1::{
    responses_api_store_server::ResponsesApiStore, AcknowledgeBackgroundJobRequest,
    AcknowledgeBackgroundJobResponse, ClaimBackgroundJobsRequest, ClaimBackgroundJobsResponse,
    DeleteResponseRequest, DeleteResponseResponse, EnqueueBackgroundJobRequest,
    EnqueueBackgroundJobResponse, EnsureConsumerGroupRequest, EnsureConsumerGroupResponse,
    GenerateResponseIdRequest, GenerateResponseIdResponse, GetResponseRequest, GetResponseResponse,
    HealthRequest, HealthResponse, ReconcileStaleResponseRequest, ReconcileStaleResponseResponse,
    StoreResponseRequest, StoreResponseResponse, UpdateResponseRequest, UpdateResponseResponse,
};
use tonic::{Request, Response, Status};

use crate::convert::{core_job_to_proto, core_to_proto, map_store_error, proto_to_core};

const MAX_CLAIM_COUNT: usize = 100;

pub struct ResponsesApiStoreService {
    store: ResponseStore,
    queue: BackgroundQueue,
    default_stale_seconds: i64,
}

impl ResponsesApiStoreService {
    pub async fn new(config: StoreConfig) -> Result<Self, StoreError> {
        let store = ResponseStore::connect(
            &config.redis_url,
            config.key_prefix.clone(),
            config.ttl_seconds,
            config.stale_seconds,
        )
        .await?;
        let queue = BackgroundQueue::connect(
            &config.redis_url,
            config.stream_key.clone(),
            config.stream_maxlen,
        )
        .await?;
        Ok(Self {
            store,
            queue,
            default_stale_seconds: config.stale_seconds,
        })
    }

    fn ttl_from_request(&self, ttl_seconds: u64) -> Option<u64> {
        if ttl_seconds == 0 {
            None
        } else {
            Some(ttl_seconds)
        }
    }

    fn require_record(
        record: &Option<responses_api_store_proto::v1::StoredResponse>,
    ) -> Result<&responses_api_store_proto::v1::StoredResponse, Status> {
        record
            .as_ref()
            .filter(|record| !record.response_id.is_empty())
            .ok_or_else(|| Status::invalid_argument("record.response_id is required"))
    }
}

#[tonic::async_trait]
impl ResponsesApiStore for ResponsesApiStoreService {
    async fn store_response(
        &self,
        request: Request<StoreResponseRequest>,
    ) -> Result<Response<StoreResponseResponse>, Status> {
        let request = request.into_inner();
        let record = Self::require_record(&request.record)?;
        let core = proto_to_core(record)?;
        self.store
            .store(
                &record.response_id,
                &core,
                self.ttl_from_request(request.ttl_seconds),
            )
            .await
            .map_err(map_store_error)?;
        Ok(Response::new(StoreResponseResponse {}))
    }

    async fn get_response(
        &self,
        request: Request<GetResponseRequest>,
    ) -> Result<Response<GetResponseResponse>, Status> {
        let request = request.into_inner();
        if request.response_id.is_empty() {
            return Err(Status::invalid_argument("response_id is required"));
        }

        let record = self
            .store
            .get(&request.response_id, request.reconcile_stale)
            .await
            .map_err(map_store_error)?;
        Ok(Response::new(GetResponseResponse {
            record: Some(core_to_proto(&request.response_id, &record)?),
        }))
    }

    async fn update_response(
        &self,
        request: Request<UpdateResponseRequest>,
    ) -> Result<Response<UpdateResponseResponse>, Status> {
        let request = request.into_inner();
        let record = Self::require_record(&request.record)?;
        let core = proto_to_core(record)?;
        self.store
            .update(
                &record.response_id,
                &core,
                self.ttl_from_request(request.ttl_seconds),
            )
            .await
            .map_err(map_store_error)?;
        Ok(Response::new(UpdateResponseResponse {}))
    }

    async fn delete_response(
        &self,
        request: Request<DeleteResponseRequest>,
    ) -> Result<Response<DeleteResponseResponse>, Status> {
        let request = request.into_inner();
        if request.response_id.is_empty() {
            return Err(Status::invalid_argument("response_id is required"));
        }

        if let Ok(stored) = self.store.get(&request.response_id, false).await {
            if is_in_flight_background(&stored) {
                self.store
                    .tombstone_deleted_background(&request.response_id, &stored)
                    .await
                    .map_err(map_store_error)?;
                return Ok(Response::new(DeleteResponseResponse {}));
            }
        }

        self.store
            .delete(&request.response_id)
            .await
            .map_err(map_store_error)?;
        Ok(Response::new(DeleteResponseResponse {}))
    }

    async fn enqueue_background_job(
        &self,
        request: Request<EnqueueBackgroundJobRequest>,
    ) -> Result<Response<EnqueueBackgroundJobResponse>, Status> {
        let request = request.into_inner();
        let record = Self::require_record(&request.record)?;
        let mut core = proto_to_core(record)?;
        core.enqueued_at = Some(unix_seconds_now());

        self.store
            .store(&record.response_id, &core, None)
            .await
            .map_err(map_store_error)?;
        self.queue
            .enqueue(&record.response_id)
            .await
            .map_err(map_store_error)?;

        Ok(Response::new(EnqueueBackgroundJobResponse {}))
    }

    async fn claim_background_jobs(
        &self,
        request: Request<ClaimBackgroundJobsRequest>,
    ) -> Result<Response<ClaimBackgroundJobsResponse>, Status> {
        let request = request.into_inner();
        if request.consumer_group.is_empty() || request.consumer_name.is_empty() {
            return Err(Status::invalid_argument(
                "consumer_group and consumer_name are required",
            ));
        }

        let count = (request.count.max(1) as usize).min(MAX_CLAIM_COUNT);
        let consumer_group = request.consumer_group.clone();
        let mut cursor = self
            .queue
            .get_autoclaim_cursor(&consumer_group)
            .await
            .map_err(map_store_error)?;

        let batch = self
            .queue
            .claim_jobs(
                &self.store,
                &ClaimOptions {
                    consumer_group: request.consumer_group,
                    consumer_name: request.consumer_name,
                    count,
                    block_ms: request.block_ms as usize,
                    autoclaim_min_idle_ms: request.autoclaim_min_idle_ms as usize,
                },
                &mut cursor,
            )
            .await
            .map_err(map_store_error)?;

        self.queue
            .set_autoclaim_cursor(&consumer_group, &cursor)
            .await
            .map_err(map_store_error)?;

        let jobs = batch
            .jobs
            .into_iter()
            .map(|job| core_job_to_proto(&job))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Response::new(ClaimBackgroundJobsResponse {
            jobs,
            pending_stream_ids: batch.pending_stream_ids,
        }))
    }

    async fn acknowledge_background_job(
        &self,
        request: Request<AcknowledgeBackgroundJobRequest>,
    ) -> Result<Response<AcknowledgeBackgroundJobResponse>, Status> {
        let request = request.into_inner();
        if request.stream_id.is_empty() || request.consumer_group.is_empty() {
            return Err(Status::invalid_argument(
                "stream_id and consumer_group are required",
            ));
        }

        self.queue
            .acknowledge(&request.consumer_group, &request.stream_id)
            .await
            .map_err(map_store_error)?;
        Ok(Response::new(AcknowledgeBackgroundJobResponse {}))
    }

    async fn ensure_consumer_group(
        &self,
        request: Request<EnsureConsumerGroupRequest>,
    ) -> Result<Response<EnsureConsumerGroupResponse>, Status> {
        let request = request.into_inner();
        if request.consumer_group.is_empty() {
            return Err(Status::invalid_argument("consumer_group is required"));
        }
        let start_id = if request.start_id.is_empty() {
            "0-0"
        } else {
            &request.start_id
        };
        let created = self
            .queue
            .ensure_consumer_group(&request.consumer_group, start_id)
            .await
            .map_err(map_store_error)?;
        Ok(Response::new(EnsureConsumerGroupResponse { created }))
    }

    async fn reconcile_stale_response(
        &self,
        request: Request<ReconcileStaleResponseRequest>,
    ) -> Result<Response<ReconcileStaleResponseResponse>, Status> {
        let request = request.into_inner();
        if request.response_id.is_empty() {
            return Err(Status::invalid_argument("response_id is required"));
        }

        let stale_seconds = if request.stale_seconds > 0 {
            request.stale_seconds
        } else {
            self.default_stale_seconds
        };

        let existing = self
            .store
            .get(&request.response_id, false)
            .await
            .map_err(map_store_error)?;
        let before = existing.clone();
        let updated = self
            .store
            .reconcile_stale_response(&request.response_id, &existing, stale_seconds)
            .await
            .map_err(map_store_error)?;

        Ok(Response::new(ReconcileStaleResponseResponse {
            record: Some(core_to_proto(&request.response_id, &updated)?),
            reconciled: before != updated,
        }))
    }

    async fn generate_response_id(
        &self,
        _request: Request<GenerateResponseIdRequest>,
    ) -> Result<Response<GenerateResponseIdResponse>, Status> {
        Ok(Response::new(GenerateResponseIdResponse {
            response_id: generate_response_id(),
        }))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let redis_ok = self.store.ping().await.is_ok();
        Ok(Response::new(HealthResponse {
            redis_ok,
            version: responses_api_store_core::service_version().to_string(),
        }))
    }
}
