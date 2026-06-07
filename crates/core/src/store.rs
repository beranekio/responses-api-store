use redis::AsyncCommands;
use serde_json::Value;

use crate::{
    error::{Result, StoreError},
    model::{
        is_stale_enqueued, response_store_key, should_reconcile_stale, stored_response_status,
        unix_seconds_now, StoredResponse,
    },
};

#[derive(Clone)]
pub struct ResponseStore {
    connection: redis::aio::ConnectionManager,
    key_prefix: String,
    default_ttl_seconds: u64,
    stale_seconds: i64,
}

impl ResponseStore {
    pub async fn connect(
        redis_url: &str,
        key_prefix: String,
        default_ttl_seconds: u64,
        stale_seconds: i64,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url).map_err(StoreError::Storage)?;
        let connection = redis::aio::ConnectionManager::new(client)
            .await
            .map_err(StoreError::Storage)?;
        Ok(Self {
            connection,
            key_prefix,
            default_ttl_seconds,
            stale_seconds,
        })
    }

    pub async fn ping(&self) -> Result<()> {
        let mut connection = self.connection.clone();
        redis::cmd("PING")
            .query_async::<()>(&mut connection)
            .await
            .map_err(StoreError::Storage)?;
        Ok(())
    }

    pub async fn store(
        &self,
        response_id: &str,
        response: &StoredResponse,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        let ttl = ttl_seconds.unwrap_or(self.default_ttl_seconds);
        if ttl == 0 {
            return Err(StoreError::InvalidArgument(
                "ttl_seconds must be greater than 0".to_string(),
            ));
        }

        let mut connection = self.connection.clone();
        let payload = serde_json::to_string(response)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        connection
            .set_ex::<_, _, ()>(self.key(response_id), payload, ttl)
            .await
            .map_err(StoreError::Storage)?;
        Ok(())
    }

    pub async fn load(&self, response_id: &str) -> Result<Option<StoredResponse>> {
        let mut connection = self.connection.clone();
        let response: Option<String> = connection
            .get(self.key(response_id))
            .await
            .map_err(StoreError::Storage)?;
        response
            .map(|response| {
                serde_json::from_str(&response)
                    .map_err(|e| StoreError::Serialization(e.to_string()))
            })
            .transpose()
    }

    pub async fn get(&self, response_id: &str, reconcile_stale: bool) -> Result<StoredResponse> {
        let Some(stored) = self.load(response_id).await? else {
            return Err(StoreError::NotFound(response_id.to_string()));
        };

        if stored_response_status(&stored) == Some("deleted") {
            return Err(StoreError::NotFound(response_id.to_string()));
        }

        if reconcile_stale {
            return self
                .reconcile_stale_response(response_id, &stored, self.stale_seconds)
                .await;
        }

        Ok(stored)
    }

    pub async fn delete(&self, response_id: &str) -> Result<()> {
        let mut connection = self.connection.clone();
        connection
            .del::<_, ()>(self.key(response_id))
            .await
            .map_err(StoreError::Storage)?;
        Ok(())
    }

    pub async fn reconcile_stale_response(
        &self,
        response_id: &str,
        stored: &StoredResponse,
        stale_seconds: i64,
    ) -> Result<StoredResponse> {
        if !should_reconcile_stale(stored) {
            return Ok(stored.clone());
        }

        let now = unix_seconds_now();
        if !is_stale_enqueued(stored.enqueued_at, now, stale_seconds) {
            return Ok(stored.clone());
        }

        let mut updated = stored.clone();
        updated.response = serde_json::json!({
            "id": response_id,
            "object": "response",
            "status": "failed",
            "background": true,
            "error": {
                "message": "background response stale",
                "type": "server_error"
            }
        });
        updated.pending_upstream_request = None;
        updated.upstream_authorization = None;

        self.store(response_id, &updated, None).await?;
        Ok(updated)
    }

    pub async fn tombstone_deleted_background(
        &self,
        response_id: &str,
        stored: &StoredResponse,
    ) -> Result<()> {
        let mut tombstone = stored.clone();
        tombstone.response = serde_json::json!({
            "id": response_id,
            "object": "response",
            "status": "deleted",
            "background": true,
            "deleted": true
        });
        tombstone.pending_upstream_request = None;
        tombstone.upstream_authorization = None;
        self.store(response_id, &tombstone, None).await
    }

    pub fn build_stored_from_parts(
        upstream: String,
        response: Value,
        input: Vec<Value>,
        pending_upstream_request: Option<Value>,
        upstream_authorization: Option<String>,
        enqueued_at: Option<i64>,
    ) -> StoredResponse {
        StoredResponse {
            upstream,
            response,
            input,
            pending_upstream_request,
            upstream_authorization,
            enqueued_at,
        }
    }

    fn key(&self, response_id: &str) -> String {
        response_store_key(&self.key_prefix, response_id)
    }
}
