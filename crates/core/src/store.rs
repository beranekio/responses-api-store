use redis::{AsyncCommands, Script};
use serde_json::Value;

use crate::{
    error::{Result, StoreError},
    model::{
        is_deleted_tombstone, is_stale_enqueued, response_store_key, should_reconcile_stale,
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

        if is_deleted_tombstone(&stored) {
            return Err(StoreError::NotFound(response_id.to_string()));
        }

        if reconcile_stale {
            return self
                .reconcile_stale_response(response_id, &stored, self.stale_seconds)
                .await;
        }

        Ok(stored)
    }

    pub async fn update(
        &self,
        response_id: &str,
        response: &StoredResponse,
        ttl_seconds: Option<u64>,
    ) -> Result<()> {
        let Some(existing) = self.load(response_id).await? else {
            return Err(StoreError::NotFound(response_id.to_string()));
        };
        if is_deleted_tombstone(&existing) {
            return Err(StoreError::NotFound(response_id.to_string()));
        }
        self.store(response_id, response, ttl_seconds).await
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

        self.reconcile_stale_atomic(response_id, now, stale_seconds)
            .await
    }

    async fn reconcile_stale_atomic(
        &self,
        response_id: &str,
        now: i64,
        stale_seconds: i64,
    ) -> Result<StoredResponse> {
        let mut connection = self.connection.clone();
        let (reconciled, payload): (i32, String) = Script::new(RECONCILE_STALE_SCRIPT)
            .key(self.key(response_id))
            .arg(now)
            .arg(stale_seconds)
            .arg(response_id)
            .arg(self.default_ttl_seconds)
            .invoke_async(&mut connection)
            .await
            .map_err(StoreError::Storage)?;

        if payload.is_empty() {
            return Err(StoreError::NotFound(response_id.to_string()));
        }

        let updated =
            serde_json::from_str(&payload).map_err(|e| StoreError::Serialization(e.to_string()))?;

        if reconciled == 0 {
            return Ok(updated);
        }

        tracing::debug!(response_id, "reconciled stale queued background response");
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

const RECONCILE_STALE_SCRIPT: &str = r#"
local key = KEYS[1]
local now = tonumber(ARGV[1])
local stale_seconds = tonumber(ARGV[2])
local response_id = ARGV[3]
local default_ttl = tonumber(ARGV[4])

local raw = redis.call('GET', key)
if not raw then
  return {0, ''}
end

local ok, data = pcall(cjson.decode, raw)
if not ok then
  return {0, raw}
end

local response = data['response']
if type(response) ~= 'table' then
  return {0, raw}
end

if response['status'] ~= 'queued' then
  return {0, raw}
end

local in_flight = false
if data['pending_upstream_request'] ~= nil then
  in_flight = true
elseif response['status'] == 'queued' or response['status'] == 'in_progress' then
  in_flight = true
end

if not in_flight then
  return {0, raw}
end

local enqueued_at = data['enqueued_at']
if enqueued_at == nil then
  return {0, raw}
end

if (now - enqueued_at) < stale_seconds then
  return {0, raw}
end

data['response'] = {
  id = response_id,
  object = 'response',
  status = 'failed',
  background = true,
  error = {
    message = 'background response stale',
    type = 'server_error'
  }
}
data['pending_upstream_request'] = cjson.null
data['upstream_authorization'] = cjson.null

local updated = cjson.encode(data)
local ttl = redis.call('TTL', key)
if ttl < 0 then
  ttl = default_ttl
end
redis.call('SETEX', key, ttl, updated)
return {1, updated}
"#;
