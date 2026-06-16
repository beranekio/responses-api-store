use std::sync::Arc;

use redis::{aio::ConnectionManager, AsyncCommands};
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tokio::sync::OnceCell;

use crate::{
    error::{Result, StoreError},
    model::{
        is_claimable_background, is_deleted_tombstone, is_in_progress_background,
        is_stale_enqueued, is_terminal_background_status, response_store_key,
        should_reconcile_stale, unix_seconds_now, StoredResponse,
    },
};

#[derive(Clone, Debug, PartialEq)]
pub struct ClaimBackgroundPayload {
    pub upstream: String,
    pub pending_upstream_request: Value,
    pub upstream_authorization: Option<String>,
}

#[derive(Clone)]
pub struct ResponseStore {
    connection: ConnectionManager,
    redis_client: redis::Client,
    /// Exclusive reconnecting connection for WATCH/MULTI/EXEC; must not be cloned outside the mutex.
    transaction_connection: Arc<Mutex<ConnectionManager>>,
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
        let connection = ConnectionManager::new(client.clone())
            .await
            .map_err(StoreError::Storage)?;
        let transaction_connection = ConnectionManager::new(client.clone())
            .await
            .map_err(StoreError::Storage)?;
        Ok(Self {
            connection,
            redis_client: client,
            transaction_connection: Arc::new(Mutex::new(transaction_connection)),
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

    pub async fn redis_server_info(&self) -> Result<(String, bool)> {
        let capabilities = load_redis_capabilities(&self.redis_client).await?;
        Ok((capabilities.version, capabilities.lag_supported))
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
        const MAX_ATTEMPTS: u32 = 16;
        let key = self.key(response_id);

        for attempt in 0..MAX_ATTEMPTS {
            let attempt_result = {
                let mut guard = self.transaction_connection.lock().await;
                discard_open_transaction(&mut guard).await;
                self.reconcile_stale_transaction_attempt(
                    &mut guard,
                    &key,
                    response_id,
                    now,
                    stale_seconds,
                )
                .await
            };

            match attempt_result {
                Ok(Some(updated)) => return Ok(updated),
                Ok(None) => {
                    if attempt + 1 == MAX_ATTEMPTS {
                        break;
                    }
                }
                Err(StoreError::Storage(err)) => {
                    tracing::warn!(
                        response_id,
                        error = %err,
                        "stale reconcile transaction failed; resetting dedicated connection"
                    );
                    self.reset_transaction_connection().await?;
                    if attempt + 1 == MAX_ATTEMPTS {
                        return Err(StoreError::Storage(err));
                    }
                }
                Err(err) => return Err(err),
            }
        }

        let reloaded = self
            .load(response_id)
            .await?
            .ok_or_else(|| StoreError::NotFound(response_id.to_string()))?;
        Ok(reloaded)
    }

    pub async fn claim_background_response(
        &self,
        response_id: &str,
    ) -> Result<(StoredResponse, ClaimBackgroundPayload)> {
        self.transition_atomic(response_id, prepare_claim_background)
            .await
    }

    pub async fn complete_background_response(
        &self,
        response_id: &str,
        completed_response: Value,
    ) -> Result<StoredResponse> {
        let response = completed_response;
        let (record, _) = self
            .transition_atomic(response_id, move |stored, id| {
                prepare_complete_background(stored, id, &response)
            })
            .await?;
        Ok(record)
    }

    pub async fn fail_background_response(
        &self,
        response_id: &str,
        error_message: &str,
    ) -> Result<StoredResponse> {
        let message = error_message.to_string();
        let (record, _) = self
            .transition_atomic(response_id, move |stored, id| {
                prepare_fail_background(stored, id, &message)
            })
            .await?;
        Ok(record)
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

    async fn reset_transaction_connection(&self) -> Result<()> {
        let connection = ConnectionManager::new(self.redis_client.clone())
            .await
            .map_err(StoreError::Storage)?;
        let mut guard = self.transaction_connection.lock().await;
        *guard = connection;
        Ok(())
    }

    async fn transition_atomic<T, F>(
        &self,
        response_id: &str,
        prepare: F,
    ) -> Result<(StoredResponse, T)>
    where
        F: Fn(&StoredResponse, &str) -> std::result::Result<(StoredResponse, T), StoreError>,
    {
        const MAX_ATTEMPTS: u32 = 16;
        let key = self.key(response_id);

        for attempt in 0..MAX_ATTEMPTS {
            let attempt_result = {
                let mut guard = self.transaction_connection.lock().await;
                discard_open_transaction(&mut guard).await;
                self.transition_attempt(&mut guard, &key, response_id, &prepare)
                    .await
            };

            match attempt_result {
                Ok(Some((record, result))) => return Ok((record, result)),
                Ok(None) => {
                    if attempt + 1 == MAX_ATTEMPTS {
                        break;
                    }
                }
                Err(StoreError::Storage(err)) => {
                    tracing::warn!(
                        response_id,
                        error = %err,
                        "background transition transaction failed; resetting dedicated connection"
                    );
                    self.reset_transaction_connection().await?;
                    if attempt + 1 == MAX_ATTEMPTS {
                        return Err(StoreError::Storage(err));
                    }
                }
                Err(err) => return Err(err),
            }
        }

        Err(StoreError::Unavailable(format!(
            "background transition conflict retries exhausted for {response_id}"
        )))
    }

    async fn transition_attempt<T, F>(
        &self,
        connection: &mut ConnectionManager,
        key: &str,
        response_id: &str,
        prepare: &F,
    ) -> Result<Option<(StoredResponse, T)>>
    where
        F: Fn(&StoredResponse, &str) -> std::result::Result<(StoredResponse, T), StoreError>,
    {
        redis::cmd("WATCH")
            .arg(key)
            .query_async::<()>(connection)
            .await
            .map_err(StoreError::Storage)?;

        let raw: Option<String> = connection.get(key).await.map_err(StoreError::Storage)?;
        let Some(raw) = raw else {
            let _ = redis::cmd("UNWATCH").query_async::<()>(connection).await;
            return Err(StoreError::NotFound(response_id.to_string()));
        };

        let stored: StoredResponse =
            serde_json::from_str(&raw).map_err(|e| StoreError::Serialization(e.to_string()))?;
        let (updated, result) = prepare(&stored, response_id)?;
        let payload = serde_json::to_string(&updated)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;
        let ttl: i64 = connection.ttl(key).await.map_err(StoreError::Storage)?;
        let ttl = reconcile_write_ttl(ttl, self.default_ttl_seconds);

        redis::cmd("MULTI")
            .query_async::<()>(connection)
            .await
            .map_err(StoreError::Storage)?;
        redis::cmd("SETEX")
            .arg(key)
            .arg(ttl)
            .arg(&payload)
            .query_async::<()>(connection)
            .await
            .map_err(StoreError::Storage)?;

        let exec_result: Option<Vec<redis::Value>> = redis::cmd("EXEC")
            .query_async(connection)
            .await
            .map_err(StoreError::Storage)?;

        if exec_result.is_some() {
            return Ok(Some((updated, result)));
        }

        Ok(None)
    }

    async fn reconcile_stale_transaction_attempt(
        &self,
        connection: &mut ConnectionManager,
        key: &str,
        response_id: &str,
        now: i64,
        stale_seconds: i64,
    ) -> Result<Option<StoredResponse>> {
        redis::cmd("WATCH")
            .arg(key)
            .query_async::<()>(connection)
            .await
            .map_err(StoreError::Storage)?;

        let raw: Option<String> = connection.get(key).await.map_err(StoreError::Storage)?;

        let Some(raw) = raw else {
            let _ = redis::cmd("UNWATCH").query_async::<()>(connection).await;
            return Err(StoreError::NotFound(response_id.to_string()));
        };

        let stored: StoredResponse =
            serde_json::from_str(&raw).map_err(|e| StoreError::Serialization(e.to_string()))?;

        if !should_reconcile_stale(&stored)
            || !is_stale_enqueued(stored.enqueued_at, now, stale_seconds)
        {
            let _ = redis::cmd("UNWATCH").query_async::<()>(connection).await;
            return Ok(Some(stored));
        }

        let mut updated = stored;
        apply_stale_failure(&mut updated, response_id);
        let payload = serde_json::to_string(&updated)
            .map_err(|e| StoreError::Serialization(e.to_string()))?;

        let ttl: i64 = connection.ttl(key).await.map_err(StoreError::Storage)?;
        let ttl = reconcile_write_ttl(ttl, self.default_ttl_seconds);

        redis::cmd("MULTI")
            .query_async::<()>(connection)
            .await
            .map_err(StoreError::Storage)?;
        redis::cmd("SETEX")
            .arg(key)
            .arg(ttl)
            .arg(&payload)
            .query_async::<()>(connection)
            .await
            .map_err(StoreError::Storage)?;

        let exec_result: Option<Vec<redis::Value>> = redis::cmd("EXEC")
            .query_async(connection)
            .await
            .map_err(StoreError::Storage)?;

        if exec_result.is_some() {
            tracing::debug!(response_id, "reconciled stale queued background response");
            return Ok(Some(updated));
        }

        Ok(None)
    }
}

fn prepare_claim_background(
    stored: &StoredResponse,
    response_id: &str,
) -> std::result::Result<(StoredResponse, ClaimBackgroundPayload), StoreError> {
    if is_terminal_background_status(stored) {
        return Err(StoreError::FailedPrecondition(format!(
            "response {response_id} is in a terminal state"
        )));
    }
    if is_in_progress_background(stored) {
        return Err(StoreError::FailedPrecondition(format!(
            "response {response_id} is already in progress"
        )));
    }
    if !is_claimable_background(stored) {
        return Err(StoreError::FailedPrecondition(format!(
            "response {response_id} is not claimable"
        )));
    }

    let payload = ClaimBackgroundPayload {
        upstream: stored.upstream.clone(),
        pending_upstream_request: stored
            .pending_upstream_request
            .clone()
            .expect("claimable response has pending upstream request"),
        upstream_authorization: stored.upstream_authorization.clone(),
    };

    let mut updated = stored.clone();
    updated.pending_upstream_request = None;
    updated.upstream_authorization = None;
    updated.response["status"] = Value::String("in_progress".to_string());
    updated.response["id"] = Value::String(response_id.to_string());
    updated.response["background"] = Value::Bool(true);

    Ok((updated, payload))
}

fn prepare_complete_background(
    stored: &StoredResponse,
    response_id: &str,
    completed_response: &Value,
) -> std::result::Result<(StoredResponse, ()), StoreError> {
    if is_terminal_background_status(stored) {
        return Err(StoreError::FailedPrecondition(format!(
            "response {response_id} is in a terminal state"
        )));
    }
    if !is_in_progress_background(stored) {
        return Err(StoreError::FailedPrecondition(format!(
            "response {response_id} is not in progress"
        )));
    }

    let mut updated = stored.clone();
    updated.response = completed_response.clone();
    updated.response["id"] = Value::String(response_id.to_string());
    if updated.response.get("status").is_none() {
        updated.response["status"] = Value::String("completed".to_string());
    }
    updated.pending_upstream_request = None;
    updated.upstream_authorization = None;

    Ok((updated, ()))
}

fn prepare_fail_background(
    stored: &StoredResponse,
    response_id: &str,
    error_message: &str,
) -> std::result::Result<(StoredResponse, ()), StoreError> {
    if is_terminal_background_status(stored) {
        return Err(StoreError::FailedPrecondition(format!(
            "response {response_id} is in a terminal state"
        )));
    }

    let claimable = is_claimable_background(stored);
    let in_progress = is_in_progress_background(stored);
    if !claimable && !in_progress {
        return Err(StoreError::FailedPrecondition(format!(
            "response {response_id} is not in progress"
        )));
    }

    let mut updated = stored.clone();
    updated.response = json!({
        "id": response_id,
        "object": "response",
        "status": "failed",
        "background": true,
        "error": {
            "message": error_message,
            "type": "server_error"
        }
    });
    updated.pending_upstream_request = None;
    updated.upstream_authorization = None;

    Ok((updated, ()))
}

async fn discard_open_transaction(connection: &mut ConnectionManager) {
    let _ = redis::cmd("DISCARD").query_async::<()>(connection).await;
}

fn reconcile_write_ttl(redis_ttl: i64, default_ttl_seconds: u64) -> u64 {
    match redis_ttl {
        0 => 1,
        -1 => default_ttl_seconds,
        t if t > 0 => t as u64,
        _ => default_ttl_seconds,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RedisCapabilities {
    pub version: String,
    pub lag_supported: bool,
    pub exclusive_range_supported: bool,
}

static REDIS_CAPABILITIES: OnceCell<RedisCapabilities> = OnceCell::const_new();

pub async fn load_redis_capabilities(client: &redis::Client) -> Result<RedisCapabilities> {
    REDIS_CAPABILITIES
        .get_or_try_init(|| async {
            let mut connection = ConnectionManager::new(client.clone())
                .await
                .map_err(StoreError::Storage)?;
            let info: String = redis::cmd("INFO")
                .arg("server")
                .query_async(&mut connection)
                .await
                .map_err(StoreError::Storage)?;
            let version = parse_redis_version(&info).unwrap_or_default();
            Ok(RedisCapabilities {
                version: version.clone(),
                lag_supported: redis_version_supports_lag(&version),
                exclusive_range_supported: redis_version_supports_exclusive_range(&version),
            })
        })
        .await
        .cloned()
}

fn parse_redis_version(info: &str) -> Option<String> {
    for line in info.lines() {
        if let Some(version) = line.strip_prefix("redis_version:") {
            return Some(version.trim().to_string());
        }
    }
    None
}

pub fn redis_version_supports_lag(version: &str) -> bool {
    redis_version_at_least(version, 7, 0)
}

pub fn redis_version_supports_exclusive_range(version: &str) -> bool {
    redis_version_at_least(version, 6, 2)
}

fn redis_version_at_least(version: &str, min_major: u32, min_minor: u32) -> bool {
    let mut parts = version.split('.');
    let major = parts.next().and_then(|part| part.parse::<u32>().ok());
    let minor = parts.next().and_then(|part| part.parse::<u32>().ok());
    match (major, minor) {
        (Some(major), Some(minor)) => {
            major > min_major || (major == min_major && minor >= min_minor)
        }
        (Some(major), None) => major > min_major,
        _ => false,
    }
}

fn apply_stale_failure(stored: &mut StoredResponse, response_id: &str) {
    stored.response = json!({
        "id": response_id,
        "object": "response",
        "status": "failed",
        "background": true,
        "error": {
            "message": "background response stale",
            "type": "server_error"
        }
    });
    stored.pending_upstream_request = None;
    stored.upstream_authorization = None;
}

#[cfg(test)]
mod tests {
    use super::reconcile_write_ttl;

    #[test]
    fn reconcile_write_ttl_preserves_positive_remaining_ttl() {
        assert_eq!(reconcile_write_ttl(300, 86_400), 300);
    }

    #[test]
    fn reconcile_write_ttl_uses_minimal_ttl_when_expiring_imminently() {
        assert_eq!(reconcile_write_ttl(0, 86_400), 1);
    }

    #[test]
    fn reconcile_write_ttl_applies_default_when_key_has_no_expiry() {
        assert_eq!(reconcile_write_ttl(-1, 86_400), 86_400);
    }

    #[test]
    fn redis_version_supports_lag_from_major_version() {
        assert!(super::redis_version_supports_lag("7.0.15"));
        assert!(super::redis_version_supports_lag("9.1.0"));
        assert!(!super::redis_version_supports_lag("6.2.14"));
        assert!(!super::redis_version_supports_lag(""));
    }

    #[test]
    fn redis_version_supports_exclusive_range_from_minor_version() {
        assert!(super::redis_version_supports_exclusive_range("6.2.14"));
        assert!(super::redis_version_supports_exclusive_range("7.0.0"));
        assert!(!super::redis_version_supports_exclusive_range("6.1.0"));
        assert!(!super::redis_version_supports_exclusive_range("5.0.14"));
    }
}
