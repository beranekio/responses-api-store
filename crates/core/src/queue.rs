use std::time::Duration;

use redis::{
    aio::ConnectionManager,
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamKey, StreamMaxlen,
        StreamReadOptions, StreamReadReply,
    },
    AsyncCommands, RedisError, RedisResult, Script,
};
use tokio::time::sleep;

use crate::{
    error::{Result, StoreError},
    model::{autoclaim_cursor_key, BackgroundJob},
    store::ResponseStore,
};

const LOAD_RETRY_ATTEMPTS: usize = 3;
const LOAD_RETRY_DELAY: Duration = Duration::from_millis(50);

#[derive(Clone)]
pub struct BackgroundQueue {
    command_connection: ConnectionManager,
    blocking_connection: ConnectionManager,
    stream_key: String,
    stream_maxlen: usize,
}

#[derive(Clone, Debug)]
pub struct ClaimOptions {
    pub consumer_group: String,
    pub consumer_name: String,
    pub count: usize,
    pub block_ms: usize,
    pub autoclaim_min_idle_ms: usize,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ClaimBatchResult {
    pub jobs: Vec<BackgroundJob>,
    pub pending_stream_ids: Vec<String>,
}

impl BackgroundQueue {
    pub async fn connect(
        redis_url: &str,
        stream_key: String,
        stream_maxlen: usize,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url).map_err(StoreError::Storage)?;
        let command_connection = ConnectionManager::new(client.clone())
            .await
            .map_err(StoreError::Storage)?;
        let blocking_connection = ConnectionManager::new(client)
            .await
            .map_err(StoreError::Storage)?;
        Ok(Self {
            command_connection,
            blocking_connection,
            stream_key,
            stream_maxlen,
        })
    }

    pub async fn enqueue(&self, response_id: &str) -> Result<()> {
        let mut connection = self.command_connection.clone();
        if self.stream_maxlen > 0 {
            connection
                .xadd_maxlen::<_, _, _, _, ()>(
                    &self.stream_key,
                    StreamMaxlen::Approx(self.stream_maxlen),
                    "*",
                    &[("response_id", response_id)],
                )
                .await
                .map_err(StoreError::Storage)?;
        } else {
            connection
                .xadd::<_, _, _, _, ()>(&self.stream_key, "*", &[("response_id", response_id)])
                .await
                .map_err(StoreError::Storage)?;
        }
        Ok(())
    }

    pub async fn ensure_consumer_group(
        &self,
        consumer_group: &str,
        start_id: &str,
    ) -> Result<bool> {
        let mut connection = self.command_connection.clone();
        let result: RedisResult<()> = connection
            .xgroup_create_mkstream(&self.stream_key, consumer_group, start_id)
            .await;
        match result {
            Ok(()) => Ok(true),
            Err(err) if is_busygroup(&err) => Ok(false),
            Err(err) => Err(StoreError::Storage(err)),
        }
    }

    pub async fn get_autoclaim_cursor(&self, consumer_group: &str) -> Result<String> {
        let mut connection = self.command_connection.clone();
        let key = autoclaim_cursor_key(&self.stream_key, consumer_group);
        let cursor: Option<String> = connection.get(&key).await.map_err(StoreError::Storage)?;
        Ok(cursor.unwrap_or_else(|| "0-0".to_string()))
    }

    pub async fn set_autoclaim_cursor(&self, consumer_group: &str, cursor: &str) -> Result<()> {
        let mut connection = self.command_connection.clone();
        let key = autoclaim_cursor_key(&self.stream_key, consumer_group);
        let _: i32 = Script::new(AUTOCLAIM_CURSOR_SCRIPT)
            .key(key)
            .arg(cursor)
            .invoke_async(&mut connection)
            .await
            .map_err(StoreError::Storage)?;
        Ok(())
    }

    pub async fn claim_jobs(
        &self,
        store: &ResponseStore,
        options: &ClaimOptions,
        autoclaim_cursor: &mut String,
    ) -> Result<ClaimBatchResult> {
        let mut result = ClaimBatchResult::default();

        if options.autoclaim_min_idle_ms > 0 && options.count > 0 {
            let autoclaim_count = options.count;
            let autoclaim = self
                .autoclaim(
                    &options.consumer_group,
                    &options.consumer_name,
                    options.autoclaim_min_idle_ms,
                    autoclaim_cursor,
                    autoclaim_count,
                )
                .await?;
            *autoclaim_cursor = autoclaim.next_stream_id;
            let batch = self
                .jobs_from_stream_ids(store, &options.consumer_group, &autoclaim.claimed, true)
                .await?;
            result.jobs.extend(batch.jobs);
            result.pending_stream_ids.extend(batch.pending_stream_ids);
        }

        let claimed = result.jobs.len() + result.pending_stream_ids.len();
        let remaining = options.count.saturating_sub(claimed);
        if remaining > 0 {
            let fill_block_ms = if has_claimed_entries(&result) {
                0
            } else {
                options.block_ms
            };
            let read = match self
                .read_group(
                    &options.consumer_group,
                    &options.consumer_name,
                    remaining,
                    fill_block_ms,
                )
                .await
            {
                Ok(read) => read,
                Err(err) if has_claimed_entries(&result) => {
                    tracing::warn!(
                        consumer_group = options.consumer_group,
                        error = %err,
                        "failed follow-up XREADGROUP; returning partial autoclaim batch"
                    );
                    return Ok(result);
                }
                Err(err) => return Err(err),
            };
            match self
                .jobs_from_stream_ids(store, &options.consumer_group, &read.ids, false)
                .await
            {
                Ok(batch) => {
                    result.jobs.extend(batch.jobs);
                    result.pending_stream_ids.extend(batch.pending_stream_ids);
                }
                Err(err) if has_claimed_entries(&result) => {
                    tracing::warn!(
                        consumer_group = options.consumer_group,
                        error = %err,
                        "failed hydrating follow-up XREADGROUP batch; returning partial autoclaim batch"
                    );
                }
                Err(err) => return Err(err),
            }
        }

        Ok(result)
    }

    pub async fn acknowledge(&self, consumer_group: &str, stream_id: &str) -> Result<()> {
        let mut connection = self.command_connection.clone();
        let acked: i32 = connection
            .xack(&self.stream_key, consumer_group, &[stream_id])
            .await
            .map_err(StoreError::Storage)?;
        if acked == 0 {
            return Err(StoreError::InvalidArgument(format!(
                "stream entry {stream_id} was not acknowledged for consumer group {consumer_group}"
            )));
        }
        Ok(())
    }

    async fn autoclaim(
        &self,
        consumer_group: &str,
        consumer_name: &str,
        min_idle_ms: usize,
        cursor: &str,
        count: usize,
    ) -> Result<StreamAutoClaimReply> {
        let mut connection = self.blocking_connection.clone();
        connection
            .xautoclaim_options(
                &self.stream_key,
                consumer_group,
                consumer_name,
                min_idle_ms,
                cursor,
                StreamAutoClaimOptions::default().count(count),
            )
            .await
            .map_err(StoreError::Storage)
    }

    async fn read_group(
        &self,
        consumer_group: &str,
        consumer_name: &str,
        count: usize,
        block_ms: usize,
    ) -> Result<StreamKey> {
        let mut connection = self.blocking_connection.clone();
        let opts = StreamReadOptions::default()
            .group(consumer_group, consumer_name)
            .count(count);
        let opts = if block_ms > 0 {
            opts.block(block_ms)
        } else {
            opts
        };

        let replies: StreamReadReply = connection
            .xread_options(&[&self.stream_key], &[">"], &opts)
            .await
            .map_err(StoreError::Storage)?;

        Ok(replies
            .keys
            .into_iter()
            .find(|key| key.key == self.stream_key)
            .unwrap_or(StreamKey {
                key: self.stream_key.clone(),
                ids: vec![],
            }))
    }

    async fn jobs_from_stream_ids(
        &self,
        store: &ResponseStore,
        consumer_group: &str,
        entries: &[StreamId],
        autoclaimed: bool,
    ) -> Result<ClaimBatchResult> {
        let mut jobs = Vec::with_capacity(entries.len());
        let mut pending_stream_ids = Vec::new();
        for entry in entries {
            let response_id = match entry.get::<String>("response_id") {
                Some(value) => value,
                None => continue,
            };

            let record = match self.load_with_retries(store, &response_id).await {
                Ok(Some(record)) => record,
                Ok(None) => {
                    let _ = self.acknowledge(consumer_group, &entry.id).await;
                    continue;
                }
                Err(StoreError::Serialization(err)) => {
                    tracing::warn!(
                        response_id,
                        stream_id = entry.id,
                        error = %err,
                        "skipping stream entry with corrupted stored response"
                    );
                    let _ = self.acknowledge(consumer_group, &entry.id).await;
                    continue;
                }
                Err(StoreError::Storage(err)) => {
                    tracing::warn!(
                        response_id,
                        stream_id = entry.id,
                        error = %err,
                        "deferring stream entry after transient load failure"
                    );
                    pending_stream_ids.push(entry.id.clone());
                    continue;
                }
                Err(err) => return Err(err),
            };

            jobs.push(BackgroundJob {
                stream_id: entry.id.clone(),
                response_id,
                record,
                autoclaimed,
                idle_ms: None,
            });
        }
        Ok(ClaimBatchResult {
            jobs,
            pending_stream_ids,
        })
    }

    async fn load_with_retries(
        &self,
        store: &ResponseStore,
        response_id: &str,
    ) -> Result<Option<crate::model::StoredResponse>> {
        let mut last_err = None;
        for attempt in 0..LOAD_RETRY_ATTEMPTS {
            match store.load(response_id).await {
                Ok(record) => return Ok(record),
                Err(StoreError::Storage(err)) => {
                    last_err = Some(err);
                    if attempt + 1 < LOAD_RETRY_ATTEMPTS {
                        sleep(LOAD_RETRY_DELAY).await;
                    }
                }
                Err(err) => return Err(err),
            }
        }
        Err(StoreError::Storage(
            last_err.expect("storage error after retries"),
        ))
    }
}

fn has_claimed_entries(result: &ClaimBatchResult) -> bool {
    !result.jobs.is_empty() || !result.pending_stream_ids.is_empty()
}

fn is_busygroup(err: &RedisError) -> bool {
    err.to_string().contains("BUSYGROUP")
}

const AUTOCLAIM_CURSOR_SCRIPT: &str = r#"
local key = KEYS[1]
local new_cursor = ARGV[1]
if new_cursor == '0-0' then
  redis.call('SET', key, new_cursor)
  return 1
end
local old_cursor = redis.call('GET', key)
if not old_cursor then
  redis.call('SET', key, new_cursor)
  return 1
end
local function split_id(id)
  local dash = id:find('-')
  if not dash then return tonumber(id), 0 end
  return tonumber(id:sub(1, dash - 1)), tonumber(id:sub(dash + 1))
end
local new_ms, new_seq = split_id(new_cursor)
local old_ms, old_seq = split_id(old_cursor)
if new_ms > old_ms or (new_ms == old_ms and new_seq > old_seq) then
  redis.call('SET', key, new_cursor)
  return 1
end
return 0
"#;
