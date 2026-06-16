use std::time::Duration;

use redis::{
    aio::ConnectionManager,
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamInfoGroup,
        StreamInfoGroupsReply, StreamKey, StreamMaxlen, StreamReadOptions, StreamReadReply,
    },
    AsyncCommands, AsyncConnectionConfig, RedisError, RedisResult, Script,
};
use tokio::time::sleep;

use crate::{
    error::{Result, StoreError},
    model::{autoclaim_cursor_key, BackgroundJob, PendingBackgroundJob},
    store::ResponseStore,
};

const LOAD_RETRY_ATTEMPTS: usize = 3;
const LOAD_RETRY_DELAY: Duration = Duration::from_millis(50);
/// Extra client response-timeout headroom above `block_ms` for blocking XREADGROUP.
const BLOCKING_READ_TIMEOUT_SLACK_MS: u64 = 500;

#[derive(Clone)]
pub struct BackgroundQueue {
    client: redis::Client,
    command_connection: ConnectionManager,
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
    pub pending_jobs: Vec<PendingBackgroundJob>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackgroundQueueStats {
    /// Jobs waiting to be claimed by a worker.
    pub pending: u64,
    /// Jobs claimed but not yet acknowledged (in-flight).
    pub in_progress: u64,
    /// Scalar for HPA/KEDA (pending + in_progress).
    pub workload: u64,
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
        Ok(Self {
            client,
            command_connection,
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
                .jobs_from_stream_ids(
                    store,
                    &options.consumer_group,
                    &autoclaim.claimed,
                    true,
                    Some(options.autoclaim_min_idle_ms),
                )
                .await?;
            result.jobs.extend(batch.jobs);
            result.pending_stream_ids.extend(batch.pending_stream_ids);
            result.pending_jobs.extend(batch.pending_jobs);
        }

        let claimed = result.jobs.len() + result.pending_jobs.len();
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
                .jobs_from_stream_ids(store, &options.consumer_group, &read.ids, false, None)
                .await
            {
                Ok(batch) => {
                    result.jobs.extend(batch.jobs);
                    result.pending_stream_ids.extend(batch.pending_stream_ids);
                    result.pending_jobs.extend(batch.pending_jobs);
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

    pub async fn stats(&self, consumer_group: &str) -> Result<BackgroundQueueStats> {
        let mut connection = self.command_connection.clone();
        if !self.stream_exists(&mut connection).await? {
            return Ok(BackgroundQueueStats::default());
        }

        let mut info = self.load_stream_groups(&mut connection).await?;
        if self.find_group(&info, consumer_group).is_none() && info.groups.is_empty() {
            // Cold start only: stream has entries but no consumer groups yet.
            self.ensure_consumer_group(consumer_group, "0-0").await?;
            info = self.load_stream_groups(&mut connection).await?;
        }
        let group = self.find_group(&info, consumer_group).ok_or_else(|| {
            StoreError::NotFound(format!(
                "consumer group {consumer_group} not found for stream {}",
                self.stream_key
            ))
        })?;
        self.stats_from_group(consumer_group, group)
    }

    async fn stream_exists<C>(&self, connection: &mut C) -> Result<bool>
    where
        C: AsyncCommands + Send,
    {
        connection
            .exists(&self.stream_key)
            .await
            .map_err(StoreError::Storage)
    }

    async fn load_stream_groups<C>(&self, connection: &mut C) -> Result<StreamInfoGroupsReply>
    where
        C: AsyncCommands + Send,
    {
        match connection.xinfo_groups(&self.stream_key).await {
            Ok(info) => Ok(info),
            Err(err) if is_missing_stream(&err) => Ok(StreamInfoGroupsReply::default()),
            Err(err) => Err(StoreError::Storage(err)),
        }
    }

    fn find_group<'a>(
        &'a self,
        info: &'a StreamInfoGroupsReply,
        consumer_group: &str,
    ) -> Option<&'a StreamInfoGroup> {
        info.groups
            .iter()
            .find(|group| group.name == consumer_group)
    }

    fn stats_from_group(
        &self,
        consumer_group: &str,
        group: &StreamInfoGroup,
    ) -> Result<BackgroundQueueStats> {
        let pending = match group.lag {
            Some(lag) => lag as u64,
            None => {
                return Err(StoreError::Unavailable(format!(
                    "consumer group {consumer_group} lag is unavailable; stream entries may have been trimmed between last-delivered-id and tail"
                )));
            }
        };
        let in_progress = group.pending as u64;
        Ok(BackgroundQueueStats {
            pending,
            in_progress,
            workload: pending.saturating_add(in_progress),
        })
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
        let mut connection = self.command_connection.clone();
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
        if block_ms == 0 {
            let mut connection = self.command_connection.clone();
            return self
                .read_group_with_connection(
                    &mut connection,
                    consumer_group,
                    consumer_name,
                    count,
                    block_ms,
                )
                .await;
        }

        // Blocking commands must not share a multiplexed connection: each concurrent
        // ClaimBackgroundJobs call opens its own connection for the duration of XREADGROUP.
        let response_timeout =
            Duration::from_millis(block_ms as u64 + BLOCKING_READ_TIMEOUT_SLACK_MS);
        let config = AsyncConnectionConfig::new().set_response_timeout(Some(response_timeout));
        let mut connection = self
            .client
            .get_multiplexed_async_connection_with_config(&config)
            .await
            .map_err(StoreError::Storage)?;
        self.read_group_with_connection(
            &mut connection,
            consumer_group,
            consumer_name,
            count,
            block_ms,
        )
        .await
    }

    async fn read_group_with_connection<C>(
        &self,
        connection: &mut C,
        consumer_group: &str,
        consumer_name: &str,
        count: usize,
        block_ms: usize,
    ) -> Result<StreamKey>
    where
        C: AsyncCommands + Send,
    {
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
        autoclaim_min_idle_ms: Option<usize>,
    ) -> Result<ClaimBatchResult> {
        let idle_ms = autoclaimed
            .then(|| autoclaim_min_idle_ms.map(|ms| ms as u64))
            .flatten();
        let mut jobs = Vec::with_capacity(entries.len());
        let mut pending_stream_ids = Vec::new();
        let mut pending_jobs = Vec::new();
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
                    pending_jobs.push(PendingBackgroundJob {
                        stream_id: entry.id.clone(),
                        response_id: response_id.clone(),
                        autoclaimed,
                        idle_ms,
                    });
                    continue;
                }
                Err(err) => return Err(err),
            };

            jobs.push(BackgroundJob {
                stream_id: entry.id.clone(),
                response_id,
                record,
                autoclaimed,
                idle_ms,
            });
        }
        Ok(ClaimBatchResult {
            jobs,
            pending_stream_ids,
            pending_jobs,
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
    !result.jobs.is_empty() || !result.pending_jobs.is_empty()
}

fn is_busygroup(err: &RedisError) -> bool {
    err.to_string().contains("BUSYGROUP")
}

fn is_missing_stream(err: &RedisError) -> bool {
    err.to_string().contains("no such key")
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
