use redis::{
    aio::ConnectionManager,
    streams::{
        StreamAutoClaimOptions, StreamAutoClaimReply, StreamId, StreamKey, StreamMaxlen,
        StreamReadOptions, StreamReadReply,
    },
    AsyncCommands, RedisError, RedisResult,
};

use crate::{
    error::{Result, StoreError},
    model::BackgroundJob,
    store::ResponseStore,
};

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

    pub async fn claim_jobs(
        &self,
        store: &ResponseStore,
        options: &ClaimOptions,
        autoclaim_cursor: &mut String,
    ) -> Result<Vec<BackgroundJob>> {
        let mut jobs = Vec::new();

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
            jobs.extend(
                self.jobs_from_stream_ids(store, &options.consumer_group, &autoclaim.claimed, true)
                    .await?,
            );
        }

        let remaining = options.count.saturating_sub(jobs.len());
        if remaining > 0 && jobs.is_empty() {
            let read = self
                .read_group(
                    &options.consumer_group,
                    &options.consumer_name,
                    remaining,
                    options.block_ms,
                )
                .await?;
            jobs.extend(
                self.jobs_from_stream_ids(store, &options.consumer_group, &read.ids, false)
                    .await?,
            );
        }

        Ok(jobs)
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
    ) -> Result<Vec<BackgroundJob>> {
        let mut jobs = Vec::with_capacity(entries.len());
        for entry in entries {
            let response_id = match entry.get::<String>("response_id") {
                Some(value) => value,
                None => continue,
            };

            let record = match store.load(&response_id).await {
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
        Ok(jobs)
    }
}

fn is_busygroup(err: &RedisError) -> bool {
    err.to_string().contains("BUSYGROUP")
}
