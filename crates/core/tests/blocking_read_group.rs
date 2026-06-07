use std::time::{Duration, Instant};

use responses_api_store_core::{BackgroundQueue, ClaimOptions, ResponseStore};

#[tokio::test]
async fn claim_jobs_waits_through_blocking_read_without_client_timeout() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping blocking_read_group test: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let stream_key = format!("responses-api-store:test-blocking-read:{suffix}");
    let key_prefix = format!("responses-api-store:test-blocking-store:{suffix}");
    let consumer_group = format!("blocking-read-{suffix}");

    let queue = match BackgroundQueue::connect(&redis_url, stream_key.clone(), 100).await {
        Ok(queue) => queue,
        Err(err) => {
            eprintln!("skipping blocking_read_group test: {err}");
            return;
        }
    };
    let store = match ResponseStore::connect(&redis_url, key_prefix, 300, 60).await {
        Ok(store) => store,
        Err(err) => {
            eprintln!("skipping blocking_read_group test: {err}");
            return;
        }
    };
    if store.ping().await.is_err() {
        eprintln!("skipping blocking_read_group test: redis unavailable");
        return;
    }

    queue
        .ensure_consumer_group(&consumer_group, "0-0")
        .await
        .expect("create consumer group");

    let mut cursor = queue
        .get_autoclaim_cursor(&consumer_group)
        .await
        .expect("load autoclaim cursor");
    let started = Instant::now();
    let batch = queue
        .claim_jobs(
            &store,
            &ClaimOptions {
                consumer_group: consumer_group.clone(),
                consumer_name: "worker".to_string(),
                count: 1,
                block_ms: 1000,
                autoclaim_min_idle_ms: 0,
            },
            &mut cursor,
        )
        .await
        .expect("blocking read should not hit redis client response timeout");
    let elapsed = started.elapsed();

    assert!(batch.jobs.is_empty());
    assert!(batch.pending_stream_ids.is_empty());
    assert!(
        elapsed >= Duration::from_millis(900),
        "expected blocking wait near 1000ms, got {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(5000),
        "blocking read took unexpectedly long: {elapsed:?}"
    );
}
