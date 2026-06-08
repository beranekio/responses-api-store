use responses_api_store_core::{
    build_queued_response, BackgroundQueue, BackgroundQueueStats, ClaimOptions, ResponseStore,
    StoredResponse,
};
use serde_json::json;

async fn setup(
    redis_url: &str,
) -> Option<(BackgroundQueue, ResponseStore, String, String, String)> {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let stream_key = format!("responses-api-store:test-queue-stats:{suffix}");
    let key_prefix = format!("responses-api-store:test-queue-stats-store:{suffix}");
    let consumer_group = format!("queue-stats-{suffix}");

    let queue = BackgroundQueue::connect(redis_url, stream_key, 100)
        .await
        .ok()?;
    let store = ResponseStore::connect(redis_url, key_prefix, 300, 60)
        .await
        .ok()?;
    if store.ping().await.is_err() {
        return None;
    }

    queue
        .ensure_consumer_group(&consumer_group, "0-0")
        .await
        .ok()?;

    Some((
        queue,
        store,
        consumer_group,
        format!("resp_{suffix}"),
        format!("resp2_{suffix}"),
    ))
}

async fn setup_without_group(
    redis_url: &str,
) -> Option<(BackgroundQueue, ResponseStore, String, String)> {
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let stream_key = format!("responses-api-store:test-queue-stats-cold:{suffix}");
    let key_prefix = format!("responses-api-store:test-queue-stats-cold-store:{suffix}");
    let consumer_group = format!("queue-stats-cold-{suffix}");

    let queue = BackgroundQueue::connect(redis_url, stream_key, 100)
        .await
        .ok()?;
    let store = ResponseStore::connect(redis_url, key_prefix, 300, 60)
        .await
        .ok()?;
    if store.ping().await.is_err() {
        return None;
    }

    Some((queue, store, consumer_group, format!("resp_{suffix}")))
}

fn sample_record(response_id: &str) -> StoredResponse {
    let request = json!({
        "model": "demo",
        "input": "hello",
        "background": true,
        "store": true
    });
    StoredResponse {
        upstream: "http://model/v1".to_string(),
        response: build_queued_response(response_id, "demo", &request),
        input: vec![json!({"role": "user", "content": "hello"})],
        pending_upstream_request: None,
        upstream_authorization: None,
        enqueued_at: Some(1_746_500_000),
    }
}

#[tokio::test]
async fn stats_report_zero_for_empty_queue() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping background_queue_stats test: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let Some((queue, _store, consumer_group, _, _)) = setup(&redis_url).await else {
        eprintln!("skipping background_queue_stats test: redis unavailable");
        return;
    };

    let stats = queue
        .stats(&consumer_group)
        .await
        .expect("load queue stats");
    assert_eq!(
        stats,
        BackgroundQueueStats {
            pending: 0,
            in_progress: 0,
            workload: 0,
        }
    );
}

#[tokio::test]
async fn stats_auto_create_consumer_group_for_enqueued_jobs() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping background_queue_stats test: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let Some((queue, store, consumer_group, response_id)) = setup_without_group(&redis_url).await
    else {
        eprintln!("skipping background_queue_stats test: redis unavailable");
        return;
    };

    store
        .store(&response_id, &sample_record(&response_id), None)
        .await
        .expect("store response");
    queue.enqueue(&response_id).await.expect("enqueue job");

    let stats = queue
        .stats(&consumer_group)
        .await
        .expect("load queue stats after auto-creating consumer group");
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.in_progress, 0);
    assert_eq!(stats.workload, 1);
}

#[tokio::test]
async fn stats_report_pending_jobs_before_claim() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping background_queue_stats test: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let Some((queue, store, consumer_group, response_id, _)) = setup(&redis_url).await else {
        eprintln!("skipping background_queue_stats test: redis unavailable");
        return;
    };

    store
        .store(&response_id, &sample_record(&response_id), None)
        .await
        .expect("store response");
    queue.enqueue(&response_id).await.expect("enqueue job");

    let stats = queue
        .stats(&consumer_group)
        .await
        .expect("load queue stats");
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.in_progress, 0);
    assert_eq!(stats.workload, 1);
}

#[tokio::test]
async fn stats_report_in_progress_jobs_after_claim() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping background_queue_stats test: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let Some((queue, store, consumer_group, response_id, _)) = setup(&redis_url).await else {
        eprintln!("skipping background_queue_stats test: redis unavailable");
        return;
    };

    store
        .store(&response_id, &sample_record(&response_id), None)
        .await
        .expect("store response");
    queue.enqueue(&response_id).await.expect("enqueue job");

    let mut cursor = queue
        .get_autoclaim_cursor(&consumer_group)
        .await
        .expect("load autoclaim cursor");
    let batch = queue
        .claim_jobs(
            &store,
            &ClaimOptions {
                consumer_group: consumer_group.clone(),
                consumer_name: "worker".to_string(),
                count: 1,
                block_ms: 0,
                autoclaim_min_idle_ms: 0,
            },
            &mut cursor,
        )
        .await
        .expect("claim job");
    assert_eq!(batch.jobs.len(), 1);

    let stats = queue
        .stats(&consumer_group)
        .await
        .expect("load queue stats");
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.in_progress, 1);
    assert_eq!(stats.workload, 1);
}

#[tokio::test]
async fn stats_report_pending_and_in_progress_together() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping background_queue_stats test: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let Some((queue, store, consumer_group, response_id, response_id_2)) = setup(&redis_url).await
    else {
        eprintln!("skipping background_queue_stats test: redis unavailable");
        return;
    };

    for id in [&response_id, &response_id_2] {
        store
            .store(id, &sample_record(id), None)
            .await
            .expect("store response");
        queue.enqueue(id).await.expect("enqueue job");
    }

    let mut cursor = queue
        .get_autoclaim_cursor(&consumer_group)
        .await
        .expect("load autoclaim cursor");
    let batch = queue
        .claim_jobs(
            &store,
            &ClaimOptions {
                consumer_group: consumer_group.clone(),
                consumer_name: "worker".to_string(),
                count: 1,
                block_ms: 0,
                autoclaim_min_idle_ms: 0,
            },
            &mut cursor,
        )
        .await
        .expect("claim job");
    assert_eq!(batch.jobs.len(), 1);

    let stats = queue
        .stats(&consumer_group)
        .await
        .expect("load queue stats");
    assert_eq!(stats.pending, 1);
    assert_eq!(stats.in_progress, 1);
    assert_eq!(stats.workload, 2);
}
