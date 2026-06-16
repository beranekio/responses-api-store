use std::time::Duration;

use responses_api_store_core::{
    build_queued_response, BackgroundQueue, ClaimOptions, PendingBackgroundJob, ResponseStore,
    StoredResponse,
};
use serde_json::json;
use tokio::time::sleep;

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
        pending_upstream_request: Some(json!({"model": "demo", "input": "hello"})),
        upstream_authorization: None,
        enqueued_at: Some(1_746_500_000),
    }
}

#[test]
fn pending_background_job_propagates_autoclaim_metadata() {
    let job = PendingBackgroundJob {
        stream_id: "1-0".to_string(),
        response_id: "resp_test".to_string(),
        autoclaimed: true,
        idle_ms: Some(5000),
    };
    assert!(job.autoclaimed);
    assert_eq!(job.idle_ms, Some(5000));
}

#[tokio::test]
async fn autoclaimed_jobs_include_idle_metadata() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "skipping autoclaimed_jobs_include_idle_metadata: RESPONSE_ID_STORE_URL unset"
            );
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let stream_key = format!("responses-api-store:test-pending-meta:{suffix}");
    let key_prefix = format!("responses-api-store:test-pending-meta-store:{suffix}");
    let consumer_group = format!("pending-meta-{suffix}");
    let response_id = format!("resp_{suffix}");

    let queue = match BackgroundQueue::connect(&redis_url, stream_key, 100).await {
        Ok(queue) => queue,
        Err(err) => {
            eprintln!("skipping autoclaimed_jobs_include_idle_metadata: {err}");
            return;
        }
    };
    let store = match ResponseStore::connect(&redis_url, key_prefix, 300, 60).await {
        Ok(store) => store,
        Err(err) => {
            eprintln!("skipping autoclaimed_jobs_include_idle_metadata: {err}");
            return;
        }
    };
    if store.ping().await.is_err() {
        eprintln!("skipping autoclaimed_jobs_include_idle_metadata: redis unavailable");
        return;
    }

    queue
        .ensure_consumer_group(&consumer_group, "0-0")
        .await
        .expect("ensure consumer group");
    store
        .store(&response_id, &sample_record(&response_id), None)
        .await
        .expect("store response");
    queue.enqueue(&response_id).await.expect("enqueue job");

    let mut cursor = queue
        .get_autoclaim_cursor(&consumer_group)
        .await
        .expect("load autoclaim cursor");
    let first_claim = queue
        .claim_jobs(
            &store,
            &ClaimOptions {
                consumer_group: consumer_group.clone(),
                consumer_name: "worker-a".to_string(),
                count: 1,
                block_ms: 0,
                autoclaim_min_idle_ms: 0,
            },
            &mut cursor,
        )
        .await
        .expect("first claim");
    assert_eq!(first_claim.jobs.len(), 1);
    assert!(!first_claim.jobs[0].autoclaimed);
    assert_eq!(first_claim.jobs[0].idle_ms, None);

    let autoclaim_min_idle_ms = 50;
    sleep(Duration::from_millis(100)).await;
    let second_claim = queue
        .claim_jobs(
            &store,
            &ClaimOptions {
                consumer_group: consumer_group.clone(),
                consumer_name: "worker-b".to_string(),
                count: 1,
                block_ms: 0,
                autoclaim_min_idle_ms,
            },
            &mut cursor,
        )
        .await
        .expect("autoclaim");
    assert_eq!(second_claim.jobs.len(), 1);
    assert!(second_claim.jobs[0].autoclaimed);
    assert_eq!(
        second_claim.jobs[0].idle_ms,
        Some(autoclaim_min_idle_ms as u64)
    );
}
