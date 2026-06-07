use responses_api_store_client::{
    build_queued_response, build_upstream_request, Client, StoredResponse,
};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let endpoint =
        std::env::var("STORE_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:50052".to_string());
    let mut client = Client::connect(endpoint).await?;

    let health = client.health().await?;
    println!(
        "health: redis_ok={} version={}",
        health.redis_ok, health.version
    );
    assert!(health.redis_ok);

    let response_id = client.generate_response_id().await?;
    let request = json!({
        "model": "demo",
        "input": "hello",
        "background": true,
        "store": true
    });
    let queued = build_queued_response(&response_id, "demo", &request);
    let upstream_request = build_upstream_request(&request);
    let record = StoredResponse {
        upstream: "http://model/v1".to_string(),
        response: queued,
        input: vec![json!({"role": "user", "content": "hello"})],
        pending_upstream_request: Some(upstream_request),
        upstream_authorization: None,
        enqueued_at: Some(1_746_500_000),
    };

    client.enqueue_background_job(&response_id, &record).await?;
    let loaded = client.get_response(&response_id, false).await?;
    assert_eq!(loaded.upstream, record.upstream);

    let created = client.ensure_consumer_group("smoke-test", "0-0").await?;
    println!("consumer group created={created}");

    let claim = client
        .claim_background_jobs(responses_api_store_client::ClaimBackgroundJobsRequest {
            consumer_group: "smoke-test".to_string(),
            consumer_name: "smoke-worker".to_string(),
            count: 1,
            block_ms: 1000,
            autoclaim_min_idle_ms: 0,
        })
        .await?;
    assert_eq!(claim.jobs.len(), 1);
    assert!(claim.pending_stream_ids.is_empty());
    assert_eq!(claim.jobs[0].response_id, response_id);

    client
        .acknowledge_background_job("smoke-test", &claim.jobs[0].stream_id)
        .await?;
    client.delete_response(&response_id).await?;

    println!("smoke test passed for {response_id}");
    Ok(())
}
