use responses_api_store_core::{unix_seconds_now, ResponseStore, StoredResponse};
use serde_json::json;

#[tokio::test]
async fn reconcile_preserves_empty_input_array() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "skipping reconcile_preserves_empty_input_array: RESPONSE_ID_STORE_URL unset"
            );
            return;
        }
    };

    let key_prefix = format!(
        "responses-api-store:test-reconcile:{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    );
    let store = match ResponseStore::connect(&redis_url, key_prefix, 300, 60).await {
        Ok(store) => store,
        Err(err) => {
            eprintln!("skipping reconcile_preserves_empty_input_array: {err}");
            return;
        }
    };
    if store.ping().await.is_err() {
        eprintln!("skipping reconcile_preserves_empty_input_array: redis unavailable");
        return;
    }

    let response_id = "resp_test_empty_input";
    let now = unix_seconds_now();
    let stored = StoredResponse {
        upstream: "http://test/v1".to_string(),
        response: json!({
            "id": response_id,
            "status": "queued",
            "background": true
        }),
        input: vec![],
        pending_upstream_request: Some(json!({"model": "demo"})),
        upstream_authorization: None,
        enqueued_at: Some(now - 120),
    };

    store
        .store(response_id, &stored, Some(300))
        .await
        .expect("store test record");
    let updated = store
        .reconcile_stale_response(response_id, &stored, 60)
        .await
        .expect("reconcile stale response");
    assert_eq!(updated.input, Vec::<serde_json::Value>::new());
    assert_eq!(updated.response["status"], "failed");

    let loaded = store
        .load(response_id)
        .await
        .expect("reload reconciled record")
        .expect("reconciled record should exist");
    assert_eq!(loaded.input, Vec::<serde_json::Value>::new());

    let _ = store.delete(response_id).await;
}
