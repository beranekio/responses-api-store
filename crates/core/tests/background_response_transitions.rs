use responses_api_store_core::{
    build_cancelled_response, build_queued_response, is_in_progress_background,
    is_terminal_background_status, ResponseStore, StoreError, StoredResponse,
};
use serde_json::json;

fn queued_record(response_id: &str) -> StoredResponse {
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
        upstream_authorization: Some("Bearer token".to_string()),
        enqueued_at: Some(1_746_500_000),
    }
}

async fn connect_store(redis_url: &str, suffix: u128) -> Option<ResponseStore> {
    let key_prefix = format!("responses-api-store:test-transitions:{suffix}");
    let store = ResponseStore::connect(redis_url, key_prefix, 300, 60)
        .await
        .ok()?;
    if store.ping().await.is_err() {
        return None;
    }
    Some(store)
}

#[tokio::test]
async fn claim_background_response_transitions_queued_to_in_progress() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping claim_background_response_transitions_queued_to_in_progress: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let Some(store) = connect_store(&redis_url, suffix).await else {
        eprintln!("skipping claim_background_response_transitions_queued_to_in_progress: redis unavailable");
        return;
    };

    let response_id = format!("resp_{suffix}");
    store
        .store(&response_id, &queued_record(&response_id), None)
        .await
        .expect("store queued response");

    let (record, payload) = store
        .claim_background_response(&response_id)
        .await
        .expect("claim background response");
    assert!(is_in_progress_background(&record));
    assert!(record.pending_upstream_request.is_some());
    assert_eq!(
        record.upstream_authorization.as_deref(),
        Some("Bearer token")
    );
    assert_eq!(payload.upstream, "http://model/v1");
    assert_eq!(payload.pending_upstream_request["input"], "hello");
    assert_eq!(
        payload.upstream_authorization.as_deref(),
        Some("Bearer token")
    );

    let err = store
        .claim_background_response(&response_id)
        .await
        .expect_err("second claim should fail");
    assert!(matches!(err, StoreError::FailedPrecondition(_)));
}

#[tokio::test]
async fn complete_background_response_sets_background_marker() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "skipping complete_background_response_sets_background_marker: RESPONSE_ID_STORE_URL unset"
            );
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let Some(store) = connect_store(&redis_url, suffix).await else {
        eprintln!(
            "skipping complete_background_response_sets_background_marker: redis unavailable"
        );
        return;
    };

    let response_id = format!("resp_{suffix}");
    store
        .store(&response_id, &queued_record(&response_id), None)
        .await
        .expect("store queued response");
    store
        .claim_background_response(&response_id)
        .await
        .expect("claim");

    // Upstream synchronous responses omit `background` (stripped from the request).
    let upstream_response = json!({
        "object": "response",
        "status": "completed",
        "output": [{"type": "message", "content": "done"}]
    });
    store
        .complete_background_response(&response_id, upstream_response)
        .await
        .expect("complete");

    let loaded = store
        .load(&response_id)
        .await
        .expect("load")
        .expect("record exists");
    assert_eq!(loaded.response["background"], json!(true));
    assert_eq!(loaded.response["id"], response_id);
    assert_eq!(loaded.response["status"], "completed");
}

#[tokio::test]
async fn complete_background_response_forces_completed_status() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!(
                "skipping complete_background_response_forces_completed_status: RESPONSE_ID_STORE_URL unset"
            );
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let Some(store) = connect_store(&redis_url, suffix).await else {
        eprintln!(
            "skipping complete_background_response_forces_completed_status: redis unavailable"
        );
        return;
    };

    let response_id = format!("resp_{suffix}");
    store
        .store(&response_id, &queued_record(&response_id), None)
        .await
        .expect("store queued response");
    store
        .claim_background_response(&response_id)
        .await
        .expect("claim");

    store
        .complete_background_response(
            &response_id,
            json!({
                "object": "response",
                "status": "in_progress",
                "output": [{"type": "message", "content": "done"}]
            }),
        )
        .await
        .expect("complete");

    let loaded = store
        .load(&response_id)
        .await
        .expect("load")
        .expect("record exists");
    assert_eq!(loaded.response["status"], "completed");
    assert_eq!(loaded.response["background"], json!(true));
}

#[tokio::test]
async fn complete_background_response_rejects_non_object_json() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping complete_background_response_rejects_non_object_json: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let Some(store) = connect_store(&redis_url, suffix).await else {
        eprintln!(
            "skipping complete_background_response_rejects_non_object_json: redis unavailable"
        );
        return;
    };

    let response_id = format!("resp_{suffix}");
    store
        .store(&response_id, &queued_record(&response_id), None)
        .await
        .expect("store queued response");
    store
        .claim_background_response(&response_id)
        .await
        .expect("claim");

    let err = store
        .complete_background_response(&response_id, json!([]))
        .await
        .expect_err("complete should reject non-object json");
    assert!(matches!(err, StoreError::InvalidArgument(_)));
}

#[tokio::test]
async fn complete_background_response_rejects_terminal_cancel() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping complete_background_response_rejects_terminal_cancel: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let Some(store) = connect_store(&redis_url, suffix).await else {
        eprintln!(
            "skipping complete_background_response_rejects_terminal_cancel: redis unavailable"
        );
        return;
    };

    let response_id = format!("resp_{suffix}");
    let mut queued = queued_record(&response_id);
    store
        .store(&response_id, &queued, None)
        .await
        .expect("store queued response");
    let (in_progress, _) = store
        .claim_background_response(&response_id)
        .await
        .expect("claim");

    queued = in_progress;
    queued.response = build_cancelled_response(&queued, &response_id);
    queued.pending_upstream_request = None;
    queued.upstream_authorization = None;
    store
        .store(&response_id, &queued, None)
        .await
        .expect("overwrite with cancelled");

    let err = store
        .complete_background_response(
            &response_id,
            json!({"id": response_id, "status": "completed", "background": true}),
        )
        .await
        .expect_err("complete should reject cancelled");
    assert!(matches!(err, StoreError::FailedPrecondition(_)));
}

#[tokio::test]
async fn cancel_during_claim_does_not_restore_in_progress() {
    let redis_url = match std::env::var("RESPONSE_ID_STORE_URL") {
        Ok(url) => url,
        Err(_) => {
            eprintln!("skipping cancel_during_claim_does_not_restore_in_progress: RESPONSE_ID_STORE_URL unset");
            return;
        }
    };

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let Some(store) = connect_store(&redis_url, suffix).await else {
        eprintln!("skipping cancel_during_claim_does_not_restore_in_progress: redis unavailable");
        return;
    };

    let response_id = format!("resp_{suffix}");
    store
        .store(&response_id, &queued_record(&response_id), None)
        .await
        .expect("store queued response");

    let store_for_cancel = store.clone();
    let cancel_id = response_id.clone();
    let cancel_task = tokio::spawn(async move {
        for _ in 0..32 {
            let mut cancelled = queued_record(&cancel_id);
            cancelled.response = build_cancelled_response(&cancelled, &cancel_id);
            cancelled.pending_upstream_request = None;
            cancelled.upstream_authorization = None;
            let _ = store_for_cancel.store(&cancel_id, &cancelled, None).await;
        }
    });

    let mut last_err = None;
    for _ in 0..32 {
        match store.claim_background_response(&response_id).await {
            Ok((record, _)) => {
                cancel_task.abort();
                assert!(
                    !is_terminal_background_status(&record) || is_in_progress_background(&record)
                );
                let loaded = store
                    .load(&response_id)
                    .await
                    .expect("load")
                    .expect("record exists");
                assert!(
                    is_in_progress_background(&loaded) || is_terminal_background_status(&loaded)
                );
                if is_terminal_background_status(&loaded) {
                    assert!(!is_in_progress_background(&loaded));
                }
                return;
            }
            Err(err) => last_err = Some(err),
        }
    }
    cancel_task.abort();
    let err = last_err.expect("claim attempts made");
    assert!(matches!(
        err,
        StoreError::FailedPrecondition(_) | StoreError::Unavailable(_)
    ));
}
