use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct StoredResponse {
    pub upstream: String,
    pub response: Value,
    pub input: Vec<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_upstream_request: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upstream_authorization: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enqueued_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BackgroundJob {
    pub stream_id: String,
    pub response_id: String,
    pub record: StoredResponse,
    pub autoclaimed: bool,
    pub idle_ms: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingBackgroundJob {
    pub stream_id: String,
    pub response_id: String,
}

pub fn response_store_key(prefix: &str, response_id: &str) -> String {
    format!("{prefix}:{response_id}")
}

pub fn autoclaim_cursor_key(stream_key: &str, consumer_group: &str) -> String {
    format!("{stream_key}:meta:autoclaim:{consumer_group}")
}

pub fn response_id_from_value(value: &Value) -> Option<String> {
    value
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| id.starts_with("resp_"))
        .or_else(|| {
            value
                .get("response")
                .and_then(|response| response.get("id"))
                .and_then(Value::as_str)
                .filter(|id| id.starts_with("resp_"))
        })
        .map(ToString::to_string)
}

pub fn stored_response_status(stored: &StoredResponse) -> Option<&str> {
    stored.response.get("status").and_then(Value::as_str)
}

pub fn is_deleted_tombstone(stored: &StoredResponse) -> bool {
    stored_response_status(stored) == Some("deleted")
}

pub fn is_in_flight_background(stored: &StoredResponse) -> bool {
    stored.pending_upstream_request.is_some()
        || matches!(
            stored_response_status(stored),
            Some("queued") | Some("in_progress")
        )
}

pub fn generate_response_id() -> String {
    format!("resp_{}", uuid::Uuid::new_v4().simple())
}

pub fn build_queued_response(response_id: &str, model: &str, request: &Value) -> Value {
    let mut response = json!({
        "id": response_id,
        "object": "response",
        "status": "queued",
        "model": model,
        "background": true,
        "output": []
    });
    if let Some(input) = request.get("input") {
        response["input"] = input.clone();
    }
    response
}

pub fn build_upstream_request(request: &Value) -> Value {
    let mut upstream = request.clone();
    if let Some(obj) = upstream.as_object_mut() {
        obj.remove("background");
        obj.remove("previous_response_id");
        obj.insert("store".to_string(), Value::Bool(false));
    }
    upstream
}

pub fn build_cancelled_response(stored: &StoredResponse, response_id: &str) -> Value {
    let mut response = stored.response.clone();
    response["id"] = Value::String(response_id.to_string());
    response["status"] = Value::String("cancelled".to_string());
    response["background"] = Value::Bool(true);
    response
}

pub fn should_reconcile_stale(stored: &StoredResponse) -> bool {
    is_in_flight_background(stored) && stored_response_status(stored) == Some("queued")
}

pub fn unix_seconds_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0)
}

pub fn is_stale_enqueued(enqueued_at: Option<i64>, now: i64, stale_seconds: i64) -> bool {
    let Some(enqueued_at) = enqueued_at else {
        return false;
    };
    now.saturating_sub(enqueued_at) >= stale_seconds
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_background_from_upstream_request() {
        let request = json!({
            "model": "demo",
            "input": "hello",
            "background": true,
            "previous_response_id": "resp_old",
            "store": true
        });
        assert_eq!(
            build_upstream_request(&request),
            json!({
                "model": "demo",
                "input": "hello",
                "store": false
            })
        );
    }

    #[test]
    fn detects_in_flight_background_responses() {
        let queued = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "queued", "background": true}),
            input: vec![],
            pending_upstream_request: Some(json!({"input": "hi"})),
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert!(is_in_flight_background(&queued));
    }

    #[test]
    fn detects_deleted_tombstone() {
        let deleted = StoredResponse {
            upstream: "http://model".to_string(),
            response: json!({"status": "deleted", "background": true, "deleted": true}),
            input: vec![],
            pending_upstream_request: None,
            upstream_authorization: None,
            enqueued_at: None,
        };
        assert!(is_deleted_tombstone(&deleted));
    }

    #[test]
    fn detects_stale_enqueued_responses() {
        assert!(!is_stale_enqueued(None, 1000, 60));
        assert!(!is_stale_enqueued(Some(950), 1000, 60));
        assert!(is_stale_enqueued(Some(900), 1000, 60));
    }

    #[test]
    fn builds_autoclaim_cursor_key() {
        assert_eq!(
            autoclaim_cursor_key("responses-api-store:background", "workers"),
            "responses-api-store:background:meta:autoclaim:workers"
        );
    }
}
