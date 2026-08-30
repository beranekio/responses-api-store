use axum::{
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use responses_api_store_core::{BackgroundQueue, BackgroundQueueStats, StoreError};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
struct AppState {
    queue: BackgroundQueue,
}

#[derive(Debug, Deserialize)]
struct StatsQuery {
    consumer_group: Option<String>,
}

#[derive(Debug, Serialize)]
struct StatsResponse {
    consumer_group: String,
    pending: u64,
    in_progress: u64,
    workload: u64,
}

/// HTTP error payload for metrics handlers. Kept small so `Result<_, Self>` stays
/// under clippy's `result_large_err` threshold (`axum::Response` does not).
#[derive(Debug)]
struct StatsQueryError {
    status: StatusCode,
    message: String,
}

impl StatsQueryError {
    fn new(status: StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    fn from_store(err: StoreError) -> Self {
        match err {
            StoreError::NotFound(message) => Self::new(StatusCode::NOT_FOUND, message),
            StoreError::InvalidArgument(message) => Self::new(StatusCode::BAD_REQUEST, message),
            StoreError::Unavailable(message) => {
                tracing::warn!(error = %message, "background queue stats unavailable");
                Self::new(StatusCode::SERVICE_UNAVAILABLE, message)
            }
            StoreError::Storage(err) => {
                tracing::warn!(error = %err, "failed to load background queue stats");
                Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("storage unavailable: {err}"),
                )
            }
            StoreError::Serialization(message) => {
                Self::new(StatusCode::INTERNAL_SERVER_ERROR, message)
            }
            StoreError::FailedPrecondition(message) => Self::new(StatusCode::CONFLICT, message),
        }
    }
}

impl IntoResponse for StatsQueryError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(serde_json::json!({ "error": self.message })),
        )
            .into_response()
    }
}

pub async fn serve(
    queue: BackgroundQueue,
    listener: tokio::net::TcpListener,
) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/metrics/background-queue", get(background_queue_json))
        .route("/metrics", get(background_queue_prometheus))
        .with_state(AppState { queue });

    axum::serve(listener, app)
        .await
        .map_err(|err| anyhow::anyhow!("serve metrics HTTP requests: {err}"))
}

async fn background_queue_json(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Result<impl IntoResponse, StatsQueryError> {
    let (consumer_group, stats) = stats_for_query(&state.queue, query.consumer_group).await?;
    Ok((
        StatusCode::OK,
        Json(StatsResponse {
            consumer_group,
            pending: stats.pending,
            in_progress: stats.in_progress,
            workload: stats.workload,
        }),
    ))
}

async fn background_queue_prometheus(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Result<impl IntoResponse, StatsQueryError> {
    let (consumer_group, stats) = stats_for_query(&state.queue, query.consumer_group).await?;
    let escaped_group = escape_prometheus_label_value(&consumer_group);
    let body = format!(
        "# HELP responses_api_store_background_queue_workload Background queue workload for autoscaling\n\
         # TYPE responses_api_store_background_queue_workload gauge\n\
         responses_api_store_background_queue_workload{{consumer_group=\"{escaped_group}\"}} {}\n\
         # HELP responses_api_store_background_queue_pending Jobs waiting to be claimed\n\
         # TYPE responses_api_store_background_queue_pending gauge\n\
         responses_api_store_background_queue_pending{{consumer_group=\"{escaped_group}\"}} {}\n\
         # HELP responses_api_store_background_queue_in_progress Jobs claimed but not yet acknowledged\n\
         # TYPE responses_api_store_background_queue_in_progress gauge\n\
         responses_api_store_background_queue_in_progress{{consumer_group=\"{escaped_group}\"}} {}\n",
        stats.workload, stats.pending, stats.in_progress
    );
    Ok((
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    ))
}

async fn stats_for_query(
    queue: &BackgroundQueue,
    consumer_group: Option<String>,
) -> Result<(String, BackgroundQueueStats), StatsQueryError> {
    let consumer_group = match consumer_group {
        Some(group) if !group.is_empty() => group,
        _ => {
            return Err(StatsQueryError::new(
                StatusCode::BAD_REQUEST,
                "consumer_group query parameter is required",
            ));
        }
    };

    match queue.stats(&consumer_group).await {
        Ok(stats) => Ok((consumer_group, stats)),
        Err(err) => Err(StatsQueryError::from_store(err)),
    }
}

fn escape_prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\n', "\\n")
        .replace('"', "\\\"")
}

#[cfg(test)]
mod tests {
    use super::{escape_prometheus_label_value, StatsQueryError};
    use axum::http::StatusCode;
    use responses_api_store_core::StoreError;

    #[test]
    fn escapes_prometheus_label_metacharacters() {
        assert_eq!(
            escape_prometheus_label_value(r#"duihua"background\n"#),
            r#"duihua\"background\\n"#
        );
    }

    #[test]
    fn maps_store_errors_to_http_status() {
        let cases = [
            (
                StoreError::NotFound("missing-group".into()),
                StatusCode::NOT_FOUND,
                "missing-group",
            ),
            (
                StoreError::InvalidArgument("bad group".into()),
                StatusCode::BAD_REQUEST,
                "bad group",
            ),
            (
                StoreError::Unavailable("lag unsupported".into()),
                StatusCode::SERVICE_UNAVAILABLE,
                "lag unsupported",
            ),
            (
                StoreError::Serialization("bad json".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "bad json",
            ),
            (
                StoreError::FailedPrecondition("conflict".into()),
                StatusCode::CONFLICT,
                "conflict",
            ),
        ];
        for (err, status, message) in cases {
            let mapped = StatsQueryError::from_store(err);
            assert_eq!(mapped.status, status);
            assert_eq!(mapped.message, message);
        }
    }
}
