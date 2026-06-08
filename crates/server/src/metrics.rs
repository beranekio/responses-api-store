use axum::{
    extract::{Query, State},
    http::StatusCode,
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

pub async fn serve(queue: BackgroundQueue, listen_addr: &str) -> anyhow::Result<()> {
    let app = Router::new()
        .route("/metrics/background-queue", get(background_queue_json))
        .route("/metrics", get(background_queue_prometheus))
        .with_state(AppState { queue });

    let listener = tokio::net::TcpListener::bind(listen_addr)
        .await
        .map_err(|err| anyhow::anyhow!("bind metrics HTTP listener on {listen_addr}: {err}"))?;
    axum::serve(listener, app)
        .await
        .map_err(|err| anyhow::anyhow!("serve metrics HTTP requests: {err}"))
}

async fn background_queue_json(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Response {
    match stats_for_query(&state.queue, query.consumer_group).await {
        Ok((consumer_group, stats)) => (
            StatusCode::OK,
            Json(StatsResponse {
                consumer_group,
                pending: stats.pending,
                in_progress: stats.in_progress,
                workload: stats.workload,
            }),
        )
            .into_response(),
        Err(response) => response,
    }
}

async fn background_queue_prometheus(
    State(state): State<AppState>,
    Query(query): Query<StatsQuery>,
) -> Response {
    match stats_for_query(&state.queue, query.consumer_group).await {
        Ok((consumer_group, stats)) => {
            let body = format!(
                "# HELP responses_api_store_background_queue_workload Background queue workload for autoscaling\n\
                 # TYPE responses_api_store_background_queue_workload gauge\n\
                 responses_api_store_background_queue_workload{{consumer_group=\"{consumer_group}\"}} {}\n\
                 # HELP responses_api_store_background_queue_pending Jobs waiting to be claimed\n\
                 # TYPE responses_api_store_background_queue_pending gauge\n\
                 responses_api_store_background_queue_pending{{consumer_group=\"{consumer_group}\"}} {}\n\
                 # HELP responses_api_store_background_queue_in_progress Jobs claimed but not yet acknowledged\n\
                 # TYPE responses_api_store_background_queue_in_progress gauge\n\
                 responses_api_store_background_queue_in_progress{{consumer_group=\"{consumer_group}\"}} {}\n",
                stats.workload, stats.pending, stats.in_progress
            );
            (StatusCode::OK, body).into_response()
        }
        Err(response) => response,
    }
}

async fn stats_for_query(
    queue: &BackgroundQueue,
    consumer_group: Option<String>,
) -> std::result::Result<(String, BackgroundQueueStats), Response> {
    let consumer_group = match consumer_group {
        Some(group) if !group.is_empty() => group,
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "consumer_group query parameter is required"
                })),
            )
                .into_response());
        }
    };

    match queue.stats(&consumer_group).await {
        Ok(stats) => Ok((consumer_group, stats)),
        Err(StoreError::NotFound(message)) => Err((
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": message })),
        )
            .into_response()),
        Err(StoreError::InvalidArgument(message)) => Err((
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({ "error": message })),
        )
            .into_response()),
        Err(StoreError::Storage(err)) => {
            tracing::warn!(error = %err, "failed to load background queue stats");
            Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({ "error": format!("storage unavailable: {err}") })),
            )
                .into_response())
        }
        Err(StoreError::Serialization(message)) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": message })),
        )
            .into_response()),
    }
}
