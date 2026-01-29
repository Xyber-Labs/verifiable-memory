use crate::transport::http::types::{ApiResponse, AppState};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;

#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Service is healthy (DB reachable)", body = ApiResponse),
        (status = 503, description = "Service is unhealthy (DB unreachable)", body = ApiResponse)
    )
)]
pub async fn healthcheck_handler(State(state): State<AppState>) -> impl IntoResponse {
    let db_service = state.db_service.lock().await;
    let pool = db_service.pool().clone();

    match sqlx::query("SELECT 1").execute(&pool).await {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse {
                success: true,
                data: Some(serde_json::json!({ "status": "ok" })),
                error: None,
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ApiResponse {
                success: false,
                data: Some(serde_json::json!({ "status": "unhealthy" })),
                error: Some(format!("DB ping failed: {}", e)),
            }),
        )
            .into_response(),
    }
}

