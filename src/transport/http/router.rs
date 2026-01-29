use crate::transport::http::handlers::{bootstrap, execute, health, models, schema};
use crate::transport::http::types::{
    Action, ApiRequest, ApiResponse, BootstrapRequest, ClearDataRequest, ColumnSpec, ColumnType,
    CreateBatchRequest, CurrentSchemaResponse, DbColumnSchema, DbTableSchema, PrimaryKeyKind,
    ReadBatchRequest, ReadLatestRequest, TableSpec, MigrateRequest, OrderBySpec, OrderDirection,
    UpsertBatchRequest, RepairRootsRequest,
};
use axum::routing::{get, post};
use axum::Router;
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    paths(
        health::healthcheck_handler,
        execute::execute_handler,
        models::create_batch_handler,
        models::read_batch_handler,
        models::read_latest_handler,
        models::upsert_batch_handler,
        bootstrap::bootstrap_apply_schema_handler,
        bootstrap::bootstrap_clear_data_handler,
        bootstrap::bootstrap_migrate_handler,
        bootstrap::bootstrap_repair_roots_handler,
        schema::bootstrap_get_schema_handler
    ),
    components(schemas(
        ApiRequest,
        ApiResponse,
        Action,
        CreateBatchRequest,
        ReadBatchRequest,
        ReadLatestRequest,
        OrderBySpec,
        OrderDirection,
        UpsertBatchRequest,
        BootstrapRequest,
        ClearDataRequest,
        MigrateRequest,
        RepairRootsRequest,
        TableSpec,
        ColumnSpec,
        ColumnType,
        PrimaryKeyKind,
        CurrentSchemaResponse,
        DbTableSchema,
        DbColumnSchema
    ))
)]
#[allow(dead_code)]
pub struct ApiDoc;

pub fn create_router(app_state: crate::transport::http::types::AppState) -> Router {
    Router::new()
        .route("/health", get(health::healthcheck_handler))
        .route("/api/execute", post(execute::execute_handler))
        .route("/api/models/:model/create-batch", post(models::create_batch_handler))
        .route("/api/models/:model/read-batch", post(models::read_batch_handler))
        .route("/api/models/:model/read-latest", post(models::read_latest_handler))
        .route("/api/models/:model/upsert", post(models::upsert_batch_handler))
        .route(
            "/bootstrap/apply-schema",
            post(bootstrap::bootstrap_apply_schema_handler),
        )
        .route("/bootstrap/clear-data", post(bootstrap::bootstrap_clear_data_handler))
        .route("/bootstrap/migrate", post(bootstrap::bootstrap_migrate_handler))
        .route("/bootstrap/repair-roots", post(bootstrap::bootstrap_repair_roots_handler))
        .route("/bootstrap/schema", get(schema::bootstrap_get_schema_handler))
        .with_state(app_state)
}

