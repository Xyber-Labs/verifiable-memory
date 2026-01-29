use crate::app::database_service::DatabaseService;
use crate::domain::commitment::RootManager;
use crate::domain::model::ModelRegistry;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use utoipa::ToSchema;

#[derive(Clone)]
pub struct AppState {
    pub db_service: Arc<Mutex<DatabaseService>>,
    pub model_registry: Arc<RwLock<ModelRegistry>>,
    pub root_manager: Arc<RootManager>,
}

#[derive(Deserialize, Debug, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    CreateBatch,
    ReadBatch,
}

#[derive(Deserialize, Debug, ToSchema)]
pub struct ApiRequest {
    pub model_name: String,
    pub action: Action,
    #[schema(value_type = Object)]
    pub payload: JsonValue,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct ApiResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Object)]
    pub data: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Deserialize, Debug, ToSchema)]
pub struct CreateBatchRequest {
    #[schema(value_type = Vec<Object>)]
    pub records: Vec<JsonValue>,
    /// Optional optimistic concurrency check: if provided, the server verifies it matches the
    /// current trusted `temporary_root` before applying the write.
    #[serde(default)]
    pub expected_root: Option<String>,
}

#[derive(Deserialize, Debug, ToSchema)]
pub struct ReadBatchRequest {
    pub ids: Vec<String>,
}

#[derive(Deserialize, Debug, ToSchema)]
pub struct ReadLatestRequest {
    /// Number of latest rows to read (ordered by primary key descending).
    pub limit: u32,
    /// Optional equality filters (restricted): `{ "field": value }`.
    ///
    /// Values are coerced server-side for common scalar types (e.g. `"1440"` -> int).
    #[serde(default, rename = "where")]
    #[schema(value_type = Object)]
    pub r#where: Option<HashMap<String, JsonValue>>,
    /// Optional ordering (restricted).
    #[serde(default)]
    pub order_by: Option<OrderBySpec>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct OrderBySpec {
    pub field: String,
    #[serde(default)]
    pub direction: OrderDirection,
}

#[derive(Deserialize, Serialize, Debug, ToSchema, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum OrderDirection {
    Asc,
    #[serde(other)]
    Desc,
}

impl Default for OrderDirection {
    fn default() -> Self {
        OrderDirection::Desc
    }
}

#[derive(Deserialize, Debug, ToSchema)]
pub struct UpsertBatchRequest {
    /// Records to upsert. Each record MUST contain the model's primary key field.
    #[schema(value_type = Vec<Object>)]
    pub records: Vec<JsonValue>,
    /// Optional optimistic concurrency check: if provided, the server verifies it matches the
    /// current trusted `temporary_root` before applying the write.
    #[serde(default)]
    pub expected_root: Option<String>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct BootstrapRequest {
    pub tables: Vec<TableSpec>,
    /// If true, reset roots/SMT even if schema hash matches.
    #[serde(default)]
    pub force_reset: bool,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct ClearDataRequest {
    /// Safety switch to prevent accidental wipes.
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct MigrateRequest {
    /// Safety switch to prevent accidental migrations.
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Deserialize, Serialize, Debug, ToSchema)]
pub struct RepairRootsRequest {
    /// Safety switch to prevent accidental expensive rebuilds.
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Deserialize, Serialize, Debug, ToSchema, Clone)]
pub struct TableSpec {
    pub table_name: String,
    pub primary_key_field: String,
    pub primary_key_kind: PrimaryKeyKind,
    pub columns: Vec<ColumnSpec>,
}

#[derive(Deserialize, Serialize, Debug, ToSchema, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub col_type: ColumnType,
    #[serde(default)]
    pub nullable: bool,
    #[serde(default)]
    pub unique: bool,
}

#[derive(Deserialize, Serialize, Debug, ToSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum PrimaryKeyKind {
    Serial,
    BigSerial,
    Text,
    Int,
    BigInt,
    Uuid,
}

#[derive(Deserialize, Serialize, Debug, ToSchema, Clone)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Text,
    Int,
    BigInt,
    Bool,
    Jsonb,
    Timestamptz,
    Uuid,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct CurrentSchemaResponse {
    /// Database schema name (typically `public`).
    pub schema: String,
    /// Tables currently present in the database schema.
    pub tables: Vec<DbTableSchema>,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct DbTableSchema {
    pub table_name: String,
    pub columns: Vec<DbColumnSchema>,
    /// Primary key columns in order (empty if none).
    pub primary_key: Vec<String>,
}

#[derive(Serialize, Debug, ToSchema)]
pub struct DbColumnSchema {
    pub name: String,
    /// Postgres data type as reported by the catalog (e.g. `text`, `integer`, `timestamp with time zone`).
    pub data_type: String,
    pub is_nullable: bool,
    /// Raw default expression (if any), e.g. `nextval('table_id_seq'::regclass)`.
    pub default: Option<String>,
}

// Internal tables owned by the verifiable service (not "application domain" tables).
pub const INTERNAL_TABLES: &[&str] = &[
    "merkle_nodes",
    "verifiable_models",
    "verifiable_registry_meta",
    "_sqlx_migrations",
    "schema_migrations",
];

pub fn json_422(err: JsonRejection, expected: &str) -> (StatusCode, Json<ApiResponse>) {
    (
        StatusCode::UNPROCESSABLE_ENTITY,
        Json(ApiResponse {
            success: false,
            data: None,
            error: Some(format!("Invalid JSON body: {} (expected: {})", err, expected)),
        }),
    )
}

