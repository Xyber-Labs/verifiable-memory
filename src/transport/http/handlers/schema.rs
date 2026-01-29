use crate::transport::http::types::{
    ApiResponse, AppState, CurrentSchemaResponse, DbColumnSchema, DbTableSchema, INTERNAL_TABLES,
};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use sqlx::Row;

#[utoipa::path(
    get,
    path = "/bootstrap/schema",
    responses(
        (status = 200, description = "Current schema", body = CurrentSchemaResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn bootstrap_get_schema_handler(State(state): State<AppState>) -> impl IntoResponse {
    let db_service = state.db_service.lock().await;
    let pool = db_service.pool().clone();

    let schema_name = "public";

    let table_rows = match sqlx::query(
        "SELECT table_name
         FROM information_schema.tables
         WHERE table_schema = $1 AND table_type = 'BASE TABLE'
         ORDER BY table_name",
    )
    .bind(schema_name)
    .fetch_all(&pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Failed to list tables: {}", e)),
                }),
            )
                .into_response();
        }
    };

    let mut tables: Vec<DbTableSchema> = Vec::new();

    for tr in table_rows {
        let table_name: String = match tr.try_get("table_name") {
            Ok(v) => v,
            Err(_) => continue,
        };
        if INTERNAL_TABLES.contains(&table_name.as_str()) {
            continue;
        }

        let col_rows = sqlx::query(
            "SELECT column_name, data_type, is_nullable, column_default
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2
             ORDER BY ordinal_position",
        )
        .bind(schema_name)
        .bind(&table_name)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut columns: Vec<DbColumnSchema> = Vec::new();
        for cr in col_rows {
            let name: String = cr.try_get("column_name").unwrap_or_default();
            if name.is_empty() {
                continue;
            }
            let data_type: String = cr
                .try_get("data_type")
                .unwrap_or_else(|_| "unknown".to_string());
            let is_nullable_str: String = cr
                .try_get("is_nullable")
                .unwrap_or_else(|_| "YES".to_string());
            let is_nullable = is_nullable_str.to_uppercase() == "YES";
            let default: Option<String> = cr.try_get("column_default").ok();

            columns.push(DbColumnSchema {
                name,
                data_type,
                is_nullable,
                default,
            });
        }

        let pk_rows = sqlx::query(
            r#"
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = $1
              AND tc.table_name = $2
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
            "#,
        )
        .bind(schema_name)
        .bind(&table_name)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let primary_key: Vec<String> = pk_rows
            .into_iter()
            .filter_map(|r| r.try_get::<String, _>("column_name").ok())
            .collect();

        tables.push(DbTableSchema {
            table_name,
            columns,
            primary_key,
        });
    }

    (
        StatusCode::OK,
        Json(CurrentSchemaResponse {
            schema: schema_name.to_string(),
            tables,
        }),
    )
        .into_response()
}

