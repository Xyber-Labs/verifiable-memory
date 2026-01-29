use crate::crypto::hashing::hash_value;
use crate::domain::model::{DynamicModel, ModelRegistry};
use crate::infra::solana;
use crate::transport::http::handlers::common::{column_type_to_sql, pk_kind_to_sql, validate_ident};
use crate::transport::http::types::{
    ApiResponse, AppState, BootstrapRequest, ClearDataRequest, MigrateRequest, RepairRootsRequest,
};
use axum::extract::State;
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use primitive_types::H256;
use sqlx::Row;
use std::collections::HashMap;
use std::path::Path;

#[utoipa::path(
    post,
    path = "/bootstrap/apply-schema",
    request_body = BootstrapRequest,
    responses(
        (status = 200, description = "Schema applied", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn bootstrap_apply_schema_handler(
    State(state): State<AppState>,
    request: Result<Json<BootstrapRequest>, JsonRejection>,
) -> impl IntoResponse {
    // Prevent any interleaving with background commits / other writes.
    let _root_guard = state.root_manager.lock_root().await;

    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Invalid JSON body: {}", e)),
                }),
            )
                .into_response();
        }
    };

    if request.tables.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("tables cannot be empty".to_string()),
            }),
        )
            .into_response();
    }

    // Normalize + validate + compute schema hash.
    let mut normalized_tables = request.tables.clone();
    for t in &normalized_tables {
        if !validate_ident(&t.table_name) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Invalid table_name '{}'", t.table_name)),
                }),
            )
                .into_response();
        }
        if !validate_ident(&t.primary_key_field) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid primary_key_field '{}' for table '{}'",
                        t.primary_key_field, t.table_name
                    )),
                }),
            )
                .into_response();
        }
        for c in &t.columns {
            if !validate_ident(&c.name) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        error: Some(format!(
                            "Invalid column name '{}' for table '{}'",
                            c.name, t.table_name
                        )),
                    }),
                )
                    .into_response();
            }
        }
    }

    normalized_tables.sort_by(|a, b| a.table_name.cmp(&b.table_name));
    for t in &mut normalized_tables {
        t.columns.sort_by(|a, b| a.name.cmp(&b.name));
        t.table_name = t.table_name.to_lowercase();
        t.primary_key_field = t.primary_key_field.to_lowercase();
        for c in &mut t.columns {
            c.name = c.name.to_lowercase();
        }
    }

    let schema_json = serde_json::to_value(&normalized_tables).unwrap_or_else(|_| serde_json::Value::Null);
    let schema_hash_h256 = hash_value(&schema_json);
    let schema_hash = hex::encode(schema_hash_h256.as_bytes());

    // Decide if we need a reset (single-tenant, reset-on-changes).
    let blockchain_root = solana::read_root().await.unwrap_or_else(|_| H256::zero());

    let mut db_service = state.db_service.lock().await;
    let pool = db_service.pool().clone();

    let current_hash: Option<String> = sqlx::query(
        "SELECT value FROM verifiable_registry_meta WHERE key = 'schema_hash'",
    )
    .fetch_optional(&pool)
    .await
    .ok()
    .flatten()
    .and_then(|r| r.try_get::<String, _>("value").ok());

    let existing_tables: Vec<String> = sqlx::query("SELECT table_name FROM verifiable_models")
        .fetch_all(&pool)
        .await
        .unwrap_or_default()
        .into_iter()
        .filter_map(|r| r.try_get::<String, _>("table_name").ok())
        .collect();

    let requested_tables: Vec<String> = normalized_tables.iter().map(|t| t.table_name.clone()).collect();

    let merkle_nodes_count: i64 = sqlx::query("SELECT COUNT(*)::bigint as cnt FROM merkle_nodes")
        .fetch_one(&pool)
        .await
        .ok()
        .and_then(|r| r.try_get::<i64, _>("cnt").ok())
        .unwrap_or(0);

    let schema_changed = current_hash.as_deref() != Some(schema_hash.as_str());
    let needs_reset = request.force_reset
        || schema_changed
        || (!existing_tables.is_empty() && current_hash.is_none())
        || (merkle_nodes_count > 0 && current_hash.is_none())
        || (blockchain_root != H256::zero() && current_hash.is_none());

    if needs_reset {
        // Reset on-chain + in-memory roots first.
        let _ = solana::write_root(H256::zero()).await;
        state.root_manager.clear_trusted_state_file();
        state.root_manager.reset_roots(H256::zero()).await;

        // Reset DB state for all managed tables AND the requested tables (covers first-run drift).
        let mut tables_to_drop = existing_tables.clone();
        for t in requested_tables {
            if !tables_to_drop.contains(&t) {
                tables_to_drop.push(t);
            }
        }

        if let Ok(mut tx) = pool.begin().await {
            for t in &tables_to_drop {
                let _ = sqlx::query(&format!("DROP TABLE IF EXISTS {} CASCADE", t))
                    .execute(&mut *tx)
                    .await;
            }
            let _ = sqlx::query("TRUNCATE TABLE merkle_nodes")
                .execute(&mut *tx)
                .await;
            let _ = sqlx::query("DELETE FROM verifiable_models")
                .execute(&mut *tx)
                .await;
            let _ = sqlx::query("DELETE FROM verifiable_registry_meta WHERE key = 'schema_hash'")
                .execute(&mut *tx)
                .await;
            let _ = tx.commit().await;
        }

        // Reset SMT store in memory
        let _ = db_service.reset_smt_store().await;
    }

    // Apply tables + persist registry.
    for t in &normalized_tables {
        let mut cols_sql: Vec<String> = Vec::new();
        let pk_sql = format!(
            "{} {} PRIMARY KEY",
            t.primary_key_field,
            pk_kind_to_sql(&t.primary_key_kind)
        );
        cols_sql.push(pk_sql);

        for c in &t.columns {
            if c.name == t.primary_key_field {
                continue;
            }
            let mut col = format!("{} {}", c.name, column_type_to_sql(&c.col_type));
            if !c.nullable {
                col.push_str(" NOT NULL");
            }
            // Convenience: auto-generate created_at timestamps like autogenerated IDs.
            //
            // If a table declares a `created_at` column as `timestamptz NOT NULL`, we default it to `now()`
            // so clients don't need to send it in create-batch, and the DB-returned row (used for hashing)
            // contains the canonical timestamp value.
            if matches!(c.col_type, crate::transport::http::types::ColumnType::Timestamptz)
                && c.name == "created_at"
                && !c.nullable
            {
                col.push_str(" DEFAULT now()");
            }
            if c.unique {
                col.push_str(" UNIQUE");
            }
            cols_sql.push(col);
        }

        let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", t.table_name, cols_sql.join(", "));
        if let Err(e) = sqlx::query(&create_sql).execute(&pool).await {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Failed creating table '{}': {}", t.table_name, e)),
                }),
            )
                .into_response();
        }

        let columns_json =
            serde_json::to_value(&t.columns).unwrap_or_else(|_| serde_json::Value::Null);
        let _ = sqlx::query(
            "INSERT INTO verifiable_models (table_name, primary_key_field, primary_key_kind, columns, create_table_sql)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (table_name) DO UPDATE
             SET primary_key_field = EXCLUDED.primary_key_field,
                 primary_key_kind = EXCLUDED.primary_key_kind,
                 columns = EXCLUDED.columns,
                 create_table_sql = EXCLUDED.create_table_sql,
                 updated_at = now()",
        )
        .bind(&t.table_name)
        .bind(&t.primary_key_field)
        .bind(format!("{:?}", t.primary_key_kind).to_lowercase())
        .bind(columns_json)
        .bind(&create_sql)
        .execute(&pool)
        .await;
    }

    let _ = sqlx::query(
        "INSERT INTO verifiable_registry_meta (key, value)
         VALUES ('schema_hash', $1)
         ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
    )
    .bind(&schema_hash)
    .execute(&pool)
    .await;

    // Refresh in-memory registry from normalized spec.
    let mut new_registry = ModelRegistry::new();
    for t in &normalized_tables {
        let mut column_types: HashMap<String, String> = HashMap::new();
        column_types.insert(
            t.primary_key_field.clone(),
            pk_kind_to_sql(&t.primary_key_kind).to_lowercase(),
        );
        for c in &t.columns {
            column_types.insert(
                c.name.clone(),
                column_type_to_sql(&c.col_type).to_lowercase(),
            );
        }

        let mut cols_sql: Vec<String> = Vec::new();
        cols_sql.push(format!(
            "{} {} PRIMARY KEY",
            t.primary_key_field,
            pk_kind_to_sql(&t.primary_key_kind)
        ));
        for c in &t.columns {
            if c.name == t.primary_key_field {
                continue;
            }
            let mut col = format!("{} {}", c.name, column_type_to_sql(&c.col_type));
            if !c.nullable {
                col.push_str(" NOT NULL");
            }
            if matches!(c.col_type, crate::transport::http::types::ColumnType::Timestamptz)
                && c.name == "created_at"
                && !c.nullable
            {
                col.push_str(" DEFAULT now()");
            }
            if c.unique {
                col.push_str(" UNIQUE");
            }
            cols_sql.push(col);
        }
        let create_sql = format!("CREATE TABLE IF NOT EXISTS {} ({})", t.table_name, cols_sql.join(", "));

        new_registry.register(
            t.table_name.clone(),
            DynamicModel::new(
                t.table_name.clone(),
                t.primary_key_field.clone(),
                create_sql,
                column_types,
            ),
        );
    }

    {
        let mut reg = state.model_registry.write().await;
        *reg = new_registry;
    }

    let response_data = serde_json::json!({
        "schema_hash": schema_hash,
        "reset_performed": needs_reset,
        "tables": normalized_tables.iter().map(|t| t.table_name.clone()).collect::<Vec<_>>(),
    });
    (
        StatusCode::OK,
        Json(ApiResponse {
            success: true,
            data: Some(response_data),
            error: None,
        }),
    )
        .into_response()
}

#[utoipa::path(
    post,
    path = "/bootstrap/clear-data",
    request_body = ClearDataRequest,
    responses(
        (status = 200, description = "Data cleared + roots reset", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn bootstrap_clear_data_handler(
    State(state): State<AppState>,
    request: Result<Json<ClearDataRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid JSON body: {} (expected: {{\"confirm\": true}})",
                        e
                    )),
                }),
            )
                .into_response();
        }
    };

    if !request.confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("confirm must be true to clear data".to_string()),
            }),
        )
            .into_response();
    }

    // Prevent any interleaving with background commits / other writes.
    let _root_guard = state.root_manager.lock_root().await;
    let mut db_service = state.db_service.lock().await;

    // Clear all managed (client-created) tables and SMT persistence.
    if let Err(e) = db_service.clear_db().await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(format!("Failed clearing DB data: {}", e)),
            }),
        )
            .into_response();
    }

    // Reset roots to zero: write chain root first, then sync in-memory/trusted file.
    if let Err(e) = solana::write_root(H256::zero()).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(format!("Failed resetting on-chain root: {}", e)),
            }),
        )
            .into_response();
    }

    state.root_manager.reset_roots(H256::zero()).await;

    let response_data = serde_json::json!({
        "cleared": true,
        "root": hex::encode(H256::zero().as_bytes()),
        "message": "Cleared all managed table data, truncated SMT store, and reset roots to zero."
    });
    (
        StatusCode::OK,
        Json(ApiResponse {
            success: true,
            data: Some(response_data),
            error: None,
        }),
    )
        .into_response()
}

#[utoipa::path(
    post,
    path = "/bootstrap/migrate",
    request_body = MigrateRequest,
    responses(
        (status = 200, description = "Migrations applied + SMT rebuilt + roots updated", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 422, description = "Unprocessable entity (invalid JSON body)", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn bootstrap_migrate_handler(
    State(state): State<AppState>,
    request: Result<Json<MigrateRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid JSON body: {} (expected: {{\"confirm\": true}})",
                        e
                    )),
                }),
            )
                .into_response();
        }
    };

    if !request.confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("confirm must be true to run migrations".to_string()),
            }),
        )
            .into_response();
    }

    // Serialize all DB + SMT work under the db_service mutex to avoid concurrent reads/writes
    // while schema/data are changing.
    // Prevent any interleaving with background commits / other writes.
    let _root_guard = state.root_manager.lock_root().await;
    let mut db_service = state.db_service.lock().await;
    let pool = db_service.pool().clone();

    let old_temp_root = state.root_manager.get_temporary_root().await;
    let old_main_root = state.root_manager.get_main_root().await;

    // 1) Apply server-side migrations (sqlx, loaded at runtime from ./migrations).
    let migrator = match sqlx::migrate::Migrator::new(Path::new("./migrations")).await {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Failed initializing migrator: {}", e)),
                }),
            )
                .into_response();
        }
    };

    if let Err(e) = migrator.run(&pool).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(format!("Failed applying migrations: {}", e)),
            }),
        )
            .into_response();
    }

    // 2) Schema drift handling for client-table migrations:
    //
    // If the client alters tables (ADD COLUMN, etc.), update `verifiable_models.columns`
    // from the live Postgres schema so:
    // - new columns participate in type casting on writes
    // - warm-started registry after restart stays accurate
    //
    // We do NOT try to infer UNIQUE constraints here (set to false).
    let table_rows = sqlx::query("SELECT table_name, primary_key_field FROM verifiable_models")
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

    for tr in table_rows {
        let table_name: String = match tr.try_get("table_name") {
            Ok(v) => v,
            Err(_) => continue,
        };
        let pk_field: String = tr.try_get("primary_key_field").unwrap_or_default();

        let col_rows = sqlx::query(
            "SELECT column_name, data_type, is_nullable
             FROM information_schema.columns
             WHERE table_schema = 'public' AND table_name = $1
             ORDER BY ordinal_position",
        )
        .bind(&table_name)
        .fetch_all(&pool)
        .await
        .unwrap_or_default();

        let mut cols: Vec<serde_json::Value> = Vec::new();
        for cr in col_rows {
            let name: String = cr.try_get("column_name").unwrap_or_default();
            if name.is_empty() || name == pk_field {
                continue;
            }
            let data_type: String = cr.try_get("data_type").unwrap_or_default();
            let is_nullable_str: String = cr
                .try_get("is_nullable")
                .unwrap_or_else(|_| "YES".to_string());
            let nullable = is_nullable_str.to_uppercase() == "YES";

            // Map info_schema types into our column_type strings used by ModelRegistry::load_from_db.
            let col_type = match data_type.as_str() {
                "integer" => "int",
                "bigint" => "big_int",
                "boolean" => "bool",
                "text" => "text",
                "uuid" => "uuid",
                "jsonb" => "jsonb",
                "timestamp with time zone" => "timestamptz",
                other => other,
            };

            cols.push(serde_json::json!({
                "name": name,
                "col_type": col_type,
                "nullable": nullable,
                "unique": false
            }));
        }

        let _ = sqlx::query(
            "UPDATE verifiable_models
             SET columns = $1, updated_at = now()
             WHERE table_name = $2",
        )
        .bind(serde_json::Value::Array(cols))
        .bind(&table_name)
        .execute(&pool)
        .await;
    }

    // 3) Reload the runtime registry from DB (verifiable_models) to ensure we rebuild SMT
    // from the models this service is configured to verify (and to pick up new columns).
    let new_registry = match ModelRegistry::load_from_db(&pool).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Failed loading model registry from DB: {}", e)),
                }),
            )
                .into_response();
        }
    };

    {
        let mut reg_lock = state.model_registry.write().await;
        *reg_lock = new_registry;
    }

    // 4) Recompute SMT from post-migration DB rows and force-update both roots.
    let models = {
        let reg = state.model_registry.read().await;
        let mut out = Vec::new();
        for name in reg.list_models() {
            if let Some(m) = reg.get(&name) {
                out.push(m);
            }
        }
        out
    };

    let (new_root, updated_leaves) = match db_service.rebuild_smt_from_db(models).await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Failed rebuilding SMT from DB: {}", e)),
                }),
            )
                .into_response();
        }
    };

    if let Err(e) = state.root_manager.force_set_roots_and_commit(new_root).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(format!("Failed committing new root to Solana: {}", e)),
            }),
        )
            .into_response();
    }

    let response_data = serde_json::json!({
        "migrated": true,
        "updated_leaves": updated_leaves,
        "old_temporary_root": hex::encode(old_temp_root.as_bytes()),
        "old_main_root": hex::encode(old_main_root.as_bytes()),
        "new_root": hex::encode(new_root.as_bytes()),
        "message": "Migrations applied. SMT rebuilt from post-migration DB state. temporary_root + main_root committed to Solana."
    });

    (
        StatusCode::OK,
        Json(ApiResponse {
            success: true,
            data: Some(response_data),
            error: None,
        }),
    )
        .into_response()
}

#[utoipa::path(
    post,
    path = "/bootstrap/repair-roots",
    request_body = RepairRootsRequest,
    responses(
        (status = 200, description = "SMT rebuilt from DB + roots force-set and committed", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 422, description = "Unprocessable entity (invalid JSON body)", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn bootstrap_repair_roots_handler(
    State(state): State<AppState>,
    request: Result<Json<RepairRootsRequest>, JsonRejection>,
) -> impl IntoResponse {
    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid JSON body: {} (expected: {{\"confirm\": true}})",
                        e
                    )),
                }),
            )
                .into_response();
        }
    };

    if !request.confirm {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("confirm must be true to repair roots".to_string()),
            }),
        )
            .into_response();
    }

    // Prevent any interleaving with writes/commits while we rebuild.
    let _root_guard = state.root_manager.lock_root().await;

    let mut db_service = state.db_service.lock().await;
    let pool = db_service.pool().clone();

    // Load registry from DB and rebuild SMT from current table rows (canonical row_to_json hashing).
    let reg = match crate::domain::model::ModelRegistry::load_from_db(&pool).await {
        Ok(r) => r,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Failed loading model registry from DB: {}", e)),
                }),
            )
                .into_response();
        }
    };

    if reg.list_models().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("No models found in verifiable_models; nothing to repair.".to_string()),
            }),
        )
            .into_response();
    }

    let mut models = Vec::new();
    for name in reg.list_models() {
        if let Some(m) = reg.get(&name) {
            models.push(m);
        }
    }

    let (new_root, updated_leaves) = match db_service.rebuild_smt_from_db(models).await {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Failed rebuilding SMT from DB: {}", e)),
                }),
            )
                .into_response();
        }
    };

    if let Err(e) = state.root_manager.force_set_roots_and_commit(new_root).await {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(format!("Failed committing repaired root to Solana: {}", e)),
            }),
        )
            .into_response();
    }

    let response_data = serde_json::json!({
        "repaired": true,
        "updated_leaves": updated_leaves,
        "new_root": hex::encode(new_root.as_bytes()),
        "message": "Rebuilt SMT from DB rows and force-set temporary_root + main_root to the rebuilt root."
    });

    (
        StatusCode::OK,
        Json(ApiResponse {
            success: true,
            data: Some(response_data),
            error: None,
        }),
    )
        .into_response()
}
