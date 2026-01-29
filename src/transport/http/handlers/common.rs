use crate::transport::http::types::{ApiResponse, ColumnType, PrimaryKeyKind};
use axum::http::StatusCode;
use axum::Json;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;
use chrono::DateTime;

pub async fn ensure_model_registered(
    state: &crate::transport::http::types::AppState,
    model_name: &str,
) -> Result<Arc<dyn crate::domain::model::VerifiableModel>, (StatusCode, Json<ApiResponse>)> {
    let registry = state.model_registry.read().await;
    registry.get(model_name).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(format!("Model '{}' is not registered", model_name)),
            }),
        )
    })
}

/// Like `ensure_model_registered`, but attempts a best-effort refresh of the in-memory registry
/// from `verifiable_models` to avoid intermittent "model not registered" failures after restart.
///
/// NOTE: this locks `db_service`, so callers must NOT already hold the db_service mutex.
pub async fn ensure_model_registered_refreshing(
    state: &crate::transport::http::types::AppState,
    model_name: &str,
) -> Result<Arc<dyn crate::domain::model::VerifiableModel>, (StatusCode, Json<ApiResponse>)> {
    {
        let registry = state.model_registry.read().await;
        if let Some(m) = registry.get(model_name) {
            return Ok(m);
        }
    }

    let db_service = state.db_service.lock().await;
    let pool = db_service.pool().clone();
    drop(db_service);

    if let Ok(new_registry) = crate::domain::model::ModelRegistry::load_from_db(&pool).await {
        let mut reg = state.model_registry.write().await;
        *reg = new_registry;
    }

    let registry = state.model_registry.read().await;
    registry.get(model_name).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(format!("Model '{}' is not registered", model_name)),
            }),
        )
    })
}

pub fn pk_json_to_string(pk: &JsonValue) -> Option<String> {
    if let Some(s) = pk.as_str() {
        return Some(s.to_string());
    }
    if let Some(i) = pk.as_i64() {
        return Some(i.to_string());
    }
    if let Some(u) = pk.as_u64() {
        return Some(u.to_string());
    }
    None
}

pub fn validate_ident(ident: &str) -> bool {
    let mut chars = ident.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

pub fn column_type_to_sql(t: &ColumnType) -> &'static str {
    match t {
        ColumnType::Text => "TEXT",
        ColumnType::Int => "INTEGER",
        ColumnType::BigInt => "BIGINT",
        ColumnType::Bool => "BOOLEAN",
        ColumnType::Jsonb => "JSONB",
        ColumnType::Timestamptz => "TIMESTAMPTZ",
        ColumnType::Uuid => "UUID",
    }
}

pub fn pk_kind_to_sql(pk: &PrimaryKeyKind) -> &'static str {
    match pk {
        PrimaryKeyKind::Serial => "SERIAL",
        PrimaryKeyKind::BigSerial => "BIGSERIAL",
        PrimaryKeyKind::Text => "TEXT",
        PrimaryKeyKind::Int => "INTEGER",
        PrimaryKeyKind::BigInt => "BIGINT",
        PrimaryKeyKind::Uuid => "UUID",
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct FieldError {
    pub index: usize,
    pub field: String,
    pub expected: String,
    pub got: String,
    pub value: JsonValue,
}

pub fn coerce_scalar_for_type(
    expected_sql_type: &str,
    v: &JsonValue,
) -> Result<JsonValue, String> {
    let t = expected_sql_type.to_lowercase();
    match t.as_str() {
        "int" | "int4" | "integer" => {
            if let Some(n) = v.as_i64() {
                if n < i32::MIN as i64 || n > i32::MAX as i64 {
                    return Err(format!("out of range for int: {}", n));
                }
                return Ok(JsonValue::from(n));
            }
            if let Some(s) = v.as_str() {
                let parsed = s.parse::<i64>().map_err(|_| "expected int".to_string())?;
                if parsed < i32::MIN as i64 || parsed > i32::MAX as i64 {
                    return Err(format!("out of range for int: {}", parsed));
                }
                return Ok(JsonValue::from(parsed));
            }
            Err("expected int".to_string())
        }
        "bigint" | "int8" => {
            if let Some(n) = v.as_i64() {
                return Ok(JsonValue::from(n));
            }
            if let Some(s) = v.as_str() {
                let parsed = s
                    .parse::<i64>()
                    .map_err(|_| "expected bigint".to_string())?;
                return Ok(JsonValue::from(parsed));
            }
            Err("expected bigint".to_string())
        }
        "bool" | "boolean" => {
            if let Some(b) = v.as_bool() {
                return Ok(JsonValue::from(b));
            }
            if let Some(s) = v.as_str() {
                let lc = s.trim().to_lowercase();
                return match lc.as_str() {
                    "true" | "t" | "1" => Ok(JsonValue::from(true)),
                    "false" | "f" | "0" => Ok(JsonValue::from(false)),
                    _ => Err("expected bool".to_string()),
                };
            }
            Err("expected bool".to_string())
        }
        "uuid" => {
            if let Some(s) = v.as_str() {
                return Ok(JsonValue::from(s));
            }
            Err("expected uuid string".to_string())
        }
        "timestamptz" => {
            if let Some(s) = v.as_str() {
                // Validate as RFC3339 (what we document elsewhere). We still keep the value as string;
                // sqlx bind layer can also parse it later if needed.
                DateTime::parse_from_rfc3339(s).map_err(|_| "expected RFC3339 timestamp".to_string())?;
                return Ok(JsonValue::from(s));
            }
            Err("expected timestamp string".to_string())
        }
        "jsonb" => Ok(v.clone()),
        "text" => {
            if let Some(s) = v.as_str() {
                return Ok(JsonValue::from(s));
            }
            // allow numbers/bools to stringify for text columns
            Ok(JsonValue::from(v.to_string()))
        }
        _ => Ok(v.clone()),
    }
}

pub fn parse_h256_hex(s: &str) -> Result<primitive_types::H256, String> {
    let s = s.trim();
    let s = s.strip_prefix("0x").unwrap_or(s);
    let bytes = hex::decode(s).map_err(|_| "invalid hex".to_string())?;
    if bytes.len() != 32 {
        return Err("expected 32-byte hex string".to_string());
    }
    Ok(primitive_types::H256::from_slice(&bytes))
}
