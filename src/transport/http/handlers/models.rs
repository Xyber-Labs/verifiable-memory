use crate::crypto::hashing::{hash_key, hash_value};
use crate::domain::verify::verify_smt_proof;
use crate::transport::http::handlers::common::{
    coerce_scalar_for_type, ensure_model_registered_refreshing, parse_h256_hex, pk_json_to_string,
    validate_ident, FieldError,
};
use crate::transport::http::types::{
    ApiResponse, AppState, CreateBatchRequest, OrderDirection, ReadBatchRequest, ReadLatestRequest,
    UpsertBatchRequest,
};
use axum::extract::{Path, State};
use axum::extract::rejection::JsonRejection;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::Value as JsonValue;

#[utoipa::path(
    post,
    path = "/api/models/{model}/create-batch",
    params(
        ("model" = String, Path, description = "Model name (e.g. users)")
    ),
    request_body = CreateBatchRequest,
    responses(
        (status = 200, description = "Batch created", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 422, description = "Unprocessable entity (invalid JSON body)", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn create_batch_handler(
    State(state): State<AppState>,
    Path(model): Path<String>,
    request: Result<Json<CreateBatchRequest>, JsonRejection>,
) -> impl IntoResponse {
    let model_name_str = model.trim().to_lowercase();

    let model = match ensure_model_registered_refreshing(&state, &model_name_str).await {
        Ok(m) => m,
        Err(resp) => return resp.into_response(),
    };
    let _table_name = model.table_name();

    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid JSON body: {} (expected: {{\"records\": [...]}})",
                        e
                    )),
                }),
            )
                .into_response();
        }
    };

    // Acquire root lock for the entire write critical section.
    let root_guard = state.root_manager.lock_root().await;

    // Optional optimistic concurrency: fail-fast if root changed.
    if let Some(expected) = request.expected_root.as_deref() {
        let expected_root = match parse_h256_hex(expected) {
            Ok(r) => r,
            Err(e) => {
                drop(root_guard);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        error: Some(format!("Invalid expected_root: {}", e)),
                    }),
                )
                    .into_response();
            }
        };
        let current = state.root_manager.get_temporary_root().await;
        if current != expected_root {
            drop(root_guard);
            return (
                StatusCode::CONFLICT,
                Json(ApiResponse {
                    success: false,
                    data: Some(serde_json::json!({
                        "code": "ROOT_CHANGED",
                        "expected_root": hex::encode(expected_root.as_bytes()),
                        "current_root": hex::encode(current.as_bytes())
                    })),
                    error: Some("Root changed, retry the write".to_string()),
                }),
            )
                .into_response();
        }
    }

    // Server-side coercion for common scalar types to reduce client/LLM friction (e.g. "1440" -> int).
    let mut errors: Vec<FieldError> = Vec::new();
    let mut coerced_records: Vec<JsonValue> = Vec::with_capacity(request.records.len());
    for (idx, record) in request.records.iter().enumerate() {
        let obj = match record.as_object() {
            Some(o) => o,
            None => {
                errors.push(FieldError {
                    index: idx,
                    field: "<record>".to_string(),
                    expected: "object".to_string(),
                    got: format!("{:?}", record),
                    value: record.clone(),
                });
                continue;
            }
        };
        let mut out = serde_json::Map::new();
        for (k, v) in obj {
            let expected = model.column_type(k).unwrap_or("text").to_string();
            let got = if v.is_string() {
                "string"
            } else if v.is_number() {
                "number"
            } else if v.is_boolean() {
                "bool"
            } else if v.is_null() {
                "null"
            } else if v.is_array() {
                "array"
            } else {
                "object"
            }
            .to_string();
            match coerce_scalar_for_type(&expected, v) {
                Ok(cv) => {
                    out.insert(k.clone(), cv);
                }
                Err(msg) => {
                    errors.push(FieldError {
                        index: idx,
                        field: k.clone(),
                        expected,
                        got,
                        value: v.clone(),
                    });
                    // Keep original so we can continue collecting other errors.
                    out.insert(k.clone(), v.clone());
                    // Attach message via error string aggregation.
                    let _ = msg;
                }
            }
        }
        coerced_records.push(JsonValue::Object(out));
    }
    if !errors.is_empty() {
        drop(root_guard);
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: Some(serde_json::json!({ "errors": errors })),
                error: Some("Validation/coercion failed".to_string()),
            }),
        )
            .into_response();
    }

    let trusted_root = state.root_manager.get_temporary_root().await;

    let db_service = state.db_service.lock().await;
    match db_service
        .create_records(model.clone(), &coerced_records, trusted_root)
        .await
    {
        Ok((proposed_root, _proof, inserted_records, inserted_ids)) => {
            println!("> TEE (API): Validation successful. Updating temporary_root.");
            let triggers_commit = state.root_manager.update_temporary_root(proposed_root).await;

            drop(db_service);
            drop(root_guard);

            if triggers_commit {
                println!("> TEE (API): Waiting for blockchain commit to complete...");
                state.root_manager.wait_for_commit_completion().await;
                println!("> TEE (API): Blockchain commit completed.");
            }

            let response_data = serde_json::json!({
                "ids": inserted_ids,
                "records": inserted_records,
                "verified": true,
                "meta": {
                    "proposed_root": hex::encode(proposed_root.as_bytes()),
                    "committed": triggers_commit
                }
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
        Err(e) => (
            if e.to_string().starts_with("VERIFIABLE_PROOF_FAILED") {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            },
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(e.to_string()),
            }),
        )
            .into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/api/models/{model}/read-batch",
    params(
        ("model" = String, Path, description = "Model name (e.g. users)")
    ),
    request_body = ReadBatchRequest,
    responses(
        (status = 200, description = "Batch read", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 404, description = "Not found", body = ApiResponse),
        (status = 422, description = "Unprocessable entity (invalid JSON body)", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn read_batch_handler(
    State(state): State<AppState>,
    Path(model): Path<String>,
    request: Result<Json<ReadBatchRequest>, JsonRejection>,
) -> impl IntoResponse {
    let model_name_str = model.trim().to_lowercase();

    let model = match ensure_model_registered_refreshing(&state, &model_name_str).await {
        Ok(m) => m,
        Err(resp) => return resp.into_response(),
    };
    let table_name = model.table_name();

    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid JSON body: {} (expected: {{\"ids\": [...]}})",
                        e
                    )),
                }),
            )
                .into_response();
        }
    };

    let ids_str: Vec<&str> = request.ids.iter().map(AsRef::as_ref).collect();

    let db_service = state.db_service.lock().await;

    match db_service.get_records_with_proof(model.clone(), ids_str).await {
        Ok(Some((records, proof))) => {
            let trusted_root = state.root_manager.get_temporary_root().await;
            // Helpful debug: compare DB SMT root vs trusted in-memory root
            if let Ok(smt_root) = db_service.current_smt_root().await {
                println!(
                    "> TEE (API): Read-batch root check: smt_root={} temporary_root={} match={}",
                    hex::encode(smt_root.as_bytes()),
                    hex::encode(trusted_root.as_bytes()),
                    smt_root == trusted_root
                );
            }

            let pk_field = model.primary_key_field();
            let mut leaves_to_verify = Vec::new();
            for record in &records {
                let record_obj = match record.as_object() {
                    Some(obj) => obj,
                    None => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiResponse {
                                success: false,
                                data: None,
                                error: Some("Invalid record format".to_string()),
                            }),
                        )
                            .into_response();
                    }
                };

                let pk_value = match record_obj.get(pk_field).and_then(pk_json_to_string) {
                    Some(val) => val,
                    None => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiResponse {
                                success: false,
                                data: None,
                                error: Some(format!(
                                    "Primary key field '{}' not found",
                                    pk_field
                                )),
                            }),
                        )
                            .into_response();
                    }
                };

                let leaf_key = hash_key(table_name, &pk_value);
                let leaf_value_hash = hash_value(record);
                leaves_to_verify.push((leaf_key, leaf_value_hash));
            }

            let is_valid_proof = verify_smt_proof(trusted_root, leaves_to_verify, proof);
            if !is_valid_proof {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        error: Some(
                            "Proof verification failed - data integrity cannot be verified"
                                .to_string(),
                        ),
                    }),
                )
                    .into_response();
            }

            let response_ids: Vec<String> = records
                .iter()
                .filter_map(|r| r.as_object())
                .filter_map(|o| o.get(model.primary_key_field()).and_then(pk_json_to_string))
                .collect();

            let response_data = serde_json::json!({
                "ids": if response_ids.is_empty() { request.ids } else { response_ids },
                "records": records,
                "verified": true
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
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("No records found for the given IDs.".to_string()),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(e.to_string()),
            }),
        )
            .into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/api/models/{model}/read-latest",
    params(
        ("model" = String, Path, description = "Model name (e.g. users)")
    ),
    request_body = ReadLatestRequest,
    responses(
        (status = 200, description = "Latest records read + verified", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 404, description = "Not found", body = ApiResponse),
        (status = 422, description = "Unprocessable entity (invalid JSON body)", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn read_latest_handler(
    State(state): State<AppState>,
    Path(model): Path<String>,
    request: Result<Json<ReadLatestRequest>, JsonRejection>,
) -> impl IntoResponse {
    let model_name_str = model.trim().to_lowercase();

    let model = match ensure_model_registered_refreshing(&state, &model_name_str).await {
        Ok(m) => m,
        Err(resp) => return resp.into_response(),
    };
    let table_name = model.table_name();

    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid JSON body: {} (expected: {{\"limit\": 5}})",
                        e
                    )),
                }),
            )
                .into_response();
        }
    };

    let limit = request.limit;
    if limit == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("limit must be >= 1".to_string()),
            }),
        )
            .into_response();
    }
    let limit = limit.min(100);

    let db_service = state.db_service.lock().await;

    // Validate filters + ordering (restricted).
    if let Some(where_map) = &request.r#where {
        for k in where_map.keys() {
            if !validate_ident(k) {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        error: Some(format!("Invalid where field '{}'", k)),
                    }),
                )
                    .into_response();
            }
            if model.column_type(k).is_none() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        error: Some(format!("Unknown where field '{}'", k)),
                    }),
                )
                    .into_response();
            }
        }
    }
    if let Some(ob) = &request.order_by {
        if !validate_ident(&ob.field) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Invalid order_by.field '{}'", ob.field)),
                }),
            )
                .into_response();
        }
        if model.column_type(&ob.field).is_none() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!("Unknown order_by.field '{}'", ob.field)),
                }),
            )
                .into_response();
        }
    }

    // Coerce filter values based on column types
    let mut coerced_where: Option<std::collections::HashMap<String, JsonValue>> = None;
    if let Some(where_map) = &request.r#where {
        let mut out = std::collections::HashMap::new();
        let mut errors: Vec<FieldError> = Vec::new();
        for (k, v) in where_map {
            let expected = model.column_type(k).unwrap_or("text").to_string();
            let got = if v.is_string() {
                "string"
            } else if v.is_number() {
                "number"
            } else if v.is_boolean() {
                "bool"
            } else if v.is_null() {
                "null"
            } else if v.is_array() {
                "array"
            } else {
                "object"
            }
            .to_string();
            match coerce_scalar_for_type(&expected, v) {
                Ok(cv) => {
                    out.insert(k.clone(), cv);
                }
                Err(_) => {
                    errors.push(FieldError {
                        index: 0,
                        field: k.clone(),
                        expected,
                        got,
                        value: v.clone(),
                    });
                }
            }
        }
        if !errors.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                Json(ApiResponse {
                    success: false,
                    data: Some(serde_json::json!({ "errors": errors })),
                    error: Some("Invalid where filter values".to_string()),
                }),
            )
                .into_response();
        }
        coerced_where = Some(out);
    }

    let order_by = request.order_by.as_ref().map(|ob| {
        (
            ob.field.as_str(),
            matches!(ob.direction, OrderDirection::Desc),
        )
    });

    match db_service
        .get_latest_records_with_proof_filtered(model.clone(), limit, coerced_where.as_ref(), order_by)
        .await
    {
        Ok(Some((records, ids, proof))) => {
            let trusted_root = state.root_manager.get_temporary_root().await;
            // Helpful debug: compare DB SMT root vs trusted in-memory root
            if let Ok(smt_root) = db_service.current_smt_root().await {
                println!(
                    "> TEE (API): Read-latest root check: smt_root={} temporary_root={} match={} (limit={})",
                    hex::encode(smt_root.as_bytes()),
                    hex::encode(trusted_root.as_bytes()),
                    smt_root == trusted_root,
                    limit
                );
            }

            let pk_field = model.primary_key_field();
            let mut leaves_to_verify = Vec::new();
            for record in &records {
                let record_obj = match record.as_object() {
                    Some(obj) => obj,
                    None => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiResponse {
                                success: false,
                                data: None,
                                error: Some("Invalid record format".to_string()),
                            }),
                        )
                            .into_response();
                    }
                };

                let pk_value = match record_obj.get(pk_field).and_then(pk_json_to_string) {
                    Some(val) => val,
                    None => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ApiResponse {
                                success: false,
                                data: None,
                                error: Some(format!(
                                    "Primary key field '{}' not found",
                                    pk_field
                                )),
                            }),
                        )
                            .into_response();
                    }
                };

                let leaf_key = hash_key(table_name, &pk_value);
                let leaf_value_hash = hash_value(record);
                leaves_to_verify.push((leaf_key, leaf_value_hash));
            }

            let is_valid_proof = verify_smt_proof(trusted_root, leaves_to_verify, proof);
            if !is_valid_proof {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        error: Some(
                            "Proof verification failed - data integrity cannot be verified"
                                .to_string(),
                        ),
                    }),
                )
                    .into_response();
            }

            let response_data = serde_json::json!({
                "ids": ids,
                "records": records,
                "verified": true,
                "meta": { "limit": limit }
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
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some("No records found".to_string()),
            }),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(e.to_string()),
            }),
        )
            .into_response(),
    }
}

#[utoipa::path(
    post,
    path = "/api/models/{model}/upsert",
    params(
        ("model" = String, Path, description = "Model name (e.g. users)")
    ),
    request_body = UpsertBatchRequest,
    responses(
        (status = 200, description = "Batch upserted + verified", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 422, description = "Unprocessable entity (invalid JSON body)", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn upsert_batch_handler(
    State(state): State<AppState>,
    Path(model): Path<String>,
    request: Result<Json<UpsertBatchRequest>, JsonRejection>,
) -> impl IntoResponse {
    let model_name_str = model.trim().to_lowercase();

    let model = match ensure_model_registered_refreshing(&state, &model_name_str).await {
        Ok(m) => m,
        Err(resp) => return resp.into_response(),
    };
    let _table_name = model.table_name();
    let pk_field = model.primary_key_field().to_string();

    let Json(request) = match request {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(ApiResponse {
                    success: false,
                    data: None,
                    error: Some(format!(
                        "Invalid JSON body: {} (expected: {{\"records\": [...]}})",
                        e
                    )),
                }),
            )
                .into_response();
        }
    };

    // Acquire root lock for the entire write critical section.
    let root_guard = state.root_manager.lock_root().await;

    // Optional optimistic concurrency: fail-fast if root changed.
    if let Some(expected) = request.expected_root.as_deref() {
        let expected_root = match parse_h256_hex(expected) {
            Ok(r) => r,
            Err(e) => {
                drop(root_guard);
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ApiResponse {
                        success: false,
                        data: None,
                        error: Some(format!("Invalid expected_root: {}", e)),
                    }),
                )
                    .into_response();
            }
        };
        let current = state.root_manager.get_temporary_root().await;
        if current != expected_root {
            drop(root_guard);
            return (
                StatusCode::CONFLICT,
                Json(ApiResponse {
                    success: false,
                    data: Some(serde_json::json!({
                        "code": "ROOT_CHANGED",
                        "expected_root": hex::encode(expected_root.as_bytes()),
                        "current_root": hex::encode(current.as_bytes())
                    })),
                    error: Some("Root changed, retry the write".to_string()),
                }),
            )
                .into_response();
        }
    }

    let mut errors: Vec<FieldError> = Vec::new();
    let mut coerced_records: Vec<JsonValue> = Vec::with_capacity(request.records.len());
    for (idx, record) in request.records.iter().enumerate() {
        let obj = match record.as_object() {
            Some(o) => o,
            None => {
                errors.push(FieldError {
                    index: idx,
                    field: "<record>".to_string(),
                    expected: "object".to_string(),
                    got: format!("{:?}", record),
                    value: record.clone(),
                });
                continue;
            }
        };

        if !obj.contains_key(&pk_field) {
            errors.push(FieldError {
                index: idx,
                field: pk_field.clone(),
                expected: "present".to_string(),
                got: "missing".to_string(),
                value: JsonValue::Null,
            });
            continue;
        }

        let mut out = serde_json::Map::new();
        for (k, v) in obj {
            let expected = model.column_type(k).unwrap_or("text").to_string();
            let got = if v.is_string() {
                "string"
            } else if v.is_number() {
                "number"
            } else if v.is_boolean() {
                "bool"
            } else if v.is_null() {
                "null"
            } else if v.is_array() {
                "array"
            } else {
                "object"
            }
            .to_string();
            match coerce_scalar_for_type(&expected, v) {
                Ok(cv) => {
                    out.insert(k.clone(), cv);
                }
                Err(_) => {
                    errors.push(FieldError {
                        index: idx,
                        field: k.clone(),
                        expected,
                        got,
                        value: v.clone(),
                    });
                    out.insert(k.clone(), v.clone());
                }
            }
        }
        coerced_records.push(JsonValue::Object(out));
    }
    if !errors.is_empty() {
        drop(root_guard);
        return (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse {
                success: false,
                data: Some(serde_json::json!({ "errors": errors })),
                error: Some("Validation/coercion failed".to_string()),
            }),
        )
            .into_response();
    }

    let trusted_root = state.root_manager.get_temporary_root().await;

    let db_service = state.db_service.lock().await;
    match db_service
        .upsert_records(model.clone(), &coerced_records, trusted_root)
        .await
    {
        Ok((proposed_root, _proof, upserted_records, upserted_ids)) => {
            let triggers_commit = state.root_manager.update_temporary_root(proposed_root).await;
            drop(db_service);
            drop(root_guard);
            if triggers_commit {
                state.root_manager.wait_for_commit_completion().await;
            }

            let response_data = serde_json::json!({
                "ids": upserted_ids,
                "records": upserted_records,
                "verified": true,
                "meta": {
                    "proposed_root": hex::encode(proposed_root.as_bytes()),
                    "committed": triggers_commit
                }
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
        Err(e) => (
            if e.to_string().starts_with("VERIFIABLE_PROOF_FAILED") {
                StatusCode::CONFLICT
            } else {
                StatusCode::INTERNAL_SERVER_ERROR
            },
            Json(ApiResponse {
                success: false,
                data: None,
                error: Some(e.to_string()),
            }),
        )
            .into_response(),
    }
}
