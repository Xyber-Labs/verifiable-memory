use crate::crypto::hashing::{hash_key, hash_value};
use crate::domain::verify::verify_smt_proof;
use crate::transport::http::handlers::common::{ensure_model_registered_refreshing, pk_json_to_string};
use crate::transport::http::types::{Action, ApiRequest, ApiResponse, AppState};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::Value as JsonValue;

#[utoipa::path(
    post,
    path = "/api/execute",
    request_body = ApiRequest,
    responses(
        (status = 200, description = "Execution successful", body = ApiResponse),
        (status = 400, description = "Bad request", body = ApiResponse),
        (status = 500, description = "Internal server error", body = ApiResponse)
    )
)]
pub async fn execute_handler(
    State(state): State<AppState>,
    Json(request): Json<ApiRequest>,
) -> impl IntoResponse {
    let model_name_str = request.model_name.trim().to_lowercase();

    let model = match ensure_model_registered_refreshing(&state, &model_name_str).await {
        Ok(m) => m,
        Err(resp) => return resp.into_response(),
    };
    let table_name = model.table_name();

    match request.action {
        Action::CreateBatch => {
            let records: Vec<JsonValue> = match serde_json::from_value(request.payload) {
                Ok(r) => r,
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse {
                            success: false,
                            data: None,
                            error: Some(format!("Invalid payload for create_batch: {}", e)),
                        }),
                    )
                        .into_response();
                }
            };

            // Acquire root lock for the entire write critical section.
            let root_guard = state.root_manager.lock_root().await;

            let db_service = state.db_service.lock().await;
            let trusted_root = state.root_manager.get_temporary_root().await;
            match db_service
                .create_records(model.clone(), &records, trusted_root)
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
        Action::ReadBatch => {
            let ids: Vec<String> = match serde_json::from_value(request.payload) {
                Ok(val) => match val {
                    JsonValue::Object(map) => match map.get("ids") {
                        Some(JsonValue::Array(arr)) => arr
                            .iter()
                            .map(|v| v.as_str().unwrap_or_default().to_string())
                            .collect(),
                        _ => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(ApiResponse {
                                    success: false,
                                    data: None,
                                    error: Some(
                                        "Invalid payload: 'ids' field must be an array of strings."
                                            .to_string(),
                                    ),
                                }),
                            )
                                .into_response();
                        }
                    },
                    _ => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(ApiResponse {
                                success: false,
                                data: None,
                                error: Some(
                                    "Invalid payload: expected an object with an 'ids' field."
                                        .to_string(),
                                ),
                            }),
                        )
                            .into_response();
                    }
                },
                Err(e) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ApiResponse {
                            success: false,
                            data: None,
                            error: Some(format!("Invalid payload for read_batch: {}", e)),
                        }),
                    )
                        .into_response();
                }
            };

            let ids_str: Vec<&str> = ids.iter().map(AsRef::as_ref).collect();

            let db_service = state.db_service.lock().await;
            match db_service.get_records_with_proof(model.clone(), ids_str).await {
                Ok(Some((records, proof))) => {
                    let trusted_root = state.root_manager.get_temporary_root().await;

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
    }
}

