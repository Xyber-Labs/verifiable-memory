//! End-to-end simulation test: bootstrap schema, run batched writes, verify reads.
//!
//! This is the integration-test replacement for the former `src/main.rs` simulation binary.

use chrono::Utc;
use serde_json::json;
use sqlx::Row;
use std::env;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{Mutex, RwLock};
use verifiable_memory_example::{solana, transport, DatabaseService, ModelRegistry, RootManager};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_schema_update() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

    // Keep batch size small so we see commits in logs.
    if env::var("BATCH_COMMIT_SIZE").is_err() {
        env::set_var("BATCH_COMMIT_SIZE", "3");
    }

    println!("--- test_schema_update ---");

    // Ensure Solana root account exists.
    solana::initialize().await?;

    // Start RootManager + background batching.
    let root_manager = Arc::new(RootManager::new().await?);
    root_manager.clone().start_background_commit_task();

    // Start API in-process (router) for the test.
    let model_registry = Arc::new(RwLock::new(ModelRegistry::new()));
    let db_service_arc = Arc::new(Mutex::new(DatabaseService::new().await?));
    let app_state = transport::http::AppState {
        db_service: db_service_arc.clone(),
        model_registry,
        root_manager: root_manager.clone(),
    };
    let router = transport::http::create_router(app_state);

    // Bind to an ephemeral port to avoid conflicts if an API server is already running.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let server_handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });

    let base_url = format!("http://127.0.0.1:{}", port);
    let client = reqwest::Client::new();

    // --- BOOTSTRAP: apply schema spec (single-tenant) ---
    let force_reset = env::var("CLEAR_DB").unwrap_or_else(|_| "true".to_string()) == "true";
    let bootstrap_resp = client
        .post(&format!("{}/bootstrap/apply-schema", base_url))
        .json(&json!({
            "force_reset": force_reset,
            "tables": [
                {
                    "table_name": "agents",
                    "primary_key_field": "id",
                    "primary_key_kind": "big_serial",
                    "columns": [
                        {"name":"name","col_type":"text","nullable":false,"unique":true},
                        {"name":"character","col_type":"jsonb","nullable":false,"unique":false},
                        {"name":"created_at","col_type":"timestamptz","nullable":true,"unique":false},
                        {"name":"updated_at","col_type":"timestamptz","nullable":true,"unique":false}
                    ]
                },
                {
                    "table_name": "follow_queue",
                    "primary_key_field": "id",
                    "primary_key_kind": "serial",
                    "columns": [
                        {"name":"user_id","col_type":"text","nullable":false,"unique":false},
                        {"name":"username","col_type":"text","nullable":false,"unique":false},
                        {"name":"type","col_type":"text","nullable":true,"unique":false},
                        {"name":"agent_id","col_type":"big_int","nullable":false,"unique":false},
                        {"name":"created_at","col_type":"timestamptz","nullable":true,"unique":false},
                        {"name":"updated_at","col_type":"timestamptz","nullable":true,"unique":false}
                    ]
                },
                {
                    "table_name": "agent_tweets",
                    "primary_key_field": "id",
                    "primary_key_kind": "serial",
                    "columns": [
                        {"name":"agent_id","col_type":"big_int","nullable":false,"unique":false},
                        {"name":"tweet_id","col_type":"text","nullable":false,"unique":true},
                        {"name":"content","col_type":"text","nullable":false,"unique":false},
                        {"name":"type","col_type":"text","nullable":true,"unique":false},
                        {"name":"type_of_post","col_type":"text","nullable":true,"unique":false},
                        {"name":"poll_options","col_type":"jsonb","nullable":true,"unique":false},
                        {"name":"status","col_type":"text","nullable":true,"unique":false}
                    ]
                }
            ]
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    assert!(bootstrap_resp["success"].as_bool().unwrap_or(false));

    async fn create_batch(
        client: &reqwest::Client,
        base_url: &str,
        model: &str,
        records: Vec<serde_json::Value>,
    ) -> Result<(bool, Vec<String>), Box<dyn std::error::Error>> {
        let resp = client
        .post(&format!("{}/api/models/{}/create-batch", base_url, model))
            .json(&json!({ "records": records }))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if !resp["success"].as_bool().unwrap_or(false) {
            return Err(format!(
                "create-batch failed for {}: {}",
                model,
                resp["error"].as_str().unwrap_or("unknown error")
            )
            .into());
        }
        let committed = resp["data"]["committed"].as_bool().unwrap_or(false);
        let ids = resp["data"]["ids"]
            .as_array()
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect::<Vec<_>>();
        Ok((committed, ids))
    }

    async fn read_batch(
        client: &reqwest::Client,
        base_url: &str,
        model: &str,
        ids: Vec<String>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let resp = client
        .post(&format!("{}/api/models/{}/read-batch", base_url, model))
            .json(&json!({ "ids": ids }))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if !resp["success"].as_bool().unwrap_or(false) {
            return Err(format!(
                "read-batch failed for {}: {}",
                model,
                resp["error"].as_str().unwrap_or("unknown error")
            )
            .into());
        }
        Ok(resp["data"]["verified"].as_bool().unwrap_or(false))
    }

    // --- Batched writes (3 ops triggers a commit) ---
    println!("--- Phase 1: batching writes ---");

    let start = Instant::now();
    let (c1, agent_ids_a) = create_batch(
        &client,
        &base_url,
        "agents",
        vec![
            json!({"name":"alice","character":{"role":"builder"}}),
            json!({"name":"bob","character":{"role":"runner"}}),
            json!({"name":"charlie","character":{"role":"analyst"}}),
        ],
    )
    .await?;
    println!("op1 commit={} took {:?}", c1, start.elapsed());

    let start = Instant::now();
    let (c2, agent_ids_b) = create_batch(
        &client,
        &base_url,
        "agents",
        vec![
            json!({"name":"diana","character":{"role":"writer"}}),
            json!({"name":"eve","character":{"role":"tester"}}),
            json!({"name":"frank","character":{"role":"ops"}}),
        ],
    )
    .await?;
    println!("op2 commit={} took {:?}", c2, start.elapsed());

    let mut agent_ids = Vec::new();
    agent_ids.extend(agent_ids_a);
    agent_ids.extend(agent_ids_b);

    let start = Instant::now();
    let (c3, _) = create_batch(
        &client,
        &base_url,
        "follow_queue",
        vec![
            json!({"user_id":"u_1","username":"neo","type":"follow","agent_id": agent_ids[0].parse::<i64>().unwrap(), "created_at": Utc::now()}),
            json!({"user_id":"u_2","username":"trinity","type":"follow","agent_id": agent_ids[1].parse::<i64>().unwrap(), "created_at": Utc::now()}),
            json!({"user_id":"u_3","username":"morpheus","type":"follow","agent_id": agent_ids[2].parse::<i64>().unwrap(), "created_at": Utc::now()}),
        ],
    )
    .await?;
    println!("op3 commit={} took {:?}", c3, start.elapsed());

    // --- Verify "last 3 agents" via verifiable read-batch ---
    println!("--- Phase 2: verify last 3 agents ---");
    let pool = {
        let db = db_service_arc.lock().await;
        db.pool().clone()
    };
    let last_agent_ids: Vec<String> =
        sqlx::query("SELECT id::text as id FROM agents ORDER BY id DESC LIMIT 3")
            .fetch_all(&pool)
            .await?
            .into_iter()
            .filter_map(|r| r.try_get::<String, _>("id").ok())
            .collect();

    let verified = read_batch(&client, &base_url, "agents", last_agent_ids).await?;
    assert!(verified);

    // --- Shutdown ---
    if let Err(e) = root_manager.commit_pending_root().await {
        eprintln!("commit_pending_root error: {}", e);
    }
    root_manager.shutdown();
    server_handle.abort();

    Ok(())
}

