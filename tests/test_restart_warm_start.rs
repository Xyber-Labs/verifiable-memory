//! Restart / warm-start test:
//! 1) Start server with empty registry and bootstrap schema once.
//! 2) Stop server (simulated restart).
//! 3) Start server again, but load ModelRegistry from DB (verifiable_models) without bootstrapping.
//! 4) Ensure create-batch/read-batch works immediately.

use serde_json::json;
use std::env;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use verifiable_memory_example::{solana, transport, DatabaseService, ModelRegistry, RootManager};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_restart_warm_start() -> Result<(), Box<dyn std::error::Error>> {
    dotenv::dotenv().ok();

    // Keep batch size small so commits are deterministic, but we don't assert commit timing here.
    if env::var("BATCH_COMMIT_SIZE").is_err() {
        env::set_var("BATCH_COMMIT_SIZE", "3");
    }

    // Ensure Solana root account exists.
    solana::initialize().await?;

    // Use two different ports for A/B to avoid port reuse races during "restart".
    let base_url_a = "http://127.0.0.1:3001";
    let base_url_b = "http://127.0.0.1:3002";
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    // --- Phase A: start server #1 with EMPTY registry and bootstrap schema ---
    env::set_var("CLEAR_DB", "true");
    let root_manager_a = Arc::new(RootManager::new().await?);
    root_manager_a.clone().start_background_commit_task();

    let registry_a = Arc::new(RwLock::new(ModelRegistry::new()));
    let db_a = DatabaseService::new().await?;
    let pool = db_a.pool().clone();
    let state_a = transport::http::AppState {
        db_service: Arc::new(Mutex::new(db_a)),
        model_registry: registry_a,
        root_manager: root_manager_a.clone(),
    };
    let router_a = transport::http::create_router(state_a);
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:3001").await?;
    let server_a = tokio::spawn(async move {
        axum::serve(listener_a, router_a).await.unwrap();
    });
    
    // Wait for server_a to be ready
    for _ in 0..30 {
        match tokio::net::TcpStream::connect("127.0.0.1:3001").await {
            Ok(_) => break,
            Err(_) => tokio::time::sleep(tokio::time::Duration::from_millis(100)).await,
        }
    }

    // Bootstrap (register schema).
    let bootstrap = client
        .post(&format!("{}/bootstrap/apply-schema", base_url_a))
        .json(&json!({
            "force_reset": true,
            "tables": [
                {
                    "table_name": "agents",
                    "primary_key_field": "id",
                    "primary_key_kind": "big_serial",
                    "columns": [
                        {"name":"name","col_type":"text","nullable":false,"unique":true},
                        {"name":"character","col_type":"jsonb","nullable":false,"unique":false}
                    ]
                }
            ]
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert!(bootstrap["success"].as_bool().unwrap_or(false));

    // Write one row to make sure registry + DB are used.
    let create_1 = client
        .post(&format!("{}/api/models/agents/create-batch", base_url_a))
        .json(&json!({
            "records": [
                {"name":"warm_start_seed","character":{"role":"seed"}}
            ]
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert!(create_1["success"].as_bool().unwrap_or(false));

    // Shutdown server A (simulated restart).
    if let Err(e) = root_manager_a.commit_pending_root().await {
        eprintln!("commit_pending_root error: {}", e);
    }
    root_manager_a.shutdown();
    server_a.abort();
    let _ = server_a.await;

    // --- Phase B: start server #2 WITHOUT bootstrapping, but warm-start registry from DB ---
    env::set_var("CLEAR_DB", "false");
    let root_manager_b = Arc::new(RootManager::new().await?);
    root_manager_b.clone().start_background_commit_task();

    let reg_from_db = ModelRegistry::load_from_db(&pool).await?;
    assert!(
        reg_from_db.get("agents").is_some(),
        "expected 'agents' model to be present after warm-start"
    );

    let registry_b = Arc::new(RwLock::new(reg_from_db));
    let db_b = DatabaseService::new().await?;
    let state_b = transport::http::AppState {
        db_service: Arc::new(Mutex::new(db_b)),
        model_registry: registry_b,
        root_manager: root_manager_b.clone(),
    };
    let router_b = transport::http::create_router(state_b);

    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:3002").await?;
    let server_b = tokio::spawn(async move {
        axum::serve(listener_b, router_b).await.unwrap();
    });
    
    // Wait for server_b to be ready by attempting to connect
    for _ in 0..30 {
        match tokio::net::TcpStream::connect("127.0.0.1:3002").await {
            Ok(_) => {
                // Server is ready, close the test connection
                break;
            }
            Err(_) => {
                tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            }
        }
    }

    // Create WITHOUT calling bootstrap again.
    let create_2 = client
        .post(&format!("{}/api/models/agents/create-batch", base_url_b))
        .json(&json!({
            "records": [
                {"name":"after_restart","character":{"role":"ok"}}
            ]
        }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert!(create_2["success"].as_bool().unwrap_or(false));

    // Read the inserted id and ensure verification passes.
    let id = create_2["data"]["ids"][0].as_str().unwrap().to_string();
    let read = client
        .post(&format!("{}/api/models/agents/read-batch", base_url_b))
        .json(&json!({ "ids": [id] }))
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;
    assert!(read["success"].as_bool().unwrap_or(false));
    assert!(read["data"]["verified"].as_bool().unwrap_or(false));

    // Shutdown server B.
    if let Err(e) = root_manager_b.commit_pending_root().await {
        eprintln!("commit_pending_root error: {}", e);
    }
    root_manager_b.shutdown();
    server_b.abort();
    let _ = server_b.await;
    
    // Wait a bit for cleanup
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(())
}

