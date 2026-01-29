// src/bin/api_server.rs

use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tower_http::cors::{Any, CorsLayer};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use verifiable_memory_example::transport;
use verifiable_memory_example::DatabaseService;
use verifiable_memory_example::ModelRegistry;
use verifiable_memory_example::RootManager;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --- Model Registry Initialization ---
    println!("> Initializing Model Registry (runtime, starts empty)...");
    let model_registry = Arc::new(RwLock::new(ModelRegistry::new()));

    // --- Root Manager Initialization ---
    println!("> Initializing RootManager...");
    let root_manager = Arc::new(RootManager::new().await?);
    let batch_size = std::env::var("BATCH_COMMIT_SIZE")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(10);
    root_manager.clone().start_background_commit_task();
    println!("> RootManager initialized. Background commit task started (commits every {} updates).", batch_size);

    // --- Service Initialization ---
    println!("> Initializing DatabaseService...");
    let db_service = DatabaseService::new().await?;
    // Log root alignment at startup (helps debug verification issues)
    if let Ok(smt_root) = db_service.current_smt_root().await {
        let temp_root = root_manager.get_temporary_root().await;
        let main_root = root_manager.get_main_root().await;
        println!(
            "> Startup roots: smt_root={} temporary_root={} main_root={} match(smt==temp)={} match(smt==main)={}",
            hex::encode(smt_root.as_bytes()),
            hex::encode(temp_root.as_bytes()),
            hex::encode(main_root.as_bytes()),
            smt_root == temp_root,
            smt_root == main_root
        );
    }
    let pool = db_service.pool().clone();

    // --- Optional: Warm-start model registry from DB (no schema change / no bootstrap needed) ---
    //
    // If you previously ran /bootstrap/apply-schema, the schema is persisted into `verifiable_models`.
    // On restart we can reload it so /api/models/{model}/... works immediately.
    //
    // If the registry is empty, you can still call /bootstrap/apply-schema to define models.
    match ModelRegistry::load_from_db(&pool).await {
        Ok(reg) => {
            if reg.list_models().is_empty() {
                println!("> No models found in verifiable_models. Use POST /bootstrap/apply-schema to register schema.");
            } else {
                let mut lock = model_registry.write().await;
                *lock = reg;
                println!("> Warm-started ModelRegistry from DB (verifiable_models).");
            }
        }
        Err(_) => {
            println!("> Could not load verifiable_models on startup (continuing with empty registry).");
        }
    }

    let app_state = transport::http::AppState {
        db_service: Arc::new(Mutex::new(db_service)),
        model_registry,
        root_manager: root_manager.clone(),
    };
    println!("> DatabaseService initialized successfully.");

    // --- API Server Initialization ---
    println!("> Starting API server...");
    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any);
    let app = transport::http::create_router(app_state)
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", transport::http::ApiDoc::openapi()))
        .layer(cors);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    println!("> API server listening on http://0.0.0.0:3000");
    println!("> Swagger UI available at http://localhost:3000/swagger-ui");
    println!("> Press Ctrl+C to gracefully shutdown and commit pending root to blockchain");

    // Setup graceful shutdown handler
    let root_manager_for_shutdown = root_manager.clone();
    tokio::select! {
        result = axum::serve(listener, app) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            println!("\n> Shutdown signal received (Ctrl+C)...");
            println!("> Committing pending temporary_root to blockchain...");
            if let Err(e) = root_manager_for_shutdown.commit_pending_root().await {
                eprintln!("> Error committing pending root during shutdown: {}", e);
            }
            root_manager_for_shutdown.shutdown();
            println!("> Graceful shutdown complete.");
        }
    }

    Ok(())
}
