//! Centralized configuration (environment variables + defaults).

/// Solana RPC URL (required).
pub fn solana_rpc_url() -> String {
    std::env::var("SOLANA_RPC_URL").expect("SOLANA_RPC_URL must be set")
}

/// Solana program id (required).
///
/// Set this to the Program ID you deployed (e.g. output of `anchor deploy`).
pub fn solana_program_id() -> String {
    std::env::var("SOLANA_PROGRAM_ID").expect("SOLANA_PROGRAM_ID must be set")
}

/// Batch commit size (required).
pub fn batch_commit_size() -> u64 {
    let v = std::env::var("BATCH_COMMIT_SIZE").expect("BATCH_COMMIT_SIZE must be set");
    v.parse::<u64>()
        .expect("BATCH_COMMIT_SIZE must be a valid u64")
        .max(1)
}

/// Database URL must be provided (no default) for safety.
pub fn database_url() -> String {
    std::env::var("DATABASE_URL").expect("DATABASE_URL must be set")
}

