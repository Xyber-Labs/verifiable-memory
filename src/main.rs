//! Legacy binary entrypoint.
//!
//! The original end-to-end simulation flow was moved into integration tests under `tests/`
//! (see `tests/test_schema_update.rs`) so it can be run via `cargo test` and scripts.
//!
//! This binary is intentionally kept minimal to avoid breaking `[[bin]]` wiring in Cargo.toml.

fn main() {
    println!("verifiable-memory-example: simulation moved to integration tests.");
    println!("Run:");
    println!("  ./scripts/start_simulation.sh");
    println!("or:");
    println!("  cargo test --test test_schema_update -- --nocapture");
}

