// src/performance_test.rs
// This file is responsible for testing the performance of the Sparse Merkle Tree (SmtStore) and its interaction with the PostgreSQL database.
use rand::Rng;
use serde_json::json;
use std::time::Instant;

use verifiable_memory_example::crypto::hashing;
use verifiable_memory_example::storage::smt::SmtStore;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let num_items = 1_000;
    println!(
        "--- SMT Performance Test: Calculating root for {} items ---",
        num_items
    );

    let mut smt_store = SmtStore::new().await?;
    let mut rng = rand::thread_rng();

    // --- Time the main loop: Hashing and Updating the SMT ---
    let start_time = Instant::now();

    for i in 0..num_items {
        // 1. Generate arbitrary data
        let user_id = format!("user_{}", i);
        let random_data = rng.gen::<[u8; 32]>(); // Generate 32 random bytes
        let user_data = json!({
            "id": user_id,
            "email": format!("{}@example.com", user_id),
            "last_login": chrono::Utc::now().to_rfc3339(),
            "profile_data": hex::encode(random_data),
        });

        // 2. Compute the hashes (just like in a real system)
        let key_hash = hashing::hash_key("users", &user_id);
        let value_hash = hashing::hash_value(&user_data);

        // 3. Update the SMT with the new key-value pair
        smt_store.update(key_hash, value_hash).await?;
    }

    // 4. After all updates, get the final root.
    // This step itself is very fast as the root is updated incrementally.
    let final_root = smt_store.get_root().await?;

    let duration = start_time.elapsed();
    let duration_ms = duration.as_millis();
    let avg_time_per_item = duration.as_micros() as f64 / num_items as f64;

    println!("\n--- Results ---");
    println!("Final SMT Root: {}", hex::encode(final_root.as_bytes()));
    println!("\nTotal time to hash and update {} items: {} ms", num_items, duration_ms);
    println!("Average time per item: {:.2} µs (microseconds)", avg_time_per_item);

    println!("\nNote: This test uses an in-memory store. A production system with a database would be slower due to I/O latency, but the cryptographic computation time remains the same.");
    Ok(())
}
