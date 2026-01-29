//! Manages the dual-root system: main_root (on-chain) and temporary_root (in-memory).
//!
//! The temporary_root is updated on every write operation, while the main_root
//! is committed to the blockchain periodically (configurable via BATCH_COMMIT_SIZE env var, default: 10)
//! to reduce costs and latency.

use crate::infra::solana;
use crate::infra::config;
use hex;
use primitive_types::H256;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

/// Structure for the trusted state file
#[derive(Serialize, Deserialize, Debug)]
struct TrustedState {
    root: String, // Hex encoded root
    timestamp: u64,
}

/// Manages the dual-root system for efficient batching of blockchain commits.
pub struct RootManager {
    /// The root stored on the Solana blockchain (slow-moving, globally trusted).
    main_root: Arc<Mutex<H256>>,
    /// The root in memory (updated on every write, fast).
    temporary_root: Arc<Mutex<H256>>,
    /// Counter for tracking how many times temporary_root has been updated.
    update_counter: Arc<Mutex<u64>>,
    /// Flag to control the background commit task.
    shutdown: Arc<tokio::sync::Notify>,
    /// Notification to trigger immediate commit check (when threshold is reached).
    commit_trigger: Arc<tokio::sync::Notify>,
    /// Lock to prevent writes while committing to blockchain.
    commit_in_progress: Arc<tokio::sync::Mutex<bool>>,
    /// Single-writer lock that must cover the entire critical section of:
    /// DB write -> SMT update/proof -> verify -> update temporary_root/trusted_state.
    ///
    /// The background commit task must also acquire this lock so it cannot interleave with writes.
    root_lock: Arc<tokio::sync::Mutex<()>>,
    /// Number of temporary_root updates before committing to main_root (blockchain).
    batch_commit_size: u64,
    /// Path to the trusted state file inside the TEE.
    state_file_path: PathBuf,
}

impl RootManager {
    /// Creates a new RootManager and initializes it with the current root from Solana.
    /// The batch commit size can be configured via the `BATCH_COMMIT_SIZE` environment variable.
    /// Defaults to 10 if not set.
    pub async fn new() -> anyhow::Result<Self> {
        // Initialize main_root from Solana blockchain
        let blockchain_root = solana::read_root().await?;

        let batch_commit_size = config::batch_commit_size();

        println!(
            "> RootManager: Batch commit size set to {} operations",
            batch_commit_size
        );

        // Define trusted state file path (default to "trusted_state.json" in current dir)
        let state_file_path = PathBuf::from("trusted_state.json");

        // If we're doing a "reset run" (single-tenant dev workflows), ignore any existing trusted state
        // so we don't warn about mismatches before bootstrap applies the schema + resets roots.
        let clear_db_mode = std::env::var("CLEAR_DB").unwrap_or_default() == "true";
        if clear_db_mode && state_file_path.exists() {
            if let Err(e) = fs::remove_file(&state_file_path) {
                eprintln!("> RootManager: Warning: failed to remove trusted state file: {}", e);
            } else {
                println!("> RootManager: CLEAR_DB=true -> removed trusted state file before initialization.");
            }
        }

        // Try to load trusted root from file
        let mut initial_temp_root = blockchain_root;

        if state_file_path.exists() {
            println!(
                "> RootManager: Found trusted state file at {:?}",
                state_file_path
            );
            match Self::load_root_from_file(&state_file_path) {
                Ok(trusted_root) => {
                    if trusted_root != blockchain_root {
                        println!("> RootManager: WARNING: Trusted local root differs from blockchain root!");
                        println!(
                            "  - Blockchain Root: {}",
                            hex::encode(blockchain_root.as_bytes())
                        );
                        println!(
                            "  - Trusted Local Root: {}",
                            hex::encode(trusted_root.as_bytes())
                        );
                        println!("> RootManager: Using Trusted Local Root as the source of truth.");
                        println!("> RootManager: Pending changes will be committed to blockchain shortly.");
                        initial_temp_root = trusted_root;
                    } else {
                        println!("> RootManager: Trusted local root matches blockchain root.");
                    }
                }
                Err(e) => {
                    eprintln!("> RootManager: Failed to load trusted state file: {}", e);
                    eprintln!("> RootManager: Falling back to blockchain root.");
                }
            }
        } else {
            println!("> RootManager: No trusted state file found. Initializing from blockchain root.");
            // Create the file with the initial root
            if let Err(e) = Self::save_root_to_file(&state_file_path, blockchain_root) {
                eprintln!(
                    "> RootManager: Failed to create initial trusted state file: {}",
                    e
                );
            }
        }

        let manager = Self {
            main_root: Arc::new(Mutex::new(blockchain_root)),
            temporary_root: Arc::new(Mutex::new(initial_temp_root)),
            update_counter: Arc::new(Mutex::new(0)),
            shutdown: Arc::new(tokio::sync::Notify::new()),
            commit_trigger: Arc::new(tokio::sync::Notify::new()),
            commit_in_progress: Arc::new(tokio::sync::Mutex::new(false)),
            root_lock: Arc::new(tokio::sync::Mutex::new(())),
            batch_commit_size,
            state_file_path,
        };

        Ok(manager)
    }

    /// Helper to load root from file
    fn load_root_from_file(path: &PathBuf) -> anyhow::Result<H256> {
        let content = fs::read_to_string(path)?;
        let state: TrustedState = serde_json::from_str(&content)?;
        let root_bytes = hex::decode(state.root)?;
        if root_bytes.len() != 32 {
            return Err(anyhow::anyhow!("Invalid root length in trusted state file"));
        }
        Ok(H256::from_slice(&root_bytes))
    }

    /// Helper to save root to file
    fn save_root_to_file(path: &PathBuf, root: H256) -> anyhow::Result<()> {
        let state = TrustedState {
            root: hex::encode(root.as_bytes()),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        };
        let content = serde_json::to_string_pretty(&state)?;
        fs::write(path, content)?;
        Ok(())
    }

    /// Updates the temporary_root with a new value.
    /// This is called on every successful write operation.
    /// Blocks if a blockchain commit is in progress to prevent root overwrites.
    /// Returns true if this update triggers a commit (based on batch_commit_size).
    /// If threshold is reached, triggers immediate commit check in background task.
    pub async fn update_temporary_root(&self, new_root: H256) -> bool {
        // Wait if a commit is in progress - this prevents overwriting temporary_root
        // while the blockchain commit is happening
        loop {
            let commit_lock = self.commit_in_progress.lock().await;
            if !*commit_lock {
                drop(commit_lock);
                break;
            }
            drop(commit_lock);
            // Wait a bit before checking again
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }

        // Save to trusted file FIRST
        if let Err(e) = Self::save_root_to_file(&self.state_file_path, new_root) {
            eprintln!(
                "> RootManager: CRITICAL ERROR: Failed to save root to trusted file: {}",
                e
            );
            // In a real TEE, we might want to panic or halt here as persistence failed
        }

        let mut temp_root = self.temporary_root.lock().await;
        *temp_root = new_root;
        drop(temp_root);

        let mut counter = self.update_counter.lock().await;
        *counter += 1;
        let count = *counter;
        let triggers_commit = count % self.batch_commit_size == 0;
        drop(counter);

        // If threshold reached, notify background task to commit immediately
        if triggers_commit {
            self.commit_trigger.notify_one();
        }

        triggers_commit
    }

    /// Acquires the single-writer "root lock".
    ///
    /// Hold this lock for the entire write critical section:
    /// DB write -> SMT update/proof -> verify -> update_temporary_root.
    ///
    /// IMPORTANT: do NOT hold this lock while waiting for a background commit to complete,
    /// or you can deadlock the commit task.
    pub async fn lock_root(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.root_lock.lock().await
    }

    /// Gets the current temporary_root (for read verification).
    pub async fn get_temporary_root(&self) -> H256 {
        let temp_root = self.temporary_root.lock().await;
        *temp_root
    }

    /// Gets the current main_root (for reference).
    pub async fn get_main_root(&self) -> H256 {
        let main_root = self.main_root.lock().await;
        *main_root
    }

    /// Waits for any in-progress blockchain commit to complete.
    /// This should be called after update_temporary_root returns true (triggers_commit)
    /// to ensure the commit finishes before proceeding with the next operation.
    pub async fn wait_for_commit_completion(&self) {
        // Give the background task a moment to start and set the commit_in_progress flag
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Now wait until commit_in_progress becomes false
        loop {
            let commit_lock = self.commit_in_progress.lock().await;
            if !*commit_lock {
                drop(commit_lock);
                break;
            }
            drop(commit_lock);
            // Poll every 10ms until commit completes
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
    }

    /// Commits the temporary_root to the blockchain as the new main_root.
    /// This is called by the background task based on batch_commit_size.
    pub async fn commit_temporary_to_main(&self) -> anyhow::Result<()> {
        let temp_root = self.get_temporary_root().await;

        // Write to Solana blockchain
        solana::write_root(temp_root).await?;

        // Update main_root to match temporary_root
        let mut main_root = self.main_root.lock().await;
        *main_root = temp_root;

        println!("> RootManager: Committed temporary_root to blockchain (main_root updated)");

        Ok(())
    }

    /// Force-sets the temporary_root and main_root to `new_root` and commits it to Solana immediately.
    ///
    /// This is intended for schema migrations where the SMT must be rebuilt from the post-migration DB
    /// and both roots must be updated to match that rebuilt state right away.
    pub async fn force_set_roots_and_commit(&self, new_root: H256) -> anyhow::Result<()> {
        // Ensure forced repair/migration cannot interleave with writes or background commits.
        let _root_guard = self.root_lock.lock().await;

        // Block writes during the forced commit.
        {
            let mut commit_flag = self.commit_in_progress.lock().await;
            *commit_flag = true;
        }

        // Save to trusted file first (crash recovery invariant).
        if let Err(e) = Self::save_root_to_file(&self.state_file_path, new_root) {
            eprintln!(
                "> RootManager: CRITICAL ERROR: Failed to save root to trusted file: {}",
                e
            );
        }

        {
            let mut temp_root = self.temporary_root.lock().await;
            *temp_root = new_root;
        }

        // Commit to chain and update main_root.
        let commit_res = solana::write_root(new_root).await;
        match commit_res {
            Ok(_) => {
                let mut main_root = self.main_root.lock().await;
                *main_root = new_root;

                // Reset counter so batching resumes from a clean state.
                let mut counter = self.update_counter.lock().await;
                *counter = 0;
            }
            Err(e) => {
                // Ensure we unblock writes even on error.
                {
                    let mut commit_flag = self.commit_in_progress.lock().await;
                    *commit_flag = false;
                }
                return Err(e);
            }
        }

        // Unblock writes.
        {
            let mut commit_flag = self.commit_in_progress.lock().await;
            *commit_flag = false;
        }

        Ok(())
    }

    /// Starts the background task that periodically commits the temporary_root to main_root.
    /// This task checks immediately when threshold is reached (via commit_trigger) or every second as fallback.
    pub fn start_background_commit_task(self: Arc<Self>) {
        let batch_size = self.batch_commit_size;
        tokio::spawn(async move {
            let mut interval_timer = interval(Duration::from_secs(1));
            let shutdown = self.shutdown.clone();
            let commit_trigger = self.commit_trigger.clone();
            let mut last_committed_count = 0u64;

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                        // Periodic check (fallback)
                        self.check_and_commit_if_needed(&mut last_committed_count, batch_size).await;
                    }
                    _ = commit_trigger.notified() => {
                        // Immediate check when threshold is reached
                        self.check_and_commit_if_needed(&mut last_committed_count, batch_size).await;
                    }
                    _ = shutdown.notified() => {
                        println!("> RootManager: Background commit task shutting down");
                        break;
                    }
                }
            }
        });
    }

    /// Helper method to check if commit is needed and perform it.
    /// Sets commit_in_progress flag to block writes during blockchain commit.
    async fn check_and_commit_if_needed(&self, last_committed_count: &mut u64, batch_size: u64) {
        // Prevent any interleaving with writes (DB/SMT/proof/root updates).
        let _root_guard = self.root_lock.lock().await;

        let counter = self.update_counter.lock().await;
        let count = *counter;
        drop(counter);

        // Only commit if we've reached a new multiple of batch_size that hasn't been committed yet
        if count > 0 && count % batch_size == 0 && count > *last_committed_count {
            // Check if temporary_root differs from main_root
            let temp_root = self.get_temporary_root().await;
            let main_root = self.get_main_root().await;

            if temp_root != main_root {
                // Set commit_in_progress flag to block new writes
                {
                    let mut commit_flag = self.commit_in_progress.lock().await;
                    *commit_flag = true;
                }

                println!(
                    "> RootManager: Detected batch threshold reached (operation #{}). Committing to blockchain...",
                    count
                );
                println!("> RootManager: Write operations paused during blockchain commit...");

                match self.commit_temporary_to_main().await {
                    Ok(_) => {
                        *last_committed_count = count;
                        println!(
                            "> RootManager: ✓ Successfully committed batch #{} to blockchain (root: {})",
                            count / batch_size,
                            hex::encode(temp_root.as_bytes())
                        );
                    }
                    Err(e) => {
                        eprintln!("> RootManager: ✗ ERROR committing to blockchain: {}", e);
                    }
                }

                // Clear commit_in_progress flag to allow writes again
                {
                    let mut commit_flag = self.commit_in_progress.lock().await;
                    *commit_flag = false;
                }
                println!("> RootManager: Write operations resumed.");
            } else {
                // Roots are already in sync, just update the counter to avoid re-checking
                *last_committed_count = count;
                println!(
                    "> RootManager: Batch threshold reached but roots are already in sync (operation #{})",
                    count
                );
            }
        }
    }

    /// Resets both main_root and temporary_root to a new value (typically zero after clearing DB).
    /// This is useful when the database is cleared and the blockchain root is reset.
    #[allow(dead_code)] // Reserved for future use
    pub async fn reset_roots(&self, new_root: H256) {
        let mut main_root = self.main_root.lock().await;
        *main_root = new_root;
        drop(main_root);

        let mut temp_root = self.temporary_root.lock().await;
        *temp_root = new_root;
        drop(temp_root);

        let mut counter = self.update_counter.lock().await;
        *counter = 0;

        // Also reset the trusted file
        if let Err(e) = Self::save_root_to_file(&self.state_file_path, new_root) {
            eprintln!("> RootManager: Failed to reset trusted state file: {}", e);
        }

        println!(
            "> RootManager: Roots reset to {}",
            hex::encode(new_root.as_bytes())
        );
    }

    /// Removes the trusted state file (used during full resets).
    pub fn clear_trusted_state_file(&self) {
        if self.state_file_path.exists() {
            if let Err(e) = fs::remove_file(&self.state_file_path) {
                eprintln!("> RootManager: Warning: failed to remove trusted state file: {}", e);
            } else {
                println!("> RootManager: Removed trusted state file {:?}", self.state_file_path);
            }
        }
    }

    /// Shuts down the background commit task.
    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }

    /// Commits the temporary_root to blockchain if it differs from main_root.
    /// This should be called during graceful shutdown to ensure no data is lost.
    pub async fn commit_pending_root(&self) -> anyhow::Result<()> {
        // Prevent any interleaving with in-flight writes during shutdown.
        let _root_guard = self.root_lock.lock().await;

        let start = Instant::now();
        let temp_root = self.get_temporary_root().await;
        let main_root = self.get_main_root().await;

        if temp_root != main_root {
            println!(
                "> RootManager: Shutdown detected. Committing pending temporary_root to blockchain..."
            );
            println!(
                "> RootManager: Temporary root: {}",
                hex::encode(temp_root.as_bytes())
            );
            println!(
                "> RootManager: Main root: {}",
                hex::encode(main_root.as_bytes())
            );

            // Set commit_in_progress to prevent new writes during shutdown commit
            {
                let mut commit_flag = self.commit_in_progress.lock().await;
                *commit_flag = true;
            }

            match self.commit_temporary_to_main().await {
                Ok(_) => {
                    let duration = start.elapsed();
                    println!(
                        "> RootManager: ✓ Successfully committed pending root to blockchain during shutdown (took {:?})",
                        duration
                    );
                    Ok(())
                }
                Err(e) => {
                    let duration = start.elapsed();
                    eprintln!(
                        "> RootManager: ✗ ERROR committing pending root during shutdown (took {:?}): {}",
                        duration, e
                    );
                    Err(e)
                }
            }
        } else {
            let duration = start.elapsed();
            println!(
                "> RootManager: Shutdown detected. No pending root to commit (temporary_root == main_root) (took {:?})",
                duration
            );
            Ok(())
        }
    }
}

