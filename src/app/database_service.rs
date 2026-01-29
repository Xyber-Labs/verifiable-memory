//! The Verifiable Data Service.
//!
//! This module acts as the intermediary between the TEE agent and the database.
//! It is responsible for:
//! 1.  Writing data to the primary PostgreSQL tables (e.g., `users`).
//! 2.  Maintaining the Sparse Merkle Tree (`SmtStore`) by storing its nodes
//!     in the `merkle_nodes` table.
//! 3.  Generating Merkle proofs for data retrieval requests.

use crate::domain::model::VerifiableModel;
use crate::domain::verify::verify_smt_multi_update_proof_with_old_values;
use crate::storage::smt::SmtStore;
use crate::storage::smt::{h256_to_smt, smt_to_h256, SmtBlake2bHasher};
use chrono::{DateTime, Utc};
use primitive_types::H256;
use serde_json::Value as JsonValue;
use sparse_merkle_tree::MerkleProof;
use sqlx::postgres::PgPoolOptions;
use sqlx::QueryBuilder;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tokio::sync::Mutex;
use crate::infra::config;
use crate::crypto::hashing::{hash_key, hash_value};
use std::collections::HashMap;

/// The main service that manages database interaction and the SMT.
pub struct DatabaseService {
    pool: PgPool,
    smt_store: Arc<Mutex<SmtStore>>,
    /// Held for the lifetime of the process to prevent multiple VerifiableDB API instances
    /// from mutating the same DB/SMT concurrently (which can cause root drift).
    #[allow(dead_code)]
    instance_lock: Option<sqlx::pool::PoolConnection<sqlx::Postgres>>,
}

impl DatabaseService {
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Returns the current SMT root computed from the persistent SMT store.
    pub async fn current_smt_root(&self) -> anyhow::Result<H256> {
        let smt = self.smt_store.lock().await;
        Ok(smt.get_root().await?)
    }

    pub async fn reset_smt_store(&mut self) -> anyhow::Result<()> {
        self.smt_store = Arc::new(Mutex::new(SmtStore::new_with_pool(self.pool.clone()).await?));
        Ok(())
    }

    /// Rebuilds the SMT from the current contents of the database (post-migration).
    ///
    /// This is used when the DB schema changes and the canonical `row_to_json(table.*)` shape
    /// (and therefore leaf hashes) may change.
    pub async fn rebuild_smt_from_db(
        &mut self,
        models: Vec<Arc<dyn VerifiableModel>>,
    ) -> anyhow::Result<(H256, u64)> {
        // Clear persistent SMT nodes first.
        sqlx::query("TRUNCATE TABLE merkle_nodes")
            .execute(&self.pool)
            .await?;

        // Reset SMT store in memory (it will load from the now-empty merkle_nodes table).
        self.smt_store = Arc::new(Mutex::new(SmtStore::new_with_pool(self.pool.clone()).await?));

        let mut updated_leaves: u64 = 0;

        // Hold the SMT lock for the duration of the rebuild so updates are consistent.
        let mut smt = self.smt_store.lock().await;

        for model in models {
            let table_name = model.table_name();
            let pk_field = model.primary_key_field();

            let sql = format!(
                "SELECT row_to_json({}.*) as record, {}::text as pk_value FROM {}",
                table_name, pk_field, table_name
            );

            let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;

            for row in rows {
                let record: JsonValue = row.try_get("record")?;
                let pk_value: String = row.try_get("pk_value")?;

                let key_hash = hash_key(table_name, &pk_value);
                let value_hash = hash_value(&record);
                smt.update(key_hash, value_hash).await?;
                updated_leaves += 1;
            }
        }

        let new_root = smt.get_root().await?;
        Ok((new_root, updated_leaves))
    }

    /// Creates a new instance of the DatabaseService and connects to the database.
    pub async fn new() -> Result<Self, anyhow::Error> {
        dotenv::dotenv().ok();
        let database_url = config::database_url();

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;

        // Create the merkle_nodes table (always needed)
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS merkle_nodes (
                node_hash BYTEA PRIMARY KEY,
                node_value BYTEA NOT NULL
            )",
        )
        .execute(&pool)
        .await?;

        // Persistent registry for runtime (dynamic) models.
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verifiable_models (
                table_name TEXT PRIMARY KEY,
                primary_key_field TEXT NOT NULL,
                primary_key_kind TEXT NOT NULL,
                columns JSONB NOT NULL,
                create_table_sql TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )",
        )
        .execute(&pool)
        .await?;

        sqlx::query(
            "CREATE TABLE IF NOT EXISTS verifiable_registry_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )",
        )
        .execute(&pool)
        .await?;

        // Initialize the persistent SMT store with the database connection pool.
        let smt_store = Arc::new(Mutex::new(SmtStore::new_with_pool(pool.clone()).await?));

        // Enforce single-instance by default (opt-out via ALLOW_MULTI_INSTANCE=true).
        let allow_multi = std::env::var("ALLOW_MULTI_INSTANCE").unwrap_or_default() == "true";
        let instance_lock = if allow_multi {
            None
        } else {
            let mut conn = pool.acquire().await?;
            // Arbitrary constant lock ID (must be stable across instances).
            let lock_id: i64 = 4_240_001;
            let locked: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
                .bind(lock_id)
                .fetch_one(&mut *conn)
                .await?;
            if !locked {
                return Err(anyhow::anyhow!(
                    "Another VerifiableDB API instance is already running against this Postgres (pg_advisory_lock). \
Set ALLOW_MULTI_INSTANCE=true to bypass (NOT recommended)."
                ));
            }
            Some(conn)
        };

        Ok(Self { pool, smt_store, instance_lock })
    }

    /// Clears the database.
    pub async fn clear_db(&mut self) -> Result<(), anyhow::Error> {
        // Clears all managed tables listed in the registry, plus merkle nodes.
        let tables: Vec<String> = sqlx::query("SELECT table_name FROM verifiable_models")
            .fetch_all(&self.pool)
            .await?
            .into_iter()
            .filter_map(|r| r.try_get::<String, _>("table_name").ok())
            .collect();

        for table_name in tables {
            sqlx::query(&format!("DELETE FROM {}", table_name))
                .execute(&self.pool)
                .await?;
        }

        sqlx::query("DELETE FROM merkle_nodes").execute(&self.pool).await?;

        // Also reset the SMT in memory
        self.smt_store = Arc::new(Mutex::new(SmtStore::new_with_pool(self.pool.clone()).await?));
        Ok(())
    }

    /// Creates a batch of new records for a given model, writes them to the DB,
    /// verifies the SMT state transition against `trusted_root`, and atomically commits:
    /// - application rows
    /// - `merkle_nodes` updates
    ///
    /// If proof verification fails, the SQL transaction is rolled back (no row persists).
    pub async fn create_records(
        &self,
        model: Arc<dyn VerifiableModel>,
        records_data: &[JsonValue],
        trusted_root: H256,
    ) -> Result<(H256, MerkleProof, Vec<JsonValue>, Vec<String>), anyhow::Error> {
        // Validate all records using the model's validation logic
        for record in records_data {
            model
                .validate_create_payload(record)
                .map_err(|e| anyhow::anyhow!("Validation error: {}", e))?;
        }

        if records_data.is_empty() {
            return Err(anyhow::anyhow!("records_data cannot be empty"));
        }

        let table_name = model.table_name();
        let pk_field = model.primary_key_field();
        let mut key_hashes = Vec::new();
        let mut value_hashes = Vec::new();
        let mut inserted_records: Vec<JsonValue> = Vec::with_capacity(records_data.len());
        let mut inserted_ids: Vec<String> = Vec::with_capacity(records_data.len());

        // Dynamically build and execute INSERT queries
        // For simplicity, we'll use a transaction and insert records one by one
        // In production, you might want to use batch inserts for better performance
        let mut transaction = self.pool.begin().await?;

        for record_data in records_data {
            let record_obj = record_data
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("Record must be a JSON object"))?;

            // Build dynamic INSERT query with explicit type casts
            let columns: Vec<&str> = record_obj.keys().map(|s| s.as_str()).collect();
            let mut casted_placeholders = Vec::new();

            for (idx, col) in columns.iter().enumerate() {
                let placeholder_idx = idx + 1;

                let explicit_type = model.column_type(col).map(|s| s.to_lowercase());
                match explicit_type.as_deref() {
                    Some("timestamptz") => {
                        casted_placeholders.push(format!("${}::timestamptz", placeholder_idx));
                    }
                    Some("jsonb") => {
                        casted_placeholders.push(format!("${}::jsonb", placeholder_idx));
                    }
                    Some("int") | Some("int4") => {
                        casted_placeholders.push(format!("${}::int4", placeholder_idx));
                    }
                    Some("bigint") | Some("int8") => {
                        casted_placeholders.push(format!("${}::int8", placeholder_idx));
                    }
                    Some("bool") | Some("boolean") => {
                        casted_placeholders.push(format!("${}::bool", placeholder_idx));
                    }
                    Some("uuid") => {
                        casted_placeholders.push(format!("${}::uuid", placeholder_idx));
                    }
                    Some("text") => {
                        casted_placeholders.push(format!("${}::text", placeholder_idx));
                    }
                    _ => {
                        // Fallback to heuristics
                        let is_timestamp_col = col.to_lowercase().contains("time")
                            || col.to_lowercase().contains("date")
                            || col.to_lowercase() == "last_login";
                        let is_jsonb_col = col.to_lowercase().contains("data")
                            || col.to_lowercase().contains("json")
                            || col.to_lowercase() == "profile_data";
                        if is_timestamp_col {
                            casted_placeholders
                                .push(format!("${}::timestamptz", placeholder_idx));
                        } else if is_jsonb_col {
                            casted_placeholders.push(format!("${}::jsonb", placeholder_idx));
                        } else {
                            casted_placeholders.push(format!("${}", placeholder_idx));
                        }
                    }
                }
            }

            // Rebuild SQL with type casts
            let sql_with_casts = format!(
                "INSERT INTO {} ({}) VALUES ({}) RETURNING row_to_json({}.*) as record, {}::text as pk_value",
                table_name,
                columns.join(", "),
                casted_placeholders.join(", "),
                table_name,
                pk_field
            );

            let mut query = sqlx::query(&sql_with_casts);
            for (col, value) in columns.iter().zip(record_obj.values()) {
                let explicit_type = model.column_type(col).map(|s| s.to_lowercase());
                let is_timestamp_col = matches!(explicit_type.as_deref(), Some("timestamptz"))
                    || col.to_lowercase().contains("time")
                    || col.to_lowercase().contains("date")
                    || col.to_lowercase() == "last_login";
                let is_jsonb_col = matches!(explicit_type.as_deref(), Some("jsonb"))
                    || col.to_lowercase().contains("data")
                    || col.to_lowercase().contains("json")
                    || col.to_lowercase() == "profile_data";

                // Bind values with proper types
                if value.is_null() {
                    if is_timestamp_col {
                        query = query.bind::<Option<DateTime<Utc>>>(None);
                    } else {
                        query = query.bind::<Option<String>>(None);
                    }
                } else if let Some(s) = value.as_str() {
                    if is_timestamp_col {
                        // Parse ISO8601 timestamp string
                        match DateTime::parse_from_rfc3339(s) {
                            Ok(dt) => {
                                query = query.bind(Some(dt.with_timezone(&Utc)));
                            }
                            Err(_) => {
                                // Try alternative parsing
                                match s.parse::<DateTime<Utc>>() {
                                    Ok(dt) => query = query.bind(Some(dt)),
                                    Err(_) => {
                                        // Fallback: bind as string and let PostgreSQL try to cast
                                        query = query.bind(s);
                                    }
                                }
                            }
                        }
                    } else {
                        query = query.bind(s);
                    }
                } else if let Some(n) = value.as_i64() {
                    query = query.bind(n);
                } else if let Some(n) = value.as_f64() {
                    query = query.bind(n);
                } else if let Some(b) = value.as_bool() {
                    query = query.bind(b);
                } else if is_jsonb_col && (value.is_object() || value.is_array()) {
                    // Bind JSON objects/arrays directly as JSONB
                    query = query.bind(value);
                } else {
                    // Fallback: serialize to string
                    query = query.bind(serde_json::to_string(value)?);
                }
            }

            let row = query.fetch_one(&mut *transaction).await?;
            let returned_record: JsonValue = row.try_get("record")?;
            let pk_value: String = row.try_get("pk_value")?;

            inserted_records.push(returned_record);
            inserted_ids.push(pk_value.clone());
            key_hashes.push(crate::crypto::hashing::hash_key(table_name, &pk_value));
        }

        // Hash values from the returned DB records (ensures consistency with read path)
        for record in &inserted_records {
            value_hashes.push(crate::crypto::hashing::hash_value(record));
        }

        let updates: Vec<(H256, H256)> = key_hashes
            .iter()
            .copied()
            .zip(value_hashes.iter().copied())
            .collect();

        // Generate proof against the current SMT state (no persistence yet).
        let mut smt = self.smt_store.lock().await;
        let proof = smt.generate_proof(key_hashes.clone()).await?;

        // Fetch old leaf values from merkle_nodes so we can verify updates/upserts correctly.
        let key_bytes: Vec<Vec<u8>> = key_hashes.iter().map(|k| k.as_bytes().to_vec()).collect();
        let rows = sqlx::query(
            "SELECT node_hash, node_value FROM merkle_nodes WHERE node_hash = ANY($1)",
        )
        .bind(&key_bytes)
        .fetch_all(&mut *transaction)
        .await
        .unwrap_or_default();
        let mut old_map: HashMap<Vec<u8>, H256> = HashMap::new();
        for r in rows {
            let kh: Vec<u8> = r.try_get("node_hash").unwrap_or_default();
            let vh: Vec<u8> = r.try_get("node_value").unwrap_or_default();
            if vh.len() == 32 {
                old_map.insert(kh, H256::from_slice(&vh));
            }
        }
        let old_values: Vec<H256> = key_bytes
            .iter()
            .map(|kb| old_map.get(kb).copied().unwrap_or_else(H256::zero))
            .collect();

        // Compute proposed_root from the proof + new leaf values.
        let new_leaves_smt: Vec<_> = key_hashes
            .iter()
            .copied()
            .zip(value_hashes.iter().copied())
            .map(|(k, v)| (h256_to_smt(k), h256_to_smt(v)))
            .collect();
        let proposed_root_smt = proof
            .clone()
            .compute_root::<SmtBlake2bHasher>(new_leaves_smt)
            .unwrap_or_default();
        let proposed_root = smt_to_h256(&proposed_root_smt);

        let ok = verify_smt_multi_update_proof_with_old_values(
            trusted_root,
            proposed_root,
            key_hashes.clone(),
            old_values,
            value_hashes.clone(),
            proof.clone(),
        );

        if !ok {
            transaction.rollback().await?;
            return Err(anyhow::anyhow!(
                "VERIFIABLE_PROOF_FAILED: trusted_root={} proposed_root={}",
                hex::encode(trusted_root.as_bytes()),
                hex::encode(proposed_root.as_bytes())
            ));
        }

        // Apply SMT updates + merkle_nodes persistence within the SAME SQL transaction.
        smt.apply_updates_in_tx(&mut transaction, &updates).await?;

        transaction.commit().await?;

        Ok((proposed_root, proof, inserted_records, inserted_ids))
    }

    /// Retrieves a set of records for a given model and generates a proof.
    /// Returns records as JSON values since we don't know the specific type at compile time.
    pub async fn get_records_with_proof(
        &self,
        model: Arc<dyn VerifiableModel>,
        record_ids: Vec<&str>,
    ) -> Result<Option<(Vec<JsonValue>, MerkleProof)>, anyhow::Error> {
        let table_name = model.table_name();
        let pk_field = model.primary_key_field();
        let mut key_hashes = Vec::new();

        // Prepare key hashes for SMT
        for record_id in &record_ids {
            key_hashes.push(crate::crypto::hashing::hash_key(table_name, record_id));
        }

        // Build dynamic SELECT query
        // Using JSON aggregation to return records as JSONB
        let sql = format!(
            "SELECT row_to_json({}.*) as record FROM {} WHERE {}::text = ANY($1)",
            table_name, table_name, pk_field
        );

        let rows = sqlx::query(&sql)
            .bind(record_ids)
            .fetch_all(&self.pool)
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        // Convert rows to JSON values
        let mut records = Vec::new();
        for row in rows {
            let json_value: JsonValue = row.try_get("record")?;
            records.push(json_value);
        }

        // Generate proof
        let smt = self.smt_store.lock().await;
        let proof = smt.generate_proof(key_hashes).await?;

        Ok(Some((records, proof)))
    }

    /// Retrieves the latest N records for a given model (ordered by primary key descending)
    /// and generates an SMT proof for those leaves.
    pub async fn get_latest_records_with_proof(
        &self,
        model: Arc<dyn VerifiableModel>,
        limit: u32,
    ) -> Result<Option<(Vec<JsonValue>, Vec<String>, MerkleProof)>, anyhow::Error> {
        let table_name = model.table_name();
        let pk_field = model.primary_key_field();

        let sql = format!(
            "SELECT row_to_json({}.*) as record, {}::text as pk_value FROM {} ORDER BY {} DESC LIMIT $1",
            table_name, pk_field, table_name, pk_field
        );

        let rows = sqlx::query(&sql)
            .bind(limit as i64)
            .fetch_all(&self.pool)
            .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut records: Vec<JsonValue> = Vec::with_capacity(rows.len());
        let mut ids: Vec<String> = Vec::with_capacity(rows.len());
        let mut key_hashes: Vec<H256> = Vec::with_capacity(rows.len());

        for row in rows {
            let record: JsonValue = row.try_get("record")?;
            let pk_value: String = row.try_get("pk_value")?;
            records.push(record);
            ids.push(pk_value.clone());
            key_hashes.push(crate::crypto::hashing::hash_key(table_name, &pk_value));
        }

        let smt = self.smt_store.lock().await;
        let proof = smt.generate_proof(key_hashes).await?;

        Ok(Some((records, ids, proof)))
    }

    /// Retrieves the latest N records with optional restricted filters (equality) and ordering.
    ///
    /// - `where_eq`: field -> scalar JSON value (validated/coerced by the HTTP layer)
    /// - `order_by`: (field, desc)
    pub async fn get_latest_records_with_proof_filtered(
        &self,
        model: Arc<dyn VerifiableModel>,
        limit: u32,
        where_eq: Option<&std::collections::HashMap<String, JsonValue>>,
        order_by: Option<(&str, bool)>,
    ) -> Result<Option<(Vec<JsonValue>, Vec<String>, MerkleProof)>, anyhow::Error> {
        let table_name = model.table_name();
        let pk_field = model.primary_key_field();

        let (order_field, desc) = order_by.unwrap_or((pk_field, true));
        let direction = if desc { "DESC" } else { "ASC" };

        let mut qb: QueryBuilder<sqlx::Postgres> = QueryBuilder::new("");
        qb.push("SELECT row_to_json(")
            .push(table_name)
            .push(".*) as record, ")
            .push(pk_field)
            .push("::text as pk_value FROM ")
            .push(table_name);

        if let Some(filters) = where_eq {
            if !filters.is_empty() {
                qb.push(" WHERE ");
                let mut first = true;
                for (field, value) in filters {
                    if !first {
                        qb.push(" AND ");
                    }
                    first = false;

                    let sql_type = model.column_type(field).unwrap_or("text").to_lowercase();
                    qb.push(field).push(" = ");

                    match value {
                        JsonValue::Null => {
                            qb.push("NULL");
                        }
                        JsonValue::Bool(b) => {
                            qb.push_bind(*b).push("::bool");
                        }
                        JsonValue::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                qb.push_bind(i);
                            } else if let Some(f) = n.as_f64() {
                                qb.push_bind(f);
                            } else {
                                qb.push_bind(n.to_string());
                            }
                            match sql_type.as_str() {
                                "int" | "int4" | "integer" => {
                                    qb.push("::int4");
                                }
                                "bigint" | "int8" => {
                                    qb.push("::int8");
                                }
                                _ => {}
                            }
                        }
                        JsonValue::String(s) => {
                            qb.push_bind(s);
                            match sql_type.as_str() {
                                "timestamptz" => {
                                    qb.push("::timestamptz");
                                }
                                "uuid" => {
                                    qb.push("::uuid");
                                }
                                "text" => {
                                    qb.push("::text");
                                }
                                "bool" | "boolean" => {
                                    qb.push("::bool");
                                }
                                "int" | "int4" | "integer" => {
                                    qb.push("::int4");
                                }
                                "bigint" | "int8" => {
                                    qb.push("::int8");
                                }
                                _ => {}
                            }
                        }
                        other => {
                            // jsonb equality (object/array) or fallback to text representation
                            if matches!(sql_type.as_str(), "jsonb") {
                                qb.push_bind(other).push("::jsonb");
                            } else {
                                qb.push_bind(other.to_string());
                            }
                        }
                    };
                }
            }
        }

        qb.push(" ORDER BY ")
            .push(order_field)
            .push(" ")
            .push(direction)
            .push(" LIMIT ")
            .push_bind(limit as i64);

        let rows = qb.build().fetch_all(&self.pool).await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let mut records: Vec<JsonValue> = Vec::with_capacity(rows.len());
        let mut ids: Vec<String> = Vec::with_capacity(rows.len());
        let mut key_hashes: Vec<H256> = Vec::with_capacity(rows.len());

        for row in rows {
            let record: JsonValue = row.try_get("record")?;
            let pk_value: String = row.try_get("pk_value")?;
            records.push(record);
            ids.push(pk_value.clone());
            key_hashes.push(hash_key(table_name, &pk_value));
        }

        let smt = self.smt_store.lock().await;
        let proof = smt.generate_proof(key_hashes).await?;

        Ok(Some((records, ids, proof)))
    }

    /// Upserts records by primary key (INSERT .. ON CONFLICT(pk) DO UPDATE ..) and returns
    /// a multi-update proof + proposed root, using the DB-returned rows for canonical hashing.
    pub async fn upsert_records(
        &self,
        model: Arc<dyn VerifiableModel>,
        records_data: &[JsonValue],
        trusted_root: H256,
    ) -> Result<(H256, MerkleProof, Vec<JsonValue>, Vec<String>), anyhow::Error> {
        if records_data.is_empty() {
            return Err(anyhow::anyhow!("records_data cannot be empty"));
        }

        let table_name = model.table_name();
        let pk_field = model.primary_key_field();

        let mut key_hashes = Vec::new();
        let mut value_hashes = Vec::new();
        let mut upserted_records: Vec<JsonValue> = Vec::with_capacity(records_data.len());
        let mut upserted_ids: Vec<String> = Vec::with_capacity(records_data.len());

        let mut transaction = self.pool.begin().await?;

        for record_data in records_data {
            let record_obj = record_data
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("Record must be a JSON object"))?;

            if !record_obj.contains_key(pk_field) {
                return Err(anyhow::anyhow!(
                    "Upsert record missing primary key field '{}'",
                    pk_field
                ));
            }

            let columns: Vec<&str> = record_obj.keys().map(|s| s.as_str()).collect();
            let mut casted_placeholders = Vec::new();

            for (idx, col) in columns.iter().enumerate() {
                let placeholder_idx = idx + 1;
                let explicit_type = model.column_type(col).map(|s| s.to_lowercase());
                match explicit_type.as_deref() {
                    Some("timestamptz") => {
                        casted_placeholders.push(format!("${}::timestamptz", placeholder_idx));
                    }
                    Some("jsonb") => {
                        casted_placeholders.push(format!("${}::jsonb", placeholder_idx));
                    }
                    Some("int") | Some("int4") => {
                        casted_placeholders.push(format!("${}::int4", placeholder_idx));
                    }
                    Some("bigint") | Some("int8") => {
                        casted_placeholders.push(format!("${}::int8", placeholder_idx));
                    }
                    Some("bool") | Some("boolean") => {
                        casted_placeholders.push(format!("${}::bool", placeholder_idx));
                    }
                    Some("uuid") => {
                        casted_placeholders.push(format!("${}::uuid", placeholder_idx));
                    }
                    Some("text") => {
                        casted_placeholders.push(format!("${}::text", placeholder_idx));
                    }
                    _ => {
                        casted_placeholders.push(format!("${}", placeholder_idx));
                    }
                }
            }

            let update_cols: Vec<&str> = columns
                .iter()
                .copied()
                .filter(|c| *c != pk_field)
                .collect();
            if update_cols.is_empty() {
                return Err(anyhow::anyhow!(
                    "Upsert requires at least one non-PK field to update"
                ));
            }

            let set_clause = update_cols
                .iter()
                .map(|c| format!("{} = EXCLUDED.{}", c, c))
                .collect::<Vec<_>>()
                .join(", ");

            let sql_with_casts = format!(
                "INSERT INTO {} ({}) VALUES ({}) \
                 ON CONFLICT ({}) DO UPDATE SET {} \
                 RETURNING row_to_json({}.*) as record, {}::text as pk_value",
                table_name,
                columns.join(", "),
                casted_placeholders.join(", "),
                pk_field,
                set_clause,
                table_name,
                pk_field
            );

            let mut query = sqlx::query(&sql_with_casts);
            for (col, value) in columns.iter().zip(record_obj.values()) {
                let explicit_type = model.column_type(col).map(|s| s.to_lowercase());
                let is_timestamp_col = matches!(explicit_type.as_deref(), Some("timestamptz"));
                let is_jsonb_col = matches!(explicit_type.as_deref(), Some("jsonb"));

                if value.is_null() {
                    if is_timestamp_col {
                        query = query.bind::<Option<DateTime<Utc>>>(None);
                    } else {
                        query = query.bind::<Option<String>>(None);
                    }
                } else if let Some(s) = value.as_str() {
                    if is_timestamp_col {
                        match DateTime::parse_from_rfc3339(s) {
                            Ok(dt) => query = query.bind(Some(dt.with_timezone(&Utc))),
                            Err(_) => query = query.bind(s),
                        }
                    } else {
                        query = query.bind(s);
                    }
                } else if let Some(n) = value.as_i64() {
                    query = query.bind(n);
                } else if let Some(n) = value.as_f64() {
                    query = query.bind(n);
                } else if let Some(b) = value.as_bool() {
                    query = query.bind(b);
                } else if is_jsonb_col && (value.is_object() || value.is_array()) {
                    query = query.bind(value);
                } else {
                    query = query.bind(serde_json::to_string(value)?);
                }
            }

            let row = query.fetch_one(&mut *transaction).await?;
            let returned_record: JsonValue = row.try_get("record")?;
            let pk_value: String = row.try_get("pk_value")?;

            upserted_records.push(returned_record);
            upserted_ids.push(pk_value.clone());
            key_hashes.push(hash_key(table_name, &pk_value));
        }

        for record in &upserted_records {
            value_hashes.push(hash_value(record));
        }

        let updates: Vec<(H256, H256)> = key_hashes
            .iter()
            .copied()
            .zip(value_hashes.iter().copied())
            .collect();

        let mut smt = self.smt_store.lock().await;
        let proof = smt.generate_proof(key_hashes.clone()).await?;

        let key_bytes: Vec<Vec<u8>> = key_hashes.iter().map(|k| k.as_bytes().to_vec()).collect();
        let rows = sqlx::query(
            "SELECT node_hash, node_value FROM merkle_nodes WHERE node_hash = ANY($1)",
        )
        .bind(&key_bytes)
        .fetch_all(&mut *transaction)
        .await
        .unwrap_or_default();
        let mut old_map: HashMap<Vec<u8>, H256> = HashMap::new();
        for r in rows {
            let kh: Vec<u8> = r.try_get("node_hash").unwrap_or_default();
            let vh: Vec<u8> = r.try_get("node_value").unwrap_or_default();
            if vh.len() == 32 {
                old_map.insert(kh, H256::from_slice(&vh));
            }
        }
        let old_values: Vec<H256> = key_bytes
            .iter()
            .map(|kb| old_map.get(kb).copied().unwrap_or_else(H256::zero))
            .collect();

        let new_leaves_smt: Vec<_> = key_hashes
            .iter()
            .copied()
            .zip(value_hashes.iter().copied())
            .map(|(k, v)| (h256_to_smt(k), h256_to_smt(v)))
            .collect();
        let proposed_root_smt = proof
            .clone()
            .compute_root::<SmtBlake2bHasher>(new_leaves_smt)
            .unwrap_or_default();
        let proposed_root = smt_to_h256(&proposed_root_smt);

        let ok = verify_smt_multi_update_proof_with_old_values(
            trusted_root,
            proposed_root,
            key_hashes.clone(),
            old_values,
            value_hashes.clone(),
            proof.clone(),
        );
        if !ok {
            transaction.rollback().await?;
            return Err(anyhow::anyhow!(
                "VERIFIABLE_PROOF_FAILED: trusted_root={} proposed_root={}",
                hex::encode(trusted_root.as_bytes()),
                hex::encode(proposed_root.as_bytes())
            ));
        }

        smt.apply_updates_in_tx(&mut transaction, &updates).await?;
        transaction.commit().await?;

        Ok((proposed_root, proof, upserted_records, upserted_ids))
    }
}

