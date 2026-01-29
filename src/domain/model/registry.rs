//! ModelRegistry for mapping model names to VerifiableModel implementations.

use crate::domain::model::DynamicModel;
use crate::domain::model::VerifiableModel;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::sync::Arc;

/// A registry that maps model names to their VerifiableModel implementations.
pub struct ModelRegistry {
    models: HashMap<String, Arc<dyn VerifiableModel>>,
}

impl ModelRegistry {
    /// Creates a new empty ModelRegistry.
    pub fn new() -> Self {
        Self {
            models: HashMap::new(),
        }
    }

    /// Registers a model implementation with the given name.
    pub fn register<M: VerifiableModel + 'static>(&mut self, name: String, model: M) {
        self.models.insert(name, Arc::new(model));
    }

    /// Retrieves a model implementation by name.
    /// Returns None if the model is not registered.
    pub fn get(&self, name: &str) -> Option<Arc<dyn VerifiableModel>> {
        self.models.get(name).cloned()
    }

    /// Returns all registered model names.
    pub fn list_models(&self) -> Vec<String> {
        self.models.keys().cloned().collect()
    }

    /// Returns all CREATE TABLE SQL statements for registered models.
    /// This is used during database initialization.
    pub fn get_all_create_table_sql(&self) -> Vec<&str> {
        self.models
            .values()
            .map(|model| model.get_create_table_sql())
            .collect()
    }

    /// Loads the runtime model registry from the `verifiable_models` table.
    ///
    /// This enables "warm start" behavior: after a restart, the API can serve
    /// `/api/models/{model}/...` immediately without requiring another bootstrap call,
    /// as long as the schema was previously registered.
    pub async fn load_from_db(pool: &PgPool) -> anyhow::Result<Self> {
        let rows = match sqlx::query(
            "SELECT table_name, primary_key_field, primary_key_kind, columns, create_table_sql FROM verifiable_models",
        )
        .fetch_all(pool)
        .await
        {
            Ok(r) => r,
            Err(_) => return Ok(ModelRegistry::new()),
        };

        if rows.is_empty() {
            return Ok(ModelRegistry::new());
        }

        let mut reg = ModelRegistry::new();

        for r in rows {
            let table_name: String = r.try_get("table_name")?;
            let pk_field: String = r.try_get("primary_key_field")?;
            let pk_kind: String = r.try_get("primary_key_kind")?;
            let create_table_sql: String = r.try_get("create_table_sql")?;
            let columns: serde_json::Value = r.try_get("columns")?;

            // Rebuild the DynamicModel column type map to preserve explicit type casting on writes.
            let mut column_types: HashMap<String, String> = HashMap::new();

            // pk kind values are stored like: serial, bigserial, text, integer, bigint, uuid
            // Map serial types into their underlying integer types for consistent casting.
            let pk_kind_lc = pk_kind.to_lowercase();
            let pk_sql_type = match pk_kind_lc.as_str() {
                "serial" => "int",
                "bigserial" => "bigint",
                other => other,
            };
            column_types.insert(pk_field.clone(), pk_sql_type.to_string());

            if let Some(cols) = columns.as_array() {
                for c in cols {
                    let name = c.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    if name.is_empty() {
                        continue;
                    }
                    let col_type = c
                        .get("col_type")
                        .and_then(|v| v.as_str())
                        .unwrap_or("text")
                        .to_lowercase();

                    // Map schema enum strings to actual Postgres SQL type strings (same as bootstrap).
                    let sql_type = match col_type.as_str() {
                        "text" => "text",
                        "int" => "int",
                        "big_int" => "bigint",
                        "bool" => "bool",
                        "jsonb" => "jsonb",
                        "timestamptz" => "timestamptz",
                        "uuid" => "uuid",
                        other => other,
                    };

                    column_types.insert(name.to_string(), sql_type.to_string());
                }
            }

            reg.register(
                table_name.clone(),
                DynamicModel::new(table_name, pk_field, create_table_sql, column_types),
            );
        }

        Ok(reg)
    }
}

impl Default for ModelRegistry {
    fn default() -> Self {
        Self::new()
    }
}

