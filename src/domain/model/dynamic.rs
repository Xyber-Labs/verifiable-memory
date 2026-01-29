use crate::domain::model::VerifiableModel;
use serde_json::Value as JsonValue;
use std::collections::HashMap;

/// Runtime model loaded from the `verifiable_models` registry.
pub struct DynamicModel {
    table_name: String,
    primary_key_field: String,
    create_table_sql: String,
    column_types: HashMap<String, String>,
}

impl DynamicModel {
    pub fn new(
        table_name: String,
        primary_key_field: String,
        create_table_sql: String,
        column_types: HashMap<String, String>,
    ) -> Self {
        Self {
            table_name,
            primary_key_field,
            create_table_sql,
            column_types,
        }
    }
}

impl VerifiableModel for DynamicModel {
    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn primary_key_field(&self) -> &str {
        &self.primary_key_field
    }

    fn get_create_table_sql(&self) -> &str {
        &self.create_table_sql
    }

    fn column_type(&self, column: &str) -> Option<&str> {
        self.column_types.get(column).map(|s| s.as_str())
    }

    fn validate_create_payload(&self, _payload: &JsonValue) -> Result<(), String> {
        // Dynamic models are validated at the schema/DDL layer; keep runtime validation minimal by default.
        Ok(())
    }
}

