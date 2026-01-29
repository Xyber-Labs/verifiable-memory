//! Domain model definitions for “verifiable models”.

use serde_json::Value as JsonValue;

pub mod examples;
pub mod dynamic;
pub mod registry;

pub use dynamic::DynamicModel;
pub use examples::{ProductModel, UserModel, WidgetModel};
pub use registry::ModelRegistry;

/// Trait that defines the contract for any verifiable model.
///
/// This trait allows the API/service to work with any model without knowing
/// its specific schema or business logic. Each model implementation provides:
/// - Table name and primary key information
/// - SQL schema definition
/// - Optional validation logic
pub trait VerifiableModel: Send + Sync {
    /// Returns the name of the database table for this model.
    fn table_name(&self) -> &str;

    /// Returns the name of the primary key field for this model.
    fn primary_key_field(&self) -> &str;

    /// Returns the SQL CREATE TABLE statement for this model.
    /// This will be executed during database initialization.
    fn get_create_table_sql(&self) -> &str;

    /// Optional column typing metadata for reliable SQL casting/binding.
    /// If not provided, the service falls back to heuristics.
    fn column_type(&self, _column: &str) -> Option<&str> {
        None
    }

    /// Validates the payload before creating records.
    /// Returns Ok(()) if valid, Err(String) with error message if invalid.
    ///
    /// Default implementation does no validation.
    fn validate_create_payload(&self, _payload: &JsonValue) -> Result<(), String> {
        Ok(())
    }

    /// Validates the payload before updating records.
    /// Returns Ok(()) if valid, Err(String) with error message if invalid.
    ///
    /// Default implementation does no validation.
    #[allow(dead_code)] // Reserved for future use
    fn validate_update_payload(&self, _payload: &JsonValue) -> Result<(), String> {
        Ok(())
    }
}

