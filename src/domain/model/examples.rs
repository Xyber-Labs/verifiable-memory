use super::VerifiableModel;
use serde_json::Value as JsonValue;

/// Implementation of VerifiableModel for the Users model.
pub struct UserModel;

impl VerifiableModel for UserModel {
    fn table_name(&self) -> &str {
        "users"
    }

    fn primary_key_field(&self) -> &str {
        "id"
    }

    fn get_create_table_sql(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            email TEXT NOT NULL,
            last_login TIMESTAMPTZ NOT NULL,
            profile_data JSONB NOT NULL
        )"
    }

    fn validate_create_payload(&self, payload: &JsonValue) -> Result<(), String> {
        // Example validation: ensure email field exists
        if !payload.is_object() {
            return Err("Payload must be an object".to_string());
        }
        if payload.get("email").is_none() {
            return Err("User must have an email field".to_string());
        }
        Ok(())
    }
}

/// Implementation of VerifiableModel for the Products model.
pub struct ProductModel;

impl VerifiableModel for ProductModel {
    fn table_name(&self) -> &str {
        "products"
    }

    fn primary_key_field(&self) -> &str {
        "id"
    }

    fn get_create_table_sql(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS products (
            id BIGSERIAL PRIMARY KEY,
            external_id TEXT UNIQUE,
            name TEXT NOT NULL,
            price INTEGER NOT NULL,
            in_stock BOOLEAN NOT NULL
        )"
    }

    fn validate_create_payload(&self, payload: &JsonValue) -> Result<(), String> {
        if !payload.is_object() {
            return Err("Payload must be an object".to_string());
        }
        if payload.get("name").is_none() {
            return Err("Product must have a name field".to_string());
        }
        if payload.get("price").is_none() {
            return Err("Product must have a price field".to_string());
        }
        Ok(())
    }
}

/// Implementation of VerifiableModel for the Widgets model.
pub struct WidgetModel;

impl VerifiableModel for WidgetModel {
    fn table_name(&self) -> &str {
        "widgets"
    }

    fn primary_key_field(&self) -> &str {
        "id"
    }

    fn get_create_table_sql(&self) -> &str {
        "CREATE TABLE IF NOT EXISTS widgets (
            id BIGSERIAL PRIMARY KEY,
            widget_id TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            quantity INTEGER NOT NULL,
            metadata JSONB
        )"
    }

    fn validate_create_payload(&self, payload: &JsonValue) -> Result<(), String> {
        if payload.get("widget_id").is_none() {
            return Err("Widget must have a widget_id field".to_string());
        }
        if payload.get("quantity").and_then(|q| q.as_i64()).unwrap_or(-1) < 0 {
            return Err("Widget quantity cannot be negative".to_string());
        }
        Ok(())
    }
}

