// This file is used to hash the data into a 256-bit hash.

use primitive_types::H256;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

// Domain separation constants to prevent hash collisions between different types of data.
const LEAF_DOMAIN: &[u8] = b"VERIFLEAF";
const NODE_DOMAIN: &[u8] = b"VERIFNODE";

/// A helper function to sort a JSON object's keys recursively.
/// This is essential for canonical serialization.
fn sort_json_value(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let sorted_map: BTreeMap<String, Value> = map
                .iter()
                .map(|(k, v)| (k.clone(), sort_json_value(v)))
                .collect();
            Value::Object(sorted_map.into_iter().collect())
        }
        Value::Array(arr) => {
            let sorted_arr = arr.iter().map(sort_json_value).collect();
            Value::Array(sorted_arr)
        }
        _ => value.clone(),
    }
}

/// Hashes a generic JSON value into a H256 digest.
/// It ensures canonical serialization by sorting keys.
pub fn hash_value(value: &Value) -> H256 {
    let sorted_value = sort_json_value(value);
    let canonical_string = serde_json::to_string(&sorted_value).unwrap();

    let mut hasher = Sha256::new();
    hasher.update(LEAF_DOMAIN);
    hasher.update(canonical_string.as_bytes());
    H256::from_slice(&hasher.finalize())
}

/// Creates a composite key for a database row to be used in the SMT.
pub fn hash_key(table_name: &str, primary_key: &str) -> H256 {
    let mut hasher = Sha256::new();
    hasher.update(NODE_DOMAIN);
    hasher.update(table_name.as_bytes());
    hasher.update(primary_key.as_bytes());
    H256::from_slice(&hasher.finalize())
}

