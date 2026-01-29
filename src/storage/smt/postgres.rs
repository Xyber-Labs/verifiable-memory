//! Persistent SMT store implementation using PostgreSQL.

use anyhow::Result;
use sparse_merkle_tree::{traits::Value, H256 as SmtH256};
use sqlx::{PgPool, Row};

/// SMT value wrapper for the underlying `sparse-merkle-tree` crate.
#[derive(Clone, Default, Debug, PartialEq, Eq)]
pub struct SmtValue(pub SmtH256);

impl Value for SmtValue {
    fn to_h256(&self) -> SmtH256 {
        self.0
    }

    fn zero() -> Self {
        SmtValue(SmtH256::zero())
    }
}

/// A persistent SMT store that uses a PostgreSQL connection pool.
#[derive(Clone)]
pub struct PostgresSmtStore {
    pool: PgPool,
}

impl PostgresSmtStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn get_all(&self) -> Result<Vec<(SmtH256, SmtValue)>> {
        let rows = sqlx::query("SELECT node_hash, node_value FROM merkle_nodes")
            .fetch_all(&self.pool)
            .await?;
        let mut pairs = Vec::with_capacity(rows.len());
        for row in rows {
            let key_bytes: Vec<u8> = row.try_get("node_hash")?;
            let value_bytes: Vec<u8> = row.try_get("node_value")?;
            let key_h256: [u8; 32] =
                key_bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid key length"))?;
            let value_h256: [u8; 32] =
                value_bytes
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("Invalid value length"))?;
            pairs.push((key_h256.into(), SmtValue(value_h256.into())));
        }
        Ok(pairs)
    }

    pub async fn set(&self, key: SmtH256, value: SmtValue) -> Result<()> {
        let key_bytes = key.as_slice();
        let value_h256 = value.to_h256();
        let value_bytes = value_h256.as_slice();
        sqlx::query(
            "INSERT INTO merkle_nodes (node_hash, node_value) VALUES ($1, $2)
             ON CONFLICT (node_hash) DO UPDATE SET node_value = $2",
        )
        .bind(key_bytes)
        .bind(value_bytes)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_in_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        key: SmtH256,
        value: SmtValue,
    ) -> Result<()> {
        let key_bytes = key.as_slice();
        let value_h256 = value.to_h256();
        let value_bytes = value_h256.as_slice();
        sqlx::query(
            "INSERT INTO merkle_nodes (node_hash, node_value) VALUES ($1, $2)
             ON CONFLICT (node_hash) DO UPDATE SET node_value = $2",
        )
        .bind(key_bytes)
        .bind(value_bytes)
        .execute(tx.as_mut())
        .await?;
        Ok(())
    }
}

