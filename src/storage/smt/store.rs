//! Sparse Merkle Tree (SMT) wrapper and hashing utilities.

use crate::storage::smt::postgres::{PostgresSmtStore, SmtValue};
use blake2::{Blake2b, Digest};
use primitive_types::H256;
use sparse_merkle_tree::{default_store::DefaultStore, MerkleProof, SparseMerkleTree, H256 as SmtH256};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::env;

// --- Hasher Implementation ---
#[derive(Default)]
pub struct SmtBlake2bHasher(Blake2b<sha2::digest::consts::U32>);

impl sparse_merkle_tree::traits::Hasher for SmtBlake2bHasher {
    fn write_h256(&mut self, h: &SmtH256) {
        self.0.update(h.as_slice());
    }
    fn write_byte(&mut self, b: u8) {
        self.0.update(&[b]);
    }
    fn finish(self) -> SmtH256 {
        let mut hash_bytes = [0u8; 32];
        hash_bytes.copy_from_slice(&self.0.finalize());
        hash_bytes.into()
    }
}

// --- Type Conversion Helpers ---
pub fn h256_to_smt(h: H256) -> SmtH256 {
    let mut bytes = [0u8; 32];
    bytes.copy_from_slice(h.as_bytes());
    bytes.into()
}

pub fn smt_to_h256(h: &SmtH256) -> H256 {
    H256::from_slice(h.as_slice())
}

// --- SMT Store Wrapper ---
pub struct SmtStore {
    tree: SparseMerkleTree<SmtBlake2bHasher, SmtValue, DefaultStore<SmtValue>>,
    db_store: PostgresSmtStore,
}

impl SmtStore {
    pub async fn new() -> anyhow::Result<Self> {
        dotenv::dotenv().ok();
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?;
        Self::new_with_pool(pool).await
    }

    pub async fn new_with_pool(pool: PgPool) -> anyhow::Result<Self> {
        let db_store = PostgresSmtStore::new(pool);
        let mut tree = SparseMerkleTree::default();
        let pairs = db_store.get_all().await?;
        for (key, value) in pairs {
            tree.update(key, value)?;
        }
        Ok(Self { tree, db_store })
    }

    pub async fn get_root(&self) -> anyhow::Result<H256> {
        Ok(smt_to_h256(self.tree.root()))
    }

    pub async fn update(&mut self, key: H256, value: H256) -> anyhow::Result<()> {
        let key_smt = h256_to_smt(key);
        let value_smt = SmtValue(h256_to_smt(value));
        self.tree.update(key_smt, value_smt.clone())?;
        self.db_store.set(key_smt, value_smt).await?;
        Ok(())
    }

    pub async fn generate_proof(&self, keys: Vec<H256>) -> anyhow::Result<MerkleProof> {
        let smt_keys: Vec<SmtH256> = keys.into_iter().map(h256_to_smt).collect();
        let proof = self.tree.merkle_proof(smt_keys)?;
        Ok(proof)
    }

    /// Applies updates to the in-memory tree AND persists them into `merkle_nodes` within `tx`.
    pub async fn apply_updates_in_tx(
        &mut self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        updates: &[(H256, H256)],
    ) -> anyhow::Result<()> {
        for (k, v) in updates {
            let key_smt = h256_to_smt(*k);
            let value_smt = SmtValue(h256_to_smt(*v));
            self.tree.update(key_smt, value_smt.clone())?;
            self.db_store.set_in_tx(tx, key_smt, value_smt).await?;
        }
        Ok(())
    }
}

