-- Migration script for the merkle nodes table (idempotent)
CREATE TABLE IF NOT EXISTS merkle_nodes (
    node_hash BYTEA PRIMARY KEY,
    node_value BYTEA NOT NULL
);
