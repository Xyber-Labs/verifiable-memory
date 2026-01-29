-- Test migration: add columns without losing data (idempotent).
-- These are based on the app tables shown in your current schema snapshot.

ALTER TABLE agents
  ADD COLUMN IF NOT EXISTS schema_version integer NOT NULL DEFAULT 1;

ALTER TABLE agent_tweets
  ADD COLUMN IF NOT EXISTS archived boolean NOT NULL DEFAULT false;

ALTER TABLE follow_queue
  ADD COLUMN IF NOT EXISTS processed_at timestamptz NULL;
