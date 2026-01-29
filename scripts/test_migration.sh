#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:3000}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "> Testing migration endpoint against: ${API_URL}"

echo "> Creating a schema-changing migration (ALTER TABLE ... ADD COLUMN) ..."
ts="$(date +%Y%m%d%H%M%S)"
migration_file="${PROJECT_ROOT}/migrations/${ts}_migration_test_add_columns.sql"

cat > "${migration_file}" <<'SQL'
-- Test migration: add columns without losing data (idempotent).
-- These are based on the app tables shown in your current schema snapshot.

ALTER TABLE agents
  ADD COLUMN IF NOT EXISTS schema_version integer NOT NULL DEFAULT 1;

ALTER TABLE agent_tweets
  ADD COLUMN IF NOT EXISTS archived boolean NOT NULL DEFAULT false;

ALTER TABLE follow_queue
  ADD COLUMN IF NOT EXISTS processed_at timestamptz NULL;
SQL

echo "> Wrote migration: ${migration_file}"

echo "> Calling POST /bootstrap/migrate ..."
resp="$(curl -sS -X POST "${API_URL}/bootstrap/migrate" \
  -H "Content-Type: application/json" \
  -d '{"confirm": true}')"

echo "${resp}"

if command -v jq >/dev/null 2>&1; then
  ok="$(echo "${resp}" | jq -r '.success // false')"
  if [[ "${ok}" != "true" ]]; then
    echo "> ERROR: migration endpoint returned success=false" >&2
    exit 1
  fi
  new_root="$(echo "${resp}" | jq -r '.data.new_root // empty')"
  echo "> OK: new_root=${new_root}"
else
  echo "> NOTE: jq not found; skipping response assertions."
fi

echo ""
echo "> Fetching current DB schema (GET /bootstrap/schema) ..."
curl -sS "${API_URL}/bootstrap/schema" || true

echo "> Done."

