#!/usr/bin/env bash
# Run the end-to-end simulation test for this repo (bootstrap + batching + verified reads).
#
# Prereqs:
# - PostgreSQL running (./scripts/start-db.sh)
# - Solana devnet RPC + program id available (required via .env)
#
# Usage:
#   ./scripts/start_simulation.sh
#   BATCH_COMMIT_SIZE=5 ./scripts/start_simulation.sh
#   CLEAR_DB=false ./scripts/start_simulation.sh

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load .env if present (optional). This mirrors what the Rust app does via dotenv.
if [[ -f "${PROJECT_ROOT}/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "${PROJECT_ROOT}/.env"
  set +a
fi

require_env() {
  local name="$1"
  if [[ -z "${!name:-}" ]]; then
    echo "Error: ${name} is not set. Put it in ${PROJECT_ROOT}/.env"
    exit 1
  fi
}

require_env "DATABASE_URL"
require_env "SOLANA_RPC_URL"
require_env "SOLANA_PROGRAM_ID"
require_env "BATCH_COMMIT_SIZE"

CLEAR_DB="${CLEAR_DB:-true}"

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local name="$3"
  local retries="${4:-30}"

  for i in $(seq 1 "${retries}"); do
    if (echo >/dev/tcp/"${host}"/"${port}") >/dev/null 2>&1; then
      echo "✓ ${name} is reachable at ${host}:${port}"
      return 0
    fi
    sleep 1
  done
  echo "Error: ${name} not reachable at ${host}:${port} after ${retries}s"
  return 1
}

# Basic connectivity checks
wait_for_tcp "127.0.0.1" "5432" "PostgreSQL" 10 || {
  echo "Hint: start it with: ./scripts/start-db.sh"
  exit 1
}

echo "Using SOLANA_RPC_URL=${SOLANA_RPC_URL}"
echo "Using SOLANA_PROGRAM_ID=${SOLANA_PROGRAM_ID}"

echo ""
echo "Running simulation with:"
echo "  DATABASE_URL=${DATABASE_URL}"
echo "  SOLANA_RPC_URL=${SOLANA_RPC_URL}"
echo "  BATCH_COMMIT_SIZE=${BATCH_COMMIT_SIZE}"
echo "  CLEAR_DB=${CLEAR_DB}"
echo ""

cd "${PROJECT_ROOT}"

DATABASE_URL="${DATABASE_URL}" \
SOLANA_RPC_URL="${SOLANA_RPC_URL}" \
BATCH_COMMIT_SIZE="${BATCH_COMMIT_SIZE}" \
CLEAR_DB="${CLEAR_DB}" \
cargo test --test test_schema_update -- --nocapture
