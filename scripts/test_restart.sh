#!/usr/bin/env bash
# Tests warm-start restart behavior:
# - start server, bootstrap schema, write once
# - restart server, load registry from DB (verifiable_models) without bootstrapping
# - ensure create/read works immediately
#
# Prereqs:
# - PostgreSQL running (./scripts/start-db.sh)
# - Solana devnet RPC + program id available (required via .env)
#
# Usage:
#   ./scripts/test_restart.sh
#
# Env overrides:
#   SOLANA_RPC_URL=... DATABASE_URL=... ./scripts/test_restart.sh

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

wait_for_tcp() {
  local host="$1"
  local port="$2"
  local name="$3"
  local retries="${4:-30}"

  for _ in $(seq 1 "${retries}"); do
    if (echo >/dev/tcp/"${host}"/"${port}") >/dev/null 2>&1; then
      echo "✓ ${name} is reachable at ${host}:${port}"
      return 0
    fi
    sleep 1
  done
  echo "Error: ${name} not reachable at ${host}:${port} after ${retries}s"
  return 1
}

wait_for_tcp "127.0.0.1" "5432" "PostgreSQL" 10 || {
  echo "Hint: start it with: ./scripts/start-db.sh"
  exit 1
}

# Note: We use devnet by default, so no local TCP check needed
echo "Using SOLANA_RPC_URL=${SOLANA_RPC_URL}"
echo "Using SOLANA_PROGRAM_ID=${SOLANA_PROGRAM_ID}"

echo ""
echo "Running restart warm-start test with:"
echo "  DATABASE_URL=${DATABASE_URL}"
echo "  SOLANA_RPC_URL=${SOLANA_RPC_URL}"
echo "  BATCH_COMMIT_SIZE=${BATCH_COMMIT_SIZE}"
echo ""

cd "${PROJECT_ROOT}"

DATABASE_URL="${DATABASE_URL}" \
SOLANA_RPC_URL="${SOLANA_RPC_URL}" \
BATCH_COMMIT_SIZE="${BATCH_COMMIT_SIZE}" \
cargo test --test test_restart_warm_start -- --nocapture

