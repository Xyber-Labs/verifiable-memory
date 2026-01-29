#!/usr/bin/env bash
# Start the API server (with Swagger UI) for manual interaction.
#
# This runs the `api_server` binary, which serves:
# - API:        http://localhost:3000/api/execute
# - Swagger UI: http://localhost:3000/swagger-ui
#
# Usage:
#   ./scripts/start_api.sh
#
# Notes:
# - Loads .env from repo root if present.
# - Requires env vars (no defaults).

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load .env if present (optional)
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
  local retries="${4:-20}"

  for _ in $(seq 1 "${retries}"); do
    if (echo >/dev/tcp/"${host}"/"${port}") >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  echo "Error: ${name} not reachable at ${host}:${port}"
  return 1
}

# Quick local checks for PostgreSQL
wait_for_tcp "127.0.0.1" "5432" "PostgreSQL" 5 || {
  echo "Hint: start it with: ./scripts/start-db.sh"
  exit 1
}

# Note: We use devnet by default, so no local Solana validator check needed

echo ""
echo "Starting API server with:"
echo "  DATABASE_URL=${DATABASE_URL}"
echo "  SOLANA_RPC_URL=${SOLANA_RPC_URL}"
echo "  BATCH_COMMIT_SIZE=${BATCH_COMMIT_SIZE}"
echo ""
echo "Swagger UI: http://localhost:3000/swagger-ui"
echo ""

cd "${PROJECT_ROOT}"

DATABASE_URL="${DATABASE_URL}" \
SOLANA_RPC_URL="${SOLANA_RPC_URL}" \
BATCH_COMMIT_SIZE="${BATCH_COMMIT_SIZE}" \
cargo run --bin api_server
