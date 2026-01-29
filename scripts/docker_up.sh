#!/usr/bin/env bash
# docker_up.sh
#
# Purpose: One-command local bring-up that runs the API **inside Docker**
# (deployment-like), while using Solana devnet/mainnet for anchoring.
#
# What it does:
# - Starts PostgreSQL in Docker (reuses existing container if present)
# - Validates required env vars from .env
# - Runs a Solana preflight (RPC connectivity, payer balance, program exists, PDA exists)
#   - Optional: initialize the PDA if missing
# - Builds + runs the API service container (mounts your Solana keypair + trusted_state.json)
#
# Usage:
#   ./scripts/docker_up.sh
#
# Optional:
#   INIT_PDA=true ./scripts/docker_up.sh   # initializes PDA if missing

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Load .env if present
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

echo ""
echo "==> Starting PostgreSQL (docker)..."
"${PROJECT_ROOT}/scripts/start_db.sh"

echo ""
echo "==> Solana preflight (program + PDA + payer)..."
cd "${PROJECT_ROOT}"

PREFLIGHT_ARGS=()
if [[ "${INIT_PDA:-false}" == "true" ]]; then
  PREFLIGHT_ARGS+=(--init-pda-if-missing)
fi

DATABASE_URL="${DATABASE_URL}" \
SOLANA_RPC_URL="${SOLANA_RPC_URL}" \
SOLANA_PROGRAM_ID="${SOLANA_PROGRAM_ID}" \
BATCH_COMMIT_SIZE="${BATCH_COMMIT_SIZE}" \
cargo run --quiet --bin preflight -- "${PREFLIGHT_ARGS[@]}"

echo ""
echo "==> Starting API in Docker..."
"${PROJECT_ROOT}/scripts/start_api_docker.sh"

