#!/usr/bin/env bash
# Build + run the API service inside Docker (for deployment-like behavior).
#
# Prereqs:
# - PostgreSQL container running (./scripts/start_db.sh)
# - .env present (DATABASE_URL, SOLANA_RPC_URL, SOLANA_PROGRAM_ID, BATCH_COMMIT_SIZE)
# - Solana payer key on host: ~/.config/solana/id.json (mounted into the container)
#
# Usage:
#   ./scripts/start_api_docker.sh
#
# Env overrides:
#   IMAGE_NAME=verifiable-db-api:latest ./scripts/start_api_docker.sh
#   API_PORT=3000 ./scripts/start_api_docker.sh
#   DB_CONTAINER_NAME=pg-verifiable-memory ./scripts/start_api_docker.sh
#   BUILD=auto|true|false ./scripts/start_api_docker.sh
#     - auto  (default): build only if image doesn't exist
#     - true:           always build
#     - false:          never build (fails if image missing)
#   NETWORK_MODE=bridge|host ./scripts/start_api_docker.sh
#     - bridge (default): API container runs on a user-defined bridge network and talks to Postgres by container name
#     - host:            API container uses host networking (Linux only). Useful if bridge DNS/egress to Solana RPC is flaky.

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

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker first."
  exit 1
fi

IMAGE_NAME="${IMAGE_NAME:-verifiable-db-api:latest}"
CONTAINER_NAME="${CONTAINER_NAME:-verifiable-db-api}"
API_PORT="${API_PORT:-3000}"
DB_CONTAINER_NAME="${DB_CONTAINER_NAME:-pg-verifiable-memory}"
DOCKER_NETWORK="${DOCKER_NETWORK:-verifiable-db-net}"
BUILD="${BUILD:-auto}"
NETWORK_MODE="${NETWORK_MODE:-bridge}"

KEYPAIR_HOST_PATH="${SOLANA_KEYPAIR_PATH:-${HOME}/.config/solana/id.json}"
if [[ ! -f "${KEYPAIR_HOST_PATH}" ]]; then
  echo "Error: Solana keypair not found at ${KEYPAIR_HOST_PATH}"
  echo "Hint: install Solana CLI and run: solana-keygen new (or set SOLANA_KEYPAIR_PATH)"
  exit 1
fi

DATABASE_URL_DOCKER="${DATABASE_URL}"

if [[ "${NETWORK_MODE}" == "bridge" ]]; then
  # Ensure network exists
  if ! docker network inspect "${DOCKER_NETWORK}" >/dev/null 2>&1; then
    docker network create "${DOCKER_NETWORK}" >/dev/null
  fi

  # Ensure DB container exists and is connected to the network
  if ! docker ps --format "{{.Names}}" | grep -qx "${DB_CONTAINER_NAME}"; then
    echo "Error: DB container '${DB_CONTAINER_NAME}' is not running."
    echo "Hint: start it with: ./scripts/start_db.sh"
    exit 1
  fi
  docker network connect "${DOCKER_NETWORK}" "${DB_CONTAINER_NAME}" >/dev/null 2>&1 || true

  # Rewrite DATABASE_URL for in-network access (localhost -> db container name)
  DATABASE_URL_DOCKER="${DATABASE_URL_DOCKER//@localhost:5432/@${DB_CONTAINER_NAME}:5432}"
  DATABASE_URL_DOCKER="${DATABASE_URL_DOCKER//@127.0.0.1:5432/@${DB_CONTAINER_NAME}:5432}"
  DATABASE_URL_DOCKER="${DATABASE_URL_DOCKER//@localhost:/@${DB_CONTAINER_NAME}:}"
  DATABASE_URL_DOCKER="${DATABASE_URL_DOCKER//@127.0.0.1:/@${DB_CONTAINER_NAME}:}"
elif [[ "${NETWORK_MODE}" == "host" ]]; then
  # Host network mode: DATABASE_URL should typically be localhost:5432 (host's published port).
  :
else
  echo "Error: NETWORK_MODE must be one of: bridge|host (got '${NETWORK_MODE}')"
  exit 1
fi

# Persist trusted_state.json on host (bind mount)
touch "${PROJECT_ROOT}/trusted_state.json"

echo ""
cd "${PROJECT_ROOT}"

image_exists() {
  docker image inspect "$1" >/dev/null 2>&1
}

case "${BUILD}" in
  auto)
    if image_exists "${IMAGE_NAME}"; then
      echo "Reusing existing API image '${IMAGE_NAME}' (BUILD=auto)."
    else
      echo "Building API image '${IMAGE_NAME}' (BUILD=auto, image missing)..."
      docker build -t "${IMAGE_NAME}" .
    fi
    ;;
  true)
    echo "Building API image '${IMAGE_NAME}' (BUILD=true)..."
    docker build -t "${IMAGE_NAME}" .
    ;;
  false)
    if image_exists "${IMAGE_NAME}"; then
      echo "Reusing existing API image '${IMAGE_NAME}' (BUILD=false)."
    else
      echo "Error: API image '${IMAGE_NAME}' does not exist and BUILD=false."
      echo "Hint: run once with BUILD=true to build it."
      exit 1
    fi
    ;;
  *)
    echo "Error: BUILD must be one of: auto|true|false (got '${BUILD}')"
    exit 1
    ;;
esac

echo ""
echo "Starting API container '${CONTAINER_NAME}' on port ${API_PORT}..."

# Replace running container
if docker ps -a --format "{{.Names}}" | grep -qx "${CONTAINER_NAME}"; then
  docker rm -f "${CONTAINER_NAME}" >/dev/null
fi

DOCKER_RUN_ARGS=(
  -d
  --name "${CONTAINER_NAME}"
  -e "DATABASE_URL=${DATABASE_URL_DOCKER}"
  -e "SOLANA_RPC_URL=${SOLANA_RPC_URL}"
  -e "SOLANA_PROGRAM_ID=${SOLANA_PROGRAM_ID}"
  -e "BATCH_COMMIT_SIZE=${BATCH_COMMIT_SIZE}"
  -v "${KEYPAIR_HOST_PATH}:/home/appuser/.config/solana/id.json:ro"
  -v "${PROJECT_ROOT}/trusted_state.json:/app/trusted_state.json"
)

if [[ "${NETWORK_MODE}" == "bridge" ]]; then
  DOCKER_RUN_ARGS+=(--network "${DOCKER_NETWORK}" -p "${API_PORT}:3000")
else
  # host networking (Linux only): no port publish needed
  DOCKER_RUN_ARGS+=(--network host)
fi

docker run "${DOCKER_RUN_ARGS[@]}" "${IMAGE_NAME}" >/dev/null

# Fail fast if container exited (so users don't go hunting for why Swagger won't load)
sleep 0.5
if ! docker ps --format "{{.Names}}" | grep -qx "${CONTAINER_NAME}"; then
  echo ""
  echo "Error: API container exited immediately. Recent logs:"
  docker logs --tail 200 "${CONTAINER_NAME}" || true
  exit 1
fi

echo ""
echo "✓ API container started."
echo "  API:        http://localhost:${API_PORT}/api/execute"
echo "  Swagger UI: http://localhost:${API_PORT}/swagger-ui"
echo ""
echo "Logs:"
echo "  docker logs -f ${CONTAINER_NAME}"

