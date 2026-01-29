#!/usr/bin/env bash
# Start (or reuse) a local PostgreSQL container for this repo.
#
# Matches README defaults:
#   DATABASE_URL="postgres://postgres:password@localhost:5432/verifiable_memory"
#
# Usage:
#   ./scripts/start-db.sh
#   DB_PORT=5433 ./scripts/start-db.sh

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CONTAINER_NAME="${CONTAINER_NAME:-pg-verifiable-memory}"
DB_NAME="${DB_NAME:-verifiable_memory}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-password}"
DB_PORT="${DB_PORT:-5432}"
POSTGRES_IMAGE="${POSTGRES_IMAGE:-postgres:16}"
VOLUME_NAME="${VOLUME_NAME:-pg-verifiable-memory-data}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed (required to start PostgreSQL)."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker first."
  exit 1
fi

# Prefer an image that already exists locally to avoid large pulls.
# You can always override with: POSTGRES_IMAGE=postgres:XX ./scripts/start-db.sh
if [[ -z "${POSTGRES_IMAGE:-}" || "${POSTGRES_IMAGE}" == "postgres:16" ]]; then
  if docker image inspect postgres:17 >/dev/null 2>&1; then
    POSTGRES_IMAGE="postgres:17"
  elif docker image inspect postgres:latest >/dev/null 2>&1; then
    POSTGRES_IMAGE="postgres:latest"
  elif docker image inspect postgres:15-alpine >/dev/null 2>&1; then
    POSTGRES_IMAGE="postgres:15-alpine"
  else
    # Fall back to a reasonable default; docker may need to pull it.
    POSTGRES_IMAGE="postgres:17"
  fi
fi

# If some other container is already publishing DB_PORT, try to reuse it.
PORT_IN_USE="$(docker ps --format "{{.Names}}" --filter "publish=${DB_PORT}" | head -n 1 || true)"
if [[ -n "${PORT_IN_USE}" && "${PORT_IN_USE}" != "${CONTAINER_NAME}" ]]; then
  echo "Found an existing container publishing port ${DB_PORT}: ${PORT_IN_USE}"
  echo "Trying to reuse it (create DB/user if possible)..."

  set +e
  docker exec "${PORT_IN_USE}" psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" >/dev/null 2>&1
  if [[ $? -ne 0 ]]; then
    echo "Warning: couldn't query postgres inside ${PORT_IN_USE}. It may not be a Postgres container."
  else
    DB_EXISTS="$(docker exec "${PORT_IN_USE}" psql -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'" 2>/dev/null | tr -d '[:space:]')"
    if [[ "${DB_EXISTS}" != "1" ]]; then
      docker exec "${PORT_IN_USE}" psql -U postgres -c "CREATE DATABASE ${DB_NAME};" >/dev/null 2>&1 || true
    fi

    USER_EXISTS="$(docker exec "${PORT_IN_USE}" psql -U postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}'" 2>/dev/null | tr -d '[:space:]')"
    if [[ "${USER_EXISTS}" != "1" ]]; then
      docker exec "${PORT_IN_USE}" psql -U postgres -c "CREATE USER ${DB_USER} WITH PASSWORD '${DB_PASSWORD}';" >/dev/null 2>&1 || true
    fi
    docker exec "${PORT_IN_USE}" psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};" >/dev/null 2>&1 || true
  fi
  set -e

  echo ""
  echo "PostgreSQL is assumed running on port ${DB_PORT} (container: ${PORT_IN_USE})."
  echo "DATABASE_URL=\"postgres://${DB_USER}:${DB_PASSWORD}@localhost:${DB_PORT}/${DB_NAME}\""
  exit 0
fi

if ! docker ps -a --format "{{.Names}}" | grep -qx "${CONTAINER_NAME}"; then
  echo "Creating PostgreSQL container '${CONTAINER_NAME}' on port ${DB_PORT}..."
  docker run -d \
    --name "${CONTAINER_NAME}" \
    -e POSTGRES_USER="${DB_USER}" \
    -e POSTGRES_PASSWORD="${DB_PASSWORD}" \
    -e POSTGRES_DB="${DB_NAME}" \
    -v "${VOLUME_NAME}:/var/lib/postgresql/data" \
    -p "${DB_PORT}:5432" \
    "${POSTGRES_IMAGE}" >/dev/null
else
  if docker ps --format "{{.Names}}" | grep -qx "${CONTAINER_NAME}"; then
    echo "PostgreSQL container '${CONTAINER_NAME}' is already running."
  else
    echo "Starting existing PostgreSQL container '${CONTAINER_NAME}'..."
    docker start "${CONTAINER_NAME}" >/dev/null
  fi
fi

echo "Waiting for PostgreSQL readiness..."
for i in {1..30}; do
  if docker exec "${CONTAINER_NAME}" pg_isready -U "${DB_USER}" >/dev/null 2>&1; then
    echo "PostgreSQL is ready."
    break
  fi
  if [[ "${i}" -eq 30 ]]; then
    echo "Error: PostgreSQL did not become ready within 30 seconds."
    exit 1
  fi
  sleep 1
done

echo ""
echo "PostgreSQL is running!"
echo "Container: ${CONTAINER_NAME}"
echo "Connection: postgresql://${DB_USER}:${DB_PASSWORD}@localhost:${DB_PORT}/${DB_NAME}"
echo ""
echo "To connect via psql:"
echo "  docker exec -it ${CONTAINER_NAME} psql -U ${DB_USER} -d ${DB_NAME}"
echo ""
echo "To view logs:"
echo "  docker logs -f ${CONTAINER_NAME}"
echo ""
echo "To stop:"
echo "  docker stop ${CONTAINER_NAME}"
echo ""