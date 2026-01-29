#!/usr/bin/env bash
# Clean the local PostgreSQL database used by this repo.
#
# This drops and recreates the `public` schema (removes ALL tables/data),
# which is the simplest way to reset state including `merkle_nodes`.
#
# Defaults match scripts/start_db.sh and README:
#   DB_NAME=verifiable_memory
#   DB_USER=postgres
#   DB_PASSWORD=password
#   DB_PORT=5432
#   CONTAINER_NAME=pg-verifiable-memory
#
# Usage:
#   ./scripts/clean_db.sh
#   DB_NAME=verifiable_memory ./scripts/clean_db.sh
#
# WARNING: This is destructive for the selected database/schema.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CONTAINER_NAME="${CONTAINER_NAME:-pg-verifiable-memory}"
DB_NAME="${DB_NAME:-verifiable_memory}"
DB_USER="${DB_USER:-postgres}"
DB_PASSWORD="${DB_PASSWORD:-password}"
DB_PORT="${DB_PORT:-5432}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed."
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  echo "Error: Docker is not running. Please start Docker first."
  exit 1
fi

pick_container() {
  # Prefer the configured container name if it exists/runs.
  if docker ps --format "{{.Names}}" | grep -qx "${CONTAINER_NAME}"; then
    echo "${CONTAINER_NAME}"
    return 0
  fi

  # Otherwise, try to find a running container publishing DB_PORT.
  local by_port
  by_port="$(docker ps --format "{{.Names}}" --filter "publish=${DB_PORT}" | head -n 1 || true)"
  if [[ -n "${by_port}" ]]; then
    echo "${by_port}"
    return 0
  fi

  return 1
}

TARGET_CONTAINER="$(pick_container || true)"
if [[ -z "${TARGET_CONTAINER}" ]]; then
  echo "Error: could not find a running postgres container."
  echo "Hint: start it with: ./scripts/start_db.sh"
  exit 1
fi

echo "Cleaning database '${DB_NAME}' (schema: public) in container '${TARGET_CONTAINER}'..."

# Note: We don't need DB_PASSWORD here because docker exec runs inside the container.
docker exec "${TARGET_CONTAINER}" psql -U "${DB_USER}" -d "${DB_NAME}" -v ON_ERROR_STOP=1 -c \
  "DROP SCHEMA IF EXISTS public CASCADE;
   CREATE SCHEMA public;
   GRANT ALL ON SCHEMA public TO ${DB_USER};
   GRANT ALL ON SCHEMA public TO public;"

echo ""
echo "✓ Database cleaned."
echo "Connection: postgresql://${DB_USER}:${DB_PASSWORD}@localhost:${DB_PORT}/${DB_NAME}"

