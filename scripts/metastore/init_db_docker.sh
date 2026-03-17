#!/usr/bin/env bash
set -euo pipefail

# init_db.sh
# Initialize the metastore Postgres DB inside the metastore container.
# Usage:
#   ./init_db.sh [container-name] [--build]
# Defaults: container-name=kionas-postgres
# Environment:
#   METASTORE_DB_USER (default: kionas)
#   METASTORE_DB_PASS (default: kionas)
#   METASTORE_DB_NAME (default: kionas_metastore)

CONTAINER=${1:-kionas-postgres}
DO_BUILD=false
if [[ "${2:-}" == "--build" ]]; then
  DO_BUILD=true
fi

DB_USER=${METASTORE_DB_USER:-kionas}
DB_PASS=${METASTORE_DB_PASS:-kionas}
DB_NAME=${METASTORE_DB_NAME:-kionas_metastore}

SCRIPT_HOST_PATH="$(dirname "$0")/init.sql"
SCRIPT_CONTAINER_PATH="/tmp/init_kionas_metastore.sql"

echo "Metastore init: container=$CONTAINER db_user=$DB_USER db_name=$DB_NAME build=${DO_BUILD}"

if ! docker ps --format '{{.Names}}' | grep -qw "$CONTAINER"; then
  echo "Error: container '$CONTAINER' not running. Start the metastore container first." >&2
  exit 2
fi

echo "Copying init SQL to container..."
docker cp "$SCRIPT_HOST_PATH" "$CONTAINER":"$SCRIPT_CONTAINER_PATH"

# helper to run psql inside container
run_psql() {
  local db=$1; shift
  if [[ -n "$DB_PASS" ]]; then
    docker exec -i -e PGPASSWORD="$DB_PASS" "$CONTAINER" psql -U "$DB_USER" -d "$db" -tAc "$@"
  else
    docker exec -i "$CONTAINER" psql -U "$DB_USER" -d "$db" -tAc "$@"
  fi
}

echo "Checking for psql inside container..."
if ! docker exec "$CONTAINER" sh -c "command -v psql >/dev/null 2>&1"; then
  echo "Warning: 'psql' not found inside container. Attempting to install postgresql-client (apt-get)."
  if docker exec "$CONTAINER" sh -c "command -v apt-get >/dev/null 2>&1"; then
    docker exec -i "$CONTAINER" sh -c "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-client || true"
  else
    echo "Cannot install psql inside container automatically. Please install 'psql' or run this script from a host with psql access." >&2
  fi
fi

echo "Checking if database '$DB_NAME' exists..."
DB_EXISTS=$(run_psql postgres "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME';" | tr -d '[:space:]' || echo "")
if [[ "$DB_EXISTS" == "1" ]]; then
  echo "Database $DB_NAME exists. Checking for transactions table..."
  TX_EXISTS=$(run_psql "$DB_NAME" "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='transactions');" | tr -d '[:space:]' || echo "f")
  if [[ "$TX_EXISTS" == "t" || "$TX_EXISTS" == "true" ]]; then
    echo "Metastore already initialized (transactions table present)."
  else
    echo "Transactions table not found — running init SQL to migrate DB..."
    if [[ -n "$DB_PASS" ]]; then
      docker exec -i -e PGPASSWORD="$DB_PASS" "$CONTAINER" psql -U "$DB_USER" -d postgres -f "$SCRIPT_CONTAINER_PATH"
    else
      docker exec -i "$CONTAINER" psql -U "$DB_USER" -d postgres -f "$SCRIPT_CONTAINER_PATH"
    fi
    echo "Initialization script applied."
  fi
else
  echo "Database $DB_NAME does not exist — running init SQL to create and initialize the metastore..."
  if [[ -n "$DB_PASS" ]]; then
    docker exec -i -e PGPASSWORD="$DB_PASS" "$CONTAINER" psql -U "$DB_USER" -d postgres -f "$SCRIPT_CONTAINER_PATH"
  else
    docker exec -i "$CONTAINER" psql -U "$DB_USER" -d postgres -f "$SCRIPT_CONTAINER_PATH"
  fi
  echo "Initialization script applied."
fi

if $DO_BUILD; then
  echo "Regenerating protobuf bindings by building the metastore crate..."
  if command -v cargo >/dev/null 2>&1; then
    cargo build -p metastore
  else
    echo "cargo not found on PATH; cannot build. Install Rust toolchain or run 'cargo build -p metastore' manually." >&2
  fi
fi

echo "Done."
