#!/usr/bin/env bash
set -euo pipefail

# init_db_container.sh
# Run this inside the metastore container (no Docker CLI usage).
# It connects to a Postgres server (default host: kionas-postgres)
# and ensures the metastore DB and schema (including transactions table)
# are created by running the provided init SQL script if necessary.
#
# Usage:
#   ./init_db_container.sh [--build]
# Options:
#   --build    : after DB init, run `cargo build -p metastore` to regenerate protobuf bindings
# Environment variables (optional):
#   PGHOST (default: kionas-postgres)
#   PGPORT (default: 5432)
#   METASTORE_DB_USER (default: kionas)
#   METASTORE_DB_PASS (optional)
#   METASTORE_DB_NAME (default: kionas_metastore)
#   INIT_SQL_PATH (default: ./scripts/metastore/init.sql)

DO_BUILD=false
if [[ "${1:-}" == "--build" ]]; then
  DO_BUILD=true
fi

PGHOST=${PGHOST:-kionas-postgres}
PGPORT=${PGPORT:-5432}
DB_USER=${METASTORE_DB_USER:-kionas}
DB_PASS=${METASTORE_DB_PASS:-}
DB_NAME=${METASTORE_DB_NAME:-kionas_metastore}
SCRIPT_PATH=${INIT_SQL_PATH:-./scripts/metastore/init.sql}

echo "Metastore init (container mode): pghost=$PGHOST pgport=$PGPORT db_user=$DB_USER db_name=$DB_NAME build=$DO_BUILD"

if [[ ! -f "$SCRIPT_PATH" ]]; then
  echo "Error: init SQL script not found at $SCRIPT_PATH" >&2
  echo "Set INIT_SQL_PATH to the correct location inside the container." >&2
  exit 2
fi

if ! command -v psql >/dev/null 2>&1; then
  echo "psql client not found in container."
  if command -v apt-get >/dev/null 2>&1; then
    echo "Attempting to install postgresql-client via apt-get..."
    apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-client || true
  else
    echo "Please install 'psql' (postgres client) in the container, or run this script from a host with psql." >&2
    exit 3
  fi
fi

run_psql() {
  local db="$1"; shift
  if [[ -n "$DB_PASS" ]]; then
    PGPASSWORD="$DB_PASS" psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d "$db" -tAc "$@"
  else
    psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d "$db" -tAc "$@"
  fi
}

echo "Checking if database '$DB_NAME' exists..."
DB_EXISTS=$(run_psql postgres "SELECT 1 FROM pg_database WHERE datname = '$DB_NAME';" || echo "")
DB_EXISTS=$(echo "$DB_EXISTS" | tr -d '[:space:]')
if [[ "$DB_EXISTS" == "1" ]]; then
  echo "Database $DB_NAME exists. Checking for transactions table..."
  TX_EXISTS=$(run_psql "$DB_NAME" "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='transactions');" || echo "f")
  TX_EXISTS=$(echo "$TX_EXISTS" | tr -d '[:space:]')
  if [[ "$TX_EXISTS" == "t" || "$TX_EXISTS" == "true" ]]; then
    echo "Metastore already initialized (transactions table present)."
  else
    echo "Transactions table not found â€” applying init SQL..."
    if [[ -n "$DB_PASS" ]]; then
      PGPASSWORD="$DB_PASS" psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d postgres -f "$SCRIPT_PATH"
    else
      psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d postgres -f "$SCRIPT_PATH"
    fi
    echo "Initialization script applied."
  fi
else
  echo "Database $DB_NAME does not exist â€” applying init SQL to create and initialize the metastore..."
  if [[ -n "$DB_PASS" ]]; then
    PGPASSWORD="$DB_PASS" psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d postgres -f "$SCRIPT_PATH"
  else
    psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d postgres -f "$SCRIPT_PATH"
  fi
  echo "Initialization script applied."
fi

if $DO_BUILD; then
  echo "Attempting to build metastore crate to regenerate protobuf bindings..."
  if command -v cargo >/dev/null 2>&1; then
    cargo build -p metastore
  else
    echo "cargo not found inside container; please install Rust toolchain or run 'cargo build -p metastore' from your build environment." >&2
  fi
fi

# Check for RBAC tables. If missing execute scripts/metastore/rbac.sql
RBAC_EXISTS=$(run_psql "$DB_NAME" "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='public' AND table_name='users_rbac');" || echo "f")
RBAC_EXISTS=$(echo "$RBAC_EXISTS" | tr -d '[:space:]')
if [[ "$RBAC_EXISTS" != "t" && "$RBAC_EXISTS" != "true" ]]; then
  echo "RBAC tables not found â€” applying RBAC SQL..."
  RBAC_SCRIPT_PATH=${RBAC_SQL_PATH:-./scripts/metastore/rbac.sql}
  if [[ -n "$DB_PASS" ]]; then
    PGPASSWORD="$DB_PASS" psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d "$DB_NAME" -f "$RBAC_SCRIPT_PATH"
  else
    psql -h "$PGHOST" -p "$PGPORT" -U "$DB_USER" -d "$DB_NAME" -f "$RBAC_SCRIPT_PATH"
  fi
  echo "RBAC SQL applied."
fi

echo "Done."
