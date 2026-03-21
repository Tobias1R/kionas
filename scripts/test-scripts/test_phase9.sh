#!/bin/bash
# Phase 9 Predicate Test Suite - Docker-optimized test runner
# Runs SQL test queries and reports results

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="$SCRIPT_DIR/phase9_simple_test.sql"
USERNAME="${KIONAS_USERNAME:-kionas}"
PASSWORD="${KIONAS_PASSWORD:-kionas}"

echo "=========================================================================="
echo "PHASE 9 PREDICATE TEST SUITE - NOT BETWEEN & Type Coercion Validation"
echo "=========================================================================="
echo ""
echo "Test File: $SQL_FILE"
echo "Timestamp: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Check if SQL file exists
if [ ! -f "$SQL_FILE" ]; then
    echo "ERROR: SQL test file not found: $SQL_FILE"
    exit 1
fi

echo "Connecting to kionas-warehouse and executing tests..."
echo ""
echo "=========================================================================="
echo ""

# Run through client
if command -v cargo &> /dev/null; then
    cargo run -p client -- --username "$USERNAME" --password "$PASSWORD" < "$SQL_FILE" 2>&1
elif [ -f "./target/debug/client" ]; then
    ./target/debug/client --username "$USERNAME" --password "$PASSWORD" < "$SQL_FILE" 2>&1
else
    echo "ERROR: Cannot find cargo or compiled client binary"
    exit 1
fi

echo ""
echo "=========================================================================="
echo "TEST COMPLETE"
echo "=========================================================================="
echo ""
echo "Key test queries to validate NOT BETWEEN implementation:"
echo ""
echo "1. Statement 23 (NOT BETWEEN):"
echo "   SELECT id, name, price FROM phase9_simple.test.data WHERE price NOT BETWEEN 100 AND 300;"
echo "   Expected: 3 rows (id=1, id=2, id=3 - prices outside [100,300] range)"
echo ""
echo "2. Statement 24 (Complex - BETWEEN AND IN):"
echo "   Should return 5 rows combining BETWEEN and IN predicates"
echo ""

exit 0
