#!/bin/bash
# Script to run the Kionas client with required parameters

CLIENT_BIN="docker-target/debug/client"
USERNAME="kionas"
PASSWORD="kionas"
SERVER_ADDR="kionas-warehouse:443"

LOG_EXECUTION_FILE="e2e.log"
START_TIME=$(date +%s)

$CLIENT_BIN --username $USERNAME --password $PASSWORD --query-file /workspace/scripts/test-scripts/testv2.sql

END_TIME=$(date +%s)
ELAPSED_TIME=$((END_TIME - START_TIME))
echo "[$(date +'%Y-%m-%d %H:%M:%S')] E2E test completed in $ELAPSED_TIME seconds" | tee -a $LOG_EXECUTION_FILE
