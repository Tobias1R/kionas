#!/bin/bash
# Script to run the Kionas client with required parameters

CLIENT_BIN="cargo run -p client --"
USERNAME="kionas"
PASSWORD="kionas"
SERVER_ADDR="kionas-warehouse:443"

$CLIENT_BIN --username $USERNAME --password $PASSWORD --server-url $SERVER_ADDR
