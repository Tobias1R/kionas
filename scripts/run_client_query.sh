#!/bin/bash
# Script to run the Kionas client with required parameters

CLIENT_BIN="cargo run -p client --"
USERNAME="kionas"
PASSWORD="kionas"
SERVER_ADDR="kionas-warehouse:443"

if [ "$1" == "b" ]; then
    CLIENT_BIN="./target/debug/client"
fi

$CLIENT_BIN --username $USERNAME --password $PASSWORD
