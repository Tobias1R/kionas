#!/bin/bash
CMD="cargo run -p client -- --server-url=https://warehouse.warehouse_network:443 --username=kionas --password=kionas"

if [ "$1" == "b" ]; then
    CMD="./target/debug/client -- --server-url=https://warehouse.warehouse_network:443 --username=kionas --password=kionas"
fi
echo "Running client with command: $CMD"
exec $CMD