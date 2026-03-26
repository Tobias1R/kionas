#!/bin/bash
CMD="cargo run -p client -- --username=kionas --password=kionas"

if [ "$1" == "b" ]; then
    CMD="./target/debug/client --username=kionas --password=kionas"
fi
echo "Running client with command: $CMD"
exec $CMD