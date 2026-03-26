#!/bin/bash

./scripts/configure_worker.sh ./configs/kionas-worker1.json

CMD="./docker-target/debug/worker worker1"
if [ "$1" == "b" ]; then
    CMD="cargo run -p worker -- worker1"
fi
echo "Running worker with command: $CMD"
exec $CMD