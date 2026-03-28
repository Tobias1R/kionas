#!/bin/bash
sleep 20 # wait for the warehouse to be ready


./scripts/configure_worker.sh ./configs/kionas-worker1.json

CMD="./docker-target/debug/worker"
if [ "$1" == "b" ]; then
    CMD="cargo run -p worker"
fi
echo "Running worker with command: $CMD"
exec $CMD