#!/bin/bash
# Script to populate Consul with cluster config before server startup

CONSUL_URL="${CONSUL_URL:-http://kionas-consul:8500}"
CLUSTER_KEY="kionas/cluster"
CONFIGS_DIR="/workspace/configs"
CONFIGS_KEY_PREFIX="kionas/configs"

if [ ! -d "$CONFIGS_DIR" ]; then
  echo "Configs directory not found: $CONFIGS_DIR"
  exit 1
fi

# Upload cluster config if present (cluster.json or cluster.toml)
if [ -f "$CONFIGS_DIR/cluster.json" ]; then
  curl -s -X PUT --data-binary @"$CONFIGS_DIR/cluster.json" "$CONSUL_URL/v1/kv/$CLUSTER_KEY"
  if [ $? -eq 0 ]; then
    echo "Uploaded cluster.json -> $CLUSTER_KEY"
  else
    echo "Failed to upload cluster.json"
    exit 1
  fi
elif [ -f "$CONFIGS_DIR/cluster.toml" ]; then
  curl -s -X PUT --data-binary @"$CONFIGS_DIR/cluster.toml" "$CONSUL_URL/v1/kv/$CLUSTER_KEY"
  if [ $? -eq 0 ]; then
    echo "Uploaded cluster.toml -> $CLUSTER_KEY"
  else
    echo "Failed to upload cluster.toml"
    exit 1
  fi
else
  echo "No cluster config found in $CONFIGS_DIR (skipping)"
fi

# Upload all other config files to kionas/configs/<basename> (skip cluster.*)
shopt -s nullglob
for file in "$CONFIGS_DIR"/*.{json,toml}; do
  base=$(basename "$file")
  if [ "$base" = "cluster.json" ] || [ "$base" = "cluster.toml" ]; then
    continue
  fi
  name="${base%.*}"
  curl -s -X PUT --data-binary @"$file" "$CONSUL_URL/v1/kv/$CONFIGS_KEY_PREFIX/$name"
  if [ $? -eq 0 ]; then
    echo "Uploaded $base -> $CONFIGS_KEY_PREFIX/$name"
  else
    echo "Failed to upload $base -> $CONFIGS_KEY_PREFIX/$name"
    exit 1
  fi
done

shopt -u nullglob
