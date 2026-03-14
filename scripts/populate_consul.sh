#!/bin/bash
# Script to populate Consul with cluster config before server startup

CONSUL_URL="http://kionas-consul:8500"
CLUSTER_KEY="kionas/cluster"
CONFIG_FILE="/workspace/configs/cluster.json"

if [ ! -f "$CONFIG_FILE" ]; then
  echo "Cluster config file not found: $CONFIG_FILE"
  exit 1
fi

CONFIG_JSON=$(cat "$CONFIG_FILE")

curl -X PUT --data "$CONFIG_JSON" "$CONSUL_URL/v1/kv/$CLUSTER_KEY"

if [ $? -eq 0 ]; then
  echo "Consul populated with cluster config."
else
  echo "Failed to populate Consul."
  exit 1
fi

CONFIGS_HOME="/workspace/configs/"
CONFIGS_KEY="kionas/configs"
# Upload all worker*.json files to their respective key in consul.
# eg. worker1.json -> kionas/configs/worker1
for config_file in "$CONFIGS_HOME"/worker*.json; do
  if [ -f "$config_file" ]; then
    config_name=$(basename "$config_file" .json)
    config_json=$(cat "$config_file")
    curl -X PUT --data "$config_json" "$CONSUL_URL/v1/kv/$CONFIGS_KEY/$config_name"
    if [ $? -eq 0 ]; then
      echo "Consul populated with config: $config_name"
    else
      echo "Failed to populate Consul with config: $config_name"
      exit 1
    fi
  else
    echo "No config files found in $CONFIGS_HOME"
    exit 1
  fi
done
