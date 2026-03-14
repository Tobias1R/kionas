#!/bin/bash
# Cluster initialization script to set up certificates and run the worker and client
# Step 1: Generate cluster configs
python3 /workspace/scripts/generate_cluster_config.py
# Step 2: Generate worker configs
python3 /workspace/scripts/generate_worker_configs.py
# Step 3: Populate Consul with cluster and worker configs
/workspace/scripts/populate_consul.sh