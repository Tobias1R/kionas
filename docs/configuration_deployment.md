# Configuration and Deployment

## Purpose
This guide explains configuration resolution, required infrastructure, and practical deployment flows for local development and direct binary runs.

## Configuration Model
Shared typed config is defined in [kionas/src/config.rs](kionas/src/config.rs).

Main structures:
- AppConfig
- ServicesConfig
- InteropsConfig
- WarehouseConfig
- PostgresServiceConfig
- ClusterConfig
- StorageConfig

## Configuration Resolution Order
AppConfig loading behavior is implemented in [kionas/src/config.rs](kionas/src/config.rs).

Resolution order for load_for_host:
1. Consul key kionas/configs/<hostname>
2. local fallback path provided by CLI arg --config (default configs/server.toml)
3. error if neither source is valid

Cluster configuration loading:
1. Consul key kionas/cluster
2. local fallback /workspace/configs/cluster.json

## Repository Configuration Files
Primary files:
- [configs/cluster.json](configs/cluster.json)
- [configs/kionas-warehouse.json](configs/kionas-warehouse.json)
- [configs/kionas-worker1.json](configs/kionas-worker1.json)
- [configs/kionas-metastore.json](configs/kionas-metastore.json)

## Runtime Dependencies
Core infrastructure required for local stack:
- PostgreSQL
- Redis
- MinIO or compatible S3 endpoint
- Kafka broker
- Consul
- TLS certificates and keys for service endpoints

Docker compose baseline:
- [docker/docker-compose.yaml](docker/docker-compose.yaml)

Metastore SQL initialization:
- [scripts/metastore/init.sql](scripts/metastore/init.sql)

## Environment Variables
Frequently used values:
- CONSUL_URL
- KIONAS_HOME
- RUST_LOG
- REDIS_URL

Worker storage-related values often used in containerized runs:
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_REGION
- AWS_ENDPOINT_URL

## Local Docker Deployment Flow
This path uses compose services plus generated Consul config.

### 1) Start infrastructure containers
Use [docker/docker-compose.yaml](docker/docker-compose.yaml) to start:
- broker
- redis
- minio
- consul
- postgres
- metastore
- warehouse
- worker1
- devcontainer

### 2) Generate and publish cluster config
Run:
- [scripts/init.sh](scripts/init.sh)

This script executes:
- scripts/generate_cluster_config.py
- scripts/generate_worker_configs.py
- [scripts/populate_consul.sh](scripts/populate_consul.sh)

### 3) Verify metastore schema
Initialize postgres schema using:
- [scripts/metastore/init.sql](scripts/metastore/init.sql)

### 4) Start binaries in containers or via cargo
Container scripts:
- [scripts/run_warehouse_container.bat](scripts/run_warehouse_container.bat)
- [scripts/run_worker1_container.bat](scripts/run_worker1_container.bat)

In-container cargo examples:
- cargo run -p metastore
- cargo run -p server
- cargo run -p worker -- worker1

Client helper:
- [scripts/run_client_query.sh](scripts/run_client_query.sh)

## Direct Binary Deployment Flow
This path runs binaries directly from local build output.

### 1) Build workspace
- cargo build

### 2) Ensure dependencies are reachable
- postgres host/port from metastore config
- redis endpoint
- minio endpoint/credentials
- consul endpoint
- certificates at configured paths

### 3) Start services in order
Recommended order:
1. metastore
2. server
3. worker(s)
4. client

### 4) Example run commands
- cargo run -p metastore
- cargo run -p server
- cargo run -p worker -- worker1
- cargo run -p client -- --username kionas --password kionas --server-url kionas-warehouse:443

## Binary Configuration Mapping
Server consumes:
- mode=server
- services.warehouse
- services.interops
- services.security

Worker consumes:
- mode=worker
- services.interops
- cluster storage settings from cluster.json

Metastore consumes:
- mode=metastore
- services.interops
- services.postgres

## Deployment Troubleshooting Checklist
- Consul keys exist for kionas/cluster and kionas/configs/<hostname>
- certificate paths are valid inside runtime filesystem
- worker registration appears in server interops logs
- metastore postgres connectivity succeeds at startup
- storage bucket and credentials permit read/write operations

## Related Docs
- binary docs: [docs/server.md](docs/server.md), [docs/worker.md](docs/worker.md), [docs/metastore.md](docs/metastore.md)
- task and transaction deep dive: [docs/transactions_tasks.md](docs/transactions_tasks.md)
- statement flow: [docs/query_flow.md](docs/query_flow.md)
