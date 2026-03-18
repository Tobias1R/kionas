# Metastore Binary

## Overview
The metastore binary is the metadata persistence service. It exposes gRPC methods used by server to create and query database/schema/table metadata and transaction state.

Primary responsibilities:
- provide Execute RPC action routing for metadata operations
- persist metadata in PostgreSQL
- persist transaction state transitions used by server coordinator

## Entry Point and Startup
- Entrypoint: [metastore/src/main.rs](metastore/src/main.rs)

Startup sequence:
1. load AppConfig with Consul-first resolution
2. extract interops (TLS/listen) and postgres service config
3. initialize PostgresProvider connection pool
4. start TLS-enabled MetastoreService gRPC server

## Service and Provider Layers
- gRPC service router: [metastore/src/services/metastore_service.rs](metastore/src/services/metastore_service.rs)
- action handlers: [metastore/src/services/actions](metastore/src/services/actions)
- persistence provider: [metastore/src/services/provider/postgres.rs](metastore/src/services/provider/postgres.rs)

Provider behavior includes:
- create/get database metadata
- create/get schema metadata
- create/get table metadata with parent-aware context
- create/update/get transaction records

## PostgreSQL Schema Initialization
- base schema script: [scripts/metastore/init.sql](scripts/metastore/init.sql)
- optional auth script: [scripts/metastore/auth.sql](scripts/metastore/auth.sql)

Expected dependency:
- postgres service reachable using services.postgres fields from AppConfig

## Service Boundary
- inbound from server only over gRPC (MetastoreService Execute)
- no direct worker->metastore call path in normal orchestration

## Related Docs
- query flow: [docs/query_flow.md](docs/query_flow.md)
- transactions and tasks: [docs/transactions_tasks.md](docs/transactions_tasks.md)
- deployment and config: [docs/configuration_deployment.md](docs/configuration_deployment.md)
