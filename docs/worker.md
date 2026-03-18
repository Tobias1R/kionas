# Worker Binary

## Overview
The worker binary is the execution-plane component. It receives task requests from server, performs storage and data operations, and reports completion status back to server.

Primary responsibilities:
- register with server interops service
- execute operation tasks (create_database, create_schema, create_table, insert)
- run transaction prepare/commit/abort operations for staged writes
- interact with MinIO or local fallback storage

## Entry Point and Startup
- Entrypoint: [worker/src/main.rs](worker/src/main.rs)
- Cluster/bootstrap init: [worker/src/init.rs](worker/src/init.rs)

Startup sequence:
1. load AppConfig for current worker identity
2. initialize cluster info from Consul and register worker with server
3. build worker shared state
4. attach storage provider from cluster storage config
5. start WorkerService gRPC server

## Task Execution Pipeline
- gRPC service: [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs)
- execution router: [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs)

Execution path:
1. ExecuteTask receives TaskRequest
2. maestro determines operation from first task
3. delegates to operation-specific handler
4. returns TaskResponse and later sends async task_update with final status when applicable

### Operation Handlers
- create database: [worker/src/services/create_database.rs](worker/src/services/create_database.rs)
- create schema: [worker/src/services/create_schema.rs](worker/src/services/create_schema.rs)
- create table: [worker/src/services/create_table.rs](worker/src/services/create_table.rs)
- insert path and delta write orchestration: [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs)

### Storage and Delta
- MinIO/S3 provider: [worker/src/storage/minio.rs](worker/src/storage/minio.rs)
- Delta table operations: [worker/src/storage/deltalake.rs](worker/src/storage/deltalake.rs)
- staged transaction objects: [worker/src/storage/staging.rs](worker/src/storage/staging.rs)

Key behavior:
- create operations use marker files for idempotency checks
- create table initializes canonical table path under databases/schema/table hierarchy
- insert writes Arrow batches to Delta and reports result_location

## Transaction Hooks
Worker side transaction handlers:
- prepare: stage objects under staging_prefix
- commit: promote staged objects
- abort: remove staged objects

Implemented in [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs) and exposed by WorkerService methods in [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs).

## Configuration Dependencies
- worker runtime uses services.interops from AppConfig
- storage provider uses cluster storage config (bucket, endpoint, region, credentials)
- key environment variables may override AWS client behavior in containerized environments

## Related Docs
- query flow: [docs/query_flow.md](docs/query_flow.md)
- transactions and tasks: [docs/transactions_tasks.md](docs/transactions_tasks.md)
- deployment and config: [docs/configuration_deployment.md](docs/configuration_deployment.md)
