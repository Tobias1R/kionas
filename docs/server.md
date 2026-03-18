# Server Binary

## Overview
The server binary is the control-plane coordinator for SQL entry, task orchestration, session routing, and distributed transaction coordination.

Primary responsibilities:
- accept client SQL and auth requests
- route statements to dedicated handlers
- create and track worker tasks
- coordinate transaction prepare/commit/abort across participants
- persist metadata updates through metastore gRPC

## Entry Point and Startup
- Entrypoint: [server/src/main.rs](server/src/main.rs)
- Runtime bootstrap: [server/src/server.rs](server/src/server.rs)

Startup sequence:
1. load AppConfig with Consul-first resolution via kionas config loader
2. initialize shared state (sessions, warehouses, task manager, worker pools)
3. start warehouse and interops gRPC services
4. handle worker registration/heartbeat/task updates over interops

## Core Runtime Areas

### SQL Ingress
- Service: [server/src/services/warehouse_service_server.rs](server/src/services/warehouse_service_server.rs)
- Parses statements and forwards each statement to statement handlers

### Statement Routing
- Router: [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs)
- Dedicated handlers:
	- create database: [server/src/statement_handler/create_database.rs](server/src/statement_handler/create_database.rs)
	- create schema: [server/src/statement_handler/create_schema.rs](server/src/statement_handler/create_schema.rs)
	- create table: [server/src/statement_handler/create_table.rs](server/src/statement_handler/create_table.rs)

### Task Creation and Dispatch
- Helpers: [server/src/statement_handler/helpers.rs](server/src/statement_handler/helpers.rs)
- Task model/state: [server/src/tasks/mod.rs](server/src/tasks/mod.rs)
- Worker RPC client operations: [server/src/workers/mod.rs](server/src/workers/mod.rs)

Task lifecycle implemented by server components:
1. create task in TaskManager (Pending)
2. set Scheduled
3. acquire worker pooled connection with heartbeat check
4. send TaskRequest over WorkerService ExecuteTask
5. set Running/Succeeded/Failed based on response and asynchronous updates

### Asynchronous Task Status Reconciliation
- Interops receiver: [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs)

Workers send task updates with status/result_location/error. Server maps these into TaskState and updates TaskManager, notifying waiters.

### Transaction Coordination
- Coordinator: [server/src/transactions/maestro.rs](server/src/transactions/maestro.rs)

Server coordinates participants through:
- begin transaction (persist PREPARING)
- prepare with retry/backoff (persist PREPARED)
- commit (persist COMMITTING then COMMITTED)
- abort path on failures

## Service Boundaries
- server -> worker: WorkerService RPC (execute_task, prepare, commit, abort)
- server -> metastore: MetastoreService Execute RPC
- worker -> server: InteropsService (register_worker, heartbeat, task_update)

## Important Notes
- task tracking is in-memory in TaskManager for active runtime coordination
- terminal task states set finished_at timestamps
- insert execution may return quickly while worker sends final status updates asynchronously

## Related Docs
- query flow: [docs/query_flow.md](docs/query_flow.md)
- transactions and tasks: [docs/transactions_tasks.md](docs/transactions_tasks.md)
- deployment and config: [docs/configuration_deployment.md](docs/configuration_deployment.md)
