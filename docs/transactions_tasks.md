# Transactions and Tasks

## Purpose
This guide explains how tasks are created, dispatched, executed, and reconciled, and how server coordinates multi-participant transactions.

## Task Model
Task state and payload model are defined in [server/src/tasks/mod.rs](server/src/tasks/mod.rs).

Task fields include:
- id, query_id, session_id
- operation
- payload
- params
- state
- attempts and max_retries
- created_at, started_at, finished_at
- result_location and error

Task states:
- Pending
- Scheduled
- Running
- Succeeded
- Failed
- Cancelled

## How Tasks Are Created
Task creation starts in statement handlers through helper methods in [server/src/statement_handler/helpers.rs](server/src/statement_handler/helpers.rs).

High-level flow:
1. Resolve session and worker key for current session
2. Create task in TaskManager
3. Mark task Scheduled
4. Acquire worker pooled connection with heartbeat validation
5. Convert Task to Worker TaskRequest
6. Mark Running
7. Dispatch to worker ExecuteTask
8. Update task status from response and later async updates

Key entry points:
- [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs)
- [server/src/statement_handler/create_database.rs](server/src/statement_handler/create_database.rs)
- [server/src/statement_handler/create_schema.rs](server/src/statement_handler/create_schema.rs)
- [server/src/statement_handler/create_table.rs](server/src/statement_handler/create_table.rs)

## Operation Names and Payload Channels
Operations are set in task.operation. Common values:
- create_database
- create_schema
- create_table
- insert

Task payload channels:
- input/payload: operation-specific body
- params: operation-specific key/value metadata

Examples:
- create_database and create_table send typed JSON payloads
- insert sends SQL text payload and includes table_name param

## Worker Task Execution
Worker receives TaskRequest in [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs) and routes to [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs).

Routing behavior:
- create_database -> [worker/src/services/create_database.rs](worker/src/services/create_database.rs)
- create_schema -> [worker/src/services/create_schema.rs](worker/src/services/create_schema.rs)
- create_table -> [worker/src/services/create_table.rs](worker/src/services/create_table.rs)
- insert -> worker maestro insert path (Arrow batch + Delta write)

Execution results:
- immediate TaskResponse includes status/error/result_location
- asynchronous completion updates may also be sent through interops TaskUpdate

## Result and Status Reconciliation
Server receives async updates in [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs).

TaskUpdate mapping behavior:
- running -> Running
- succeeded/success/ok -> Succeeded
- failed/error -> Failed
- cancelled/canceled -> Cancelled

TaskManager update path:
- [server/src/tasks/mod.rs](server/src/tasks/mod.rs) update_from_worker
- wait_for_completion waits on notifier and returns terminal state or timeout

## Transaction Coordination (Server Maestro)
Server coordinator is in [server/src/transactions/maestro.rs](server/src/transactions/maestro.rs).

Transaction lifecycle:
1. begin_transaction
2. prepare_participants
3. commit
4. abort on failure

State progression:
- Preparing
- Prepared
- Committing
- Committed
- Aborted

Persistence behavior:
- metastore records are created/updated through metastore Execute RPC
- transaction state transitions are persisted during prepare/commit/abort phases

### Prepare Phase
For each participant:
1. resolve/create worker pool
2. acquire connection with heartbeat
3. send Prepare request with tx_id, staging_prefix, tasks
4. fail-fast on participant failure and trigger abort

### Commit Phase
For each participant:
1. send Commit request with tx_id and staging_prefix
2. on any failure, trigger abort path
3. persist Committed state after participant success

### Abort Phase
Best-effort cleanup:
- send Abort request to participants
- persist Aborted state in metastore

## Worker Transaction Endpoints
Worker implements transaction endpoints in [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs):
- prepare
- commit
- abort

Worker transaction operations are implemented in [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs):
- prepare_tx
- commit_tx
- abort_tx

Staging and promotion helpers are in [worker/src/storage/staging.rs](worker/src/storage/staging.rs).

## RPC Contracts
Contracts used in task and transaction flows:
- worker service: [kionas/proto/worker_service.proto](kionas/proto/worker_service.proto)
- interops service: [kionas/proto/interops_service.proto](kionas/proto/interops_service.proto)
- metastore service: [kionas/proto/metastore_service.proto](kionas/proto/metastore_service.proto)

## Known Runtime Characteristics
- insert may involve asynchronous completion update after initial response
- task tracking is runtime in-memory in server task manager
- transaction retry/backoff and timeout behavior is controlled in server maestro

## Cross-Reference
- architecture overview: [docs/server.md](docs/server.md), [docs/worker.md](docs/worker.md), [docs/metastore.md](docs/metastore.md)
- statement path overview: [docs/query_flow.md](docs/query_flow.md)
- deployment and config: [docs/configuration_deployment.md](docs/configuration_deployment.md)
