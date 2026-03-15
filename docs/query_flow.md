Query → Handler → Worker Flow

This document describes the end-to-end flow when a SQL query arrives at the server and is handled, dispatched to a worker, and persisted to the metastore.

1) Incoming request
- Entry point: [server/src/services/warehouse_service_server.rs](server/src/services/warehouse_service_server.rs)
- `query()` builds a `RequestContext` and performs quick direct-command handling via `maybe_handle_direct_command`.

2) Statement dispatch
- Parsing: SQL parsed with `parse_query`.
- Central handler: [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs) — `handle_statement()` decides how to handle each AST `Statement` (e.g., `CreateSchema`, `CreateTable`).

3) Task creation & dispatch
- Helpers: [server/src/statement_handler/helpers.rs](server/src/statement_handler/helpers.rs) exposes `run_task_for_input()` which:
  - Resolves session and worker key (`resolve_session_and_key`).
  - Creates a `Task` in `TaskManager` ([server/src/tasks/mod.rs](server/src/tasks/mod.rs)).
  - Acquires a pooled worker connection via `acquire_pooled_conn()` and `workers_pool` helpers.
  - Converts `Task` → `TaskRequest` and dispatches with `dispatch_task_and_record()` which calls [server/src/workers/mod.rs](server/src/workers/mod.rs)::`send_task_to_worker()`.

4) Worker execution
- Worker service: [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs)::`execute_task` performs the work (simulated or real) and reports back a `TaskUpdate` to the master via the interops gRPC endpoint.

5) TaskUpdate handling on master
- Interops service: [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs)::`task_update` receives worker status updates and updates the `Task` in `TaskManager` (state, result_location, error).
- `TaskManager::wait_for_completion()` (used by the statement helpers) polls the task entry until it reaches a terminal state (Succeeded/Failed/Cancelled) or times out.

6) Metastore persistence
- After the task completes (the statement handler observes success), statement handler invokes the metastore client to persist schema/table metadata.
- Metastore client: [server/src/services/metastore_client.rs](server/src/services/metastore_client.rs)
  - Uses a cached shared `tonic::transport::Channel` stored in `SharedState.metastore_channel` ([server/src/warehouse/state.rs](server/src/warehouse/state.rs)) to avoid reconnecting on every request.
  - Calls `MetastoreService::Execute` to apply catalog changes.

Notes and places to improve
- `TaskManager` is currently polled with `wait_for_completion()`; consider replacing polling with an async notification (watch/notify) to avoid busy-wait.
- Error handling and retries: metastore and worker RPCs could benefit from retry/backoff and invalidating the cached metastore channel on persistent RPC failures.
- Timestamps: `interops_service::task_update` should record `finished_at` on terminal states so `Task` timestamps are accurate.

Files of interest
- [server/src/services/warehouse_service_server.rs](server/src/services/warehouse_service_server.rs)
- [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs)
- [server/src/statement_handler/helpers.rs](server/src/statement_handler/helpers.rs)
- [server/src/tasks/mod.rs](server/src/tasks/mod.rs)
- [server/src/workers/mod.rs](server/src/workers/mod.rs)
- [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs)
- [server/src/services/metastore_client.rs](server/src/services/metastore_client.rs)
- [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs)

Created: docs/query_flow.md
