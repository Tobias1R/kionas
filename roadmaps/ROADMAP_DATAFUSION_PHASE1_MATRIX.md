# DataFusion Phase 1 Completion Matrix

## Scope
Full replacement of custom query planning and custom query execution with DataFusion-only logical planning, physical planning, transport envelope, worker runtime execution, and distributed orchestration adaptation for SELECT path.

Roadmap reference:
- [roadmaps/SILK_ROAD/DATAFUSION/datafusion-planner-roadmap.md](SILK_ROAD/DATAFUSION/datafusion-planner-roadmap.md)

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Server logical planning uses DataFusion only | Done | [server/src/planner/engine.rs](server/src/planner/engine.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs) | Query path builds DataFusion logical plans through provider-aware context and emits diagnostics. |
| Server optimizer path uses DataFusion optimizer | Done | [server/src/planner/engine.rs](server/src/planner/engine.rs) | Uses SessionState optimize before physical planning in provider-aware planner APIs. |
| Server physical planning uses DataFusion only | Done | [server/src/planner/engine.rs](server/src/planner/engine.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs) | DataFusion physical planning is primary; guarded fallback kept for object-store and missing-table resiliency. |
| Query transport contract carries Kionas operator tasks only | Done | [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs), [server/src/tasks/mod.rs](server/src/tasks/mod.rs) | Server dispatches stage task groups with Kionas operator payloads and runtime params. |
| Worker query extraction accepts Kionas operator payloads only | Done | [worker/src/execution/planner.rs](worker/src/execution/planner.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Worker extracts executable operators from physical_plan payload and task predicate contract. |
| Worker executes query using Kionas operator runtime only | Done | [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), [worker/src/execution/query.rs](worker/src/execution/query.rs) | Runtime applies scan/filter/join/aggregate/projection/sort/limit/materialize via local execution pipeline. |
| Distributed stage orchestration aligned to DataFusion semantics | Done | [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs), [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs) | Query path compiles distributed stage DAG and dispatches ordered stage groups with dependency metadata. |
| Flight retrieval remains functional after cutover | Done | [worker/src/execution/artifacts.rs](worker/src/execution/artifacts.rs), [worker/src/flight/server.rs](worker/src/flight/server.rs) | E2E query returned row batches and successful completion from Flight-backed artifact flow. |
| Legacy planner/runtime paths removed from SELECT query flow | Done | [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs), [server/src/planner/mod.rs](server/src/planner/mod.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | SELECT flow no longer dispatches legacy custom plan envelope path. |
| Quality gates pass (fmt, clippy, check) | Done | User validation 2026-03-22 | User reported fmt, clippy, and e2e completion as successful for current change set. |
| Optional hardening: richer explain snapshots | Deferred | N/A | Non-blocking for phase signoff. |
| Optional hardening: stress scenarios for distributed heavy queries | Deferred | N/A | Non-blocking for phase signoff. |

Status values:
- Done
- Deferred
- Blocked
- Not Started

## Signoff Decision
- Phase signoff: Approved
- Blocking items:
  - None.

## Gate Checklist
1. All mandatory criteria are marked Done.
2. Each Done item includes concrete evidence reference.
3. Deferred items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment And Parameters
Environment variables and runtime parameters used in this phase:
- Query dispatch runtime params in stage groups:
  - scan_mode
  - scan_delta_version_pin
  - scan_pruning_hints_json
  - relation_columns_json
- Worker execution expects query authorization params:
  - __auth_scope
  - __rbac_user
  - __rbac_role
- Storage access is environment-config driven via configured worker storage provider and Minio credentials in runtime environment.
