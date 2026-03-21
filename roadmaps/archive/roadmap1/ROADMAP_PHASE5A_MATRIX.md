# Phase 5A Completion Matrix

## Scope
Slice A for Phase 5 in [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md): items 6.1 to 6.3 foundations (distributed plan primitives, DAG compilation, stage coordination protocol).

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| 6.1 Distributed physical plan primitives (stage graph, partition specs) | Done | [kionas/src/planner/distributed_plan.rs](kionas/src/planner/distributed_plan.rs), [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs), [kionas/src/planner/mod.rs](kionas/src/planner/mod.rs) | Initial additive model and translation bridge added without replacing current physical plan path. |
| 6.1 Distributed plan validation (graph integrity, cycle checks) | Done | [kionas/src/planner/distributed_validate.rs](kionas/src/planner/distributed_validate.rs), [kionas/src/planner/error.rs](kionas/src/planner/error.rs) | Stage uniqueness, dependency references, self-edge rejection, and cycle detection introduced. |
| 6.2 Task DAG compilation from distributed plan | Done | [server/src/statement_handler/distributed_dag.rs](server/src/statement_handler/distributed_dag.rs), [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs) | Stage task-group compiler added; integration into live query dispatch remains pending. |
| 6.2 Stage dependency-aware scheduler in server path | Done | [server/src/statement_handler/helpers.rs](server/src/statement_handler/helpers.rs), [server/src/statement_handler/query_select.rs](server/src/statement_handler/query_select.rs) | SELECT dispatch now compiles stage groups and executes ready stages in dependency order. |
| 6.3 Worker coordination protocol extension (stage progress metadata) | Done | [kionas/proto/interops_service.proto](kionas/proto/interops_service.proto), [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs), [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs), [server/src/statement_handler/distributed_dag.rs](server/src/statement_handler/distributed_dag.rs) | Optional stage progress fields are now emitted by worker and ingested by server task updates. |
| 6.3 Retry-safe stage bookkeeping and idempotency guards | Done | [server/src/tasks/mod.rs](server/src/tasks/mod.rs), [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs) | Task updates now enforce terminal-state protection, monotonic partition progress, and invalid-progress rejection. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase 5A signoff: `Accepted`
- Blocking items:
  - None

## Gate Checklist
1. All mandatory Slice A criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected for this slice:
  - None mandatory for planner-only and server compiler foundations in this slice.
- Parameters expected for this slice:
  - Stage metadata params encoded in task params map by [server/src/statement_handler/distributed_dag.rs](server/src/statement_handler/distributed_dag.rs):
    - stage_id
    - upstream_stage_ids
    - partition_spec
