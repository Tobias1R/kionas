# Phase 5 Completion Matrix

## Scope
Phase 5 in [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md): Distributed Physical Plan and Task Graph (items 6.1 through 6.4), consolidated from Slice A and Slice B.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| 6.1 Distributed physical plan primitives and validation | Done | [roadmaps/ROADMAP_PHASE5A_MATRIX.md](roadmaps/ROADMAP_PHASE5A_MATRIX.md), [kionas/src/planner/distributed_plan.rs](kionas/src/planner/distributed_plan.rs), [kionas/src/planner/distributed_validate.rs](kionas/src/planner/distributed_validate.rs) | Initial implementation landed; integration scope still open. |
| 6.2 Task DAG compilation | Done | [roadmaps/ROADMAP_PHASE5A_MATRIX.md](roadmaps/ROADMAP_PHASE5A_MATRIX.md), [server/src/statement_handler/distributed_dag.rs](server/src/statement_handler/distributed_dag.rs) | Compiler seam is implemented and emits stage task groups with dependency metadata. |
| 6.2 Dependency-aware stage scheduling | Done | [roadmaps/ROADMAP_PHASE5A_MATRIX.md](roadmaps/ROADMAP_PHASE5A_MATRIX.md), [server/src/statement_handler/helpers.rs](server/src/statement_handler/helpers.rs), [server/src/statement_handler/query_select.rs](server/src/statement_handler/query_select.rs) | Scheduler now dispatches stage groups when upstream dependencies are satisfied. |
| 6.3 Worker coordination protocol extension | Done | [roadmaps/ROADMAP_PHASE5A_MATRIX.md](roadmaps/ROADMAP_PHASE5A_MATRIX.md), [kionas/proto/interops_service.proto](kionas/proto/interops_service.proto), [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs), [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs) | Stage progress metadata now flows worker -> interops -> task state metadata. |
| 6.3 Retry-safe stage lifecycle and idempotency guards | Done | [roadmaps/ROADMAP_PHASE5A_MATRIX.md](roadmaps/ROADMAP_PHASE5A_MATRIX.md), [server/src/tasks/mod.rs](server/src/tasks/mod.rs), [server/src/services/interops_service_server.rs](server/src/services/interops_service_server.rs) | Idempotent guards prevent terminal regressions, non-monotonic progress, and invalid partition progress updates. |
| 6.4 First distributed use-case (parallel scan plus merge) | Done | [roadmaps/ROADMAP_PHASE5B_MATRIX.md](roadmaps/ROADMAP_PHASE5B_MATRIX.md), [scripts/client_workflow.sql](scripts/client_workflow.sql#L13), [server/src/statement_handler/helpers.rs](server/src/statement_handler/helpers.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Distributed dispatch, partition fan-out, merge consumption, and manual end-to-end retrieval are validated. Global ORDER BY semantics are outside the current supported operator subset. |
| Joins and broader distributed operators | Deferred | [roadmaps/ROADMAP.md](roadmaps/ROADMAP.md) | Deferred by explicit phase decision. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase 5 signoff: `Accepted`
- Blocking items:
  - None for 6.1 to 6.4 mandatory scope. Remaining deferred items are non-blocking by phase decision.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected for this phase:
  - Runtime stack variables in [docker/docker-compose.yaml](docker/docker-compose.yaml), including service connectivity for server, worker, storage, and redis.
- Parameters expected for this phase:
  - Stage-aware task params for stage_id, dependency metadata, and partition spec.
  - Exchange artifact references for stage boundary input/output routing.
