# Intermission Worker Cleanup Completion Matrix

## Scope
Intermission scope from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Break down worker query runtime modules currently under services.
- Define a new module home for that logic.
- Start migration with compile-safe, incremental steps.

## Completion Matrix
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Architecture decision for new module home is explicit | Done | [roadmaps/dive-in/roadmap2_divein_intermission_worker_cleanup.md](roadmaps/dive-in/roadmap2_divein_intermission_worker_cleanup.md) | Top-level execution module selected. |
| Current dependency and responsibility baseline documented | Done | [roadmaps/dive-in/roadmap2_divein_intermission_worker_cleanup.md](roadmaps/dive-in/roadmap2_divein_intermission_worker_cleanup.md) | Maps services/query, query_execution, and orchestration boundaries. |
| Execution module scaffold is created | Done | [worker/src/execution/mod.rs](worker/src/execution/mod.rs) | Initial execution domain established. |
| Low-coupling query runtime modules moved into execution | Done | [worker/src/execution/query.rs](worker/src/execution/query.rs), [worker/src/execution/join.rs](worker/src/execution/join.rs) | First migration slice complete with compatibility shim. |
| Query execution imports switched to execution module paths | Done | [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Join and namespace imports now sourced from execution. |
| Services boundary cleanup started | Done | [worker/src/services/query.rs](worker/src/services/query.rs), [worker/src/services/mod.rs](worker/src/services/mod.rs) | query_join service module removed; query shim retained temporarily. |
| Runtime-plan and input-loading helpers extracted to execution planner/pipeline modules | Done | [worker/src/execution/planner.rs](worker/src/execution/planner.rs), [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | query_execution now consumes extracted helpers via execution module imports. |
| Runtime orchestration split from query_execution into execution submodules | Done | [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), [worker/src/execution/query.rs](worker/src/execution/query.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Orchestration entrypoint now lives in execution pipeline and dispatch path uses execution entrypoint; services function retained as compatibility delegate. |
| Final cut-over removes query runtime from services entirely | Not Started | N/A | Requires moving execute path and removing transition shim. |
| Optional hardening: module naming refinement (execution/planner vs execution/runtime_plan) | Deferred | N/A | Non-blocking naming cleanup. |
| Optional hardening: dedicated refactor regression tests | Deferred | N/A | Non-blocking while semantics remain unchanged. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Intermission signoff: `Pending`
- Blocking items:
- Runtime orchestration split from query_execution into execution submodules.
- Final cut-over removes query runtime from services entirely.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected for this intermission:
- None new. Existing worker runtime/storage variables remain applicable.

- Parameters expected for this intermission:
- Structural cleanup only; no query semantics expansion.
- Migration must preserve current query runtime behavior.
