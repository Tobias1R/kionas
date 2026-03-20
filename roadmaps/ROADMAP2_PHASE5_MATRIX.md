# Phase 5 Completion Matrix

## Scope
Phase 5 scope from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Introduce minimal JOIN semantics with explicit constraints (INNER equi-join first).
- Add planner/runtime contracts for multi-relation modeling and JOIN operator execution.
- Add explain and diagnostics visibility for JOIN strategy and predicates.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Query model extracts constrained JOIN metadata (INNER equi, multi-key) | Done | [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs) | JOIN metadata (`joins`) added to canonical payload model; unsupported join shapes return validation errors. |
| Logical plan carries JOIN specifications | Done | [kionas/src/planner/logical_plan.rs](kionas/src/planner/logical_plan.rs), [kionas/src/planner/join_spec.rs](kionas/src/planner/join_spec.rs), [kionas/src/planner/translate.rs](kionas/src/planner/translate.rs) | Logical join contract added and populated from query model translation. |
| Physical plan emits HashJoin with typed spec | Done | [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs), [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs) | Physical HashJoin now carries relation and key metadata. |
| Planner validation allows constrained HashJoin and enforces pipeline invariants | Done | [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs), [kionas/src/planner/error.rs](kionas/src/planner/error.rs) | HashJoin is accepted with key/relation checks and ordering constraints. |
| Worker payload validation accepts constrained HashJoin shape | Done | [worker/src/services/query.rs](worker/src/services/query.rs) | Worker-side operator allowlist and ordering checks updated for HashJoin. |
| Worker runtime executes constrained HashJoin | Done | [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs), [worker/src/services/query_join.rs](worker/src/services/query_join.rs), [worker/src/services/mod.rs](worker/src/services/mod.rs) | Runtime now parses HashJoin and executes inner equi join using right relation scan. |
| Explain/diagnostics include JOIN details | Done | [kionas/src/planner/explain.rs](kionas/src/planner/explain.rs), [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs) | Explain text renders join type, right relation, and keys; payload carries joins diagnostics. |
| Distributed JOIN stage decomposition for staged execution | Done | [kionas/src/planner/distributed_plan.rs](kionas/src/planner/distributed_plan.rs), [kionas/src/planner/distributed_validate.rs](kionas/src/planner/distributed_validate.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | JOIN plans are split into scan/filter upstream stage and join/finalize downstream stage with dependency wiring. |
| End-to-end JOIN verification query set execution | Done | [scripts/client_workflow.sql](scripts/client_workflow.sql), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs), [worker/src/services/query_join.rs](worker/src/services/query_join.rs) | Verified successful execution for semantic projection JOIN (`t1.id,t1.name,t2.document`) and physical projection JOIN (`t1.c1,t1.c2,t2.table2_c2`) with expected ordered rowsets. |
| Optional hardening: outer/non-equi/multi-join support | Deferred | N/A | Out of scope for Phase 5 minimum slice. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase signoff: `Accepted`
- Blocking items:
  - None.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected:
  - Existing worker/server/storage variables already used by query execution and exchange persistence paths.
- Parameters expected:
  - JOIN capability gate currently constrained to `INNER` equi-join with one or more key pairs.
  - Physical payload includes `HashJoin.spec` with `right_relation` and `keys`.
