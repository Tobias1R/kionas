# Phase 6 Completion Matrix

## Scope
Phase 6 scope from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Introduce GROUP semantics with explicit constraints and deterministic aggregate behavior.
- Add planner/runtime contracts for aggregate partial/final metadata.
- Add observability hooks for aggregation execution diagnostics.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Query model extracts GROUP BY metadata into canonical payload | Done | [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs) | Added `group_by` extraction and payload emission fields. |
| Logical plan carries grouping keys and aggregate expressions | Done | [kionas/src/planner/logical_plan.rs](kionas/src/planner/logical_plan.rs), [kionas/src/planner/aggregate_spec.rs](kionas/src/planner/aggregate_spec.rs), [kionas/src/planner/translate.rs](kionas/src/planner/translate.rs), [kionas/src/planner/validate.rs](kionas/src/planner/validate.rs) | Added aggregate contracts and logical validation constraints. |
| Physical plan emits typed AggregatePartial/AggregateFinal operators | Done | [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs), [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs), [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs) | Typed aggregate metadata now flows from logical to physical with ordering validation. |
| Distributed planning supports aggregate partial/final stage decomposition | Done | [kionas/src/planner/distributed_plan.rs](kionas/src/planner/distributed_plan.rs) | Aggregate-final split adds 2-stage dependency and hash partition metadata from grouping keys. |
| Worker payload validation accepts constrained aggregate operators | Done | [worker/src/execution/query.rs](worker/src/execution/query.rs) | Added aggregate operator allowlist and pipeline placement checks. |
| Worker runtime parses and executes aggregate partial/final stages | Done | [worker/src/execution/planner.rs](worker/src/execution/planner.rs), [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), [worker/src/execution/aggregate/mod.rs](worker/src/execution/aggregate/mod.rs) | Runtime supports partial and final aggregate stages with SQL-consistent NULL handling baseline. |
| Per-function aggregate modules exist for COUNT/SUM/MIN/MAX/AVG | Done | [worker/src/execution/aggregate/aggregate_count.rs](worker/src/execution/aggregate/aggregate_count.rs), [worker/src/execution/aggregate/aggregate_sum.rs](worker/src/execution/aggregate/aggregate_sum.rs), [worker/src/execution/aggregate/aggregate_min.rs](worker/src/execution/aggregate/aggregate_min.rs), [worker/src/execution/aggregate/aggregate_max.rs](worker/src/execution/aggregate/aggregate_max.rs), [worker/src/execution/aggregate/aggregate_avg.rs](worker/src/execution/aggregate/aggregate_avg.rs) | Implemented as separate modules per requested plan constraint. |
| Explain diagnostics include aggregate operator metadata | Done | [kionas/src/planner/explain.rs](kionas/src/planner/explain.rs) | Explain output now renders aggregate keys and aggregate output descriptors. |
| Non-test compile verification for impacted crates | Done | [kionas/src/planner/mod.rs](kionas/src/planner/mod.rs), [worker/src/execution/mod.rs](worker/src/execution/mod.rs) | `cargo check -p kionas` and `cargo check -p worker` completed successfully (warnings only). |
| Optional hardening: strict clippy clean at workspace scope | Deferred | N/A | Existing unrelated `-D warnings` failures remain outside Phase 6 change set. |
| Optional hardening: end-to-end and unit test execution | Deferred | N/A | Deferred per user instruction: "no tests". |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase signoff: `Accepted`
- Blocking items:
  - None. Optional hardening items remain deferred and non-blocking by decision.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected:
  - Existing worker/server/storage env vars used by query and stage exchange runtime.
- Parameters expected:
  - Aggregate operators are expressed in `physical_plan.operators` payload as `AggregatePartial { spec }` and `AggregateFinal { spec }`.
  - Worker stage parameters (`stage_id`, `partition_count`, `partition_index`, `upstream_stage_ids`) remain the execution coordination contract.
