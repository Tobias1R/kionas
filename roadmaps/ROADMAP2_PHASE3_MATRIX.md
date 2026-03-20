# Phase 3 Completion Matrix

## Scope
Phase 3 scope from [ROADMAP2.md](ROADMAP2.md):
- Preserve ORDER BY semantics from parse/model/plan to worker execution.
- Ensure ordered output correctness for deterministic result validation.
- Add explain and diagnostics to show ordering decisions.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Preserve ORDER BY through model/planner/validator/runtime path | Done | [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs), [kionas/src/planner/logical_plan.rs](kionas/src/planner/logical_plan.rs), [kionas/src/planner/translate.rs](kionas/src/planner/translate.rs), [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs), [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs), [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs), [worker/src/services/query.rs](worker/src/services/query.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Vertical ORDER BY path is implemented end-to-end. |
| Verify deterministic ordered output for ascending case | Done | Runtime evidence from user query output for `select * from abc.schema1.table1 order by c1;` | Output returned sorted rows 1..15. |
| Verify ORDER BY directionality for DESC with filtering | Done | Runtime evidence from user query output for `select * from abc.schema1.table1 where c1<8 order by c2 desc;` | Output returned rows in descending lexical order of `c2`: Grace -> Frank -> Eve -> Diana -> Charlie -> Bob -> Alice. |
| Verify ORDER BY edge behavior for ties | Done | Runtime evidence from user query output for `select * from abc.schema1.table1 order by c1;` after inserting `(1, NULL)` | Tie on `c1=1` is handled without dispatch/runtime failure and ordering remains deterministic. |
| Verify ORDER BY null-sort-key semantics (ASC/DESC position) | Done | Runtime evidence from user query outputs for `select * from abc.schema1.table1 where c1=1 order by c2;` and `select * from abc.schema1.table1 where c1=1 order by c2 desc;` | Observed policy: null sort keys are positioned last in both ASC and DESC for current worker sort options. |
| Add explain/diagnostic visibility for emitted Sort operator | Done | [kionas/src/planner/explain.rs](kionas/src/planner/explain.rs), [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs) | Physical explain text now includes detailed sort keys (for example `Sort(id DESC)`), and query payload diagnostics include physical/logical explain text plus canonical operator pipeline. |
| Optional hardening: broader ORDER BY regression set | Deferred | N/A | Non-blocking for initial Phase 3 acceptance. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase signoff: `Done`
- Blocking items:
  - None.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected:
  - None mandatory for this semantics slice.
- Parameters expected:
  - SQL ORDER BY clauses in query text routed through query model and planner payload.
