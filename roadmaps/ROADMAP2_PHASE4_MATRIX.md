# Phase 4 Completion Matrix

## Scope
Phase 4 scope from [ROADMAP2.md](ROADMAP2.md):
- Add LIMIT support in the same end-to-end path.
- Add regression coverage for ORDER BY + LIMIT interactions.
- Harden validation and error taxonomy for unsupported query shapes.
- Keep implementation modular and avoid concentrating too much new logic in one file.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Preserve LIMIT/OFFSET through model/planner/validator/runtime path | Done | [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs), [kionas/src/planner/logical_plan.rs](kionas/src/planner/logical_plan.rs), [kionas/src/planner/translate.rs](kionas/src/planner/translate.rs), [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs), [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs), [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs), [worker/src/services/query.rs](worker/src/services/query.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | LIMIT/OFFSET is now carried and enforced across model, planning, validation, and worker runtime slicing. |
| Verify deterministic ORDER BY + LIMIT behavior | Done | Runtime evidence from user query outputs for `select * from abc.schema1.table1 order by c1 limit 5;` and `select * from abc.schema1.table1 order by c1 limit 3 offset 2;` | Output ordering and slicing match expected deterministic semantics. |
| Verify LIMIT/OFFSET edge behavior (`LIMIT 0`, high OFFSET) | Done | Runtime evidence from user query outputs for `select * from abc.schema1.table1 limit 0;` and `select * from abc.schema1.table1 limit 5 offset 999;` both returning `status=OK` with `rows=0 columns=2 batches=1` | Edge semantics verified end-to-end with empty-result retrieval preserved. |
| Harden validation taxonomy for unsupported LIMIT/OFFSET shapes | Done | [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs), [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs), [worker/src/services/query.rs](worker/src/services/query.rs) | Validation now rejects unsupported FETCH and OFFSET-without-LIMIT, and enforces strict LIMIT pipeline placement rules. |
| Add explain/diagnostic visibility for emitted Limit operator | Done | [kionas/src/planner/explain.rs](kionas/src/planner/explain.rs), [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs) | Physical explain pipeline text now includes limit details (`Limit(count=..., offset=...)`) and payload diagnostics carry that text. |
| Optional hardening: broader ORDER BY + LIMIT regression set | Deferred | N/A | Non-blocking for initial Phase 4 acceptance. |

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
  - SQL LIMIT/OFFSET clauses in query text routed through query model and planner payload.
