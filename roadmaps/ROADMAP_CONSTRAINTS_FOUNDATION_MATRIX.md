# Phase CONSTRAINTS FOUNDATION Completion Matrix

## Scope
Discovery, contract normalization, and initial NOT NULL enforcement for the first constraints implementation slice, as defined in [roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md](roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md).

## Completion Matrix
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Mandatory criterion 1: Comparison discovery created (DATATYPES vs CONSTRAINTS) | Done | [roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md) | Includes decision rubric and recommendation. |
| Mandatory criterion 2: CONSTRAINTS roadmap created and linked | Done | [roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md](roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md) | FOUNDATION/ENFORCEMENT/OPTIMIZATION phases scaffolded. |
| Mandatory criterion 3: Next-iteration plan declared | Done | [roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md) | Clear implementation handoff target. |
| Mandatory criterion 4: Matrix signoff gate created | Done | [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md) | This file. |
| Mandatory criterion 5: Constraint metadata contract normalized | Done | [kionas/src/sql/constraints.rs](kionas/src/sql/constraints.rs), [kionas/src/planner/validate.rs](kionas/src/planner/validate.rs), [server/src/statement_handler/create_table.rs](server/src/statement_handler/create_table.rs) | Canonical contract with versioning and server-side validation wired into CREATE TABLE payload shaping. |
| Mandatory criterion 6: NOT NULL enforcement added in mutation path | Done | [server/src/statement_handler/insert.rs](server/src/statement_handler/insert.rs), [worker/src/transactions/constraints/insert_not_null.rs](worker/src/transactions/constraints/insert_not_null.rs), [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs) | Server derives NOT NULL and ordered table columns from metadata; worker rejects violating INSERT payloads with actionable, stable business-coded errors. |
| Mandatory criterion 7: Module isolation preserved for constraints logic | Done | [server/src/statement_handler/insert.rs](server/src/statement_handler/insert.rs), [worker/src/transactions/constraints/mod.rs](worker/src/transactions/constraints/mod.rs), [worker/src/transactions/constraints/insert_not_null.rs](worker/src/transactions/constraints/insert_not_null.rs) | Constraint logic implemented in new modules instead of extending large existing handlers. |
| Optional hardening 1: Full constraint error taxonomy draft | Deferred | N/A | Non-blocking for discovery signoff. |
| Optional hardening 2: Datatype follow-on matrix scaffold | Deferred | N/A | Non-blocking for this phase. |

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
- No mandatory new environment variables were introduced in this phase.
- Runtime parameters introduced by this phase:
  - `not_null_columns_json` in worker task params for INSERT enforcement.
  - `table_columns_json` in worker task params to map implicit VALUES columns positionally.
  - `constraint_contract_version` in worker task params for forward-compatible constraint contract evolution.
