# Plan: DATATYPES FOUNDATION PHASE D

## Objective
Define validation gates to fail fast and prevent silent datatype fallback across planner and server boundaries.

## Status
Completed.

## Completion Notes
1. Planner datatype contract validation now rejects unsupported declared types, non-temporal timezone policy usage, and invalid decimal contract shape in [kionas/src/planner/validate.rs](kionas/src/planner/validate.rs).
2. Worker INSERT flow now fails fast when datatype contract mode is enabled but type hints are missing, empty, or incomplete in [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs).
3. Worker CREATE TABLE schema mapping now rejects unsupported declared types instead of silently falling back to Utf8 in [worker/src/transactions/ddl/create_table.rs](worker/src/transactions/ddl/create_table.rs).
4. Failure outcomes remain deterministic via existing server-side mapping for malformed type hints in [server/src/statement_handler/insert.rs](server/src/statement_handler/insert.rs).

## Steps
1. Define validation boundaries in planner and server flows.
2. Define unsupported type combinations and error outcomes.
3. Define malformed and partial payload validation behavior.
4. Define consistency checks between contracts, coercion, and runtime scope.

## Deliverables
1. Validation gate specification.
2. Unsupported-combination matrix.
3. Failure behavior taxonomy and mapping notes.

## Dependencies
1. Depends on PHASE A outputs in [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md).
2. Depends on PHASE B outputs in [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-B.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-B.md).
3. Depends on PHASE C outputs in [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-C.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-C.md).

## Acceptance Criteria
1. Validation rules close silent fallback paths.
2. Unsupported shapes are rejected with clear outcomes.
3. Boundaries are deterministic and auditable.
4. Phase evidence can satisfy matrix mandatory criteria.

## Scope Out
1. Runtime feature additions beyond declared foundation scope.
