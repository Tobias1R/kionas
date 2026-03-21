# Plan: DATATYPES FOUNDATION PHASE A

## Objective
Freeze contract and metadata baseline for distinct TIMESTAMP and DATETIME with timezone-aware semantics and decimal contract rules.

## Status
Completed.

## Steps
1. Define logical type distinctions between TIMESTAMP and DATETIME in model contracts.
2. Define timezone-aware policy and normalization rules.
3. Define canonical metadata representation requirements for temporal semantics.
4. Define decimal schema-coercion contract and bounds handling.
5. Record explicit scope boundaries and exclusions.

## Deliverables
1. Contract decisions documented and frozen.
2. Metadata representation requirements documented.
3. Decimal coercion baseline contract documented.
4. Evidence links prepared for matrix criteria.

## Dependencies
1. Depends on discovery decisions in [roadmaps/SILK_ROAD/DATATYPES/discovery/discovery-DATATYPES-FOUNDATION.md](roadmaps/SILK_ROAD/DATATYPES/discovery/discovery-DATATYPES-FOUNDATION.md).

## Acceptance Criteria
1. DATETIME is documented as distinct from TIMESTAMP.
2. Timezone-aware policy is explicit and unambiguous.
3. Decimal coercion contract has clear success and failure rules.
4. Phase outputs are linkable in the DATATYPES matrix.

## Scope Out
1. Runtime implementation work.
2. Temporal arithmetic expansion.
3. Performance optimization.

## Completion Notes
1. Datatype contract was introduced and frozen with distinct TIMESTAMP and DATETIME semantics in [kionas/src/sql/datatypes.rs](kionas/src/sql/datatypes.rs).
2. Planner validation now enforces temporal timezone policy and temporal canonical-type consistency in [kionas/src/planner/validate.rs](kionas/src/planner/validate.rs).
3. CREATE TABLE now builds, validates, and forwards datatype contract metadata in [server/src/statement_handler/create_table.rs](server/src/statement_handler/create_table.rs).
