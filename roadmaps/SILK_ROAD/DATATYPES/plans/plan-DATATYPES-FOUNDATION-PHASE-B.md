# Plan: DATATYPES FOUNDATION PHASE B

## Objective
Define mutation-side coercion strategy for temporal and decimal values aligned to table schema.

## Status
Completed.

## Steps
1. Define accepted temporal literal formats for TIMESTAMP and DATETIME.
2. Define coercion rules from insert literals to target schema types.
3. Define decimal coercion and precision-scale failure behavior.
4. Define deterministic error taxonomy for coercion failures.
5. Define malformed payload handling expectations.

## Deliverables
1. Mutation coercion specification.
2. Temporal literal acceptance matrix.
3. Decimal coercion decision table.
4. Error taxonomy for mutation and coercion failures.

## Dependencies
1. Depends on PHASE A outputs in [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md).

## Acceptance Criteria
1. Coercion behavior is deterministic for success and failure paths.
2. Error categories are actionable and stable.
3. Rules are schema-aware and avoid silent fallback.
4. Phase outputs map to matrix mandatory items.

## Scope Out
1. Query operator runtime expansion.
2. Planner optimization behavior.

## Completion Notes
1. INSERT now accepts schema-driven column type hints from server metastore metadata in [server/src/statement_handler/insert.rs](server/src/statement_handler/insert.rs) and [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs).
2. Temporal coercion rules and deterministic failures were implemented for TIMESTAMP and DATETIME literals in [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs).
3. Decimal coercion strictness and precision/scale validation were implemented with stable failure taxonomy in [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs).
