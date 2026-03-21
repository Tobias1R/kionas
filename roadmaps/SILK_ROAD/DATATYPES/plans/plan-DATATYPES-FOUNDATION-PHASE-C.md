# Plan: DATATYPES FOUNDATION PHASE C

## Objective
Define query/runtime compatibility boundaries for temporal behavior and metadata consistency.

## Status
Completed.

## New code
- Runtime temporal compatibility was implemented across filter, join, grouping, and Flight metadata schema reconstruction.

## Completion Notes
1. Filter temporal compatibility was added for `Timestamp`, `Date32`, and `Date64` in [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs).
2. Join key temporal compatibility was added for `Timestamp`, `Date32`, and `Date64` in [worker/src/execution/join.rs](worker/src/execution/join.rs).
3. Group-by temporal compatibility was added for `Timestamp`, `Date32`, and `Date64` in [worker/src/execution/aggregate/mod.rs](worker/src/execution/aggregate/mod.rs).
4. Flight metadata temporal alignment support was added for `Date32`, `Date64`, and timestamp debug encodings in [worker/src/flight/server.rs](worker/src/flight/server.rs).
5. Unsupported-shape behavior is explicit for timezone-bearing temporal runtime keys and unquoted temporal literals.

## Steps
1. Define temporal compatibility scope for filter behavior.
2. Define temporal compatibility scope for join keys.
3. Define temporal compatibility scope for grouping behavior.
4. Define flight metadata alignment requirements for temporal columns.
5. Define unsupported query shapes and expected responses.

## Deliverables
1. Operator-level compatibility matrix.
2. Temporal metadata alignment requirements.
3. Unsupported-shape behavior notes for runtime.

## Dependencies
1. Depends on PHASE A outputs in [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md).
2. Depends on PHASE B outputs in [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-B.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-B.md).

## Acceptance Criteria
1. Filter, join, and group temporal behavior is explicitly scoped.
2. Flight metadata expectations are explicit.
3. Unsupported behavior is clearly documented.
4. Outputs are matrix-evidence ready.

## Scope Out
1. New non-temporal operator families.
2. Performance tuning.
