# Plan: DATATYPES FOUNDATION

## Objective
Deliver a safe and verifiable rich-datatype foundation by defining and implementing distinct TIMESTAMP and DATETIME contracts, timezone-aware behavior, and decimal coercion policy.

## Status
Planned.

## Phase Files
1. Phase A: [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-A.md)
2. Phase B: [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-B.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-B.md)
3. Phase C: [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-C.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-C.md)
4. Phase D: [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-D.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-D.md)
5. Phase E: [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-E.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION-PHASE-E.md)

## Phase A: Contract and metadata baseline
1. Define TIMESTAMP and DATETIME as distinct logical types in model and metadata contracts.
2. Define timezone-aware policy and normalization behavior.
3. Define canonical metadata representation path for temporal semantics.
4. Define decimal schema-coercion contract and bounds behavior.

## Phase B: Mutation coercion strategy
1. Define insert-time temporal parsing and coercion rules aligned to target schema.
2. Define accepted literal forms and rejection conditions.
3. Define decimal coercion behavior with strict failure rules.
4. Define deterministic error categories for type/coercion failures.

## Phase C: Query runtime capability alignment
1. Define temporal compatibility scope for filter behavior.
2. Define temporal compatibility scope for join keys.
3. Define temporal compatibility scope for grouping behavior.
4. Define flight metadata alignment requirements for temporal columns.

## Phase D: Validation gates
1. Add fail-fast validation boundaries in planner and server paths to prevent silent fallback.
2. Define unsupported shape handling with actionable errors.
3. Ensure deterministic behavior under partial and malformed payloads.

## Phase E: Verification and signoff
1. Validate distinct DATETIME and TIMESTAMP semantics.
2. Validate timezone-aware behavior.
3. Validate temporal insert coercion positive and negative scenarios.
4. Validate decimal coercion positive and negative scenarios.
5. Validate query runtime compatibility for declared FOUNDATION scope.
6. Complete matrix evidence for mandatory criteria before signoff.

## Dependencies
1. Discovery decisions in [roadmaps/SILK_ROAD/DATATYPES/discovery/discovery-DATATYPES-FOUNDATION.md](roadmaps/SILK_ROAD/DATATYPES/discovery/discovery-DATATYPES-FOUNDATION.md) must be frozen before implementation.
2. Matrix gating in [roadmaps/ROADMAP_DATATYPES_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_DATATYPES_FOUNDATION_MATRIX.md) is mandatory before phase closure.
3. Phase dependency order is A -> B -> C -> D -> E.

## Explicit Exclusions
1. No full temporal arithmetic expansion in this phase.
2. No broad non-temporal type expansion in this phase.
3. No performance optimization commitments in this phase.
