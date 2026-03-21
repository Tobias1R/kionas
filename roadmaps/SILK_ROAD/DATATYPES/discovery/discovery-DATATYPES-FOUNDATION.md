# SILK_ROAD Discovery: DATATYPES FOUNDATION

## Objective
Define the next Silk Road path focused on rich datatype correctness with emphasis on TIMESTAMP and DATETIME, while preserving safe incremental rollout and matrix-gated closure.

## Closure Context
- CONSTRAINTS FOUNDATION is closed and accepted in [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md).
- DATATYPES is now the next discovery and planning track.

## Decisions Confirmed
1. DATETIME remains distinct from TIMESTAMP.
2. Timezone-aware semantics are required.
3. FOUNDATION scope includes temporal support plus decimal coercion strategy.
4. This phase is discovery and planning only. No runtime behavior change is included in this artifact.

## Current Type Support Inventory

### DDL type mapping surface
Evidence: [worker/src/transactions/ddl/create_table.rs](worker/src/transactions/ddl/create_table.rs)

Observed support:
1. Integer families (Int16, Int32, Int64)
2. Boolean
3. Float32 and Float64
4. Date
5. Timestamp
6. Binary
7. Fallback to Utf8 for unrecognized values

### INSERT scalar parsing surface
Evidence: [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs)

Observed support:
1. Int
2. Bool
3. Null
4. String fallback for other literals

Gaps:
1. No explicit temporal scalar parsing path as first-class insert semantics.
2. Decimal coercion behavior is not declared as schema-driven policy.

### Storage type mapping surface
Evidence: [worker/src/storage/deltalake.rs](worker/src/storage/deltalake.rs)

Observed support:
1. Arrow Date to Delta DATE
2. Arrow Timestamp to Delta TIMESTAMP
3. Arrow Decimal to Delta DECIMAL
4. Standard primitive mappings

Gap:
1. Runtime insert/query behavior must be aligned with storage type declarations to avoid semantic drift.

### Query runtime surfaces
Evidence:
- [worker/src/execution/query.rs](worker/src/execution/query.rs)
- [worker/src/execution/join.rs](worker/src/execution/join.rs)
- [worker/src/execution/aggregate/mod.rs](worker/src/execution/aggregate/mod.rs)

Gap:
1. Temporal behavior is not yet specified as a complete capability set across filter/join/group execution.

### Flight metadata surface
Evidence: [worker/src/flight/server.rs](worker/src/flight/server.rs)

Gap:
1. Temporal metadata behavior needs explicit compatibility guarantees in the foundation plan.

## Problem Statement
The platform has partial temporal primitives at DDL and storage boundaries but lacks an explicit, validated, end-to-end model for:
1. Distinct DATETIME behavior
2. Timezone-aware TIMESTAMP and DATETIME semantics
3. Insert-time schema-aligned temporal coercion
4. Decimal coercion consistency tied to target schema

## Risks If Unaddressed
1. Silent fallback behavior that weakens correctness confidence
2. Inconsistent temporal interpretation between create, insert, and query paths
3. Ambiguous timezone handling and cross-system drift
4. Late type mismatch failures in runtime execution

## FOUNDATION Scope (In)
1. Define distinct logical contracts for TIMESTAMP and DATETIME.
2. Define timezone-aware semantics policy and normalization contract.
3. Define insert-time temporal coercion rules and failure taxonomy.
4. Define decimal coercion strategy aligned to target schema.
5. Define planner and server validation gates to avoid silent fallback.
6. Define verification scenarios and matrix-ready evidence expectations.

## FOUNDATION Scope (Out)
1. Full temporal arithmetic expansion
2. Full datatype lattice redesign
3. Performance optimization commitments
4. Broad non-temporal type expansion beyond this foundation scope

## Recommendation
Proceed with DATATYPES FOUNDATION as the next Silk Road implementation slice after roadmap and matrix scaffolding.

## Handoff Targets
- Roadmap: [roadmaps/SILK_ROAD/DATATYPES/ROADMAP_DATATYPES.md](roadmaps/SILK_ROAD/DATATYPES/ROADMAP_DATATYPES.md)
- Plan: [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION.md)
- Matrix: [roadmaps/ROADMAP_DATATYPES_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_DATATYPES_FOUNDATION_MATRIX.md)
