# Phase 2 Dive-In: Main Blocker

## Objective
Provide a technical discovery of the main Phase 2 blocker:

- Advanced operators are represented in planning catalogs but constrained by current validation and/or execution capability.

This document is discovery only. It is not an implementation plan.

## System Path Under Analysis
1. SQL intake and query-model extraction.
2. Logical plan construction.
3. Physical plan translation.
4. Physical capability validation.
5. Worker runtime execution.
6. Flight retrieval path compatibility.

## Evidence Map

### 1) Operator catalog exists
- Evidence: [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs)
- Observed: advanced operators are enumerated (`Sort`, `Limit`, `AggregatePartial`, `AggregateFinal`, `HashJoin`, `Exchange*`, `Union`, `Values`).
- Conclusion: representation exists at the enum level.

### 2) Capability validation explicitly blocks advanced operators
- Evidence: [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs)
- Observed: validation returns `UnsupportedPhysicalOperator` for advanced operators.
- Observed: predicate validation rejects `BETWEEN`, `IN`, `LIKE`, `ILIKE`, `EXISTS`, `ANY`, `ALL`, and `OVER` usage.
- Conclusion: advanced operators are intentionally gated off before dispatch.

### 3) Physical translation emits only minimal pipeline
- Evidence: [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs)
- Observed: translator always emits `TableScan -> optional Filter -> Projection -> Materialize`.
- Conclusion: no path currently emits `Sort` or other advanced operators.

### 4) Logical plan model is intentionally minimal
- Evidence: [kionas/src/planner/logical_plan.rs](kionas/src/planner/logical_plan.rs)
- Observed: logical plan currently carries relation, projection, optional selection, and sql string.
- Conclusion: advanced semantics are not first-class in the logical model yet.

### 5) Query model extraction does not preserve advanced clause semantics
- Evidence: [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs)
- Observed: model includes projection and selection, but no explicit ordering/window/qualify structures.
- Conclusion: advanced semantics are not guaranteed to survive model boundary as structured data.

### 6) Worker runtime executes only minimal operator subset
- Evidence: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)
- Observed: runtime accepts scan/filter/projection/materialize and rejects others as non-executable in this phase.
- Conclusion: execution capability is intentionally bounded.

## Root-Cause Decomposition
The blocker is not a single missing function; it is a layered contract mismatch:

1. Catalog layer:
- Advanced operators exist.

2. Translation layer:
- Advanced operators are not emitted.

3. Validation layer:
- Advanced operators are hard-rejected.

4. Runtime layer:
- Advanced operators are not executable.

Any single-layer change is insufficient; the path must be unblocked as a vertical slice.

## Why ORDER BY Fails in Practice
Observed runtime behavior confirms unsorted output despite `ORDER BY` query text.

Technical reason chain:
1. ORDER BY appears at SQL surface.
2. Structured query model/logical model does not carry enforceable sort contract end-to-end.
3. Physical translation does not produce `Sort`.
4. Physical validation rejects `Sort` if introduced.
5. Worker runtime has no sort stage.

## Operator State Matrix

| Operator family | Cataloged | Emitted | Validated | Executable |
|---|---|---|---|---|
| TableScan | Yes | Yes | Yes | Yes |
| Filter | Yes | Yes | Yes (subset predicates) | Yes |
| Projection | Yes | Yes | Yes | Yes |
| Materialize | Yes | Yes | Yes | Yes |
| Sort | Yes | No | No | No |
| Limit | Yes | No | No | No |
| AggregatePartial/Final | Yes | No | No | No |
| HashJoin/NestedLoopJoin | Yes | No | No | No |
| Exchange* | Yes | No (for local physical plan path) | No | No |

## Technical Unblock Dimensions (Discovery)

### A) Model
1. Preserve advanced semantics as structured query-model fields.
2. Extend logical plan contract to carry those semantics explicitly.

### B) Translation
1. Emit advanced operators where model semantics require them.
2. Preserve deterministic operator ordering and explainability.

### C) Validation
1. Replace blanket rejections with capability-aware allow paths.
2. Keep strict and explicit errors for still-unsupported semantics.

### D) Runtime
1. Add execution stages for newly validated operators.
2. Define deterministic behavior for ordering, null handling, and ties.

### E) Verification
1. Validate behavior with deterministic end-to-end query outputs.
2. Ensure no regression in existing minimal pipeline semantics.

## Constraints and Risks
1. Opening validation without runtime support will produce runtime failures.
2. Adding runtime support without model/translation changes will not change behavior.
3. Feature additions must maintain stable response and handle contracts.
4. Discovery confirms UPDATE/DELETE should remain out of this scope until mutation gates are defined.

## Discovery Outcome
Phase 2 main blocker is now decomposed into concrete technical choke points across model, translation, validation, and runtime layers, with file-level evidence suitable for building the next implementation plan.

