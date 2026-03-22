# Plan: EXECUTION EP-4

## Project coding standards and guidelines
1. tests go under the folder src/tests/<module_name>_tests.rs; avoid inline test additions at the end of non-test files.

## Coding considerations for this phase
1. Any connection to storage must be pooled and reused; no new unpooled connections.
2. Keep deterministic output ordering across join and aggregate operator paths.
3. Expand capability through phase-scoped, auditable slices rather than broad operator rewrites.

## Goal
Expand the worker runtime envelope for join and aggregate execution while preserving deterministic correctness, producing explicit unsupported-semantics diagnostics, and creating matrix-grade evidence for null/type edge cases and non-regression behavior.

Measurable EP-4 outcomes:
1. Join and aggregate capability set for EP-4 is explicitly enumerated and mapped to tests.
2. Unsupported semantics fail with stable and actionable diagnostics.
3. Existing covered execution behavior remains non-regressed under full quality gates.

## Inputs
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP4.md](../discovery/discovery-EXECUTION-EP4.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md](../../../ROADMAP_EXECUTION_EP4_MATRIX.md)
4. Prior phase signoff reference: [roadmaps/ROADMAP_EXECUTION_EP3_MATRIX.md](../../../ROADMAP_EXECUTION_EP3_MATRIX.md)

## Scope Boundaries
In scope:
1. Runtime join and aggregate capability expansion inside existing worker execution contract.
2. Join and aggregate null/type edge-case behavior verification and diagnostics hardening.
3. Planner-to-runtime contract validation for join and aggregate operator metadata.
4. EP-4 completion evidence packaging for join/aggregate envelope criteria.

Out of scope:
1. SQL parser redesign.
2. Client protocol redesign.
3. Broad distributed resilience redesign (EP-5 scope).
4. Non-join/non-aggregate operator family expansion beyond EP-4 needs.

## Guardrails
1. Preserve worker contract boundaries: worker executes Kionas task operators from validated physical plans.
2. Keep phase work incremental and matrix-gated.
3. Prefer deterministic and explicit behavior over heuristic broadening where correctness is at risk.
4. Degrade gracefully on unsupported semantics with stable, actionable diagnostics.
5. Keep telemetry and diagnostics low-cardinality and operationally parseable.

## Current Discovery Findings (EP-4 Baseline)
1. Join type contract is currently constrained at planner-model level to `Inner` only (`kionas/src/planner/join_spec.rs`).
2. Worker join runtime path executes hash equi-join with right-side materialization and deterministic chunked fanout flush (`worker/src/execution/join.rs`).
3. Physical validation already rejects deferred operators and enforces pipeline ordering constraints, but join/aggregate semantic breadth is still intentionally narrow (`kionas/src/planner/physical_validate.rs`).
4. Aggregate function set exists for `Count`, `Sum`, `Min`, `Max`, `Avg`, with partial/final merge pipeline and deterministic grouped ordering (`worker/src/execution/aggregate/mod.rs`).
5. Existing targeted tests validate a subset of deterministic join and aggregate behavior, but EP-4-specific capability matrix coverage is not yet complete (`worker/src/tests/execution_join_tests.rs`, `worker/src/tests/execution_aggregate_mod_tests.rs`).

## Workstreams

### Workstream A: Join Envelope Definition And Gap Matrix
Objectives:
1. Define EP-4 join capability envelope in explicit, testable terms.
2. Distinguish supported semantics from deferred semantics with stable diagnostics requirements.
3. System is not accepting alias for columns. We need fix this in the join code and add tests for it.

Tasks:
0. Fix alias handling in select code and add tests for it.
1. Enumerate current join behavior and constraints from planner/worker code paths.
2. Define EP-4 target join envelope (phase-bounded) and deferred join semantics list.
3. Define error taxonomy for unsupported join semantics with actionable messaging.


Deliverables:
1. Join capability matrix (current vs EP-4 target vs deferred).
2. Unsupported-join diagnostics contract list.

### Workstream B: Aggregate Envelope Definition And Edge Semantics
Objectives:
1. Define EP-4 aggregate coverage boundaries and edge-case behavior contract.
2. Ensure null/type handling expectations are explicit and test-driven.

Tasks:
1. Enumerate aggregate function/input constraints and finalization behavior.
2. Define EP-4 edge-case matrix (null, mixed-type, empty-group, partial/final merge invariants).
3. Define stable diagnostics for unsupported aggregate input semantics.

Deliverables:
1. Aggregate capability and edge-case matrix.
2. Aggregate diagnostics contract list.

### Workstream C: Planner-Runtime Contract Alignment For Join/Aggregate
Objectives:
1. Ensure planner output and runtime assumptions remain aligned during envelope expansion.
2. Prevent silent semantic drift between translation, validation, and execution.

Tasks:
1. Review `PhysicalJoinSpec`, `PhysicalAggregateSpec`, and runtime extraction/validation flow.
2. Define invariant checks required for EP-4 signoff (operator ordering, required fields, key/expr validity).
3. Define compatibility checks to avoid regressions in already-supported query shapes.

Deliverables:
1. Contract invariant checklist for EP-4.
2. Regression-sensitive query-shape list for validation.

### Workstream D: EP-4 Evidence And Verification Packaging
Objectives:
1. Convert EP-4 mandatory criteria into matrix-grade evidence.
2. Package test and quality-gate outputs with explicit criterion mapping.

Tasks:
1. Define criteria-to-test mapping for join/aggregate envelope requirements.
2. Define required command evidence set for phase closure.
3. Prepare EP-4 matrix skeleton with mandatory criteria and optional hardening placeholders.

Deliverables:
1. EP-4 evidence checklist and command package blueprint.
2. Matrix-ready criterion evidence mapping.

## Canonical Mode Decision Table

| Domain | Mode | Entry Condition | Runtime Behavior | Deferred/Failure Behavior | Evidence Target |
|---|---|---|---|---|---|
| Join | `inner_equi` | `PhysicalOperator::HashJoin` with non-empty `keys` and valid right relation | Deterministic hash join with chunked fanout flush and duplicate-right-column rename handling | N/A | join correctness and order tests |
| Join | `unsupported_join_semantic` | Join semantic requested outside EP-4 supported set | N/A | Fail fast with actionable diagnostic identifying unsupported semantic | negative-path diagnostics tests |
| Aggregate | `partial_final_grouped` | Aggregate partial/final pair present with valid expressions and outputs | Deterministic grouped partial/final execution with supported aggregate functions | N/A | aggregate function and grouping tests |
| Aggregate | `unsupported_aggregate_semantic` | Unsupported aggregate input/shape encountered | N/A | Fail fast with actionable diagnostic identifying unsupported aggregate behavior | negative-path diagnostics tests |

## Criteria-To-Workstream Traceability

| EP-4 Mandatory Criterion | Primary Workstream(s) | Evidence Target |
|---|---|---|
| Targeted join/aggregate capabilities are explicitly enumerated and verified | A, B, D | capability matrix + focused verification tests |
| Null/type edge-case behavior is tested and documented in matrix evidence | B, D | edge-case test outputs + matrix references |
| Unsupported semantics fail clearly with actionable diagnostics | A, B, C, D | negative-path diagnostics tests + error contract mapping |
| Added runtime support does not regress existing covered scenarios | C, D | non-regression test pack + comparison evidence |
| Quality gate evidence captured (fmt/clippy/check) | D | command outputs and matrix references |

## Sequence And Dependencies
1. Workstream A and Workstream B start first to define phase-bounded capability contracts.
2. Workstream C depends on A/B capability decisions to lock planner-runtime invariants and regression coverage.
3. Workstream D depends on A-C outputs to package matrix-grade evidence and closure artifacts.
4. EP-4 implementation should not begin before EP-3 matrix signoff is complete.

## Milestones
1. M1: Join capability matrix and deferred-join diagnostics contract defined.
2. M2: Aggregate capability and null/type edge-case matrix defined.
3. M3: Planner-runtime invariant checklist for join/aggregate finalized.
4. M4: EP-4 verification and matrix evidence package prepared.

## Risks And Mitigations
1. Risk: Join envelope expansion introduces correctness regressions in row cardinality or output ordering.
Mitigation: deterministic join-order regression suite and explicit cardinality checks for representative fanout patterns.

2. Risk: Aggregate edge semantics drift between partial and final stages.
Mitigation: phase-scoped aggregate edge-case matrix with explicit partial/final merge invariants and targeted tests.

3. Risk: Planner/runtime contract drift causes silent semantic mismatch.
Mitigation: explicit invariant checklist and validation gates for join/aggregate operator specs.

4. Risk: Scope creep expands EP-4 into broad operator or protocol redesign.
Mitigation: strict in-scope/out-of-scope enforcement and matrix-gated workstream closure.

## E2E Scenarios
1. Deterministic inner equi-join with duplicate right column names:
Expected outcome: stable row order and deterministic right-column renaming.

2. Join with no matching rows:
Expected outcome: empty output with expected schema contract preserved.

3. Grouped aggregation with `COUNT`, `SUM`, and `AVG` across null-containing inputs:
Expected outcome: function-specific null handling behavior remains deterministic and documented.

4. Unsupported join or aggregate semantic request:
Expected outcome: explicit, actionable diagnostic without silent fallback to incorrect behavior.

5. Non-regression query shapes already covered in prior phases:
Expected outcome: no correctness regression under EP-4 envelope changes.

## Matrix Packaging Checklist
1. Create/update [roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md](../../../ROADMAP_EXECUTION_EP4_MATRIX.md).
2. For each mandatory criterion, include concrete evidence references.
3. Mark deferred optional hardening with explicit non-blocking rationale.
4. Record final EP-4 signoff decision in the matrix file.

## Evidence Locations
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP4.md](../discovery/discovery-EXECUTION-EP4.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md](../../../ROADMAP_EXECUTION_EP4_MATRIX.md)

## Discovery Summary
1. EP-4 starts from a deliberately constrained runtime envelope: inner equi hash join and grouped aggregate partial/final flow.
2. Core EP-4 challenge is controlled capability expansion with explicit unsupported semantics diagnostics and non-regression guarantees.
3. The next artifact should be the EP-4 implementation plan that converts this discovery into executable slices and matrix-bound evidence gates.
