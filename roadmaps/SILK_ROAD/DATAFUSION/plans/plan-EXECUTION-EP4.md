# Plan: EXECUTION EP-4

## Project coding standards and guidelines
1. Tests go under src/tests/<module_name>_tests.rs; avoid inline test additions at the end of non-test files.

## Coding considerations for this phase
1. Any connection to storage must be pooled and reused; no new unpooled connections.
2. Keep deterministic output ordering across join and aggregate operator paths.
3. Expand capability through phase-scoped, auditable slices rather than broad operator rewrites.

## Goal
Deliver EP-4 Expanded Join/Aggregate Runtime Envelope by broadening supported runtime semantics in controlled slices, hardening unsupported-semantics diagnostics, and producing matrix-grade evidence for null/type edge cases and non-regression behavior.

## Inputs
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP4.md](../discovery/discovery-EXECUTION-EP4.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md](../../../ROADMAP_EXECUTION_EP4_MATRIX.md)
4. Prior phase signoff reference: [roadmaps/ROADMAP_EXECUTION_EP3_MATRIX.md](../../../ROADMAP_EXECUTION_EP3_MATRIX.md)

## Scope Boundaries
In scope:
1. Runtime join and aggregate capability expansion inside existing worker execution contract.
2. Alias handling corrections in join-related projection/select path with dedicated tests.
3. Join and aggregate null/type edge-case behavior verification and diagnostics hardening.
4. Planner-to-runtime contract validation for join and aggregate operator metadata.
5. EP-4 matrix-ready quality gate and verification evidence packaging.

Out of scope:
1. SQL parser redesign.
2. Client protocol redesign.
3. Broad distributed resilience redesign (EP-5 scope).
4. Non-join/non-aggregate operator family expansion beyond EP-4 needs.

## Guardrails
1. Preserve worker contract boundaries: worker continues executing Kionas task operators.
2. Keep changes incremental and phase-gated with matrix signoff.
3. Prefer deterministic behavior over optimistic heuristics when correctness is at risk.
4. Degrade gracefully when unsupported semantics are requested.
5. Maintain strict quality gates before phase signoff evidence is accepted.

## Workstreams

### Workstream A: Join Envelope And Alias Correctness
Objectives:
1. Define and implement EP-4 join envelope slices with deterministic behavior.
2. Fix alias handling in select/join paths and prove correctness with focused tests.
3. Ensure unsupported join semantics fail with stable diagnostics.

Tasks:
1. Implement alias handling fix in join-related select/projection flow and add dedicated tests.
2. Enumerate current join behavior and EP-4 target/deferred semantics matrix.
3. Add join semantics validation/diagnostics for unsupported requests.
4. Extend join regression pack for deterministic order, schema naming, and alias coverage.

Deliverables:
1. Alias handling fix with focused regression tests.
2. Join capability matrix (supported vs deferred).
3. Unsupported-join diagnostics contract and negative-path tests.

### Workstream B: Aggregate Envelope And Edge Semantics
Objectives:
1. Expand aggregate runtime envelope within EP-4 phase boundaries.
2. Lock null/type edge-case behavior with explicit tests and diagnostics.

Tasks:
1. Enumerate aggregate function/input constraints and finalization behavior.
2. Implement EP-4 aggregate edge-case handling slices (null, mixed-type, empty-group, partial/final invariants).
3. Add or update aggregate diagnostics for unsupported aggregate semantics.

Deliverables:
1. Aggregate capability and edge-case matrix.
2. Aggregate edge-case regression suite.
3. Unsupported-aggregate diagnostics contract and negative-path tests.

### Workstream C: Planner-Runtime Contract And Non-Regression
Objectives:
1. Keep planner translation, physical validation, and runtime behavior aligned during envelope growth.
2. Prevent silent semantic drift and preserve existing covered scenarios.

Tasks:
1. Validate `PhysicalJoinSpec` and `PhysicalAggregateSpec` assumptions across translator, validator, and runtime executor.
2. Add contract checks where needed for operator ordering, required fields, key/expr validity, and error paths.
3. Execute non-regression query-shape pack covering previously supported scenarios.

Deliverables:
1. Contract invariant checklist with evidence references.
2. Non-regression query-shape verification pack and pass evidence.

### Workstream D: Verification And Signoff Packaging
Objectives:
1. Convert EP-4 criteria into auditable matrix evidence.
2. Execute quality gates and targeted verification commands.
3. Finalize EP-4 matrix and signoff decision.

Tasks:
1. Build criteria-to-workstream traceability table.
2. Run quality gates and capture evidence:
- cargo fmt --all
- cargo clippy --all-targets --all-features -- -D warnings
- cargo check
3. Run EP-4 targeted join/aggregate verification command set.
4. Populate [roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md](../../../ROADMAP_EXECUTION_EP4_MATRIX.md).
5. Record signoff decision and deferred optional items.

Deliverables:
1. EP-4 command/evidence package.
2. Matrix update and signoff recommendation.

## Canonical Mode Decision Table

| Domain | Mode | Entry Condition | Runtime Behavior | Deferred/Failure Behavior | Evidence Target |
|---|---|---|---|---|---|
| Join | inner_equi | `PhysicalOperator::HashJoin` with non-empty keys and valid right relation | Deterministic hash join with chunked fanout flush and duplicate-right-column rename handling | N/A | join correctness and order tests |
| Join | unsupported_join_semantic | Join semantic requested outside EP-4 supported set | N/A | Fail fast with actionable diagnostic identifying unsupported semantic | negative-path diagnostics tests |
| Aggregate | partial_final_grouped | Aggregate partial/final pair present with valid expressions and outputs | Deterministic grouped partial/final execution with supported aggregate functions | N/A | aggregate function and grouping tests |
| Aggregate | unsupported_aggregate_semantic | Unsupported aggregate input/shape encountered | N/A | Fail fast with actionable diagnostic identifying unsupported aggregate behavior | negative-path diagnostics tests |

## Criteria-To-Workstream Traceability

| EP-4 Mandatory Criterion | Primary Workstream(s) | Evidence Target |
|---|---|---|
| Targeted join/aggregate capabilities are explicitly enumerated and verified | A, B, D | capability matrix and focused verification tests |
| Null/type edge-case behavior is tested and documented in matrix evidence | B, D | edge-case test outputs and matrix references |
| Unsupported semantics fail clearly with actionable diagnostics | A, B, C, D | negative-path diagnostics tests and error contract mapping |
| Added runtime support does not regress existing covered scenarios | C, D | non-regression test pack and comparison evidence |
| Quality gate evidence captured (fmt/clippy/check) | D | command outputs and matrix references |

## Sequence And Dependencies
1. Workstream A and Workstream B start first to lock capability boundaries and edge-case behavior.
2. Alias-fix slice is first implementation priority in Workstream A before broader join expansion.
3. Workstream C depends on A/B capability decisions to lock planner-runtime invariants and non-regression checks.
4. Workstream D depends on A-C evidence to package matrix-grade closure and signoff.

## Milestones
1. M1: Alias handling fix and join capability matrix completed.
2. M2: Aggregate capability and null/type edge-case matrix completed.
3. M3: Planner-runtime invariant checks and non-regression suite completed.
4. M4: EP-4 command/evidence package and matrix signoff recommendation completed.

## Risks And Mitigations
1. Risk: Join envelope changes regress row cardinality or output ordering.
Mitigation: deterministic join-order regression suite and explicit cardinality checks on representative fanout patterns.

2. Risk: Alias handling fix introduces projection/schema naming regressions.
Mitigation: dedicated alias-focused tests on join/select paths before and after join execution.

3. Risk: Aggregate edge semantics drift between partial and final stages.
Mitigation: phase-scoped aggregate edge-case matrix with explicit partial/final merge invariants and targeted tests.

4. Risk: Planner/runtime contract drift causes silent semantic mismatch.
Mitigation: explicit invariant checklist and validation gates for join/aggregate operator specs.

## Verification Command Set
1. cargo fmt --all
2. cargo clippy --all-targets --all-features -- -D warnings
3. cargo check

Targeted EP-4 command slots (to be filled during implementation):
1. join alias handling test suite
2. join envelope regression suite
3. aggregate edge-case regression suite
4. unsupported semantics diagnostics test suite
5. non-regression query-shape suite

Command log template:

| Command | Status | Evidence |
|---|---|---|
| cargo fmt --all | Pending | attach terminal output reference |
| cargo clippy --all-targets --all-features -- -D warnings | Pending | attach terminal output reference |
| cargo check | Pending | attach terminal output reference |
| join alias handling test suite | Pending | attach terminal output reference |
| join envelope regression suite | Pending | attach terminal output reference |
| aggregate edge-case regression suite | Pending | attach terminal output reference |
| unsupported semantics diagnostics test suite | Pending | attach terminal output reference |
| non-regression query-shape suite | Pending | attach terminal output reference |

## E2E Scenarios
1. Deterministic inner equi-join with duplicate right column names and aliased selected columns.
Expected outcome: stable row order, deterministic column naming, and alias-resolved output schema.

2. Join with no matching rows.
Expected outcome: empty output with expected schema contract preserved.

3. Grouped aggregation with COUNT, SUM, and AVG across null-containing inputs.
Expected outcome: function-specific null handling remains deterministic and documented.

4. Unsupported join or aggregate semantic request.
Expected outcome: explicit, actionable diagnostic without silent fallback to incorrect behavior.

5. Non-regression query shapes already covered in prior phases.
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

## Execution Record

### Workstream A Execution Record (Iteration 2)

Status:
1. In progress.
2. Alias-handling implementation slice has initial validation evidence captured.

Validation evidence:
1. `cargo test -p worker applies_projection_raw_identifier_with_alias`: completed successfully.
- worker lib target: 1 passed, 0 failed (`services::query_execution::tests::applies_projection_raw_identifier_with_alias`).
- worker bin target: 1 passed, 0 failed (`services::query_execution::tests::applies_projection_raw_identifier_with_alias`).
2. `cargo test -p worker parses_projection_identifier_with_alias`: completed successfully.
- worker lib target: 1 passed, 0 failed (`services::query_execution::tests::parses_projection_identifier_with_alias`).
- worker bin target: 1 passed, 0 failed (`services::query_execution::tests::parses_projection_identifier_with_alias`).
3. `cargo test -p worker join_rejects_spec_with_empty_keys`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::join::tests::join_rejects_spec_with_empty_keys`).
- worker bin target: 1 passed, 0 failed (`execution::join::tests::join_rejects_spec_with_empty_keys`).
4. `cargo test -p worker join_rejects_spec_with_empty_right_relation`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::join::tests::join_rejects_spec_with_empty_right_relation`).
- worker bin target: 1 passed, 0 failed (`execution::join::tests::join_rejects_spec_with_empty_right_relation`).
5. `cargo test -p worker aggregate_rejects_predicate_input_expression`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::aggregate::tests::aggregate_rejects_predicate_input_expression`).
- worker bin target: 1 passed, 0 failed (`execution::aggregate::tests::aggregate_rejects_predicate_input_expression`).
6. Alias, join, and aggregate diagnostics tests are located in:
- `worker/src/tests/services_query_execution_tests.rs`
- `worker/src/tests/execution_join_tests.rs`
- `worker/src/tests/execution_aggregate_mod_tests.rs`

Interpretation for Workstream A:
1. The select/projection alias path now has direct regression coverage for raw identifier alias parsing and application behavior.
2. This satisfies the first executable slice defined in EP-4 Workstream A and de-risks join-envelope evolution steps that depend on alias-resolved projection behavior.

Updated pending Workstream A items:
1. Complete join capability matrix evidence for supported vs deferred semantics.
2. Add additional unsupported-semantics diagnostics coverage beyond current join/aggregate negative-path set.
3. Extend join regression coverage for additional schema/alias boundary cases tied to EP-4 criteria.

### Workstream B Execution Record (Iteration 1)

Status:
1. In progress.
2. Aggregate null/type edge-case evidence capture now includes both null semantics and input-type diagnostics.

Validation evidence:
1. `cargo test -p worker aggregate_count_and_sum_apply_null_semantics_per_group`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::aggregate::tests::aggregate_count_and_sum_apply_null_semantics_per_group`).
- worker bin target: 1 passed, 0 failed (`execution::aggregate::tests::aggregate_count_and_sum_apply_null_semantics_per_group`).
2. `cargo test -p worker aggregate_rejects_non_numeric_input_type_for_sum`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::aggregate::tests::aggregate_rejects_non_numeric_input_type_for_sum`).
- worker bin target: 1 passed, 0 failed (`execution::aggregate::tests::aggregate_rejects_non_numeric_input_type_for_sum`).
3. Aggregate edge-case tests are located in:
- `worker/src/tests/execution_aggregate_mod_tests.rs`

Interpretation for Workstream B:
1. Grouped null semantics for COUNT(column) and SUM(column) are now explicitly covered and confirmed deterministic for mixed null/non-null inputs.
2. Unsupported non-numeric aggregate input types now fail with explicit diagnostics (`unsupported aggregate input type Utf8`).
3. This completes the minimum null/type edge-case evidence set required by the EP-4 mandatory criterion.

Updated pending Workstream B items:
1. Extend aggregate edge-case matrix coverage (empty-group/finalization invariants) as additional hardening evidence.

### Workstream C Execution Record (Iteration 1)

Status:
1. In progress.
2. Non-regression verification now covers deterministic ordering, duplicate-right-column naming, and empty-match schema preservation.

Validation evidence:
1. `cargo test -p worker join_keeps_deterministic_match_order`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::join::tests::join_keeps_deterministic_match_order`).
- worker bin target: 1 passed, 0 failed (`execution::join::tests::join_keeps_deterministic_match_order`).
2. `cargo test -p worker join_renames_duplicate_right_columns`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::join::tests::join_renames_duplicate_right_columns`).
- worker bin target: 1 passed, 0 failed (`execution::join::tests::join_renames_duplicate_right_columns`).
3. `cargo test -p worker join_with_no_matches_returns_empty_batch_with_expected_schema`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::join::tests::join_with_no_matches_returns_empty_batch_with_expected_schema`).
- worker bin target: 1 passed, 0 failed (`execution::join::tests::join_with_no_matches_returns_empty_batch_with_expected_schema`).

Interpretation for Workstream C:
1. EP-4 join execution preserves deterministic expansion order for multi-match fanout scenarios.
2. Duplicate-right-column naming behavior remains stable via deterministic table-prefixed schema output.
3. Empty-match join execution preserves expected output schema without row regressions.
4. This completes the current non-regression slice for covered join scenarios in EP-4.

Updated pending Workstream C items:
1. Add representative planner-runtime contract checks for join/aggregate invariants.

### Workstream D Execution Record (Iteration 1)

Status:
1. Not started.
2. EP-4 plan scaffold is created from discovery and ready for implementation slices.

Initial pending EP-4 items:
1. Continue Workstream A from alias slice into full join envelope matrix and diagnostics evidence.
2. Extend Workstream B aggregate edge-case matrix hardening coverage.
3. Continue Workstream C planner-runtime invariants and non-regression suite.
4. Package Workstream D evidence and finalize EP-4 matrix signoff decision.

### Workstream D Execution Record (Iteration 2)

Status:
1. In progress.
2. EP-4 quality-gate command evidence is now captured.

Validation evidence:
1. `cargo fmt --all`: completed successfully (no formatting changes reported).
2. `cargo clippy --all-targets --all-features -- -D warnings`: completed successfully.
3. `cargo check`: completed successfully.

Interpretation for Workstream D:
1. Required EP-4 quality-gate commands are now executed with successful outcomes.
2. Remaining Workstream D closure depends on final matrix criterion reconciliation and signoff update.
