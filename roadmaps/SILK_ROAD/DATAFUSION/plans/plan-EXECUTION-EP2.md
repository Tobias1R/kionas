
# Plan: EXECUTION EP-2

## Project coding standards and guidelines
1. tests go under the folder src/tests/<module_name>_tests.rs; avoid inline test additions at the end of non-test files.

## Coding considerations for this phase
1. Any connection to storage must be pooled and reused; no new unpooled connections.

## Goal
Deliver EP-2 Scan Pruning Completion And Scan-Mode Truthfulness by making pruning behavior operationally truthful, telemetry-normalized, and fully verified for fallback safety and correctness.

## Inputs
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP2.md](../discovery/discovery-EXECUTION-EP2.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP2_MATRIX.md](../../../ROADMAP_EXECUTION_EP2_MATRIX.md)
4. Prior phase signoff reference: [roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md](../../../ROADMAP_EXECUTION_EP1_MATRIX.md)

## Scope Boundaries
In scope:
1. Base-scan pruning truth model (requested mode vs effective mode).
2. Low-cardinality fallback reason taxonomy for metadata-pruned scans.
3. Structured pruning decision telemetry contract and validation hooks.
4. Targeted regression coverage for stale/invalid/missing metadata scenarios.
5. EP-2 matrix-ready quality gate evidence packaging.

Out of scope:
1. Join/aggregate feature expansion (EP-4 scope).
2. Memory/throughput optimization broad work (EP-3 scope).
3. Protocol redesign or breaking contract changes.

## Guardrails
1. Preserve worker contract boundaries and task param compatibility. Improve when necessary.
2. Keep behavior deterministic under fallback and metadata unavailability.
3. Prefer explicit observability truth over implicit heuristics.
4. Keep fallback reasons bounded and stable.
5. Treat matrix evidence as strict closure gate for signoff.

## Workstreams

### Workstream A: Scan-Mode Truth Model
Objectives:
1. Establish canonical semantics for requested mode and effective mode.
2. Remove contradictory/legacy scan-mode messaging.
3. Make mode transitions explicit in runtime diagnostics.

Tasks:
1. Define canonical terms:
- requested_mode: value from task params (`scan_mode`)
- effective_mode: runtime mode used for file read decision (`full` or `metadata_pruned`)
- fallback_reason: bounded reason when requested metadata_pruned degrades to effective full
2. Define decision rules for effective_mode:
- when pruning is applied
- when fallback to full occurs
- when fallback is forced by invalid/unavailable metadata
3. Identify and replace legacy contradictory messages with truthful decision logs/events.
4. Add contract notes for non-breaking compatibility with existing task params.

Deliverables:
1. EP-2 truth-model specification section in this plan.
2. Canonical mode decision table.
3. Legacy log/message replacement checklist.

### Workstream B: Fallback Reason Taxonomy
Objectives:
1. Standardize fallback reasons into low-cardinality classes.
2. Ensure stable reason naming for operations and tests.
3. Eliminate ad-hoc reason drift.

Tasks:
1. Define bounded fallback reason set:
- hints_missing
- hints_invalid
- predicate_ast_missing
- delta_pin_mismatch
- delta_version_unavailable
- delta_stats_unavailable
- source_prefix_invalid
- table_prefix_invalid
2. Map each current fallback path to one canonical reason.
3. Define rules for unknown/new fallback paths (safe default class).
4. Define reason-review process to prevent cardinality creep.

Deliverables:
1. Fallback reason catalog and mapping table.
2. Reason naming and evolution policy.

### Workstream C: Telemetry Normalization
Objectives:
1. Expose pruning decisions with a structured, parseable contract.
2. Distinguish requested and effective scan behavior clearly.
3. Allow reliable aggregation of pruning wins vs fallbacks.

Tasks:
1. Define one canonical decision event contract for base scan pruning.
2. Define required dimensions:
- query_id
- stage_id
- task_id
- requested_mode
- effective_mode
- reason
- delta_version_pin
- cache_hit
- total_files
- retained_files
- pruned_files
3. Define emission points and frequency constraints.
4. Define log/event compatibility strategy for incremental rollout.
5. Define acceptance snapshots for observability evidence.

Deliverables:
1. Event schema table and example payloads.
2. Emission boundary checklist.
3. Observability acceptance checklist.

### Workstream D: Regression Coverage Expansion
Objectives:
1. Prove pruning/fallback behavior for EP-2 mandatory scenarios.
2. Prove telemetry truthfulness for mode and reason dimensions.
3. Prevent regressions in deterministic ordering and correctness.

Tasks:
1. Add/expand tests for requested metadata_pruned with effective full by reason class.
2. Add/expand tests for requested metadata_pruned with effective metadata_pruned and file reduction.
3. Add/expand tests for requested full with hints present but ignored behavior.
4. Add tests for stale metadata and invalid hint payloads.
5. Add tests for empty and non-empty table edge behavior under scan-mode permutations.
6. Ensure tests reside in dedicated module test files (no inline end-of-file test additions).

Deliverables:
1. EP-2 scenario-to-test matrix.
2. Targeted command list for reproducible test evidence.

### Workstream E: Verification And Signoff Packaging
Objectives:
1. Convert EP-2 criteria into concrete, auditable evidence.
2. Produce matrix-ready completion data and signoff inputs.
3. Ensure quality gates are captured for phase closure.

Tasks:
1. Build criteria-to-evidence traceability table (see section below).
2. Execute quality gates and capture command evidence:
- cargo fmt --all
- cargo clippy --all-targets --all-features -- -D warnings
- cargo check
3. Run targeted test suite commands for EP-2 scenarios.
4. Populate [roadmaps/ROADMAP_EXECUTION_EP2_MATRIX.md](../../../ROADMAP_EXECUTION_EP2_MATRIX.md) with status and evidence links.
5. Record signoff decision and residual deferred items.

Deliverables:
1. EP-2 command/evidence checklist with outcomes.
2. Matrix update package and signoff recommendation.

## Canonical Mode Decision Table

| Requested Mode | Pruning Eligibility | Metadata Health | Effective Mode | Expected Reason |
|---|---|---|---|---|
| full | any | any | full | mode_full_requested |
| metadata_pruned | false | any | full | ineligible_or_missing_reason or mapped bounded class |
| metadata_pruned | true | healthy | metadata_pruned | pruning_applied |
| metadata_pruned | true | stale/invalid/unavailable | full | mapped bounded fallback reason |

## Criteria-To-Workstream Traceability

| EP-2 Mandatory Criterion | Primary Workstream(s) | Evidence Target |
|---|---|---|
| Pruning flow implemented for targeted scenarios and verified | B, D | scenario tests + deterministic behavior checks |
| Telemetry indicates selected mode and fallback reasons | A, C, D | structured decision event contract + telemetry assertions |
| Stale metadata degrades safely with explicit diagnostics | B, C, D | fallback reason mapping + stale metadata tests |
| Runtime correctness for empty/non-empty table edge cases | D | edge-case scenario coverage and expected outcomes |
| Quality gate evidence captured (fmt/clippy/check) | E | command log evidence and matrix references |

## Sequence And Dependencies
1. Workstream A starts first and establishes vocabulary and decision truth model.
2. Workstream B depends on A for canonical reason classes.
3. Workstream C depends on A and B to emit normalized telemetry.
4. Workstream D depends on A/B/C to validate behavior and telemetry contracts.
5. Workstream E closes phase after A-D evidence is complete.

## Milestones
1. M1: Truth model and fallback taxonomy locked.
2. M2: Structured pruning decision telemetry contract finalized.
3. M3: EP-2 targeted regression scenarios implemented and passing.
4. M4: Matrix evidence package completed.
5. M5: EP-2 signoff decision recorded.

## Risks And Mitigations
1. Risk: telemetry cardinality growth from free-form reasons.
Mitigation: bounded reason taxonomy with review gate for additions.

2. Risk: behavior/telemetry drift between requested and effective mode.
Mitigation: explicit decision table + telemetry assertions in tests.

3. Risk: stale metadata edge cases create hidden fallback loops.
Mitigation: dedicated stale/invalid metadata scenario tests and reason coverage.

4. Risk: correctness regressions in empty/non-empty table paths.
Mitigation: edge-case regression suite tied to requested/effective mode permutations.

## Verification Command Set
1. cargo fmt --all
2. cargo clippy --all-targets --all-features -- -D warnings
3. cargo check

Targeted EP-2 test command slots (to be filled during implementation):
1. worker pruning/fallback decision tests
2. worker scan telemetry contract tests
3. server-to-worker scan hint contract tests

Command log template:

| Command | Status | Evidence |
|---|---|---|
| cargo fmt --all | Pending | attach terminal output reference |
| cargo clippy --all-targets --all-features -- -D warnings | Pending | attach terminal output reference |
| cargo check | Pending | attach terminal output reference |
| targeted EP-2 test command 1 | Pending | attach terminal output reference |
| targeted EP-2 test command 2 | Pending | attach terminal output reference |
| targeted EP-2 test command 3 | Pending | attach terminal output reference |

## E2E SQL Scenario Set (EP-2 Validation)
1. Baseline full scan:
- SELECT id FROM sales.public.users WHERE id > 5;
- Expected: requested_mode=full, effective_mode=full.

2. Eligible metadata-pruned scan:
- SELECT id FROM sales.public.users WHERE id > 5;
- With planner-emitted eligible hints.
- Expected: requested_mode=metadata_pruned, effective_mode=metadata_pruned, retained_files < total_files for seeded dataset.

3. Pin mismatch fallback:
- Same query with stale delta pin.
- Expected: requested_mode=metadata_pruned, effective_mode=full, reason=delta_pin_mismatch.

4. Invalid hints fallback:
- Same query with malformed scan_pruning_hints_json.
- Expected: requested_mode=metadata_pruned, effective_mode=full, reason=hints_invalid.

5. No usable stats fallback:
- Same query where delta stats are absent.
- Expected: requested_mode=metadata_pruned, effective_mode=full, reason=delta_stats_unavailable.

6. Empty-table correctness:
- SELECT id FROM sales.public.empty_users WHERE id > 0;
- Expected: deterministic empty result behavior without incorrect pruning failure classification.

## Matrix Packaging Checklist
1. Create/update [roadmaps/ROADMAP_EXECUTION_EP2_MATRIX.md](../../../ROADMAP_EXECUTION_EP2_MATRIX.md).
2. For each mandatory criterion, include concrete evidence references.
3. Mark deferred optional hardening with explicit non-blocking rationale.
4. Record final EP-2 signoff decision in the matrix file.

## Evidence Locations
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP2.md](../discovery/discovery-EXECUTION-EP2.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP2_MATRIX.md](../../../ROADMAP_EXECUTION_EP2_MATRIX.md)
