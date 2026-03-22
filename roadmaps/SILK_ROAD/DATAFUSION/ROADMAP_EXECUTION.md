# DataFusion Execution Pipeline Roadmap

## Scope
This roadmap governs execution-pipeline evolution for the DataFusion server-planned and worker-executed query path.

In scope:
- Worker execution-pipeline correctness, observability, and resilience.
- Stage/task runtime behavior and exchange/materialization reliability.
- Execution-time performance and runtime envelope growth for supported operators.

Out of scope:
- SQL parser redesign.
- Client protocol redesign.
- Immediate implementation details in this document.

## Why This Roadmap Now
Execution planning and provider integration are in place, but the runtime pipeline now needs a structured hardening sequence to improve reliability, debuggability, and throughput before broader feature expansion.

## Guardrails
1. Preserve worker contract boundaries: worker continues executing Kionas task operators.
2. Keep changes incremental and phase-gated with matrix signoff.
3. Prefer deterministic behavior over optimistic heuristics when correctness is at risk.
4. Degrade gracefully when external systems (storage/network) are unstable.
5. Maintain strict quality gates before phase signoff evidence is accepted.

## Discovery And Planning Artifacts
Roadmap-level inputs:
- Primary discovery basis: `roadmaps/discover/execution_pipeline_discovery.md`

Phase discovery placeholders (future):
- EP-1 discovery: `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP1.md`
- EP-2 discovery: `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP2.md`
- EP-3 discovery: `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP3.md`
- EP-4 discovery: `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP4.md`
- EP-5 discovery: `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP5.md`

Phase plans placeholders (future):
- EP-1 plan: `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP1.md`
- EP-2 plan: `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP2.md`
- EP-3 plan: `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP3.md`
- EP-4 plan: `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md`
- EP-5 plan: `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP5.md`

## Cross-Phase Dependency Model
1. EP-1 is a hard prerequisite for all subsequent phases due to required observability and taxonomy consistency.
2. EP-2 should be completed before EP-3 to ensure performance metrics can distinguish true pruning wins from fallback scans.
3. EP-3 and EP-4 may overlap partially once EP-1 and EP-2 are complete, but signoff remains phase-local.
4. EP-5 depends on stable behavior from EP-1 through EP-4 to avoid masking baseline correctness issues.

## Signoff Model
Each phase requires a dedicated completion matrix and explicit gate decision before the next dependent phase is marked in-progress.

Matrix file conventions:
- EP-1 matrix: `roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md`
- EP-2 matrix: `roadmaps/ROADMAP_EXECUTION_EP2_MATRIX.md`
- EP-3 matrix: `roadmaps/ROADMAP_EXECUTION_EP3_MATRIX.md`
- EP-4 matrix: `roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md`
- EP-5 matrix: `roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md`

Status vocabulary follows `roadmaps/ROADMAP_PHASE_MATRIX_TEMPLATE.md`:
- Done
- Deferred
- Blocked
- Not Started

---

## EP-1: Observability And Error Taxonomy Hardening

### Objectives
1. Establish structured, stable execution error taxonomy across validation, execution, exchange, and artifact paths.
2. Ensure every critical runtime failure includes query/stage/task context for incident triage.
3. Improve explain and runtime diagnostics so execution decisions are inspectable.

### Mandatory Completion Criteria
1. Error codes and categories are standardized for execution-pipeline failure classes.
2. Critical error paths include actionable context fields (query_id, stage_id, task_id where applicable).
3. Runtime decision points relevant to pipeline behavior are observable and testable.
4. Regression coverage verifies classification and context propagation for representative failures.
5. Quality gate evidence captured:
   - cargo fmt --all
   - cargo clippy --all-targets --all-features -- -D warnings
   - cargo check

### Optional Hardening
1. Add compact operator-level trace spans for bottleneck localization.
2. Introduce structured remediation hints for common infra failure classes.

### Discovery Artifacts
- Discovery (future): `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP1.md`
- Plan (future): `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP1.md`

### Main Blockers And Challenges
1. Existing error surfaces may be partially string-based and inconsistent.
2. Deep helper chains can lose context unless propagation is systematically enforced.
3. Taxonomy stabilization must avoid accidental contract breakage for existing consumers.

### Signoff Matrix
- `roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md`

---

## EP-2: Scan Pruning Completion And Scan-Mode Truthfulness

### Objectives
1. Complete metadata-driven pruning behavior for base scan paths.
2. Ensure logs/metrics distinguish true pruning from full-scan fallback.
3. Validate behavior for stale or incomplete pruning metadata.

### Mandatory Completion Criteria
1. Pruning flow is implemented for targeted scan scenarios and verified by tests.
2. Telemetry clearly indicates selected scan mode and fallback reasons.
3. Stale metadata scenarios degrade safely with explicit diagnostics.
4. Runtime behavior remains correct for empty and non-empty table edge cases.
5. Quality gate evidence captured:
   - cargo fmt --all
   - cargo clippy --all-targets --all-features -- -D warnings
   - cargo check

### Optional Hardening
1. Add low-cardinality metrics for pruning selectivity effectiveness.
2. Add canary-style validation mode for pruning decision sanity checks.

### Discovery Artifacts
- Discovery (future): `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP2.md`
- Plan (future): `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP2.md`

### Main Blockers And Challenges
1. Partial pruning support can hide correctness/performance tradeoffs.
2. Metadata freshness and pin consistency can produce subtle fallback behavior.
3. Without strong telemetry, pruning gains are hard to prove and regressions hard to detect.

### Signoff Matrix
- `roadmaps/ROADMAP_EXECUTION_EP2_MATRIX.md`

---

## EP-3: Memory And Throughput Optimization

### Objectives
1. Reduce peak memory pressure in key execution stages.
2. Improve throughput predictability under mixed query workloads.
3. Add measurable baselines for memory and operator throughput.

### Mandatory Completion Criteria
1. Baseline and post-change performance evidence is captured for representative workloads.
2. Memory-intensive paths are optimized with no correctness regressions.
3. Throughput variance is reduced or bounded for selected benchmark profiles.
4. Runtime telemetry surfaces memory/throughput indicators needed for operations.
5. Quality gate evidence captured:
   - cargo fmt --all
   - cargo clippy --all-targets --all-features -- -D warnings
   - cargo check

### Optional Hardening
1. Add stress suites for large-batch and skew-heavy workloads.
2. Add guardrails for adaptive batching under constrained memory.

### Discovery Artifacts
- Discovery (future): `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP3.md`
- Plan (future): `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP3.md`

### Main Blockers And Challenges
1. Existing batch materialization patterns can amplify memory usage.
2. Join/aggregate heavy workloads can produce non-linear pressure.
3. Optimization work must remain incremental to preserve debuggability.

### Signoff Matrix
- `roadmaps/ROADMAP_EXECUTION_EP3_MATRIX.md`

---

## EP-4: Expanded Join/Aggregate Runtime Envelope

### Objectives
1. Broaden supported join semantics while maintaining deterministic correctness.
2. Expand aggregate coverage and strengthen edge-case handling.
3. Improve runtime behavior parity with planner intent for supported patterns.

### Mandatory Completion Criteria
1. Targeted join/aggregate capabilities are explicitly enumerated and verified.
2. Null/type edge-case behavior is tested and documented in matrix evidence.
3. Unsupported semantics fail clearly with actionable diagnostics.
4. Added runtime support does not regress existing covered scenarios.
5. Quality gate evidence captured:
   - cargo fmt --all
   - cargo clippy --all-targets --all-features -- -D warnings
   - cargo check

### Optional Hardening
1. Add compatibility matrix for query-shape coverage.
2. Add shadow-run checks for selected aggregate boundary scenarios.

### Discovery Artifacts
- Discovery (future): `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP4.md`
- Plan (future): `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md`

### Main Blockers And Challenges
1. Join semantic expansion can quickly increase complexity and risk.
2. Aggregate partial/final behavior introduces correctness-sensitive edge cases.
3. Scope control is required to avoid overloading a single phase.

### Signoff Matrix
- `roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md`

---

## EP-5: Distributed Resilience And Artifact Integrity Validation

### Objectives
1. Improve runtime resilience to transient storage/network failures.
2. Harden exchange/result artifact integrity checks and recovery paths.
3. Establish deterministic retry/remediation behavior for distributed execution.

### Mandatory Completion Criteria
1. Retry and failure-handling semantics are clearly defined and validated.
2. Artifact integrity checks exist at critical exchange/materialization boundaries.
3. Fault-injection scenarios verify graceful degradation and recoverability.
4. Operational diagnostics allow fast root-cause identification for distributed failures.
5. Quality gate evidence captured:
   - cargo fmt --all
   - cargo clippy --all-targets --all-features -- -D warnings
   - cargo check

### Optional Hardening
1. Add deeper checksum/manifest validation tradeoff analysis.
2. Add bounded backoff strategy tuning guidance for unstable environments.

### Discovery Artifacts
- Discovery (future): `roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP5.md`
- Plan (future): `roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP5.md`

### Main Blockers And Challenges
1. Artifact dependency chains are sensitive to partial failures.
2. Stronger integrity checks can add measurable latency overhead.
3. Retry semantics must prevent duplicate effects while preserving liveness.

### Signoff Matrix
- `roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md`

---

## Gate Checklist (All Phases)
1. All mandatory criteria for the phase are marked Done in its matrix.
2. Every Done criterion has concrete evidence references.
3. Deferred items are explicitly marked non-blocking with rationale.
4. Final signoff decision is recorded in the phase matrix.
5. No dependent phase advances without prerequisite phase signoff.

## Environment And Parameters (Roadmap-Level Tracking)
Populate per phase during implementation and matrix signoff:
- Required environment variables (name, crate/module usage, expected effect).
- Required execution parameters/flags (name, default, tuning guidance, safety notes).
- Failure mode expectations when variables/parameters are missing or invalid.

## Change Control Notes
1. Keep this roadmap synchronized with future EP discovery and plan artifacts.
2. Update dependencies if scope shifts during discovery.
3. Do not mark phase completion in this file; completion authority is the per-phase matrix.
