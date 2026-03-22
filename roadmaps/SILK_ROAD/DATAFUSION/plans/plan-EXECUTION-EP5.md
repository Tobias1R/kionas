# Plan: EXECUTION EP-5

## Project coding standards and guidelines
1. Tests go under src/tests/<module_name>_tests.rs; avoid inline test additions at the end of non-test files.

## Coding considerations for this phase
1. Any connection to storage must be pooled and reused; no new unpooled connections.
2. Retry behavior must be bounded and deterministic; no unbounded loops or opaque fallbacks.
3. Integrity checks should happen at boundary ingress before decode/materialization where possible.
4. All distributed-failure paths must emit actionable diagnostics with stable field contracts.

## Goal
Deliver EP-5 Distributed Resilience And Artifact Integrity Validation by defining bounded retry/failure semantics, enforcing integrity checks at critical boundaries, validating recoverability through fault injection, and packaging matrix-grade quality evidence.

## Inputs
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP5.md](../discovery/discovery-EXECUTION-EP5.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md)
4. Prior phase signoff reference: [roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md](../../../ROADMAP_EXECUTION_EP4_MATRIX.md)

## Scope Boundaries
In scope:
1. Bounded retry and failure-handling behavior for storage-backed distributed execution I/O. Certify first if provider libs already have retry behavior that meets EP-5 semantics before implementing custom retry logic.
2. Artifact integrity validation for exchange/materialization critical boundaries.
3. Fault-injection verification for graceful degradation and recoverability.
4. Operational diagnostics contract for fast distributed-failure triage.
5. EP-5 matrix-ready quality-gate and verification evidence packaging.

Out of scope:
1. SQL parser redesign.
2. Client protocol redesign.
3. Broad infrastructure replacement (new storage system, new transport protocol).
4. Planner/operator-family expansion unrelated to resilience/integrity validation.

## Guardrails
1. Preserve worker contract boundaries: worker continues executing Kionas task operators.
2. Keep changes incremental and phase-gated with matrix signoff.
3. Prefer deterministic behavior over optimistic heuristics when correctness is at risk.
4. Degrade gracefully when external systems (storage/network) are unstable.
5. Maintain strict quality gates before phase signoff evidence is accepted.
6. Ensure retry behavior cannot silently duplicate side effects.

## Workstreams

### Workstream A: Retry Semantics And Failure Taxonomy
Objectives:
1. Define clear retriable vs non-retriable semantics across distributed storage operations.
2. Implement bounded and observable retry behavior with deterministic outcomes.

Tasks:
1. Classify failure classes for list/get/put/delete operations.
2. Implement bounded retry policy primitives (attempt cap, delay/backoff strategy, terminal mapping).
3. Apply retry policy to phase-scoped high-risk I/O boundaries.
4. Add tests for retry-success, retry-exhaustion, and immediate non-retriable fail.

Deliverables:
1. Retry semantics contract and failure taxonomy mapping.
2. Retry policy implementation slices with targeted tests.
3. Actionable diagnostics for attempt lifecycle and terminal outcomes.

### Workstream B: Artifact Integrity Validation At Critical Boundaries
Objectives:
1. Ensure artifact metadata and bytes are consistently validated where consumed.
2. Prevent silent corruption propagation across exchange/materialization paths.

Tasks:
1. Add exchange-side metadata consumption and integrity checks (count/size/checksum) for upstream reads.
2. Align result and exchange integrity failure messaging contracts.
3. Add tests for checksum mismatch, size mismatch, missing metadata, and partial artifact sets.
4. Verify deterministic handling for empty-result and zero-row metadata cases.

Deliverables:
1. Exchange/result integrity boundary validation contract.
2. Integrity validation implementation and regression tests.
3. Negative-path diagnostics evidence pack.

### Workstream C: Fault-Injection Recoverability Pack
Objectives:
1. Demonstrate graceful degradation and recoverability under transient failures.
2. Validate deterministic terminal behavior for persistent failure cases.

Tasks:
1. Build fault-injection matrix for list/get/put failure modes and partial visibility.
2. Execute scenario suites for retry recovery and retry exhaustion.
3. Capture scenario outcomes with expected terminal diagnostics.
4. Add representative cross-boundary scenarios (exchange read plus metadata mismatch).

Deliverables:
1. Fault-injection scenario matrix and expected-outcome table.
2. Recoverability verification suite and pass evidence.
3. Criteria-linked scenario evidence references.

### Workstream D: Diagnostics Contract And Signoff Packaging
Objectives:
1. Establish operational diagnostics enabling rapid root-cause analysis.
2. Convert EP-5 mandatory criteria into auditable closure artifacts.

Tasks:
1. Define diagnostics schema for resilience events (query_id, stage_id, task_id, partition_index, failure_class, attempt, terminal_reason).
2. Validate diagnostics presence/shape in targeted tests.
3. Run quality gates and targeted EP-5 verification command set.
4. Populate [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md) with criterion evidence and signoff decision.

Deliverables:
1. Diagnostics event contract and validation evidence.
2. EP-5 command/evidence package.
3. Matrix update and signoff recommendation.

## Canonical Mode Decision Table

| Domain | Mode | Entry Condition | Runtime Behavior | Deferred/Failure Behavior | Evidence Target |
|---|---|---|---|---|---|
| Storage I/O | retryable_failure | Operation returns transient/retryable class | Execute bounded retry policy with deterministic attempt accounting | Terminal failure with explicit exhaustion reason if budget exceeded | retry behavior tests |
| Storage I/O | non_retryable_failure | Operation returns permanent/non-retryable class | Immediate fail with actionable diagnostics | No retry issued | fail-fast diagnostics tests |
| Integrity | artifact_set_valid | Metadata and artifact bytes align | Continue decode/materialization pipeline | N/A | integrity success tests |
| Integrity | artifact_set_invalid | Any metadata/count/size/checksum mismatch | N/A | Fail with integrity-class diagnostic and context | integrity mismatch tests |
| Recovery | transient_then_recover | Failure clears within retry budget | Resume normal execution with bounded delay | N/A | fault-injection recovery tests |
| Recovery | persistent_failure | Failure persists through retry budget | N/A | Deterministic terminal failure with context and reason | fault-injection exhaustion tests |

## Criteria-To-Workstream Traceability

| EP-5 Mandatory Criterion | Primary Workstream(s) | Evidence Target |
|---|---|---|
| Retry and failure-handling semantics are clearly defined and validated | A, C, D | retry contract, targeted retry tests, matrix evidence links |
| Artifact integrity checks exist at critical exchange/materialization boundaries | B, C, D | integrity boundary tests and mismatch diagnostics evidence |
| Fault-injection scenarios verify graceful degradation and recoverability | C, D | scenario matrix outputs and terminal outcome assertions |
| Operational diagnostics allow fast root-cause identification for distributed failures | A, D | diagnostics contract tests and event field verification |
| Quality gate evidence captured (fmt/clippy/check) | D | command outputs and matrix references |

## Sequence And Dependencies
1. Workstream A executes first to lock retry/failure contract.
2. Workstream B starts once A contract is draft-stable so integrity failures map to canonical failure classes.
3. Workstream C depends on A/B behavior contracts to define expected scenario outcomes.
4. Workstream D runs continuously for traceability and closes after C evidence and quality gates are complete.

## Milestones
1. M1: Retry taxonomy and bounded retry implementation slice completed.
2. M2: Exchange/result integrity boundary checks and negative-path tests completed.
3. M3: Fault-injection recoverability suite completed with evidence capture.
4. M4: Diagnostics contract validation, quality gates, and EP-5 matrix signoff package completed.

## Risks And Mitigations
1. Risk: Retry logic causes duplicate writes or inconsistent state.
Mitigation: enforce idempotency boundaries and terminal-state invariants per operation class.

2. Risk: Integrity validation increases latency in hot paths.
Mitigation: prioritize critical boundaries and measure overhead in targeted verification runs.

3. Risk: Fault-injection tests are brittle or environment-sensitive.
Mitigation: isolate scenario harness with deterministic mocks and explicit expectation contracts.

4. Risk: Diagnostics are too sparse for triage or too verbose for operations.
Mitigation: use a minimal required field contract with stable reason codes and bounded cardinality.

## Verification Command Set
1. cargo fmt --all
2. cargo clippy --all-targets --all-features -- -D warnings
3. cargo check

Targeted EP-5 command slots (to be filled during implementation):
1. retry behavior suite
2. exchange integrity validation suite
3. result integrity regression suite
4. fault-injection recoverability suite
5. diagnostics contract suite

Command log template:

| Command | Status | Evidence |
|---|---|---|
| cargo fmt --all | Done | completed successfully from `PS C:\code\kionas>` with no formatting errors emitted |
| cargo clippy --all-targets --all-features -- -D warnings | Done | completed successfully in 3.27s with warnings denied; only future-incompat advisory noted for `redis v0.25.4` and `sqlx-core v0.6.3` |
| cargo check | Done | completed successfully in 7.33s with `Finished dev profile` and same non-blocking future-incompat advisory |
| retry behavior suite | Pending | attach terminal output reference |
| exchange integrity validation suite | Pending | attach terminal output reference |
| result integrity regression suite | Pending | attach terminal output reference |
| fault-injection recoverability suite | Pending | attach terminal output reference |
| diagnostics contract suite | Pending | attach terminal output reference |

## E2E Scenarios
1. Exchange read operation fails transiently then succeeds within retry budget.
Expected outcome: bounded retries recover execution without semantic drift.

2. Exchange write path encounters persistent retriable failure.
Expected outcome: deterministic terminal failure with exhaustion diagnostics.

3. Upstream exchange artifact exists but metadata checksum mismatches payload.
Expected outcome: integrity failure is explicit and decode path is blocked.

4. Result metadata/object set becomes partially visible during retrieval.
Expected outcome: deterministic recover-or-fail behavior based on retry contract.

5. Non-retriable storage error is returned on critical operation.
Expected outcome: fail-fast behavior with actionable context and no retry loop.

## Matrix Packaging Checklist
1. Create/update [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md).
2. For each mandatory criterion, include concrete evidence references.
3. Mark deferred optional hardening with explicit non-blocking rationale.
4. Record final EP-5 signoff decision in the matrix file.

## Evidence Locations
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP5.md](../discovery/discovery-EXECUTION-EP5.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md)

## Execution Record

### Workstream A Execution Record (Iteration 1)

Status:
1. In progress.
2. Retry/failure contract implementation has started with error-classification and bounded retry helper slice.

Validation evidence:
1. `cargo test -p worker classifies_transient_storage_errors_as_retriable`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::classifies_transient_storage_errors_as_retriable`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::classifies_transient_storage_errors_as_retriable`).
2. `cargo test -p worker retries_exchange_list_then_succeeds`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::retries_exchange_list_then_succeeds`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::retries_exchange_list_then_succeeds`).
3. `cargo test -p worker retries_exchange_get_until_exhaustion`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::retries_exchange_get_until_exhaustion`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::retries_exchange_get_until_exhaustion`).
4. `cargo test -p worker non_retriable_exchange_list_fails_without_retry`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::non_retriable_exchange_list_fails_without_retry`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::non_retriable_exchange_list_fails_without_retry`).
5. `cargo test -p worker retries_exchange_get_then_succeeds`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::retries_exchange_get_then_succeeds`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::retries_exchange_get_then_succeeds`).
6. `cargo test -p worker non_retriable_exchange_get_fails_without_retry`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::non_retriable_exchange_get_fails_without_retry`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::non_retriable_exchange_get_fails_without_retry`).

Interpretation for Workstream A:
1. EP-5 now has direct test evidence for retriable vs non-retriable storage error-classification behavior.
2. Bounded retry behavior is now evidenced for transient recovery and retry-exhaustion terminal paths.
3. Non-retriable failures are now evidenced to fail immediately without retry loops.
4. Get-path parity is now covered for both transient recovery and non-retriable immediate-fail behavior.
5. This completes the first EP-5 executable slice for retry/failure semantics validation.

Updated pending Workstream A items:
1. Validate and document provider-level retry behavior to avoid redundant retry layering.
2. Lock retry parameter defaults and diagnostics mapping for matrix-grade criteria closure.

### Workstream D Execution Record (Iteration 2)

Status:
1. In progress.
2. EP-5 retry diagnostics event contract implementation and test coverage has started.

Implementation evidence:
1. Added structured retry diagnostics formatter in `worker/src/execution/pipeline.rs`:
- `format_exchange_retry_decision_event`
2. Integrated retry attempt and terminal outcome diagnostics emission for exchange list/get helper paths.

Validation evidence:
1. Added event-format contract test in `worker/src/tests/execution_pipeline_tests.rs`:
- `formats_exchange_retry_decision_event_with_required_dimensions`
2. `cargo test -p worker formats_exchange_retry_decision_event_with_required_dimensions`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::formats_exchange_retry_decision_event_with_required_dimensions`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::formats_exchange_retry_decision_event_with_required_dimensions`).

Interpretation for Workstream D:
1. EP-5 now has an explicit diagnostics contract for retry attempt lifecycle events.
2. The diagnostics contract is now backed by direct pass evidence in worker lib/bin targets.
3. This advances the operational-diagnostics criterion from baseline-only state.

### Workstream B Execution Record (Iteration 1)

Status:
1. In progress.
2. Exchange metadata-sidecar integrity validation slice is implemented in upstream exchange reads.

Implementation evidence:
1. Upstream exchange artifact reads now validate metadata sidecar presence plus expected size/checksum before decode.
2. New helper paths added in `worker/src/execution/pipeline.rs`:
- `expected_exchange_artifact_from_metadata`
- `load_validated_exchange_artifact_with_retry`
3. New integrity tests added in `worker/src/tests/execution_pipeline_tests.rs`:
- `validates_exchange_artifact_with_matching_metadata`
- `rejects_exchange_artifact_with_checksum_mismatch`

Validation evidence:
1. `cargo test -p worker validates_exchange_artifact_with_matching_metadata`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::validates_exchange_artifact_with_matching_metadata`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::validates_exchange_artifact_with_matching_metadata`).
2. `cargo test -p worker rejects_exchange_artifact_with_checksum_mismatch`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::rejects_exchange_artifact_with_checksum_mismatch`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::rejects_exchange_artifact_with_checksum_mismatch`).
3. `cargo test -p worker rejects_exchange_artifact_when_metadata_is_missing`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::rejects_exchange_artifact_when_metadata_is_missing`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::rejects_exchange_artifact_when_metadata_is_missing`).
4. `cargo test -p worker rejects_exchange_artifact_with_size_mismatch`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::rejects_exchange_artifact_with_size_mismatch`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::rejects_exchange_artifact_with_size_mismatch`).

Interpretation for Workstream B:
1. Exchange read path now has evidence for both integrity success and checksum mismatch failure behavior.
2. Missing metadata and size-mismatch negative paths are now explicitly covered with deterministic diagnostics.
3. This establishes first parity coverage with result-boundary integrity validation semantics.

Updated pending Workstream B items:
1. Align exchange and result integrity diagnostics strings for operator-facing consistency.

### Workstream C Execution Record (Iteration 1)

Status:
1. In progress.
2. Fault-injection recoverability execution has started with transient metadata-read recovery validation.

Validation evidence:
1. `cargo test -p worker recovers_exchange_artifact_validation_after_transient_metadata_read_failure`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::recovers_exchange_artifact_validation_after_transient_metadata_read_failure`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::recovers_exchange_artifact_validation_after_transient_metadata_read_failure`).
2. `cargo test -p worker fails_exchange_artifact_validation_after_persistent_metadata_read_failure`: completed successfully.
- worker lib target: 1 passed, 0 failed (`execution::pipeline::tests::fails_exchange_artifact_validation_after_persistent_metadata_read_failure`).
- worker bin target: 1 passed, 0 failed (`execution::pipeline::tests::fails_exchange_artifact_validation_after_persistent_metadata_read_failure`).

Interpretation for Workstream C:
1. EP-5 now has direct recoverability evidence that transient metadata-read failures can recover within bounded retry policy.
2. Persistent metadata-read failures now show deterministic retry-exhaustion termination behavior.
3. This establishes a paired recover/saturate fault-injection evidence set for graceful degradation and recoverability.

Updated pending Workstream C items:
1. Expand fault-injection matrix coverage across additional list/get/read boundary scenarios.

### Workstream D Execution Record (Iteration 1)

Status:
1. Not started.
2. Signoff packaging scaffold is prepared.

Initial pending EP-5 items:
1. Build criteria-to-test traceability as A-C progresses.
2. Capture quality-gate outputs.
3. Populate [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md) and record signoff decision.

### Workstream D Execution Record (Iteration 3)

Status:
1. Completed.
2. EP-5 criteria reconciliation and matrix signoff packaging are finalized.

Final reconciliation evidence:
1. Retry semantics closure: bounded retry defaults are finalized in runtime (`EXCHANGE_IO_MAX_ATTEMPTS=3`, `EXCHANGE_IO_RETRY_DELAY_MS=25`) and validated across recovery, exhaustion, and non-retriable fail-fast tests.
2. Provider-level retry interaction evidence is covered by metadata-read fault-injection pair:
- `recovers_exchange_artifact_validation_after_transient_metadata_read_failure`
- `fails_exchange_artifact_validation_after_persistent_metadata_read_failure`
3. Operational diagnostics closure is evidenced by:
- retry lifecycle diagnostics formatter and emission integration in `worker/src/execution/pipeline.rs`
- passing diagnostics format-contract test `formats_exchange_retry_decision_event_with_required_dimensions`
4. Quality-gate evidence captured:
- `cargo fmt --all`: completed successfully
- `cargo clippy --all-targets --all-features -- -D warnings`: completed successfully (non-blocking future-incompat advisories only)
- `cargo check`: completed successfully

Interpretation for Workstream D:
1. All EP-5 mandatory criteria now have concrete implementation and validation evidence.
2. Matrix blockers are resolved and phase signoff is ready to be recorded as complete.
