# Plan: EXECUTION EP-5

## Project coding standards and guidelines
1. tests go under the folder src/tests/<module_name>_tests.rs; avoid inline test additions at the end of non-test files.

## Coding considerations for this phase
1. Any connection to storage must be pooled and reused; no new unpooled connections.
2. Retry and recovery behavior must be deterministic and bounded; no unbounded loops.
3. Artifact integrity checks should execute before decode/materialization when feasible.
4. Failure diagnostics must remain low-cardinality, actionable, and phase-auditable.

## Goal
Harden distributed execution resilience and artifact integrity so transient storage/network failures degrade safely, artifact corruption is detected at critical boundaries, and recovery behavior is explicit, testable, and observable.

Measurable EP-5 outcomes:
1. Retry/failure semantics for critical distributed paths are documented and enforced with bounded behavior.
2. Artifact integrity checks are present at critical exchange and result boundaries with explicit failure diagnostics.
3. Fault-injection evidence proves graceful degradation and recoverability for representative transient failure scenarios.
4. Operational diagnostics expose enough context to localize distributed failures quickly.

## Inputs
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP5.md](../discovery/discovery-EXECUTION-EP5.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md)
4. Prior phase signoff reference: [roadmaps/ROADMAP_EXECUTION_EP4_MATRIX.md](../../../ROADMAP_EXECUTION_EP4_MATRIX.md)

## Scope Boundaries
In scope:
1. Define and implement bounded retry/failure semantics for distributed execution storage I/O paths.
2. Add or tighten artifact integrity validation at exchange and query-result boundaries.
3. Build fault-injection coverage for transient read/write/list failures and partial artifact states.
4. Extend diagnostics for rapid root-cause identification with query/stage/task/partition context.
5. Package EP-5 matrix-grade evidence and quality-gate outputs.

Out of scope:
1. SQL parser redesign.
2. Client protocol redesign.
3. Planner operator-family expansion beyond resilience/integrity needs.
4. Major object-store backend replacement or infrastructure redesign.

## Guardrails
1. Preserve worker contract boundaries: worker continues executing Kionas task operators.
2. Keep changes incremental and phase-gated with matrix signoff.
3. Prefer deterministic behavior over optimistic heuristics when correctness is at risk.
4. Degrade gracefully when external systems (storage/network) are unstable.
5. Maintain strict quality gates before phase signoff evidence is accepted.
6. Avoid duplicate side effects under retries by enforcing idempotent write/read decision points.

## Current Discovery Findings (EP-5 Baseline)
1. Query and exchange artifact writers already emit metadata with size and checksum fields (`checksum_fnv64`) during persistence flows in [worker/src/execution/artifacts.rs](worker/src/execution/artifacts.rs).
2. Flight result retrieval path validates metadata alignment, artifact size, and checksum before returning decoded batches in [worker/src/flight/server.rs](worker/src/flight/server.rs), with targeted parser/alignment tests in [worker/src/tests/flight_server_tests.rs](worker/src/tests/flight_server_tests.rs).
3. Upstream exchange reads currently enumerate parquet keys and decode bytes directly without consuming exchange metadata sidecars for checksum/size validation in [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs).
4. Storage interactions use direct single-attempt provider calls (`put_object`, `get_object`, `list_objects`, `delete_object`) with no shared retry policy layer in [worker/src/storage/mod.rs](worker/src/storage/mod.rs).
5. Execution telemetry includes decision events (scan/exchange/materialization) from [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), but resilience-specific attempt/fallback diagnostics are not yet standardized for distributed failure handling.
6. Existing validation coverage emphasizes shape/integrity parsing in flight retrieval, but phase-scoped fault-injection scenarios for transient I/O and partial artifact states are not yet represented.

## Workstreams

### Workstream A: Distributed Failure Semantics And Retry Contract
Objectives:
1. Define bounded retry policy and failure taxonomy for distributed storage interactions.
2. Ensure retry behavior is deterministic, idempotent-aware, and auditable. And make sure they are not already in place by underlying storage provider/libs behavior.

Tasks:
1. Classify retriable vs non-retriable failure classes for list/get/put/delete operations.
2. Define EP-5 retry envelope (attempt cap, backoff shape, jitter policy, terminal failure mapping).
3. Define idempotency and duplicate-effect guardrails for write/publish paths.
4. Map failure taxonomy to actionable runtime diagnostics.

Deliverables:
1. EP-5 retry and failure-semantics contract.
2. Retriable/non-retriable decision matrix.
3. Error/diagnostics mapping for distributed storage faults.

### Workstream B: Artifact Integrity At Exchange And Materialization Boundaries
Objectives:
1. Close integrity gaps between persisted metadata and consumed artifacts.
2. Enforce deterministic artifact validation at critical exchange/result boundaries.

Tasks:
1. Inventory current integrity checks for exchange and result flows.
2. Define required metadata sidecar consumption and validation points for exchange reads.
3. Define consistent handling of checksum/size/manifest mismatches.
4. Define boundary behavior for missing metadata and partial artifact sets.

Deliverables:
1. Integrity boundary checklist (writer and reader sides).
2. Exchange/result artifact validation contract.
3. Negative-path diagnostics set for integrity violations.

### Workstream C: Fault-Injection And Recoverability Verification
Objectives:
1. Prove graceful degradation under transient storage/network instability.
2. Verify recoverability behavior and deterministic terminal outcomes.

Tasks:
1. Define fault-injection matrix for list/get/put failures, timeouts, and partial object visibility.
2. Define scenario pack for retry success, retry exhaustion, and non-retriable immediate fail.
3. Define artifact-corruption scenarios (checksum mismatch, size mismatch, metadata mismatch).
4. Define acceptance criteria for graceful degradation and recoverability evidence.

Deliverables:
1. EP-5 fault-injection scenario matrix.
2. Recoverability verification pack.
3. Evidence mapping from scenarios to mandatory criteria.

### Workstream D: Operational Diagnostics And Signoff Packaging
Objectives:
1. Ensure distributed failure diagnostics are sufficient for fast triage.
2. Convert EP-5 criteria into matrix-grade closure evidence.

Tasks:
1. Define required diagnostics fields for distributed resilience events (query_id, stage_id, task_id, partition_index, failure_class, attempt, terminal_reason).
2. Define operational event contract for retry attempt and terminal outcomes.
3. Define EP-5 quality-gate and targeted verification command set.
4. Prepare EP-5 matrix scaffold and criteria-to-evidence mapping.

Deliverables:
1. Diagnostics contract and event taxonomy for EP-5.
2. EP-5 command/evidence packaging blueprint.
3. Matrix-ready signoff bundle structure.

## Canonical Mode Decision Table

| Domain | Mode | Entry Condition | Runtime Behavior | Deferred/Failure Behavior | Evidence Target |
|---|---|---|---|---|---|
| Exchange I/O | transient_storage_error | Storage provider returns retriable error class | Bounded retry policy executes with deterministic attempt accounting | Terminal failure with actionable diagnostics after retry budget is exhausted | fault-injection retry tests |
| Exchange I/O | non_retriable_storage_error | Storage provider returns non-retriable error class | Immediate fail-fast with structured context | No retry loop; explicit terminal reason emitted | negative-path diagnostics tests |
| Artifact Integrity | valid_artifact_set | Metadata and artifact bytes align (count, size, checksum) | Artifact decoding and downstream execution proceed | N/A | integrity success-path tests |
| Artifact Integrity | integrity_mismatch | Missing metadata or checksum/size/count mismatch detected | N/A | Fail with explicit integrity diagnostic and bounded cleanup/degradation path | integrity mismatch tests |
| Distributed Recovery | partial_visibility_recovery | Temporary object-list/read inconsistency | Retry and reconcile within bounded policy | Terminal failure if inconsistency persists beyond budget | recoverability scenario tests |

## Criteria-To-Workstream Traceability

| EP-5 Mandatory Criterion | Primary Workstream(s) | Evidence Target |
|---|---|---|
| Retry and failure-handling semantics are clearly defined and validated | A, C, D | retry contract, fault-injection results, diagnostics mapping |
| Artifact integrity checks exist at critical exchange/materialization boundaries | B, C, D | integrity checklist, boundary tests, mismatch diagnostics |
| Fault-injection scenarios verify graceful degradation and recoverability | C, D | scenario matrix, pass/fail evidence, terminal outcome assertions |
| Operational diagnostics allow fast root-cause identification for distributed failures | A, D | event contract, context field verification tests |
| Quality gate evidence captured (fmt/clippy/check) | D | command outputs and matrix references |

## Sequence And Dependencies
1. Workstream A starts first to establish retry/failure semantics and prevent ad hoc behavior.
2. Workstream B starts in parallel after A taxonomy draft is stable, because integrity checks need compatible failure mapping.
3. Workstream C depends on A/B contracts to define fault-injection expectations and expected terminal behavior.
4. Workstream D runs continuously for criteria mapping, then finalizes after C verification evidence is complete.
5. EP-5 implementation begins only after EP-4 signoff matrix remains Done.

## Milestones
1. M1: Retry/failure taxonomy and bounded retry contract approved.
2. M2: Integrity boundary checklist and exchange/result validation requirements locked.
3. M3: Fault-injection matrix executed with recoverability outcomes captured.
4. M4: Diagnostics contract, quality gates, and EP-5 matrix signoff package completed.

## Risks And Mitigations
1. Risk: Retry logic introduces duplicate side effects or non-deterministic behavior.
Mitigation: enforce idempotency boundaries, bounded attempts, and explicit terminal-state invariants.

2. Risk: Integrity validation overhead impacts latency-sensitive paths.
Mitigation: scope checks to critical boundaries first and measure overhead as part of EP-5 evidence.

3. Risk: Fault-injection coverage misses production-like failure sequences.
Mitigation: define scenario matrix from real I/O operation classes (list/get/put/delete) plus partial-visibility patterns.

4. Risk: Diagnostics become too verbose/noisy for operations.
Mitigation: keep low-cardinality event taxonomy with mandatory context fields and bounded reason codes.

## E2E Scenarios
1. Transient upstream exchange read failure followed by retry success.
Expected outcome: stage recovers within retry budget and produces deterministic output.

2. Persistent exchange list/get failure beyond retry budget.
Expected outcome: stage fails with terminal diagnostic containing query/stage/task/partition and failure class.

3. Result artifact checksum mismatch during retrieval.
Expected outcome: retrieval fails with explicit integrity error; no silent fallback.

4. Missing metadata with present parquet objects (and inverse).
Expected outcome: deterministic failure classification and actionable remediation path.

5. Partial artifact visibility across distributed partitions.
Expected outcome: bounded retry and deterministic terminal outcome (recovered or explicit fail) without hangs.

## Matrix Packaging Checklist
1. Create/update [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md).
2. For each mandatory criterion, include concrete evidence references.
3. Mark deferred optional hardening with explicit non-blocking rationale.
4. Record final EP-5 signoff decision in the matrix file.

## Evidence Locations
1. Discovery: [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP5.md](../discovery/discovery-EXECUTION-EP5.md)
2. Roadmap: [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](../ROADMAP_EXECUTION.md)
3. Matrix target: [roadmaps/ROADMAP_EXECUTION_EP5_MATRIX.md](../../../ROADMAP_EXECUTION_EP5_MATRIX.md)

## Discovery Summary
1. Current runtime already emits artifact metadata and validates result integrity on Flight retrieval, giving EP-5 a strong baseline.
2. Key EP-5 gap is distributed exchange resilience: bounded retry semantics and exchange-side artifact integrity validation are not yet explicit or uniformly enforced.
3. EP-5 implementation should proceed as contract-first slices (retry taxonomy, integrity boundaries, fault-injection, diagnostics), with matrix-bound evidence at each gate.
