# Plan: INDEXING FOUNDATION

## Objective
Implement metadata-driven file skipping in the query scan path to reduce unnecessary parquet reads while preserving correctness through conservative fallback.

## Scope Boundaries
In scope:
1. Introduce a minimal scan strategy contract for planner/runtime handoff.
2. Add conservative predicate eligibility and pruning metadata propagation.
3. Implement worker-side pre-read file pruning with deterministic fallback to full scan.
4. Add diagnostics proving pruning decisions and fallback reasons.

Out of scope:
1. Full sidecar index catalog lifecycle.
2. New physical access operators beyond scan strategy extension.
3. Broad benchmark suite and advanced cost model.

## Implementation Steps

### Phase A: Scan Strategy Contract
1. Extend scan representation to carry optional pruning intent and fallback semantics.
2. Keep compatibility with existing task payloads where scan metadata is absent.
3. Add validation guardrails so unsupported/invalid scan strategy falls back safely.

Candidate files:
- [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs)
- [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs)
- [worker/src/execution/planner.rs](worker/src/execution/planner.rs)

### Phase B: Predicate Eligibility And Metadata Propagation
1. Define first-pass eligible predicate classes for pruning (simple equality/range on primitive columns).
2. Emit scan pruning hints in distributed task payload metadata.
3. Ensure missing hints do not alter current behavior.

Candidate files:
- [server/src/statement_handler/distributed_dag.rs](server/src/statement_handler/distributed_dag.rs)
- [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs)
- [worker/src/execution/query.rs](worker/src/execution/query.rs)

### Phase C: Worker Pre-Read File Pruning
1. Introduce a pruning step in scan loading before per-file object reads.
2. Use available metadata and eligibility signals to compute candidate file set.
3. Preserve deterministic key ordering among retained files.
4. Fall back to full scan when metadata is missing, stale, or invalid.

Candidate files:
- [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs)
- [worker/src/storage/deltalake.rs](worker/src/storage/deltalake.rs)

### Phase D: Observability And Safety
1. Emit logs for total files, pruned files, and fallback reason.
2. Ensure result correctness parity between pruned and full-scan modes.
3. Add explicit tests for fallback behavior.

Candidate files:
- [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs)
- [worker/tests](worker/tests)

## Verification
1. cargo fmt --all
2. cargo clippy --all-targets --all-features -- -D warnings
3. docker run --rm -v "${PWD}:/workspace" -w /workspace docker-devcontainer cargo check -p server
4. docker run --rm -v "${PWD}:/workspace" -w /workspace docker-devcontainer cargo check -p worker
5. Focused tests validating:
- Full fallback when pruning metadata absent.
- Deterministic behavior when pruning selects subset.
- Equivalence of query results between full-scan and pruning modes for covered predicates.

## Deliverables
1. Planner/runtime scan strategy contract committed.
2. Worker scan pruning implementation with safety fallback.
3. Tests covering happy path and fallback path.
4. Updated completion matrix evidence for FOUNDATION implementation progress.
