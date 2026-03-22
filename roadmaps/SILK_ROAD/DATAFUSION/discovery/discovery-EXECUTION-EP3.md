# EP-3 Discovery: Memory And Throughput Optimization

## Scope
This discovery focuses only on EP-3 goals from ROADMAP_EXECUTION:
1. Reduce peak memory pressure in key execution stages.
2. Improve throughput predictability under mixed query workloads.
3. Add measurable baselines for memory and operator throughput.

In scope:
- Worker runtime pipeline memory/throughput behavior.
- Batch lifecycle across scan, join, aggregate, sort, and artifact persistence.
- Existing observability surfaces for execution decisions and artifact metadata.
- EP-3 benchmark and telemetry readiness gaps.

Out of scope:
- Protocol redesign.
- Join semantic expansion beyond current EP-4 scope.
- Broad resilience redesign (EP-5 scope).

## Key Evidence Reviewed
- worker/src/execution/pipeline.rs
- worker/src/execution/join.rs
- worker/src/execution/artifacts.rs
- worker/src/execution/query.rs
- roadmaps/discover/execution_pipeline_discovery.md
- roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md

## Current Runtime Architecture Slice (EP-3 Relevant)

### Pipeline data flow
Current behavior:
1. Query execution flows through a sequential operator chain over Vec<RecordBatch>.
2. Base scans load all parquet objects for a prefix and decode them into memory.
3. Join path loads and decodes right-side input fully, then performs concat-based hash join indexing.
4. Sort normalization and final artifact persistence operate on the accumulated batch set.

Observed implications:
- Multiple pipeline boundaries hold complete in-memory batch collections.
- Operator-heavy paths can amplify memory pressure as rows and columns expand.

### Join and aggregation pressure points
Current behavior:
1. Hash join uses concat_batches for both sides before building a full right-side hash index.
2. Join match vectors (left_indices/right_indices) can grow with multiplicative fanout.
3. Aggregate partial/final stages execute after possible join expansion.

Observed implications:
- Join-heavy workloads are likely the dominant EP-3 memory and variance hotspot.
- Throughput can vary by skew and join multiplicity, not only file count.

### Artifact persistence and serialization behavior
Current behavior:
1. Exchange and final artifacts encode full batch slices to parquet payloads.
2. Metadata sidecars expose row_count, batch_count, artifact count, and artifact size.
3. Persistence is deterministic but serialization currently depends on in-memory batch payloads.

Observed implications:
- Artifact metadata provides a useful baseline seed but lacks operator-stage timing and memory indicators.
- Throughput bottlenecks can be hard to localize without stage-level counters.

## Observability and Baseline Gaps

## Finding 1: No phase-ready baseline benchmark contract
Impact:
- EP-3 criterion 1 requires before/after evidence for representative workloads.
- Current repo lacks a phase-scoped benchmark matrix and result capture template.

Severity:
- High for signoff readiness.

## Finding 2: Memory pressure is implicit, not measured
Impact:
- Current diagnostics capture decision events and artifact metadata, but not peak-batch, stage memory hints, or operator timing.
- Hard to attribute regressions to scan, join, aggregate, sort, or persistence boundaries.

Severity:
- High.

## Finding 3: Throughput variance risk is concentrated in join/aggregate/materialization boundaries
Impact:
- concat-based join and full-batch persistence can create bursty latency for skew-heavy inputs.
- Without bounded batch-size controls or stage counters, runtime predictability is difficult to enforce.

Severity:
- Medium to high.

## EP-3 Risk Register
1. Regression risk:
- Memory optimizations can accidentally alter output ordering or schema handling.

2. Benchmark drift risk:
- Inconsistent datasets or execution parameters can invalidate before/after comparisons.

3. Telemetry cardinality risk:
- Rich metrics without bounded dimensions can add noise and cost.

4. Scope creep risk:
- EP-3 can drift into EP-4 feature expansion if optimization and capability changes are mixed.

## Recommended EP-3 Workstream Inputs

### Workstream A: Baseline and benchmark contract
1. Define representative workload set (scan-only, filter+sort, join-heavy, aggregate-heavy).
2. Define fixed dataset scale and execution parameters.
3. Define required evidence fields: runtime, rows/sec, artifact bytes, and selected stage counters.

### Workstream B: Memory hotspot reduction
1. Identify highest-allocation boundaries in load/decode, join, and sort normalization paths.
2. Introduce incremental memory-safe improvements with deterministic output preservation.
3. Keep optimization slices small and test-backed.

### Workstream C: Throughput variance control
1. Add bounded execution knobs where safe (batch-size and chunking boundaries).
2. Validate variance reduction under mixed workload profiles.
3. Keep deterministic semantics and EP-1/EP-2 observability contracts intact.

### Workstream D: Runtime telemetry for EP-3 operations
1. Add low-cardinality stage/operator throughput indicators.
2. Add memory-oriented counters suitable for production diagnostics.
3. Define operator-level event dimensions and emission boundaries.

### Workstream E: Verification and matrix packaging
1. Map each EP-3 mandatory criterion to concrete evidence artifacts.
2. Capture quality gate and benchmark command outputs.
3. Update EP-3 matrix with phase signoff decision.

## EP-3 Completion Readiness Mapping
Against roadmap mandatory criteria:

1. Baseline and post-change performance evidence:
- Not started; benchmark contract and evidence table required.

2. Memory-intensive path optimization with no correctness regression:
- Not started; optimization slices and regression suite needed.

3. Throughput variance reduced or bounded:
- Not started; mixed-profile benchmark evidence required.

4. Runtime telemetry surfaces memory/throughput indicators:
- Not started; operator-stage metrics contract required.

5. Quality gate evidence:
- Deferred to EP-3 implementation closure.

## Dependencies and Assumptions
1. EP-2 signoff is complete before EP-3 execution.
2. Worker execution contract remains Kionas operator driven.
3. Optimization work remains incremental and phase-gated.
4. No protocol changes are required for EP-3.

## Discovery Output Summary
1. Current pipeline is functionally stable but eager in-memory batching creates EP-3 pressure points.
2. Join concat/index behavior is a primary memory and throughput variance hotspot.
3. Artifact metadata exists but does not yet provide EP-3-required stage throughput/memory visibility.
4. EP-3 should start with benchmark contract and telemetry baselines before optimization slices.

## Proposed Next Artifact
Create EP-3 implementation plan at:
- roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP3.md

Plan should convert this discovery into:
1. Workstreamed implementation slices and dependencies.
2. Criteria-to-workstream traceability.
3. Benchmark and quality-gate evidence checklist for ROADMAP_EXECUTION_EP3_MATRIX.md.
