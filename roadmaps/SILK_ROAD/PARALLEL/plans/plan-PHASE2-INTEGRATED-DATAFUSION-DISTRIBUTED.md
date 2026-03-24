# PARALLEL Silk Road: Phase 2 Integration Plan
## DataFusion Full Migration + Distributed Streaming Architecture

**Date**: March 23, 2026  
**Status**: Execution In Progress (Validation + Integration Slices Active)  
**Focus**: Unified roadmap combining DataFusion ExecutionPlan + Stage Extraction + Worker-to-Worker Streaming

---
**KEEP THIS DOCUMENT UPDATED WITH PHASE PROGRESS!**
## Executive Summary

### Strategic Shift

The original Phase 2 plan (monolithic ExecutionPlan per worker) is now **replaced** with a fundamentally different architecture:

**Before**: Server sends entire ExecutionPlan → Single worker executes all stages sequentially  
**Now**: Server breaks ExecutionPlan into stages → Multiple workers execute stages in parallel with streaming

This is not an incremental improvement; it's a **complete architectural redesign** that enables true horizontal scalability.

---

## Architecture Alignment

### Original Phase 2 (Monolithic)

```
Server:
  SQL → DataFusion Planner → ExecutionPlan (full)
  └─ Send to Worker A (bincode)

Worker A:
  Deserialize ExecutionPlan → Execute sequentially → Results
  
Result: Single worker bottleneck; no parallelism
```

### New Phase 2 (Distributed Streaming)

```
Server:
  SQL → DataFusion Planner → ExecutionPlan (full)
  └─ extract_stages() at RepartitionExec boundaries
  └─ Generate per-stage tasks + partition mapping
  
Worker A (Stage 0): Execute & stream via Arrow Flight
      ↓ (RecordBatches flow over network)
Worker B (Stage 1): Consume & execute & stream
      ↓ (Pipelined; no wait for Stage 0 complete)
Worker C (Stage 2): Consume & execute → Results

Result: Multi-worker parallelism + pipelined execution
```

### Overlap Analysis

| Component | Original Plan | New Plan | Integration |
|-----------|---------------|----------|-------------|
| **Server Planner** | DataFusion planner | Same DataFusion planner | ✅ Identical |
| **Task Model** | execution_plan: Vec<u8> | **EXTENDED**: execution_plan + output_destinations + input_connections | 🔄 Superset |
| **Worker Executor** | Deserialize → execute locally | **EXTENDED**: Stage extraction + routing + Flight streaming | 🔄 Superset |
| **Exchange Layer** | S3 fallback for spill | **ENHANCED**: MPMC channels + Arrow Flight + S3 fallback | 🔄 Superset |
| **Flight-Proxy** | Result delivery only | **ENHANCED**: Aggregates final output, may route inter-stage data | 🔄 Extended scope |

**Conclusion**: New plan **subsumes** original plan. No backtracking; just expanded scope.

---

## Implementation Phases (Revised)

## Phase Progress Snapshot (Current)

| Workstream | Status | Evidence | Notes |
|---|---|---|---|
| Phase 2a.1 Server-Side DataFusion Integration | In Progress | [server/src/planner/engine.rs](server/src/planner/engine.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs), [server/src/tests/planner_engine_tests.rs](server/src/tests/planner_engine_tests.rs) | DataFusionQueryPlanner wrapper introduced and wired into select handling; artifact and translation parity tests now cover wrapper equivalence with direct planner functions. |
| Phase 2a.2 Integrate Phase 1 Extraction into DataFusion Path | In Progress | [server/src/tests/planner_engine_tests.rs](server/src/tests/planner_engine_tests.rs), [server/src/tests/statement_handler_query_select_tests.rs](server/src/tests/statement_handler_query_select_tests.rs) | Graph consistency, partitioning metadata integrity, and topology fallback safety tests added. |
| Phase 2b.1 Worker Flight Server + Client | In Progress | [worker/src/flight/server.rs](worker/src/flight/server.rs), [worker/src/tests/flight_server_tests.rs](worker/src/tests/flight_server_tests.rs), [flight_proxy/src/main.rs](flight_proxy/src/main.rs) | worker do_put/do_exchange now share validated ingest flow, proxy do_put/do_exchange now forward request streams and upstream responses to worker endpoints using descriptor worker scope routing, and proxy stream-validation tests now cover empty/missing-descriptor request rejection paths. |
| Phase 2b.2 RecordBatch <-> Arrow IPC Conversion | In Progress | [worker/src/flight/server.rs](worker/src/flight/server.rs), [worker/src/tests/flight_server_tests.rs](worker/src/tests/flight_server_tests.rs) | do_put and do_exchange decode Flight IPC payloads into RecordBatch values, reject malformed or descriptor-only streams, and persist parquet+metadata sidecars. |
| Phase 2b.3 Stage Partition Execution + Typed Contract Validation | In Progress | [worker/src/services/worker_service_server.rs](worker/src/services/worker_service_server.rs), [worker/src/execution/planner.rs](worker/src/execution/planner.rs), [worker/src/tests/services_worker_service_server_tests.rs](worker/src/tests/services_worker_service_server_tests.rs), [worker/src/tests/execution_planner_tests.rs](worker/src/tests/execution_planner_tests.rs), [worker/src/tests/execution_query_tests.rs](worker/src/tests/execution_query_tests.rs) | Zero-based stage-id acceptance fixed and mixed legacy/staged contract detection tests added. |
| Phase 2b.4 Backpressure & Flow Control | In Progress | [worker/src/flight/server.rs](worker/src/flight/server.rs), [worker/src/tests/flight_server_tests.rs](worker/src/tests/flight_server_tests.rs) | Worker Flight ingest now enforces per-request decoded batch/row limits plus aggregate wire-byte limits and returns `ResourceExhausted` when streams exceed limits; do_put/do_exchange regression tests cover both batch-limit and wire-byte-limit enforcement. |
| Phase 2c.1 Partition Routing Logic | In Progress | [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs), [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs), [server/src/tests/statement_handler_shared_distributed_dag_tests.rs](server/src/tests/statement_handler_shared_distributed_dag_tests.rs) | Distributed DAG compilation now emits deterministic downstream partition-to-worker routing metadata (round-robin worker assignment over configured worker pool) with explicit unit tests for routing/fallback behavior and output-destination worker-address cardinality/order guarantees per downstream partition count; routing now consumes runtime worker addresses from shared state (workers first, warehouses fallback), falls back to CSV/JSON env parsing with deterministic deduplication when runtime pool is unavailable, emits routing-source diagnostics (`workers`/`warehouses`/`fallback`) in dispatch logs and stage params, includes a low-noise `fallback_active` marker in logs/params, and warns when fallback routing is active for multi-stage distributed plans. |
| Phase 2c.2 Multi-Worker Partition Distribution | In Progress | [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), [worker/src/tests/execution_pipeline_tests.rs](worker/src/tests/execution_pipeline_tests.rs) | Worker exchange-input loading now supports many-to-one upstream-to-downstream partition assignment via deterministic modulo mapping when upstream fanout exceeds downstream fanout, while retaining one-to-one behavior for compatible fanouts; tests cover mapping helpers, persisted-artifact load-path validation, and cross-partition completeness/no-overlap guarantees across downstream partitions. |
| Phase 2d.1 Full Pipeline + Contract + Upgrade Validation | In Progress | [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs) | Added mixed-version fail-fast and post-upgrade success scheduler-path tests. |
| Phase 2d.2 Performance Benchmarking | Not Started | N/A | Benchmarking work has not started in this sequence. |
| Phase 2d.3 Telemetry Key Validation | In Progress | [server/src/tests/statement_handler_query_select_tests.rs](server/src/tests/statement_handler_query_select_tests.rs), [server/src/tests/statement_handler_shared_helpers_tests.rs](server/src/tests/statement_handler_shared_helpers_tests.rs) | Telemetry key presence validated in helper and scheduler execution paths. |
| Phase 2d.4 Documentation & Deployment | Not Started | N/A | Pending implementation completion before deployment docs signoff. |

### Current Gate Summary

- Completed in this execution sequence:
  - Worker zero-based stage-id contract fix and regression coverage.
  - Scheduler mixed-version upgrade-path validation tests.
  - Telemetry key propagation validation tests.
  - DataFusion stage extraction integration safety tests.
  - Worker Flight do_put scaffolding implemented with auth/descriptors, IPC decode validation, and task-scoped parquet+metadata persistence.
  - Worker Flight ingest backpressure guards added (decoded batch/row limits + wire-byte limits) with ResourceExhausted coverage tests for do_put and do_exchange.
  - Server distributed DAG routing metadata now includes deterministic downstream partition-to-worker assignments for partition fanout.
  - Server routing contract tests now assert output destination worker-address vectors align with downstream partition counts and deterministic ordering.
  - Server routing now prefers runtime worker pools from shared state (workers then warehouses) and retains CSV/JSON env parsing with deterministic deduplication as fallback.
  - Server query dispatch now records routing source/worker count plus `fallback_active` metadata on staged task params, and warns when distributed execution relies on fallback routing.
  - Server routing observability emission is now centralized through dedicated helper paths to keep distributed routing logs and staged params contract-consistent.
  - Server routing worker-count observability now reports the effective pool used for DAG routing (runtime or env/default fallback), avoiding runtime-only undercount when fallback pool is selected.
  - Worker exchange-input routing now applies deterministic downstream partition selection across upstream exchange artifacts for multi-partition stage fanout.
  - Worker many-to-one exchange distribution is validated through persisted artifact reads in pipeline tests (not helper-only selection), including downstream-partition completeness and no-overlap coverage.
- Remaining critical path (in order):
  - 2a.1 implementation
  - 2b.1-2b.4 transport layer hardening completion
  - 2c end-to-end multi-worker distribution validation and hardening
  - 2d.2 performance validation and 2d.4 deployment runbook completion
- Quality gate note:
  - `cargo fmt --all` and `cargo clippy --all-targets --all-features -- -D warnings` are being executed per slice.
  - `cargo test` and `cargo check` may be constrained by local execution policy in this environment.

---

### Phase 0: Prerequisites (Completed in Phase 1)

**Must complete before Phase 2 begins**:
- ✅ Stage extraction at RepartitionExec boundaries
- ✅ DAG-aware async scheduler for stage orchestration
- ✅ Task model redesign with stage and routing metadata
- ✅ In-memory correctness harness (byte-identical baseline validation)

**Status**: Assumed complete; Phase 2 builds on this.

---

### Phase 2a: DataFusion Planner + Stage Extraction
**Duration**: 3-4 weeks | **Effort**: 6-7 person-days

**Goal**: Establish DataFusion as query planner; prototype stage extraction at RepartitionExec boundaries.

#### Workstream 2a.1: Server-Side DataFusion Integration (Weeks 1-2)

**Tasks**:
1. **A1.1**: Add DataFusion dependency to server Cargo.toml (0.5d)
2. **A1.2**: Create DataFusion query planner wrapper (3d)
3. **A1.3**: Update task model to include stage_id + execution_plan fields (1d)
4. **A1.4**: Integrate planner into query handler (2d)
5. **A1.5**: Basic testing (Scan, Filter, Project) (1.5d)

**Deliverable**:
- Server generates DataFusion ExecutionPlans
- Stage extraction integrated into planning path
- Task model consumed as the default contract (breaking change accepted)

**Risk**: DataFusion planner behaviour differs from Kionas  
**Mitigation**: Start with simple queries; add complexity gradually; extensive validation

---

#### Workstream 2a.2: Integrate Phase 1 Stage Extraction into DataFusion Planner (Weeks 2-3)

**Note**: Stage extraction, task model, and mapping infrastructure are **already complete from Phase 1** ([server/src/planner/stage_extractor.rs](server/src/planner/stage_extractor.rs), [server/src/tasks/mod.rs](server/src/tasks/mod.rs), [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs)). This workstream integrates existing components into the DataFusion planner path.

**Tasks**:
1. **A2.1**: Verify RepartitionExec extraction works with DataFusion-generated plans (1d)
2. **A2.2**: Wire stage extraction into DataFusion planner output path (1.5d)
3. **A2.3**: Validate task model compatibility with DataFusion ExecutionPlan serialization (1d)
4. **A2.4**: Integration tests (DataFusion query → stages → tasks) (1d)

**Deliverable**:
- DataFusion planner output feeds directly into existing stage extraction pipeline
- Task generation uses existing typed metadata model
- No reimplementation; integration only

**Risk**: DataFusion plan structure incompatible with RepartitionExec extraction assumptions  
**Mitigation**: Early integration testing; validate on suite of DataFusion query plans

---

### Phase 2b: Arrow Flight Worker-to-Worker Integration
**Duration**: 3-4 weeks | **Effort**: 6-7 person-days

**Goal**: Workers execute stage partitions and stream results via Arrow Flight (direct worker-to-worker).

#### Workstream 2b.1: Worker Flight Server + Client (Weeks 1-2)

**Tasks**:
1. **B1.1**: Implement FlightService trait on each worker (2d)
2. **B1.2**: Add do_get() endpoint for stage output streaming (1.5d)
3. **B1.3**: Add do_put() endpoint for receiving repartitioned data (1.5d)
4. **B1.4**: Connection pooling + lifecycle management (1d)
5. **B1.5**: Error handling (upstream death mid-stream) (1d)

**Deliverable**:
- Each worker is a Flight server (port 7778)
- Can publish and consume streams
- Ready for partition execution

**Risk**: Network reliability; mid-stream failures  
**Mitigation**: Retry logic + circuit breaker patterns

---

#### Workstream 2b.2: RecordBatch → Arrow IPC Conversion (Week 2)

**Tasks**:
1. **B2.1**: Implement batch_to_flight_data() (0.5d)
2. **B2.2**: Implement flight_data_to_batch() (0.5d)
3. **B2.3**: Performance benchmarking (serialization overhead) (1d)
4. **B2.4**: Integration with Flight streaming (1d)

**Deliverable**:
- RecordBatch efficiently converted to/from Arrow IPC format
- Serialization overhead profiled + acceptable
- Ready for streaming

---

#### Workstream 2b.3: Stage Partition Execution + Typed Contract Validation (Weeks 2-3)

**Tasks**:
1. **B3.1**: Deserialize ExecutionPlan from task.execution_plan bytes (1d)
2. **B3.2**: Execute stage partition locally (DataFrame execution) (2d)
3. **B3.3**: Stream results to output_destinations via Flight (1.5d)
4. **B3.4**: Validate typed stage partition contract: stage_id + partition_id + output_destinations correct (1d)
5. **B3.5**: Tests (single-stage, multi-partition) + contract validation scenarios (1.5d)

**Deliverable**:
- Workers execute individual stage partitions with correct typed metadata
- Results streamed via Flight
- Stage partition contract validation proves typed metadata flows correctly end-to-end
- No S3 materialization for inter-stage data

**Note**: Task B3.4 ensures workers correctly interpret StagePartitionExecution contract from Phase 1; critical for upgrade path safety.

**Risk**: Performance regression vs. current model; typed contract misinterpretation  
**Mitigation**: Profile early; optimize hot paths (batch size tuning); comprehensive contract validation tests

---

#### Workstream 2b.4: Backpressure & Flow Control (Week 4)

**Tasks**:
1. **B4.1**: Implement MPMC channel routing (bounded queue) (1.5d)
2. **B4.2**: gRPC backpressure configuration (0.5d)
3. **B4.3**: Test slow consumer → fast producer throttling (1d)
4. **B4.4**: Metrics collection (throughput, latency, backpressure events) (0.5d)

**Deliverable**:
- Backpressure works end-to-end
- Metrics visualizable
- Ready for load testing

---

### Phase 2c: Multi-Partition Mapping & Routing
**Duration**: 2-3 weeks | **Effort**: 5-6 person-days

**Goal**: Implement partition-aware routing for Hash, RoundRobin, Range, SinglePartition schemes.

#### Workstream 2c.1: Partition Routing Logic (Weeks 1-2)

**Tasks**:
1. **C1.1**: Implement route_batch() for Hash partitioning (1.5d)
2. **C1.2**: Implement route_batch() for RoundRobin (0.5d)
3. **C1.3**: Implement route_batch() for Range (1d)
4. **C1.4**: Implement route_batch() for SinglePartition (0.5d)
5. **C1.5**: Unit tests per scheme (1.5d)

**Deliverable**:
- All partitioning schemes working
- Tested on synthetic batches
- Ready for integration

---

#### Workstream 2c.2: Multi-Worker Partition Distribution (Week 2-3)

**Tasks**:
1. **C2.1**: Extend output_destinations to support many downstream workers (1d)
2. **C2.2**: Implement fan-out from single partition to multiple downstream partitions (1.5d)
3. **C2.3**: Efficient MPMC channel management (per-destination) (1d)
4. **C2.4**: Integration tests (Scan 4 partitions → Join 4 partitions → Aggregate 1) (1d)

**Deliverable**:
- Rows correctly distributed across downstream partitions
- No data loss or duplication
- Ready for end-to-end testing

---

### Phase 2d: End-to-End Integration & Validation
**Duration**: 2-3 weeks | **Effort**: 5-6 person-days

**Goal**: Prove all components work together; validate correctness + performance.

#### Workstream 2d.1: Full Pipeline Testing + Contract & Upgrade Validation (Week 1-2)

**Tasks**:
1. **D1.1**: End-to-end test: Scan → RepartitionExec → Join → Aggregate (2d)
2. **D1.2**: Validate result correctness (byte-for-byte vs. local execution) (1d)
3. **D1.3**: Test with 4 workers, 3 stages (1d)
4. **D1.4**: Chaos testing (worker failure mid-stream) (1d)
5. **D1.5**: **Breaking-change contract validation**: Verify typed stage metadata (stage_id, partition_id, output_destinations) flows correctly across server→worker dispatch; assert Stage ID 0 accepted (regression test from Phase 1 fix) (1d)
6. **D1.6**: **Upgrade sequence testing**: Validate mixed-version scenarios during cluster upgrade (server upgraded first, worker follows); confirm stage contract compatibility during transition (1d)

**Deliverable**:
- All existing query patterns execute correctly
- Results byte-identical to monolithic execution
- Partial failure recovery works
- Breaking-change contract validated end-to-end
- Upgrade path proven safe for staged server→worker transition

**Note**: Tasks D1.5 and D1.6 map directly to Phase 1 outcome requirements (outcome file §Post-upgrade Validation, §Binary Upgrade Order, §Compatibility Expectations).

---

#### Workstream 2d.2: Performance Benchmarking (Weeks 1-2)

**Tasks**:
1. **D2.1**: Latency benchmarks (single query, varying worker count) (1d)
2. **D2.2**: Throughput benchmarks (multiple concurrent queries) (1d)
3. **D2.3**: Memory usage profiling (per-worker, per-partition) (1d)
4. **D2.4**: Network bandwidth analysis (inter-stage traffic) (0.5d)
5. **D2.5**: Optimization (batch size tuning, channel buffer sizes) (1d)

**Deliverable**:
- Latency improvement documented (target: 15-30% vs. monolithic)
- Memory footprint reduced (spill-to-disk less frequent)
- Scaling characteristics validated (more workers → less latency)

---

#### Workstream 2d.3: Telemetry Key Validation (Week 2)

**Tasks**:
1. **D3.1**: Verify all Phase 1 telemetry keys are emitted during dispatch (1d)
   - `distributed_dag_metrics_json` (DAG metrics)
   - `distributed_plan_validation_status` (planner validation flags)
   - `stage_extraction_mismatch` (extraction diagnostic)
   - `datafusion_stage_count` (count from DataFusion planner)
   - `distributed_stage_count` (count from distributed scheduler)
2. **D3.2**: Validate telemetry is observable in stage params on all dispatched tasks (1d)
3. **D3.3**: Test telemetry completeness across query types (1-3 stages, various partitioning) (0.5d)

**Deliverable**:
- All Phase 1 telemetry keys present and correctly transmitted
- Operator can validate cluster upgrade using telemetry (proof of Phase 1 outcome signoff requirements)

**Note**: Directly fulfills Phase 1 outcome requirement: "Verify dispatch telemetry keys exist on stage params" (outcome file §Post-upgrade Validation).

---

#### Workstream 2d.4: Documentation & Deployment (Week 2-3)

**Tasks**:
1. **D4.1**: Architecture documentation (stage extraction + routing) (1d)
2. **D4.2**: Operational runbook (how to scale workers; partition assignment; upgrade path reference) (0.5d)
3. **D4.3**: Deployment strategy (preview → canary → full rollout; link to Phase 1 outcome upgrade path) (0.5d)
4. **D4.4**: Team training + knowledge transfer (1d)

**Deliverable**:
- Ready for production deployment
- Team trained
- Monitoring in place
- Upgrade path documented and linked to Phase 1 outcome file

---

## Timeline Overview

```
Phase 0 (Prerequisite - Phase 1):
└─ Control plane foundation complete (stage extraction + DAG scheduler + task model, 3-4 weeks)

Phase 2a: DataFusion + Planner Integration (Reduced Scope - Integration Only)
├─ 2a.1: Server planner integration (2 weeks, 6-7d)
└─ 2a.2: Integrate Phase 1 extraction into DataFusion path (1.5 weeks, 4-5d)
         [Stage extraction, task model, mapping complete from Phase 1]
Total: 2-2.5 weeks (Reduced from 3-4 weeks; Phase 1 prerequisites met)

Phase 2b: Arrow Flight Streaming (4 workers in parallel, 3-4 weeks)
├─ 2b.1: Flight server/client (2 weeks, 6-7d)
├─ 2b.2: RecordBatch ↔ Arrow IPC (1 week, 2-3d)
├─ 2b.3: Stage execution + streaming (2 weeks, 5-6d)
└─ 2b.4: Backpressure + metrics (1 week, 2-3d)
Total: 3-4 weeks

Phase 2c: Partition Routing (2-3 weeks)
├─ 2c.1: Routing logic (1 week, 5-6d)
└─ 2c.2: Multi-worker distribution (1-2 weeks, 4-5d)
Total: 2-3 weeks

Phase 2d: Integration + Validation (2-3 weeks)
├─ 2d.1: End-to-end testing + contract + upgrade validation (1-2 weeks, 6-8d)
├─ 2d.2: Performance benchmarking (1-2 weeks, 4-5d)
├─ 2d.3: Telemetry key validation (1 week, 2-3d)
└─ 2d.4: Documentation (1 week, 2-3d)
Total: 2-3 weeks

GRAND TOTAL: 9-12.5 weeks (~23-28 person-days across 4-5 engineers)
[Reduced from original 10-14 weeks due to Phase 1 prerequisite completion]
```

**Critical Path**: 2a → 2b → 2c → 2d (sequential; constrained by dependencies)

**Phase 1 Alignment Note**: Phase 2a is now **integration-focused** (not re-implementation) since stage extraction, task model, and mapping infrastructure are complete from Phase 1. Phase 2d is **expanded to validate Phase 1 outcome requirements**: contract semantics validation (stage_id=0 acceptance), upgrade safety (mixed-version server→worker transitions), and telemetry key presence. This ensures Phase 2 delivery satisfies operator signoff criteria from the breaking-changes outcome document.

---

## Comparison: Original vs. Enhanced Phase 2

| Aspect | Original (Monolithic) | Enhanced (Distributed) |
|--------|----------------------|----------------------|
| **Stages** | All in one task | Split at RepartitionExec |
| **Workers per query** | 1 (sequential within worker) | N (parallel across workers) |
| **Data transport** | S3 (full materialization) | Arrow Flight (streaming) |
| **Partitioning** | Not needed (single worker) | Critical (Hash, RoundRobin, etc.) |
| **Latency** | Cumulative (T0 + T1 + T2) | Pipelined (overlap) |
| **Effort** | ~10 weeks | ~9-12.5 weeks (Phase 1 prerequisites reduce scope) |
| **Complexity** | Medium | High (networking, coordination) |
| **Scalability** | Limited (1 worker) | Unlimited (N workers) |

**Trade-off**: +2-4 weeks of effort for **unlimited horizontal scaling** and **pipelined execution**.

---

## Success Criteria (Phase 2 Signoff)

### Mandatory (All must be "Done")

1. ✅ **Query Correctness**: All existing query types execute via distributed stages; results byte-identical to monolithic
2. ✅ **Multi-Worker Execution**: Single query spans 3+ workers simultaneously (proven in tests)
3. ✅ **Streaming Integration**: RecordBatches stream between workers via Arrow Flight without full buffering
4. ✅ **Partition Mapping**: Hash/RoundRobin partitioning distributes rows correctly across downstream partitions
5. ✅ **Performance**: Latency ≥ baseline (target: 15-30% improvement); memory usage reduced
6. ✅ **Fault Handling**: Worker death mid-stream handled gracefully (query fails fast, no hang)
7. ✅ **Breaking-Change Contract**: Typed stage partition metadata (stage_id, partition_id, output_destinations) flows correctly end-to-end; Stage ID 0 accepted (no regression)
8. ✅ **Upgrade Path Validation**: Mixed-version (server upgraded first, then workers) scenarios tested; stage contract compatibility proven during transition
9. ✅ **Telemetry Keys**: All Phase 1 telemetry keys present in dispatched stage params (distributed_dag_metrics_json, distributed_plan_validation_status, stage_extraction_mismatch, datafusion_stage_count, distributed_stage_count)
10. ✅ **Code Quality**: Complexity < 25; clippy clean; comprehensive test coverage

### Optional (Hardening)

- 🔄 Range partitioning support (if time permits)
- 🔄 Speculative execution (re-run slow stages on different workers)
- 🔄 Adaptive batch size tuning (per-stage optimization)

---

## Key Decisions

### Decision 1: Worker-to-Worker Direction?

**Question**: Should workers connect directly (Pattern A) or route through flight-proxy (Pattern B)?

**Recommendation**: **Pattern A (direct worker-to-worker)** for Phase 2
- Mirrors DataFusion's local execution model
- Reduces latency (no proxy overhead)
- Flight-proxy handles final result delivery to client

**Alternative**: Could switch to Pattern B later if topology requires it (e.g., DMZ restrictions).

---

### Decision 2: Fallback to S3?

**Question**: When do we spill inter-stage data to S3?

**Recommendation**: **Only on memory pressure** (> 90% utilization)
- Default: MPMC channels (fast, in-memory)
- Fallback: S3 spill (graceful degradation)
- Never: Full S3 materialization between stages

---

### Decision 3: Partitioning Priority?

**Question**: Which schemes to support first?

**Recommendation**: 
1. **Phase 2c.1**: Hash (covers 80% of queries)
2. **Phase 2c.2**: RoundRobin (simple; fallback)
3. **Post-Phase 2**: Range (complex; lower priority)

---

## Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|-----------|
| **Network latency** adds overhead | Performance regression vs. local | Profile early; optimize batch size; validate acceptable |
| **Stage extraction** incorrect boundaries | Execution failures | Comprehensive unit tests on known-good plans |
| **Worker failure** mid-stream | Query hangs or crashes | Error propagation + fast-fail semantics |
| **Partition routing** bugs | Data loss or duplication | Extensive validation tests; checksum verification |
| **Backpressure** not working | Memory explosion | Bounded channels + gRPC configuration; load testing |
| **Serialization** overhead (bincode) | Task submission slow | Profile; consider alternatives (protobuf) if needed |

---

## Rollback Plan

**If critical issues discovered**:

1. **Phase 2a-2b instability**: Keep distributed architecture, reduce blast radius
  - Keep: stage extraction + distributed task model + Flight integration
  - Limit: cap max workers per query; temporarily reduce stage fan-out
  - Impact: lower throughput during stabilization, no architectural backtracking

2. **Phase 2c routing instability**: Freeze to supported routing subset
  - Keep: Hash + SinglePartition only until fixes land
  - Defer: Range and advanced fan-out paths
  - Impact: reduced flexibility, preserves distributed execution direction

---

## Conclusion

This is a **complete architectural redesign** that replaces both the monolithic Phase 2 plan AND the original Kionas operator stack. It's not a quick fix; it's a **strategic investment** in unlimited horizontal scalability.

**The payoff**: 
- 45-65% cumulative latency reduction (Phase 1 + Phase 2)
- Unlimited worker scaling
- Tech debt eliminated
- Pipelined, streaming execution

**The cost**: 12-14 weeks, 25-28 person-days, high complexity.

**The timing**: Start Phase 2a immediately after Phase 1 completes.

