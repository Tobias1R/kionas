# PARALLEL Silk Road: Phase 2 Integration Plan
## DataFusion Full Migration + Distributed Streaming Architecture

**Date**: March 23, 2026  
**Status**: Planning (Revised Architecture)  
**Focus**: Unified roadmap combining DataFusion ExecutionPlan + Stage Extraction + Worker-to-Worker Streaming

---

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

### Phase 0: Prerequisites (Completed in Phase 1)

**Must complete before Phase 2 begins**:
- ✅ Async scheduler (server-side stage orchestration)
- ✅ Tokio runtime integration (worker-side async execution)
- ✅ Partition checkpoint logic (for recovery)

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
- Task model extended (but not yet using stage extraction)
- All existing queries still work (backward compatible)

**Risk**: DataFusion planner behaviour differs from Kionas  
**Mitigation**: Start with simple queries; add complexity gradually; extensive validation

---

#### Workstream 2a.2: Stage Extraction Algorithm (Weeks 2-3)

**Tasks**:
1. **A2.1**: Implement RepartitionExec traversal logic (2d)
2. **A2.2**: Build ExecutionStage struct + extract_stages() function (2d)
3. **A2.3**: Handle edge cases (single-stage plans, multiple repartitions) (1d)
4. **A2.4**: Unit tests for stage extraction (1.5d)

**Deliverable**:
- Stage extraction algorithm proven on test plans
- ExecutionStage structure finalized
- Ready for server integration

**Risk**: Incorrect stage boundaries → execution failures  
**Mitigation**: Comprehensive unit tests; validate on known-good plans

---

#### Workstream 2a.3: Task Generation with Mapping (Week 3-4)

**Tasks**:
1. **A3.1**: Extend Task model with stage_id, output_destinations, input_connections (1d)
2. **A3.2**: Implement server-side partition mapping calculation (2d)
3. **A3.3**: Task serialization (bincode) (0.5d)
4. **A3.4**: Integration tests (query → stages → tasks) (1d)

**Deliverable**:
- Server generates full task pipeline per query
- Each task knows where output goes
- Ready for worker deployment

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

#### Workstream 2b.3: Stage Partition Execution (Weeks 2-3)

**Tasks**:
1. **B3.1**: Deserialize ExecutionPlan from task.execution_plan bytes (1d)
2. **B3.2**: Execute stage partition locally (DataFrame execution) (2d)
3. **B3.3**: Stream results to output_destinations via Flight (1.5d)
4. **B3.4**: Tests (single-stage, multi-partition) (1d)

**Deliverable**:
- Workers execute individual stage partitions
- Results streamed via Flight
- No S3 materialization for inter-stage data

**Risk**: Performance regression vs. current model  
**Mitigation**: Profile early; optimize hot paths (batch size tuning)

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

#### Workstream 2d.1: Full Pipeline Testing (Week 1)

**Tasks**:
1. **D1.1**: End-to-end test: Scan → RepartitionExec → Join → Aggregate (2d)
2. **D1.2**: Validate result correctness (byte-for-byte vs. local execution) (1d)
3. **D1.3**: Test with 4 workers, 3 stages (1d)
4. **D1.4**: Chaos testing (worker failure mid-stream) (1d)

**Deliverable**:
- All existing query patterns execute correctly
- Results byte-identical to monolithic execution
- Partial failure recovery works

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

#### Workstream 2d.3: Documentation & Deployment (Week 2-3)

**Tasks**:
1. **D3.1**: Architecture documentation (stage extraction + routing) (1d)
2. **D3.2**: Operational runbook (how to scale workers; partition assignment) (0.5d)
3. **D3.3**: Deployment strategy (preview → canary → full rollout) (0.5d)
4. **D3.4**: Team training + knowledge transfer (1d)

**Deliverable**:
- Ready for production deployment
- Team trained
- Monitoring in place

---

## Timeline Overview

```
Phase 0 (Prerequisite - Phase 1):
└─ Async scheduler + Tokio integration (5 weeks, completed before Phase 2)

Phase 2a: DataFusion + Stage Extraction
├─ 2a.1: Server planner integration (2 weeks, 6-7d)
├─ 2a.2: Stage extraction algorithm (2 weeks, 6-7d)
└─ 2a.3: Task generation with mapping (1 week, 4-5d)
Total: 3-4 weeks

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
├─ 2d.1: End-to-end testing (1 week, 5-6d)
├─ 2d.2: Performance benchmarking (1-2 weeks, 4-5d)
└─ 2d.3: Documentation (1 week, 2-3d)
Total: 2-3 weeks

GRAND TOTAL: 10-14 weeks (~25-28 person-days across 4-5 engineers)
```

**Critical Path**: 2a → 2b → 2c → 2d (sequential; constrained by dependencies)

---

## Comparison: Original vs. Enhanced Phase 2

| Aspect | Original (Monolithic) | Enhanced (Distributed) |
|--------|----------------------|----------------------|
| **Stages** | All in one task | Split at RepartitionExec |
| **Workers per query** | 1 (sequential within worker) | N (parallel across workers) |
| **Data transport** | S3 (full materialization) | Arrow Flight (streaming) |
| **Partitioning** | Not needed (single worker) | Critical (Hash, RoundRobin, etc.) |
| **Latency** | Cumulative (T0 + T1 + T2) | Pipelined (overlap) |
| **Effort** | ~10 weeks | ~12-14 weeks (+2-4 weeks for complexity) |
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
7. ✅ **Code Quality**: Complexity < 25; clippy clean; comprehensive test coverage

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

1. **Phase 2a-2b merged failure**: Revert to monolithic ExecutionPlan model (no stage extraction)
   - Keep: DataFusion planner
   - Revert: Worker-to-worker Flight routing
   - Impact: Lose parallelism, keep planner benefits

2. **Phase 2c failure**: Use SinglePartition by default
   - All data converges to single output partition
   - No data loss; just inefficient
   - Impact: No parallelism for complex joins

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

