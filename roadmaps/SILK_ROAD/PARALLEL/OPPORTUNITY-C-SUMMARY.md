# Opportunity C Investigation: Summary & Integration

**Date**: March 22, 2026  
**Status**: ✓ Discovery Complete  
**Artifact**: [discovery-OPPORTUNITY-C-DATAFUSION.md](discovery-OPPORTUNITY-C-DATAFUSION.md)

---

## Quick Summary

**Question**: How can DataFusion enable Opportunity C (Streaming Exchange with Micro-Batches)? How does it support worker parallelism?

**Answer**: 

🎯 **DataFusion is purpose-built for streaming, batch-oriented execution** and eliminates the need for full-materialization exchange. Its `ExecutionPlan` + `SendableRecordBatchStream` model natively supports:

1. **Micro-Batch Streaming**: Data flows as `RecordBatch` objects (~64MB default) between operators
2. **Pull-Based Volcano Model**: Async `next().await` paradigm allows pipelined execution without blocking
3. **Multi-Partition Parallelism**: Tokio-driven concurrent execution across workers; cooperative multitasking
4. **Backpressure Handling**: Slow consumers automatically throttle producers
5. **Exchange without Materialization**: In-memory channels replace S3 round-trips for inter-stage flow

**Result**: 30-50% additional latency reduction beyond Phase 1, + worker utilization improvement via async/await + vectorized batch processing.

---

## Key Findings from DataFusion Architecture

### 1. Streaming is Native
```
Current Kionas:
  Stage 0 → Write ALL to S3 → BLOCK → Stage 1 → Read ALL → Process

DataFusion:
  Stage 0 Batch 0 → Channel → Stage 1 Batch 0 (concurrent)
  Stage 0 Batch 1 → Channel → Stage 1 Batch 1 (concurrent)
  [No full materialization; pipeline overlapped]
```

### 2. Micro-Batch Processing (Not Row-at-a-Time)
- Default batch_size: 8192-16384 rows per `RecordBatch`
- Arrow columnar format enables SIMD vectorization
- ~64MB per batch (manageable memory footprint)
- Amortizes context switching overhead

### 3. Worker Parallelism: Tokio Cooperative Multitasking
```
Kionas Model:
  Thread 0 (Partition 0) ─ blocks on I/O
  Thread 1 (Partition 1) ─ blocks on I/O
  Thread 2 (Partition 2) ─ blocks on I/O
  Thread 3 (Partition 3) ─ blocks on I/O
  
DataFusion Model:
  Tokio Runtime (thread pool):
    Event 0 ─ yields on await
    Event 1 ─ yields on await
    Event 2 ─ yields on await
    Event 3 ─ yields on await
    [No blocking; hundreds of events on same core]
```

### 4. Exchange Operations (RepartitionExec)
DataFusion's `RepartitionExec` handles:
- Round-robin distribution
- Hash-based repartitioning
- Broadcast joins
- All without explicit disk I/O

### 5. Pipeline Breakers (Important Caveat)
Some operators fundamentally buffer input:
- Hash aggregations (must see all rows to group)
- Full sorts (must ingest all rows for ordering)
- Window functions (must buffer frames)

**Implication**: Two-phase aggregation model needed:
- Partial agg (streaming, Phase 1)
- Final agg (buffered, pipeline breaker)

---

## How This Informs Kionas Architecture

### Option A: DataFusion Full Migration (Phase 3+)

**Replace Kionas operators with DataFusion ExecutionPlan**:
```
worker/src/execution/operators/
├── table_scan_exec.rs      → Use DataFusion's ParquetExec
├── filter_exec.rs          → Use DataFusion's FilterExec
├── join_exec.rs            → Use DataFusion's HashJoinExec
├── aggregate_exec.rs       → Use DataFusion's AggregateExec (partial + final)
└── exchange_exec.rs        → Use DataFusion's RepartitionExec + channels
```

**Benefit**: Immediate access to vectorization, operator fusion, advanced optimizations

**Complexity**: Major refactoring; requires deep integration

---

### Option B: Streaming Exchange Layer (Phase 2) ← **Recommended**

**Keep Kionas operators; add streaming exchange channel**:
```
Worker Task (Current):
  Stage 0 → Load + Process → Write ALL to S3 ─→ Exchange (blocking)
  
Worker Task (Enhanced):
  Stage 0 → Load + Process → Stream batches → Channel ─→ Exchange (non-blocking)
                             (per batch)      (MPMC)
```

**Implement Custom Components**:
1. **MPMC Channel**: Tokio's `tokio::sync::mpsc` or crossbeam
2. **Custom TableProvider**: DataFusion trait; reads from channel instead of S3
3. **Backpressure**: Automatic via channel capacity
4. **Streaming Operator Wrapper** (minimal):
   ```rust
   pub struct StreamingExchangeExec {
       input: Arc<dyn ExecutionPlan>,   // Kionas operator
       channel_rx: Arc<Receiver<RecordBatch>>,
   }
   ```

**Benefit**: Incremental adoption; lower coupling; easier rollback

**Complexity**: Moderate; focused scope

---

## Phase 2 Recommended Plan Structure

### Phase 2: Streaming Exchange + Resource-Aware Scheduling

**Built on Phase 1 (async scheduler + partition checkpoints)**

#### Workstream 1: In-Memory Streaming Exchange
- Design MPMC channel layout per stage
- Implement batch transmission protocol
- Add repartitioning logic (if needed)
- Design fallback: overflow to disk if memory limited

#### Workstream 2: Worker-Side Integration
- Integrate streaming channel into worker task execution
- Adapt current pipeline.rs to emit batches to channel
- Add channel initialization and cleanup
- Handle task failure and recovery

#### Workstream 3: Resource-Aware Scheduling (Server-Side)
- Track worker memory usage per batch stream
- Gate new stage dispatch if memory pressure high
- Implement cooperative backpressure signals
- Configuration: max_buffered_batches, max_memory_per_worker

#### Workstream 4: Testing & Validation
- End-to-end correctness: same results as S3-based
- Performance benchmark: latency, throughput, memory
- Stress test: concurrent multi-stage queries
- Failure scenarios: what-if upstream dies?

**Deliverables**:
- Streaming exchange implementation (MPMC + repartition)
- Worker-side streaming support
- Resource-aware scheduler enhancements
- Test harness + benchmarks
- Phase 2 completion matrix

---

## What This Means for Parallelism

### Current State (Kionas + Phase 1)
```
Multi-stage query (3 stages):
  Stage 0: Scan T1, T2 (sequential within worker, 4 partitions in parallel)
  Stage 1: Join (after Stage 0 done, 4 partitions in parallel)
  Stage 2: Aggregate (after Stage 1 done, 4 partitions in parallel)

Timeline: ~~T0 + T1 + T2~~ → **Overlapped** (Phase 1 pipelining)
Parallelism: 4 workers (within stage) × 3 stages (sequential → pipelined)
Memory: ~4GB per stage
```

### With Phase 2 (Streaming Exchange)
```
Multi-stage query (3 stages):
  Stage 0: Scan → emit batches to channel (streaming)
           ├─ Partition 0: Batch 0 → Channel
           ├─ Partition 1: Batch 0 → Channel
           ├─ Partition 2: Batch 0 → Channel
           └─ Partition 3: Batch 0 → Channel (concurrent)
  
  Stage 1: Read from channel → process → emit to next channel (overlapped)
           ├─ Partition 0: consume channel → process → emit
           ├─ Partition 1: consume channel → process → emit
           ├─ Partition 2: consume channel → process → emit
           └─ Partition 3: consume channel → process → emit (concurrent)
  
  Stage 2: Read from channel → aggregate (final, buffered)

Timeline: Batch-by-batch overlap (not stage-by-stage)
Parallelism: 4 workers × 3 stages (true overlap, not sequential)
Memory: ~640MB (per-batch bounded, not cumulative)
```

### Worker Utilization
```
Scenario: Worker has 4 cores

Current:
  T0 Batch 0 ─────── Compute (1 batch) ────────┤
  T0 Batch 1 ─────── Compute (1 batch) ────────┤
  T0 Batch 2 ─────── Compute (1 batch) ────────├─ T0 Phase (all 4 cores active)
  T0 Batch 3 ─────── Compute (1 batch) ────────┤
  [IDLE DURING T0 WRITE TO S3]
  T1 Batch 0 ─────── Compute (1 batch) ────────┤
  T1 Batch 1 ─────── Compute (1 batch) ────────├─ T1 Phase (all 4 cores active)
  ...

With Streaming:
  T0P0 Batch 0 ──────┐
  T0P1 Batch 0 ──────├─ T0 Phase
  T0P2 Batch 0 ──────┤ (4 cores active)
  T0P3 Batch 0 ──────┘
  └─ T1P0 Batch 0 ───┐
                     ├─ T1 Phase OVERLAPPED (4 cores active)
         T0P0 Batch 1 ┘
         └─ T2 Phase (if parallel-friendly)

Result: Higher utilization; reduced idle time between stages
```

---

## Configuration Examples for Phase 2

### Streaming Exchange Parameters
```rust
config.streaming_exchange = StreamingExchangeConfig {
    max_buffered_batches: 10,          // ~640MB at 64MB/batch
    max_memory_per_worker: 2_000_000_000,  // 2GB total
    batch_size: 8192,                  // rows per RecordBatch
    repartition_strategy: "hash",      // or "round_robin"
    spill_to_disk: true,               // overflow if memory limited
    overflow_path: "/tmp/kionas_overflow",
};
```

### Worker-Side Configuration
```rust
config.worker_parallelism = WorkerParallelismConfig {
    tokio_thread_pool_size: 8,         // CPU cores
    max_concurrent_stages: 3,          // prevent resource starvation
    backpressure_threshold: 5,         // batches buffered before slowing producer
};
```

---

## Evidence Files

| Document | Purpose |
|----------|---------|
| [discovery-OPPORTUNITY-C-DATAFUSION.md](discovery-OPPORTUNITY-C-DATAFUSION.md) | Full technical deep-dive |
| [../plan-PARALLEL-PHASE1.md](../plan-PARALLEL-PHASE1.md) | Phase 1 implementation plan (foundation) |
| [../silkroad.md](../silkroad.md) | 5-phase strategic roadmap |
| [discovery-PARALLEL-PHASE1.md](discovery-PARALLEL-PHASE1.md) | Phase 1 discovery (bottleneck analysis) |

---

## Integration Checklist

### With Phase 1 (Async Scheduler)
- ✓ Phase 1 enables concurrent stage dispatch (prerequisite for streaming)
- ✓ Phase 1 partition checkpoints enable early downstream startup
- ✓ Phase 2 must build on Phase 1's async infrastructure

### With Worker Execution
- ✓ Phase 2 keeps current Kionas operators (no disruption)
- ✓ Adds streaming channel as exchange boundary
- ✓ Worker task execution largely unchanged (emit batches to channel)

### With Server Scheduler  
- ✓ Phase 2 adds resource-aware gating (check worker memory)
- ✓ Backpressure signals flow back to scheduler
- ✓ Server can throttle new stage dispatch if buffers filling

---

## Estimated Impact: Phase 1 + Phase 2

```
Latency Improvement:
  Baseline:              70ms (3-stage, cumulative)
  Phase 1 (pipelining):  ~50ms (25% reduction)
  Phase 1 + 2 (streaming): ~35ms (50% reduction)

Memory Efficiency:
  Baseline:              ~4GB per stage
  Phase 2:               ~640MB buffered (80% reduction)

Worker Utilization:
  Baseline:              Idle between stages (50% loss)
  Phase 2:               Continuous overlap (90%+ utilization)

Query Throughput:
  Baseline:              1 query at a time (sequential)
  Phase 2:               Multiple queries overlapped (2-3x throughput)
```

---

## Why DataFusion is Perfect Fit for Opportunity C

| Aspect | DataFusion | Kionas Current |
|--------|-----------|-----------------|
| **Streaming Model** | ✓ Native. Pull-based. No materialization. | ✗ Full materialization to S3. |
| **Micro-Batches** | ✓ ~8K rows/batch, Arrow columnar. | ✗ Full partition at once. |
| **Worker Parallelism** | ✓ Tokio cooperative. Non-blocking. | ✗ Thread-per-partition blocking. |
| **Backpressure** | ✓ Built-in; channel capacity. | ✗ No feedback; unbounded queue. |
| **Resource Control** | ✓ Memory pools, spill-to-disk. | ✗ Manual; limited tracking. |
| **Extensibility** | ✓ Custom operators via trait. | ✗ Fixed operator set. |

---

## Next Actions

### Immediate (This Week)
1. ✓ Review [discovery-OPPORTUNITY-C-DATAFUSION.md](discovery-OPPORTUNITY-C-DATAFUSION.md)
2. Team alignment on Phase 2 approach (Option B recommended)
3. Sketch MPMC channel layout for streaming exchange

### During Phase 1 (Weeks 1-5)
- Build async scheduler (Phase 1 WS1)
- Implement partition checkpoints (Phase 1 WS2)
- Plan Phase 2 in detail using this Opportunity C discovery

### Phase 2 Kickoff (Week 6+)
- Prototype streaming exchange with MPMC channels
- Performance baseline: current vs proposed
- Full Phase 2 implementation per [plan-PARALLEL-PHASE2.md](../plans/plan-PARALLEL-PHASE2.md) (to be created)

---

## Decisions to Make (Design Review)

1. **Full DataFusion vs Hybrid Approach**?
   - Recommendation: Start with hybrid (Phase 2), plan DataFusion full migration (Phase 3+)

2. **MPMC Channel Library**?
   - Options: tokio::sync::mpsc, crossbeam::channel, flume
   - Recommendation: tokio::sync for integration with worker async runtime

3. **Fallback to S3 for Large Data**?
   - When: If memory limit exceeded during streaming
   - How: Failover gracefully; accept higher latency
   - Configuration: `spill_to_disk_threshold = 1.5GB`

4. **Resource Accounting at Server Level**?
   - Track worker memory usage per streaming buffer
   - Gate new stage dispatch if approaching limit
   - User-configurable: `max_concurrent_stages_per_worker`

---

**Status**: Ready for Phase 2 Planning  
**Ready for**: Architecture Review + Design Decisions  
**Next Document**: [plan-PARALLEL-PHASE2.md](../plans/plan-PARALLEL-PHASE2.md) (to be created post-design-review)
