# PARALLEL Silk Road: Phase 1 Discovery

## Scope

**Investigation Focus**: Server-side query orchestration bottlenecks and parallelism opportunities.

**In Scope**:
- Current sequential stage scheduler and blocking patterns
- Stage dependency model and DAG topology
- Partition-level parallelism and within-stage execution
- Exchange (inter-stage data flow) materialization patterns
- Task dispatch and worker load balancing
- Orchestration constraints preventing true parallelism
- Task scheduling and resource awareness

**Out of Scope**:
- Worker-side operator parallelism (separate concern)
- Network protocol changes (leveraging existing exchange)
- SQL query planning logic (input assumption: valid distributed physical plan)
- RBAC/authentication policy changes
- Code changes (discovery only)

## Why This Matters

Current Kionas execution is fundamentally **sequential at the stage level**. A 3-stage query has cumulative latency: `Stage1_Time + Stage2_Time + Stage3_Time`. Even with 4-partition within-stage parallelism, downstream stages idle while waiting for upstream completion. This creates:

- **Unused worker capacity**: Workers sit idle waiting for inter-stage data
- **Extended query latency**: Multi-stage queries accumulate stage boundaries
- **Poor multi-query throughput**: Sequential blocking prevents interleaving
- **Inefficient resource utilization**: Peak memory usage per stage rather than amortized

Real parallelism would allow independent stages (or partial upstream stages) to overlap with downstream execution, reducing cumulative latency to overlapping execution traces.

## Executive Summary

### Current Execution Model

```
┌─ Stage 0  (Scan T1, T2) ────────────────────┐
│  │ Partition 0              ← ┐             │
│  │ Partition 1              ← │ Parallel    │
│  │ Partition 2              ← │             │
│  │ Partition 3              ← ┘             │
│  │ [All-or-Nothing] ────────────────────────┘
│  └─ Write to Exchange (S3)
│
├─ BLOCK: Wait for Stage 0 complete
│
└─ Stage 1  (Join) ──────────────────────────┐
   │ Partition 0              ← ┐             │
   │ Partition 1              ← │ Parallel    │
   │ Partition 2              ← │             │
   │ Partition 3              ← ┘             │
   │ [All-or-Nothing] ────────────────────────┘
   └─ Write to Exchange (S3)
   
   Timeline: T0 + T1 + overhead
```

### Bottleneck: Sequential Stage Scheduler

**Location**: [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs#L270) 
**Function**: `run_stage_groups_for_input()` 
**Pattern**: Synchronous while loop with blocking

```rust
while !ready_stages.is_empty() || !completed_stages.contains_all(final_stages) {
    let next_stage = ready_stages.pop_first(); // Must wait for deps
    dispatch_stage_partitions(next_stage);     // Parallel within stage
    wait_for_all_partitions(next_stage);       // ALL-OR-NOTHING block
    mark_stage_complete(next_stage);
}
```

**Key Constraint**: No downstream stage can begin until ALL partitions of ALL dependencies complete.

### Parallelism Opportunities Identified

#### **Opportunity A: Concurrent Independent Stages**
- **Scope**: Stages with no data dependency (e.g., separate table scans in federated queries)
- **Current State**: Not yet present in single-query model, but architecturally possible
- **Win**: Execute independent branches in parallel

#### **Opportunity B: Pipelined Downstream Stages (Partial Exchange)**
- **Scope**: Downstream stage begins consuming upstream partitions as soon as they're available
- **Current State**: All-or-nothing; downstream waits for partition 0..3 complete before reading any
- **Win**: Overlap upstream write + downstream read; reduce stage boundary latency

#### **Opportunity C: Streaming Exchange (Micro-Batches)**
- **Scope**: Replace full-materialization exchange with streaming buffers
- **Current State**: Complete write to S3 → complete read from S3
- **Win**: Reduce peak memory; enable earlier downstream start; reduce round-trip latency

#### **Opportunity D: Adaptive Partition Count & Load Balancing**
- **Scope**: Adjust partition count based on runtime metrics; detect/mitigate stragglers
- **Current State**: Fixed at plan time (default KIONAS_STAGE_PARTITION_FANOUT=4)
- **Win**: Adapt to workload skew; prevent slow partitions from blocking

#### **Opportunity E: Resource-Aware Scheduling**
- **Scope**: Track worker capacity (CPU, memory, I/O) and schedule new tasks only when resources available
- **Current State**: Blind dispatch; relies on worker queue management
- **Win**: Prevent worker overload; improve predictability

### Key Architectural Components

#### **1. Query DAG Compilation**
- **Evidence**: [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs#L69)
- **Function**: `compile_stage_task_groups()`
- **Output**: Stages with explicit dependencies encoded in task params
- **Constraint**: Dependency graph is static; evaluated at compile time

#### **2. Task Model & Dispatch**
- **Evidence**: [server/src/tasks/mod.rs](server/src/tasks/mod.rs#L1)
- **Function**: `Task` struct, task manager state
- **Current State**: Tasks carry all context inline; no streaming job scheduling

#### **3. Exchange Pattern (Inter-Stage Data)**
- **Evidence**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs#L1400)
- **Pattern**: Full materialization to S3 per partition
- **Deterministic Path**: `s3://<warehouse>/<session_id>/<query_run_id>/stage_<id>/partition_<idx>/`
- **Semantics**: Write-once; read-only; immutable artifacts

#### **4. Worker-Side Exchange Reading**
- **Evidence**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs#L1678)
- **Function**: `load_upstream_exchange_batches()`
- **Constraint**: Synchronous; must read complete artifact before proceeding

#### **5. Partition Semantics & Determinism**
- **Evidence**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs#L1795)
- **Function**: `partition_input_batches()`
- **Guarantee**: Deterministic row-modulus partitioning ensures consistent data distribution

### Current Execution Patterns

#### **Pattern 1: Single-Stage Query (Scan + Filter + Project)**
- Execution: One stage, 4 partitions in parallel
- Latency: Single stage time
- Opportunity: Minimal (already parallelized within stage)

#### **Pattern 2: Single Join (2 scans + join in separate stage)**
- Execution: 
  - Stage 0: Scan T1, Scan T2 (sequential within worker)
  - Stage 1: Join (after Stage 0 complete)
- Latency: T0_scan + T0_write + T1_join + T1_write
- Opportunity: Parallelism in scans; pipelining join reads

#### **Pattern 3: Multi-Level Aggregation (3+ stages)**
- Execution: Stage 0 (group scans) → Stage 1 (partial agg) → Stage 2 (final agg)
- Latency: Cumulative; no overlap
- Opportunity: Heavy; pipelining could reduce by 30-50%

#### **Pattern 4: Broadcast Join**
- Execution: One stage produces single partition; consumed by all downstream partitions
- Latency: Broadcast write + fanout read
- Opportunity: Skew mitigation if broadcast is straggler

### Constraints Preventing Real Parallelism

#### **At Server Scheduler Level**
1. **Blocking While Loop** → No concurrent stage dispatch
2. **All-or-Nothing Completion** → Any partition failure stalls entire stage
3. **Static Task Graph** → Dependencies decided at compile time; no dynamic adaptation
4. **Fixed Partition Count** → Set at plan time; cannot adjust for load or workload skew

#### **At Exchange Level**
1. **Full Materialization** → Must write complete outputs before downstream reads
2. **Deterministic Paths** → Fixed S3 paths prevent dynamic routing or work-stealing
3. **Synchronous Round-Trip** → Read blocks until S3 fetch completes
4. **No Streaming Semantics** → Batch boundaries not exposed for pipelining

#### **At Worker Level**
1. **Sequential Upstream Reads** → Must read all upstream partitions before proceeding
2. **No Partial Consumption** → Cannot begin join while upstream scans still running
3. **Tight Coupling** → Worker task tightly bound to specific operator; no task migration

#### **At Resource Level**
1. **No Capacity Tracking** → Scheduler blind to worker CPU/memory/I/O availability
2. **No Backpressure** → Unbounded task queues lead to overload
3. **No Straggler Mitigation** → Slow partitions block entire stage

### Architectural Decision Points for Parallelism

#### **Decision 1: Stage-Level Concurrency Model**
- **Option A**: Topological sort with ready queue; async dispatch independent stages
- **Option B**: DAG fusion; merge stages where possible to reduce boundaries
- **Option C**: Hybrid; prefer fusion, but support concurrent independent branches

#### **Decision 2: Exchange Streaming Model**
- **Option A**: Keep full materialization; add pipelined reads (partial consumption)
- **Option B**: Introduce micro-batch streaming (producers push to consumers)
- **Option C**: Hybrid; streaming for small/hot data; materialization for fault tolerance

#### **Decision 3: Partition Adaptation Strategy**
- **Option A**: Runtime metrics-driven; adjust partition count per stage based on performance
- **Option B**: Straggler mitigation; speculative cloning of slow partitions
- **Option C**: Cost-based; model I/O and compute; optimize for latency or throughput

#### **Decision 4: Resource-Aware Scheduling**
- **Option A**: Track worker metrics (CPU, memory); gate new task dispatch
- **Option B**: Cooperative backpressure via task queue depth thresholds
- **Option C**: Predictive; estimate memory usage per stage; pre-allocate worker resources

### High-Level Execution Timeline Comparison

#### **Current Sequential Execution**
```
Time:      0      5     10     15     20     25
Stage 0:   |===== |
  Write:         |===|
Stage 1:              |===== |
  Write:                    |===|
Final:                           Result ready at T22
```

#### **With Pipelined Exchange (Opportunity B)**
```
Time:      0      5     10     15     20
Stage 0:   |===== |
  Write:         |===|
Stage 1:            |===== |  ← Starts before Stage 0 complete
  Write:                  |===|
Final:                        Result ready at T19 (3s faster)
```

#### **With Streaming + Resource Awareness (Opportunity B+E)**
```
Time:      0      5     10     15     20
Stage 0:   |=======|
Stage 1:        |=======|    ← Can overlap if resources allow
Final:                    Result ready at T17 (5s faster)
```

### Gaps in Current Implementation

1. **No stage pipelining logic**: Exchange assumes all-or-nothing completion
2. **No resource tracking**: Scheduler unaware of worker capacity
3. **No straggler detection**: No metrics to identify slow partitions
4. **No streaming exchange**: I/O between stages is batched, not streamed
5. **No DAG fusion**: Separate logical stages not merged even when independent
6. **No adaptive scheduling**: Partition count and distribution fixed at plan time

### Related Evidence & References

| Component | File | Function | Key Insight |
|-----------|------|----------|-------------|
| Query planning | [select.rs](server/src/statement_handler/query/select.rs#L635) | `handle_select_query()` | Creates distributed physical plan with stages |
| DAG compilation | [distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs#L69) | `compile_stage_task_groups()` | Static DAG; no fusion or dynamic adaptation |
| **Stage scheduler** | [helpers.rs](server/src/statement_handler/shared/helpers.rs#L270) | `run_stage_groups_for_input()` | **BOTTLENECK**: while loop blocks on all-or-nothing |
| Task dispatch | [helpers.rs](server/src/statement_handler/shared/helpers.rs#L270) | `dispatch_task_and_record()` | Blind dispatch; no capacity awareness |
| Worker exchange | [pipeline.rs](worker/src/execution/pipeline.rs#L1678) | `load_upstream_exchange_batches()` | Synchronous full read; no streaming |
| Exchange storage | [exchange.rs](worker/src/storage/exchange.rs#L1) | Exchange key/path generation | Deterministic paths; no routing logic |
| Task model | [mod.rs](server/src/tasks/mod.rs#L1) | `Task` struct | Carries all context; no streaming job model |

---

## Closure Checklist

- [x] Scope boundaries defined
- [x] Current sequential scheduler bottleneck identified and located
- [x] Parallelism opportunities enumerated (A, B, C, D, E)
- [x] Architectural components mapped with evidence
- [x] Current execution patterns analyzed
- [x] Key constraints preventing parallelism documented
- [x] Architectural decision points identified
- [x] Timeline comparison provided
- [x] Key files and functions cross-referenced

---

## Next Steps

Advance to Phase 1 planning in [plan-PARALLEL-PHASE1.md](../plans/plan-PARALLEL-PHASE1.md) to define workstreams, tasks, and implementation approach for staged parallelism enablement.
