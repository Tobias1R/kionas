# PARALLEL Silk Road: DataFusion Full Migration Phase 2 Discovery

**Date**: March 22, 2026  
**Status**: Discovery Artifact (Strategic Pivot)  
**Scope**: Complete replacement of Kionas operator stack with Apache DataFusion ExecutionPlan model

---

## Executive Summary

**Strategic Decision**: Adopt DataFusion as the primary execution engine for all query execution, replacing the entire Kionas custom operator pipeline.

**Rationale**:
- DataFusion provides **production-grade streaming execution** (not a bolt-on feature)
- Eliminates technical debt of maintaining parallel operator implementations
- Unlocks vectorization, operator fusion, and advanced optimizations (already in DataFusion)
- Simplifies worker-side code dramatically (ExecutionPlan trait instead of custom operator dispatch)
- Native support for multiple data sources (Parquet, CSV, DuckDB, Delta, and custom TableProvider)
- Strong community support and ongoing optimization work

**Trade-off Analysis**:
| Aspect | Remaining Custom | DataFusion | Winner |
|--------|-----------------|-----------|--------|
| **Maintenance burden** | High (4+ operators) | Low (community-owned) | DF |
| **Feature velocity** | Slow (custom work) | Fast (active project) | DF |
| **Vectorization** | Limited (single-row) | Native (Arrow columnar) | DF |
| **Optimization** | Manual | Automatic (rule-based) | DF |
| **Testing burden** | High (custom tests) | Lower (shared test suite) | DF |
| **Learning curve** | Already paid | New (manageable via docs) | Mixed |

**Result**: Net positive. One-time migration cost justified by long-term gains.

---

## Current Architecture (To Be Replaced)

### Kionas Operator Stack

```
Server (Query Planning):
  ├─ SQLParser → DistributedPlan
  ├─ LogicalPlan → PhysicalPlan
  └─ PhysicalPlan → Stage DAG (task graph)

Worker (Execution):
  ├─ Task receives operator specs (ScanSpec, FilterSpec, JoinSpec, etc.)
  ├─ Pipeline orchestrates operators:
  │  ├─ TableScanOperator (manual Parquet read)
  │  ├─ FilterOperator (row filtering)
  │  ├─ JoinOperator (hash join)
  │  ├─ AggregateOperator (partial + final)
  │  └─ ProjectionOperator (column selection)
  └─ Results → flight_proxy for client transmission
```

### Key Constraints

| Layer | Component | Limitation |
|-------|-----------|-----------|
| **Server** | Plan serialization | Task carries inline operator specs; no plan reuse |
| **Server** | Distribution | Static partition count; no adaptive allocation |
| **Worker** | Operators | Single-row semantics; limited vectorization |
| **Worker** | Memory** | Cumulative buffering (no spill-to-disk strategy) |
| **Worker** | Parallelism | Thread-per-partition blocking model |
| **Exchange** | Transport | Full S3 materialization required |
| **Exchange** | Streaming | No support for micro-batch pipelining |

---

## DataFusion Architecture (Proposed Replacement)

### High-Level Execution Model

```
Server (Query Planning):
  ├─ SQLParser
  ├─ LogicalPlan (arrow-datafusion)
  ├─ Optimizer (rule-based; automatic)
  └─ ExecutionPlan (fully optimized, ready to execute)
       └─ Serialized to worker (bincode or protobuf)

Worker (Execution):
  ├─ Task deserializes ExecutionPlan
  ├─ ExecutionContext::execute(plan)
  │  └─ Returns SendableRecordBatchStream (async stream)
  │     ├─ Vectorized operators (Arrow-native)
  │     ├─ Operator fusion (filters fused to scans)
  │     ├─ Automatic spill-to-disk (for breakers like sort/agg)
  │     └─ Micro-batch emission (RecordBatch at ~8K rows)
  └─ Stream → flight_proxy adapter (NEW)
```

### Key Components

#### 1. **Server-Side: ExecutionPlan Generation**

**Current**: Task carries operator specs (ScanSpec, JoinSpec, etc.)
**New**: Task carries ExecutionPlan serialization

```rust
// Current
struct Task {
    query_id: String,
    stage_id: usize,
    partition_id: usize,
    specs: OperatorSpecs,  // Custom spec types
}

// New
struct Task {
    query_id: String,
    stage_id: usize,
    partition_id: usize,
    execution_plan: Vec<u8>,  // Serialized ExecutionPlan (bincode)
}
```

**Changes Required**:
- Replace Kionas planner with DataFusion planner (or wrapper)
- Serialize ExecutionPlan to task (bincode or protobuf)
- Handle remote execution context (worker is remote; need SessionConfig, RuntimeEnv)

**Benefits**:
- Optimizer runs server-side (single pass; expensive computation)
- Worker receives pre-optimized plan (minimal overhead)
- Consistent optimization rules across all queries

#### 2. **Worker-Side: ExecutionPlan Deserialization & Execution**

**Current**: Manual pipeline orchestration

```rust
// Current pattern
let mut batches = Vec::new();
while let Some(batch) = scan_operator.next()? {
    let filtered = filter_operator.filter(batch)?;
    let joined = join_operator.join(filtered, other)?;
    batches.push(joined);
}
```

**New**: Single ExecutionPlan execution call

```rust
// New pattern
let ctx = ExecutionContext::new();
let plan = deserialize_execution_plan(task.execution_plan)?;
let stream = ctx.execute(plan).await?;

while let Some(batch) = stream.next().await {
    // Each batch is optimized, vectorized, pre-filtered
    send_to_flight_proxy(batch)?;  // NEW: Direct streaming
}
```

**Benefits**:
- No custom operator pipeline code needed
- Vectorized execution (SIMD within each batch)
- Automatic operator fusion (filter-scan fusion, etc.)
- Spill-to-disk automatically managed

#### 3. **Exchange: DataFusion RepartitionExec**

**Current**: Manual S3 write + read

```rust
// Current
write_all_batches_to_s3()?;         // Blocking; full materialization
let all_batches = read_all_from_s3()?;  // Synchronous read
```

**New**: DataFusion's RepartitionExec

```rust
// New
let exchange_exec = RepartitionExec::new(
    Box::new(upstream_plan),
    Partitioning::Hash(keys, num_partitions)
);

// Executed by downstream partition
let stream = exchange_exec.execute(partition_id)?;
// Automatically handles repartitioning without S3 for in-memory workloads
```

**Benefits**:
- Repartitioning is logical (no temp materialization for small data)
- Falls back to disk spill if needed (memory-aware)
- Automatic backpressure via channel capacity
- Native streaming (no all-or-nothing blocking)

---

## Architecture Changes by Layer

### 1. Server-Side Changes

#### Current Query Flow
```
SQL Query
  ↓
SQLParser → LogicalPlan → PhysicalPlan
  ↓
Kionas Optimizer (minimal)
  ↓
Stage DAG compilation (static)
  ↓
Task dispatcher (carries inline OperatorSpecs)
```

#### New Query Flow
```
SQL Query
  ↓
SQLParser → LogicalPlan → PhysicalPlan
  ↓
DataFusion Optimizer (rule-based; aggressive)
  ↓
ExecutionPlan (fully optimized)
  ↓
Serialize ExecutionPlan to task
  ↓
Task dispatcher (carries ExecutionPlan bytes)
```

**Key Decision**: Where does the server run DataFusion's optimizer?

**Option A: DataFusion on Server**
```rust
use datafusion::prelude::*;
use datafusion::execution::context::ExecutionContext;

let ctx = ExecutionContext::new();
let df_plan = ctx.sql("SELECT ...").await?;
let physical_plan = df_plan.create_physical_plan().await?;
// Serialize physical_plan to task
```

**Option B: Kionas Server acts as Client to DataFusion Planning**
```rust
// Kionas server uses DataFusion SDK to plan queries
// But plan optimization happens in separate DataFusion process (for isolation)
// Risk: Network overhead for planning; complexity of distributed planning
```

**Recommendation**: **Option A** (DataFusion on server). Simpler; planning is single-threaded bottleneck anyway.

#### Files to Modify

| File | Change | Impact |
|------|--------|--------|
| `server/src/statement_handler/query/select.rs` | Use DataFusion planner instead of Kionas planner | Medium |
| `server/src/tasks/mod.rs` | Task.execution_plan field (Vec<u8>) instead of specs | Low |
| `server/src/statement_handler/distributed_dag.rs` | ExecutionPlan serialization logic | Medium |
| `server/build.rs` | Add DataFusion dependency (large!) | Low |
| `server/Cargo.toml` | datafusion crate dependencies | Low |

#### New Dependency Considerations

DataFusion adds ~500MB to binary (most due to optimizer rules). Consider:
- Feature flags to reduce size (disable unused sources like GCS, etc.)
- Conditional compilation if needed

### 2. Worker-Side Changes

#### Current Execution Flow

```
Worker receives Task
  ↓
Load operator specs (ScanSpec, JoinSpec, AggSpec, etc.)
  ↓
Initialize pipeline:
  stage0: TableScanOperator(files=[...])
  stage1: FilterOperator(condition=...)
  stage2: JoinOperator(...)
  ↓
Manual batching loop:
  while has_input {
    batch = scan_op.next()
    filtered = filter_op.filter(batch)
    joined = join_op.join(filtered, ...)
    results.push(joined)
  }
  ↓
Write results to exchange (S3)
```

#### New Execution Flow

```
Worker receives Task with ExecutionPlan bytes
  ↓
Deserialize ExecutionPlan (bincode)
  ↓
Create ExecutionContext
  ↓
Execute plan:
  stream = ctx.execute(plan).await?
  while let Some(batch) = stream.next().await {
    // Batch is already vectorized, fused, optimized
    send_to_flight_proxy(batch)  // Stream directly (no S3 temp)
  }
```

#### Files to Modify

| File | Change | Impact |
|------|--------|--------|
| `worker/src/execution/pipeline.rs` | Replace manual operator orchestration with DataFusion executor | **High** |
| `worker/src/execution/planner.rs` | Remove custom RuntimePlan; deserialize ExecutionPlan instead | **High** |
| `worker/src/execution/operators/*.rs` | DELETE all operator implementations | **High** |
| `worker/src/storage/exchange.rs` | Update to use DataFusion's RepartitionExec semantics | Medium |

#### New Files Needed

```
worker/src/execution/
├── datafusion_executor.rs        ← Wraps DataFusion execution
├── flight_streaming_adapter.rs   ← NEW: Converts RecordBatch → flight::FlightData
└── execution_context.rs          ← SessionConfig, catalog, runtime env setup
```

### 3. Exchange Layer Changes

#### Current Exchange Pattern

```
Upstream Worker:
  1. Read source data
  2. Materialize all batches in memory
  3. Write complete partition to S3 (parquet)

Downstream Worker:
  1. Check if upstream S3 partition exists
  2. Read complete parquet file from S3
  3. Begin downstream operator execution
```

#### New Exchange Pattern (with DataFusion)

```
Upstream Worker:
  RecordBatchStream → RepartitionExec
    (determines which downstream partitions each row goes to)
    → Channel (in-memory MPMC)
    → Downstream Worker receives batches

Downstream Worker:
  1. Create RemoteExec (reads from upstream channel)
  2. Pass to downstream operators
  3. Data flows through pipeline as it arrives
```

**For S3 Fallback** (when memory limited):
```
RepartitionExec detects memory pressure
  → Spills batches to S3 (managed by DataFusion)
  → Reading partitions read from S3 as needed
```

---

## Critical Integration: Flight-Proxy Streaming

### Current Pattern (Single-Partition Results)

```
Worker:
  results = execute_query(task)
  for batch in results {
    write_to_flight_response(batch)  // Sent immediately
  }
  
Flight-Proxy:
  receives batch
  buffers in client_response_queue
  client polls → receives batch
```

### New Pattern (DataFusion RecordBatchStream)

```
Worker ExecutionContext:
  stream = execute(plan)           // SendableRecordBatchStream (async)
  
while let Some(batch) = stream.next().await {
    // batch is Arrow::RecordBatch
    convert_to_flight_data(batch)?  // ← NEW ADAPTER LOGIC
    send_to_flight_proxy(batch)?
}

Flight-Proxy:
  receives flight::FlightData
  extends for Arrow IPC data
  client receives streamed data
```

### Key Integration Points

#### 1. **RecordBatch → Arrow IPC Format**

DataFusion returns `Arrow::RecordBatch` (in-memory columnar format).  
Flight protocol uses Arrow IPC (Arrow Interprocess Communication) format.

```rust
// Pseudo-code conversion
pub fn batch_to_flight_data(batch: &RecordBatch) -> Result<flight::FlightData> {
    // Encode batch to Arrow IPC format
    let ipc_bytes = arrow::ipc::write::stream_to_bytes(&[batch])?;
    
    Ok(flight::FlightData {
        flight_descriptor: None,
        data_header: ipc_bytes,  // Arrow IPC header
        app_metadata: vec![],
        data_body: batch_data,   // Columnar data
    })
}
```

#### 2. **Streaming Semantics**

Current: Worker collects ALL results, then streams to client.
**New**: Worker streams batches as produced.

```rust
// Current (collect-then-stream)
let mut all_batches = Vec::new();
for batch in execute_query()? {
    all_batches.push(batch);
}
flight_response.write_all_batches(&all_batches)?;

// New (stream-as-produced)
let stream = execute_plan().await?;
while let Some(batch) = stream.next().await {
    flight_response.write_batch(batch)?;  // Write immediately
    // Client can begin processing while more data streams
}
```

**Benefits**:
- Lower latency (first rows appear faster)
- Lower memory on worker (no full result collection)
- Better for streaming workloads (partial results useful)

#### 3. **Error Handling in Stream**

DataFusion's `SendableRecordBatchStream` can return errors mid-stream.

```rust
// Stream may error at any point
while let Ok(Some(batch)) = stream.next().await {
    flight_response.write_batch(batch)?;
}
// If error, stream terminates; client sees partial results + error

// Must handle:
// - Query execution error (e.g., divide by zero in filter)
// - I/O error (e.g., parquet file parse error)
// - Operator error (e.g., OOM in sort operator)
```

**Flight Protocol Error Handling**:
```protobuf
message FlightData {
    FlightDescriptor flight_descriptor = 1;
    bytes data_header = 2;
    bytes app_metadata = 3;
    bytes data_body = 4;
}
```

When error occurs:
1. Stop writing data_body
2. Write error metadata to app_metadata
3. Client detects error in stream; rolls back or handles gracefully

---

## Implementation Phases

### Phase 2a: Server-Side DataFusion Planning (Weeks 1-2)

**Goal**: Server generates ExecutionPlans; workers receive serialized plans

**Tasks**:
1. Add DataFusion to server dependencies (Cargo.toml, build.rs)
2. Implement DataFusion query planner wrapper (uses datafusion::sql)
3. Serialize ExecutionPlan to task (bincode encoding)
4. Test with single query type (select)
5. Verify plan optimization (check optimizer output)

**Deliverable**: Server can serialize ExecutionPlans to tasks

### Phase 2b: Worker-Side Execution Adapter (Weeks 2-4)

**Goal**: Workers execute DataFusion plans; stream results to flight-proxy

**Tasks**:
1. Add DataFusion to worker dependencies
2. Create `DataFusionExecutor` wrapper (deserialize + execute plan)
3. Implement `RecordBatch → flight::FlightData` converter
4. Replace manual operator pipeline with DataFusion execution
5. Integrate streaming adapter with flight-proxy
6. Handle errors in stream (partial results + error signaling)

**Deliverable**: Worker can execute DataFusion plans and stream results

### Phase 2c: Exchange Integration (Weeks 4-5)

**Goal**: Replace S3 exchange with DataFusion RepartitionExec logic

**Tasks**:
1. Implement in-memory MPMC channels for small repartitioning
2. Add S3 fallback for memory pressure
3. Integrate RepartitionExec semantics with exchange
4. Test pipelined repartitioning (data flows without full materialization)
5. Validate exchange correctness

**Deliverable**: Repartitioning happens via streaming channels; S3 fallback works

### Phase 2d: Correctness & Performance (Weeks 5-6)

**Goal**: All existing queries work; meet latency targets

**Tasks**:
1. Regression test suite (all existing queries)
2. Performance benchmarking (latency, throughput)
3. Memory profiling (spill-to-disk behavior)
4. Error scenario testing (query failures, stragglers)
5. Fix issues; optimize hot paths

**Deliverable**: Migration complete; no regressions

---

## Risk Assessment

### Risk 1: DataFusion Dependency Size & Complexity

**Severity**: Medium  
**Current**: Worker binary ~50MB  
**With DataFusion**: ~150-200MB (optimizer rules, plan repr, etc.)

**Mitigation**:
- Feature gates: disable unused sources (GCS, Delta, etc.)
- Separate binary builds if needed
- Accept size increase as trade-off for maintainability

### Risk 2: Serialization/Deserialization Overhead

**Severity**: Low  
**Issue**: ExecutionPlan serialized on server, deserialized on worker

**Mitigation**:
- Use efficient encoding (bincode, not JSON)
- Profile serialization overhead (~1-5% of query time)
- Accept cost as one-time per query

### Risk 3: Custom Operator Loss

**Severity**: Medium  
**Issue**: Kionas-specific operators (if any) cannot be used

**Mitigation**:
- DataFusion supports custom operators via `ExecutionPlan` trait
- Build Kionas-specific operators as DataFusion extensions
- Remove Kionas-only operators if not critical

### Risk 4: Flight-Proxy Integration Issues

**Severity**: High  
**Issue**: Streaming RecordBatch → FlightData conversion must be robust

**Mitigation**:
- Thorough testing of streaming adapter
- Clear error handling in stream
- Fallback to batch-at-a-time if streaming fails
- Runbook for debugging stream issues

### Risk 5: Query Plan Compatibility

**Severity**: Medium  
**Issue**: DataFusion's ExecutionPlan may not support all Kionas queries

**Mitigation**:
- Start migration with simple queries (select, filter, project)
- Add complex operators incrementally (joins, aggregations, windows)
- Build custom operators for Kionas-specific logic (if needed)

---

## Comparison: Current vs. Proposed Architecture

| Aspect | Current Kionas | DataFusion-Based |
|--------|----------------|-----------------|
| **Optimizer** | Manual; minimal | Rule-based; aggressive |
| **Operators** | Custom (~4 types) | 20+ built-in; extensible |
| **Vectorization** | Limited | Native (Arrow columnar) |
| **Exchange** | S3 materialization | Streaming + optional S3 |
| **Memory mgmt** | Manual buffering | Automatic spill-to-disk |
| **Error handling** | Custom recovery | Built-in retry logic |
| **Testing** | Custom test suite | Shared ecosystem tests |
| **Maintenance** | Kionas team | Community + Kionas extensions |
| **Performance** | Baseline | +20-40% (vectorization + fusion) |
| **Complexity** | Low | Higher (more features) |
| **TTM for features** | Slow | Fast (community driven) |

---

## Success Criteria

1. **All existing queries execute correctly** (byte-identical results)
2. **No performance regression** (latency >= baseline)
3. **Streaming works** (results appear as produced, not batch-at-end)
4. **Memory efficiency** (spill-to-disk works for large intermediate)
5. **Error handling** (mid-stream errors handled gracefully)
6. **Code simplification** (eliminate ~2000 LOC of custom operators)
7. **Flight integration** (RecordBatch → FlightData conversion robust)

---

## Dependencies & Prerequisites

1. **Phase 1**: Async scheduler + partition checkpoints (prerequisite)
2. **Flight protocol** understanding (Arrow IPC format)
3. **DataFusion documentation** (query planning, execution context)
4. **Arrow recombination** (RecordBatch format)

---

## References

- [DataFusion ExecutionPlan](https://docs.rs/datafusion/latest/datafusion/physical_plan/trait.ExecutionPlan.html)
- [Arrow IPC Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-format)
- [SendableRecordBatchStream](https://docs.rs/datafusion/latest/datafusion/physical_plan/type.SendableRecordBatchStream.html)
- [Flight Protocol](https://arrow.apache.org/docs/format/Flight.html)

---

**Status**: Ready for Phase 2 planning  
**Next Step**: Create [plan-DATAFUSION-MIGRATION-PHASE2.md](../plans/plan-DATAFUSION-MIGRATION-PHASE2.md) with detailed workstreams
