# PARALLEL Silk Road: Phase 2 DataFusion Full Migration Implementation Plan

**Phase Goal**: Replace entire Kionas operator stack with Apache DataFusion ExecutionPlan model. Enable streaming execution from server planning through worker execution to flight-proxy output.

**Success Criteria**:
- All existing Kionas queries execute via DataFusion 
- Server generates optimized ExecutionPlans; workers deserialize and execute
- Results stream from worker to client without full-materialization buffering
- No performance regression; target 10-30% latency improvement (vectorization)
- Memory efficiency via automatic spill-to-disk
- Flight-proxy receives Arrow IPC-formatted data

**Timeline**: ~6 weeks  
**Complexity**: High (major architectural shift)

---

## Workstream 1: Server-Side ExecutionPlan Generation (Weeks 1-2)

### Objective
Replace Kionas query planner with DataFusion planner. Server generates optimized ExecutionPlans and serializes them to tasks.

### Current State
```
SQL Query
  ↓
Kionas Parser + Planner
  ├─ LogicalPlan (minimal)
  ├─ PhysicalPlan (custom Kionas plan)
  └─ Stage DAG compiler
  ↓
Task.specs = {ScanSpec, FilterSpec, JoinSpec, ...}  // Inline specs
```

### Proposed State
```
SQL Query
  ↓
DataFusion Planner
  ├─ Parse SQL
  ├─ Generate LogicalPlan
  ├─ Apply optimizer rules (automatic)
  └─ Generate optimized ExecutionPlan
  ↓
Task.execution_plan = serialize(ExecutionPlan)  // Bincode
```

### Technical Approach

#### A1.1: Add DataFusion Dependency

**File**: `server/Cargo.toml`

```toml
[dependencies]
datafusion = { version = "52.2.0", features = ["parquet", "sql"] }
tokio = { version = "1", features = ["full"] }
```

**File**: `server/build.rs`

Add DataFusion to build if not already present (check for conflicts with worker/kionas).

**Effort**: 30 min (1 developer)

#### A1.2: Create DataFusion Query Planner Wrapper

**File**: `server/src/query_planning/planner.rs` (NEW)

```rust
/// DataFusion query planner for Kionas distributed execution
pub struct DataFusionPlanner {
    ctx: ExecutionContext,
}

impl DataFusionPlanner {
    pub fn new() -> Result<Self> {
        let ctx = ExecutionContext::new();
        // Register Kionas-specific functions if needed
        Ok(Self { ctx })
    }
    
    /// Plan a SQL query to an optimized ExecutionPlan
    pub async fn plan_query(
        &self,
        sql: &str,
        session_config: SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Parse SQL
        let df_plan = self.ctx.sql(sql).await?;
        
        // Apply DataFrame transformations if needed (e.g., partitioning hints)
        // Return ExecutionPlan (already optimized by DataFusion)
        df_plan.create_physical_plan().await
    }
    
    /// Handle distributed partitioning for each stage
    pub fn add_repartition(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        partition_count: usize,
        keys: Vec<Expr>,
    ) -> Arc<dyn ExecutionPlan> {
        Box::new(RepartitionExec::new(
            plan,
            Partitioning::Hash(keys, partition_count)
        ))
    }
}
```

**Effort**: 4-6 hours (1 architect + 1 developer)

#### A1.3: Task Model Update

**File**: `server/src/tasks/mod.rs`

```rust
// Current
pub struct Task {
    query_id: String,
    stage_id: usize,
    partition_id: usize,
    specs: OperatorSpecs,  // REMOVE THIS
}

// New
pub struct Task {
    query_id: String,
    stage_id: usize,
    partition_id: usize,
    execution_plan: Vec<u8>,  // Serialized ExecutionPlan (bincode)
}
```

**Changes**:
- Remove `specs` field
- Add `execution_plan` field (Vec<u8>)
- Update serialization/deserialization logic

**Effort**: 2-3 hours

#### A1.4: Planner Integration into Query Handler

**File**: `server/src/statement_handler/query/select.rs`

**Current Flow**:
```rust
pub async fn handle_select_query(req: SelectRequest) -> Result<QueryResponse> {
    let plan = kionas_planner.plan(sql)?;  // Kionas planner
    let stages = compile_stage_dag(plan)?;
    for stage in stages {
        dispatch_task(stage)?;
    }
}
```

**New Flow**:
```rust
pub async fn handle_select_query(req: SelectRequest) -> Result<QueryResponse> {
    let planner = DataFusionPlanner::new()?;
    let execution_plan = planner.plan_query(&sql, session_config).await?;
    
    // Still create stage DAG for distributed execution
    // But each task carries ExecutionPlan instead of specs
    let stages = compile_stage_dag_for_execution_plan(execution_plan)?;
    
    for stage in stages {
        let task = Task {
            execution_plan: bincode::serialize(&stage.plan)?,
            // ... other fields
        };
        dispatch_task(task)?;
    }
}
```

**Effort**: 3-4 hours

#### A1.5: Testing & Validation

**File**: `server/src/query_planning/tests/datafusion_planner_tests.rs` (NEW)

```rust
#[tokio::test]
async fn test_simple_select() {
    let planner = DataFusionPlanner::new().unwrap();
    let plan = planner.plan_query(
        "SELECT * FROM table1",
        SessionConfig::new()
    ).await.unwrap();
    
    assert!(!plan.schema().is_empty());
}

#[tokio::test]
async fn test_filter_pushdown() {
    let planner = DataFusionPlanner::new().unwrap();
    let plan = planner.plan_query(
        "SELECT * FROM table1 WHERE col1 > 100",
        SessionConfig::new()
    ).await.unwrap();
    
    // Verify filter is pushed to scan
    // (DataFusion optimizer should do this automatically)
}
```

**Effort**: 4-6 hours

### Deliverables
- DataFusion integrated into server build
- Query planner generates ExecutionPlans
- Tasks carry serialized ExecutionPlans (not inline specs)
- Basic query types (select, filter, project) tested and working

### Risks
- DataFusion planner behavior differs from Kionas planner
- **Mitigation**: Start simple queries; add complexity gradually

---

## Workstream 2: Worker-Side ExecutionPlan Execution (Weeks 2-4)

### Objective
Workers deserialize ExecutionPlans and execute them efficiently. Stream results to flight-proxy without S3 materialization for intermediate results.

### Current State
```
Worker Task:
  ├─ Load specs (ScanSpec, FilterSpec, JoinSpec)
  ├─ Initialize pipeline operators (manual orchestration)
  ├─ Execute loop:
  │  while has_input {
  │    batch = scan_op.next()
  │    filtered = filter_op.filter(batch)
  │    joined = join_op.join(filtered, ...)
  │    results.push(joined)
  │  }
  └─ Write results to S3; signal completion
```

### Proposed State
```
Worker Task:
  ├─ Deserialize ExecutionPlan from task.execution_plan bytes
  ├─ Create ExecutionContext with proper SessionConfig
  ├─ Execute plan: stream = ctx.execute(plan).await?
  ├─ Stream loop:
  │  while let Some(batch) = stream.next().await {
  │    convert_batch_to_flight_data(batch)
  │    send_to_flight_proxy(batch)
  │  }
  └─ Streaming complete; results streamed directly (no S3 temp)
```

### Technical Approach

#### B2.1: Create DataFusion Executor Wrapper

**File**: `worker/src/execution/datafusion_executor.rs` (NEW)

```rust
/// Wraps DataFusion execution context for Kionas distributed execution
pub struct DataFusionExecutor {
    ctx: ExecutionContext,
}

impl DataFusionExecutor {
    pub async fn new() -> Result<Self> {
        let cfg = SessionConfig::new();
        let ctx = ExecutionContext::new_with_config(cfg);
        Ok(Self { ctx })
    }
    
    /// Execute a serialized ExecutionPlan
    pub async fn execute_plan(
        &self,
        plan_bytes: &[u8],
    ) -> Result<SendableRecordBatchStream> {
        // Deserialize plan
        let plan: Arc<dyn ExecutionPlan> = bincode::deserialize(plan_bytes)?;
        
        // Execute (returns streaming iterator)
        let stream = plan.execute(0)?;  // partition_id = 0 for simplicity
        Ok(stream)
    }
}
```

**Effort**: 2-3 hours

#### B2.2: Flight-Proxy Streaming Adapter

**File**: `worker/src/execution/flight_streaming_adapter.rs` (NEW)

```rust
/// Converts Arrow RecordBatch to Flight::FlightData for Arrow IPC transport
pub struct FlightStreamingAdapter;

impl FlightStreamingAdapter {
    /// Convert RecordBatch to FlightData (Arrow IPC format)
    pub fn batch_to_flight_data(batch: &RecordBatch) -> Result<flight::FlightData> {
        // Encode batch to Arrow IPC format
        let mut writer = arrow::ipc::writer::StreamWriter::new(/* ... */)?;
        writer.write(&batch)?;
        
        let ipc_bytes = writer.into_inner()?;
        
        Ok(flight::FlightData {
            flight_descriptor: None,
            data_header: ipc_bytes,
            app_metadata: vec![],
            data_body: vec![],  // Data is in header for IPC format
        })
    }
    
    /// Send batch directly to flight response
    pub fn send_to_flight(
        batch: &RecordBatch,
        flight_response: &mut FlightResponseWriter,
    ) -> Result<()> {
        let flight_data = Self::batch_to_flight_data(batch)?;
        flight_response.write(flight_data)?;
        Ok(())
    }
}
```

**Effort**: 3-4 hours

#### B2.3: Replace Manual Pipeline with DataFusion Execution

**File**: `worker/src/execution/pipeline.rs`

**Current** (`execute_query_task` function):
```rust
pub async fn execute_query_task(task: Task) -> Result<()> {
    // Load specs from task
    let scan_spec = task.specs.scan_spec()?;
    let filter_spec = task.specs.filter_spec()?;
    
    // Initialize operators manually
    let mut scan_op = TableScanOperator::new(scan_spec)?;
    let mut filter_op = FilterOperator::new(filter_spec)?;
    
    // Manual execution loop
    let mut all_results = Vec::new();
    while let Some(batch) = scan_op.next()? {
        let filtered = filter_op.filter(batch)?;
        all_results.push(filtered);
    }
    
    // Write to S3
    write_to_exchange(&all_results, task.stage_id, task.partition_id)?;
}
```

**New**:
```rust
pub async fn execute_query_task(task: Task) -> Result<()> {
    // Deserialize ExecutionPlan
    let executor = DataFusionExecutor::new().await?;
    let mut stream = executor.execute_plan(&task.execution_plan).await?;
    
    // Stream batches directly to flight-proxy (no S3 temp)
    while let Some(batch) = stream.next().await {
        let batch = batch?;  // Handle RecordBatch errors
        
        // Convert to flight format
        FlightStreamingAdapter::send_to_flight(&batch, flight_response)?;
        
        // For partition-based exchange still needed:
        // (Deferred to WS3: exchange integration)
    }
    
    Ok(())
}
```

**Effort**: 4-6 hours

#### B2.4: SessionConfig & Execution Context Setup

**File**: `worker/src/execution/execution_context.rs` (NEW)

```rust
/// Initialize DataFusion ExecutionContext with Kionas-specific configuration
pub fn create_execution_context() -> Result<ExecutionContext> {
    let mut cfg = SessionConfig::new();
    
    // Tuning parameters
    cfg = cfg
        .with_target_partitions(num_cpus::get())  // Use all cores
        .with_batch_size(8192)  // Default batch size
        .with_memory_limit(mem_available() / 2);  // 50% of available memory
    
    let ctx = ExecutionContext::new_with_config(cfg);
    Ok(ctx)
}

pub fn mem_available() -> usize {
    // Query system memory; use sys-info or similar crate
}
```

**Effort**: 2-3 hours

#### B2.5: Error Handling in Stream

**File**: `worker/src/execution/stream_error_handler.rs` (NEW)

```rust
/// Handle errors that occur mid-stream execution
pub struct StreamErrorHandler;

impl StreamErrorHandler {
    /// Handle error occurring in middle of batch stream
    pub async fn handle_stream_error(
        e: DataFusionError,
        flight_response: &mut FlightResponseWriter,
    ) -> Result<()> {
        // Options:
        // 1. Send partial results + error metadata
        // 2. Rollback and retry
        // 3. Return error immediately
        
        // Recommended: Send error metadata via app_metadata field
        let error_metadata = format!("Error: {}", e.to_string());
        
        flight_response.write(flight::FlightData {
            app_metadata: error_metadata.into(),
            ..Default::default()
        })?;
        
        Ok(())
    }
}
```

**Effort**: 2-3 hours

#### B2.6: Testing & Validation

**File**: `worker/src/execution/tests/datafusion_executor_tests.rs` (NEW)

```rust
#[tokio::test]
async fn test_simple_scan_execution() {
    let executor = DataFusionExecutor::new().await.unwrap();
    
    // Create a simple scan plan
    let plan = create_scan_plan("SELECT * FROM test_table").unwrap();
    let plan_bytes = bincode::serialize(&plan).unwrap();
    
    let mut stream = executor.execute_plan(&plan_bytes).await.unwrap();
    
    // Should get at least one batch
    assert!(stream.next().await.is_some());
}

#[tokio::test]
async fn test_flight_streaming_adapter() {
    let batch = create_test_batch();
    let flight_data = FlightStreamingAdapter::batch_to_flight_data(&batch).unwrap();
    
    assert!(!flight_data.data_header.is_empty());
}
```

**Effort**: 4-6 hours

### Deliverables
- DataFusion executor integrated into worker
- ExecutionPlans deserialized and executed
- Results stream to flight-proxy as RecordBatches
- All existing queries produce byte-identical results
- Error handling for mid-stream failures

### Risks
- RecordBatch format incompatibility with flight-proxy
- **Mitigation**: Extensive testing of flight adapter; fallback to batch-at-end if needed

---

## Workstream 3: Exchange Integration & Repartitioning (Weeks 4-5)

### Objective
Replace manual S3 exchange with DataFusion's RepartitionExec semantics. Support both in-memory streaming and S3 fallback.

### Current State
```
Upstream Task writes all batches to S3
Downstream Task reads all batches from S3
No overlap; all-or-nothing blocking
```

### Proposed State
```
Upstream ExecutionPlan includes RepartitionExec
  ├─ Determines which partition each row goes to
  ├─ Sends via channel (in-memory MPMC)
  └─ Large data spills to S3 if memory exceeded

Downstream Task reads from channel
  └─ Streams without waiting for completion
```

### Technical Approach

#### C3.1: MPMC Channel Setup

**File**: `server/src/statement_handler/shared/distributed_dag.rs`

Assign channel IDs to inter-stage boundaries:

```rust
pub struct StageBoundary {
    upstream_stage: usize,
    downstream_stage: usize,
    channel_id: String,  // NEW
}

// For each boundary, create MPMC channel
pub fn create_stage_exchanges(boundary_count: usize) -> Result<Vec<(Sender, Receiver)>> {
    (0..boundary_count)
        .map(|i| {
            let (tx, rx) = tokio::sync::mpsc::channel(10);  // Buffer 10 batches
            Ok((tx, rx))
        })
        .collect()
}
```

**Effort**: 2 hours

#### C3.2: RepartitionExec Configuration

Update `distributed_dag.rs` to inject RepartitionExec between stages:

```rust
pub fn add_repartition_between_stages(
    upstream_plan: Arc<dyn ExecutionPlan>,
    partition_keys: Vec<Expr>,
    num_partitions: usize,
) -> Arc<dyn ExecutionPlan> {
    Box::new(RepartitionExec::new(
        upstream_plan,
        Partitioning::Hash(partition_keys, num_partitions)
    ))
}
```

**Effort**: 2 hours

#### C3.3: S3 Fallback for Memory Pressure

**File**: `worker/src/storage/exchange_spill.rs` (NEW)

```rust
/// Spill repartitioned data to S3 if memory limit exceeded
pub struct ExchangeSpiller {
    max_memory: usize,
    current_memory: usize,
    spill_path: String,
}

impl ExchangeSpiller {
    pub async fn spill_to_s3(
        batch: RecordBatch,
        partition_id: usize,
    ) -> Result<()> {
        // Write batch to S3 path
        // Return S3 reference instead of in-memory
        write_batch_to_s3(&batch, partition_id).await?;
        Ok(())
    }
    
    pub fn should_spill(&self) -> bool {
        self.current_memory > self.max_memory * 90 / 100  // 90% threshold
    }
}
```

**Effort**: 3-4 hours

#### C3.4: Downstream Channel Reading

**File**: `worker/src/execution/exchange_reader.rs` (UPDATE)

```rust
pub async fn read_from_exchange_channel(
    rx: &mut Receiver<RecordBatch>,
) -> Result<SendableRecordBatchStream> {
    // Convert async channel receiver into RecordBatchStream
    // Downstream ExecutionPlan receives batches as they arrive
    
    Box::pin(async_stream::stream! {
        while let Some(batch) = rx.recv().await {
            yield Ok(batch);
        }
    })
}
```

**Effort**: 2-3 hours

#### C3.5: Error Handling in Exchange

Handle upstream errors propagating through exchange:

```rust
pub async fn handle_exchange_error(
    error: DataFusionError,
    downstream_tasks: &[TaskId],
) -> Result<()> {
    // Notify downstream tasks that exchange will not provide data
    // Downstream tasks should fail gracefully
    for task_id in downstream_tasks {
        notify_task_dependency_failed(task_id, error.clone())?;
    }
    Ok(())
}
```

**Effort**: 2 hours

#### C3.6: Testing & Validation

**File**: `worker/src/storage/tests/exchange_tests.rs` (UPDATE)

```rust
#[tokio::test]
async fn test_repartition_via_channel() {
    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    
    // Upstream sends batches
    for i in 0..5 {
        let batch = create_test_batch(i);
        tx.send(batch).await.unwrap();
    }
    drop(tx);
    
    // Downstream reads
    let stream = read_from_exchange_channel(&mut rx).await.unwrap();
    let mut count = 0;
    while let Some(_) = stream.next().await {
        count += 1;
    }
    assert_eq!(count, 5);
}

#[tokio::test]
async fn test_spill_to_disk_on_memory_limit() {
    // TODO: Test that spill triggers and works
}
```

**Effort**: 4-6 hours

### Deliverables
- MPMC channels for inter-stage data flow
- RepartitionExec integrated into stage DAG
- S3 fallback for memory-limited scenarios
- Channel reading integrated with downstream tasks
- End-to-end pipelined query execution (no full materialization)

### Risks
- Channel capacity not sufficient (batches drop)
- **Mitigation**: Configurable buffer size; monitor backpressure

---

## Workstream 4: Correctness Testing & Performance Validation (Weeks 5-6)

### Objective
Ensure all existing queries work correctly via DataFusion. Meet latency targets. Debug issues.

### Testing Strategy

#### D4.1: Regression Test Suite

**File**: `tests/datafusion_regression_tests.rs` (NEW)

```rust
#[tokio::test]
async fn test_all_existing_queries() {
    // For each query in existing test suite:
    //   1. Execute via old Kionas operators
    //   2. Execute via new DataFusion operators
    //   3. Compare results (byte-identical)
    //   4. Verify no errors
}
```

**Query types to test**:
- Simple selects (1 table)
- Filters and projects
- Joins (inner, left, right)
- Aggregations (group by, sum, count)
- Aggregations with multiple stages
- Broadcasts

**Effort**: 8-10 hours

#### D4.2: Performance Benchmarking

**File**: `benches/datafusion_performance.rs` (NEW)

```rust
// Benchmark suite comparing old vs new execution

#[bench]
fn bench_simple_select_old(b: &mut Bencher) { /* ... */ }

#[bench]
fn bench_simple_select_datafusion(b: &mut Bencher) { /* ... */ }

#[bench]
fn bench_3stage_agg_old(b: &mut Bencher) { /* ... */ }

#[bench]
fn bench_3stage_agg_datafusion(b: &mut Bencher) { /* ... */ }
```

**Metrics**:
- Query latency (ms)
- Memory peak (MB)
- Operator fusion effectiveness
- Vectorization gains

**Effort**: 6-8 hours

#### D4.3: Memory Profiling

Profile spill-to-disk behavior:

```rust
#[tokio::test]
async fn test_large_aggregation_memory_efficiency() {
    // Generate large intermediate result
    // Monitor memory usage
    // Verify spill to disk triggered
    // Check query still succeeds
}
```

**Effort**: 4 hours

#### D4.4: Error Scenario Testing

Test error handling:

```rust
#[tokio::test]
async fn test_mid_stream_error_handling() {
    // Inject error in middle of batch stream
    // Verify error propagates correctly
    // Verify flight-proxy receives error metadata
}

#[tokio::test]
async fn test_upstream_task_failure_during_exchange() {
    // Upstream task fails after sending some batches
    // Downstream should detect and fail gracefully
}
```

**Effort**: 4-6 hours

#### D4.5: Flight-Proxy Integration Testing

```rust
#[tokio::test]
async fn test_flight_response_streaming() {
    // Execute query via DataFusion
    // Verify flight response contains RecordBatches
    // Parse flight response; verify data correctness
}
```

**Effort**: 3-4 hours

#### D4.6: Load & Stress Testing

```rust
#[tokio::test]
async fn test_concurrent_queries() {
    // Run 10 queries concurrently
    // Verify no interference
    // Check resource contention
}
```

**Effort**: 4 hours

### Deliverables
- Regression test suite (all existing queries pass)
- Performance benchmarks (latency, memory, vectorization metrics)
- Error scenario tests
- Flight integration tests
- Optimization recommendations report (from profiling)

### Risks
- Performance regression (DataFusion slower than expected)
- **Mitigation**: Profile hot paths; optimize serialization

---

## Task Summary Table

| WS | Task ID | Title | Effort | Owner |
|----|---------|----- |--------|-------|
| 1 | A1.1 | Add DataFusion dependency | 0.5d | DevOps |
| 1 | A1.2 | Create planner wrapper | 1d | Architect |
| 1 | A1.3 | Update Task model | 0.5d | Backend lead |
| 1 | A1.4 | Integrate planner into query handler | 1d | Backend engineer |
| 1 | A1.5 | Server-side tests | 1d | QA |
| 2 | B2.1 | DataFusion executor | 0.5d | Architect |
| 2 | B2.2 | Flight streaming adapter | 1d | Flight specialist |
| 2 | B2.3 | Replace manual pipeline | 1.5d | Backend engineer |
| 2 | B2.4 | ExecutionContext setup | 0.5d | Backend engineer |
| 2 | B2.5 | Stream error handling | 0.5d | Backend engineer |
| 2 | B2.6 | Worker-side tests | 1d | QA |
| 3 | C3.1 | MPMC channel setup | 0.5d | Architect |
| 3 | C3.2 | RepartitionExec config | 0.5d | Architect |
| 3 | C3.3 | S3 spill fallback | 1d | Storage engineer |
| 3 | C3.4 | Downstream channel reading | 0.5d | Backend engineer |
| 3 | C3.5 | Exchange error handling | 0.5d | Backend engineer |
| 3 | C3.6 | Exchange tests | 1d | QA |
| 4 | D4.1 | Regression test suite | 2d | QA |
| 4 | D4.2 | Performance benchmarking | 1.5d | Perf engineer |
| 4 | D4.3 | Memory profiling | 1d | Perf engineer |
| 4 | D4.4 | Error scenario tests | 1d | QA |
| 4 | D4.5 | Flight integration testing | 1d | Flight specialist |
| 4 | D4.6 | Load stress testing | 1d | QA |

**Total Effort**: ~26 person-days (~6 weeks, 4-5 engineers)

---

## Implementation Timeline

```
Week 1:
  Mon-Tue: A1.1-A1.2 (DataFusion setup)
  Wed-Fri: A1.3-A1.4 (Task model, query handler)

Week 2:
  Mon-Tue: A1.5 (Server tests)
  Wed-Fri: B2.1-B2.2 (Executor, flight adapter)

Week 3:
  Mon-Fri: B2.3-B2.4 (Replace pipeline, context setup)

Week 4:
  Mon-Wed: B2.5-B2.6 (Error handling, worker tests)
  Thu-Fri: C3.1-C3.2 (Exchange setup)

Week 5:
  Mon-Tue: C3.3-C3.4 (Spill, channel reading)
  Wed-Fri: C3.5-C3.6 (Error handling, exchange tests)

Week 6:
  Mon-Fri: D4.1-D4.6 (Full testing & profiling)
  End: Phase 2 signoff
```

---

## Success Metrics (Phase 2 Completion Matrix)

| Criterion | Status | Evidence |
|-----------|--------|----------|
| All queries execute via DataFusion | Pending | Regression test pass rate = 100% |
| No performance regression | Pending | Latency benchmark: >= baseline |
| Streaming works end-to-end | Pending | Flight response analysis |
| Memory efficiency verified | Pending | Memory profile shows 20-30% reduction |
| Error handling robust | Pending | All error scenarios pass |
| Code simplification achieved | Pending | Delete ~2000 LOC of custom operators |
| Phase 2 signoff | Pending | All 7 criteria marked Done |

---

## Key Decision Points

1. **Serialization Format**: Bincode (chosen) vs Protocol Buffers (more stable but larger)
2. **Planner Location**: Server-side (chosen) vs distributed (complex coordination)
3. **Channel Capacity**: 10 batches (tunable; may adjust based on perf)
4. **Spill Threshold**: 90% memory utilization (conservative; can be tuned)
5. **Error Strategy**: Partial results + error metadata (vs all-or-nothing rollback)

---

## Deployment Strategy

### Phase 2a: Preview Mode
- Run DataFusion in parallel with Kionas
- Compare results (shadow execution)
- Enable for internal testing only
- Duration: ~1 week

### Phase 2b: Canary Rollout
- 10% of queries via DataFusion
- 90% via Kionas (with comparison)
- Monitor for anomalies
- Duration: ~1 week

### Phase 2c: Full Rollout
- 100% of queries via DataFusion
- Remove Kionas operator code (once stable)
- Sunset old code path
- Duration: Ongoing

---

**Status**: Ready for Phase 2 implementation kickoff  
**Next Step**: Create completion matrix [ROADMAP_PARALLEL_PHASE2_MATRIX.md](../../../ROADMAP_PARALLEL_PHASE2_MATRIX.md)
