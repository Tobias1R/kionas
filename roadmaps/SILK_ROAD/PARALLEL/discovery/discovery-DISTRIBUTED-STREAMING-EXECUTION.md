# PARALLEL Silk Road: Distributed Streaming ExecutionPlan Architecture

**Date**: March 23, 2026  
**Status**: Discovery (Architectural Refinement)  
**Focus**: Breaking ExecutionPlans into stages server-side; streaming between workers over network channels

---

## The Problem with Monolithic ExecutionPlan

### Current Proposal (Phase 2 v1)
```
Server:
  SQL Query → DataFusion Planner → ExecutionPlan(full)
    └─ Serialize entire plan (bincode)
    
Worker:
  Receive Vec<u8> → Deserialize → Execute sequentially
    ├─ Scan stage (local)
    ├─ Filter stage (local)
    ├─ Join stage (local with local data)
    └─ Aggregate stage (local)
    
Result: Single worker executes entire query sequentially
        No parallelism across workers for this query
```

### Issues
1. **Single-worker execution**: Query latency constrained by 1 worker's CPU
2. **No cross-stage parallelism**: Worker A (Scan) idle while Worker B (Join) waits
3. **Monolithic plan**: Harder to reason about; hard to split for partitions
4. **Lost Kionas design**: Original design was stage-based distribution

---

## The Better Architecture: Distributed Streaming ExecutionPlan

### What You're Proposing

```
Server (Query Planning):
  SQL Query → DataFusion Planner → ExecutionPlan(full)
    ↓ BREAK INTO STAGES
  Stage 0: [TableScan(files=[...]) → Filter(...)]
  Stage 1: [Join(...)]
  Stage 2: [Aggregate(...)]
  ↓ DISPATCH TO WORKERS
  
Worker A (Partition 0, Stage 0):
  Execute: scan_exec.execute(0) → SendableRecordBatchStream
  ├─ Batch 0: rows 0-8191   → Send via Network Channel
  ├─ Batch 1: rows 8192-...  → Send via Network Channel
  └─ Continue streaming...
  
Worker B (Partition 0, Stage 1):
  Execute: join_exec.execute(0) → SendableRecordBatchStream
  │
  └─ receive_from_network_channel(Stage0→Stage1)
       │
       ├─ Batch 0 arrives → process → emit Batch 0
       ├─ Batch 1 arrives → process → emit Batch 1
       └─ Continue...

Worker C (Partition 0, Stage 2):
  Execute: agg_exec.execute(0) → SendableRecordBatchStream
  │
  └─ receive_from_network_channel(Stage1→Stage2)
       └─ Process & emit batches to client
```

### Timeline Comparison

**Sequential (single worker)**:
```
Time:     0  5  10 15 20 25 30
Stage 0:  |===Scan===|
Stage 1:            |===Join===|
Stage 2:                      |===Agg===|
Total:                                  30ms
```

**Distributed Streaming (3 workers)**:
```
Time:     0  5  10 15 20 25
Worker A: |===Scan===|
              ↓ (batches stream)
Worker B:    |===Join===|
                 ↓ (batches stream)
Worker C:       |===Agg===|
Total:                    25ms (overlapped!)
```

### Key Benefits

1. **True parallelism**: Multiple workers execute simultaneously (different stages)
2. **Overlapped execution**: No waiting for entire upstream stage
3. **Network streaming**: RecordBatches flow between workers without disk materialization
4. **Matches Kionas design**: Stage-based distribution with worker parallelism
5. **Scales horizontally**: More workers → more stages running in parallel

---

## How It Works: Technical Model

### 1. Server-Side: Breaking ExecutionPlan into Stages

**Problem**: DataFusion's ExecutionPlan is a DAG; how do we "break" it into distributable pieces?

**Solution**: Stage extraction at RepartitionExec boundaries (natural stage breaks)

```rust
pub struct ExecutionStage {
    stage_id: usize,
    input_stage_ids: Vec<usize>,          // Dependencies
    execution_plan: Arc<dyn ExecutionPlan>, // Scan → Filter → Join (up to next repartition)
    partition_count: usize,
}

// Example: Full query plan
// Scan(files) → Filter(id > 100) → RepartitionExec → Join(t1, t2) → RepartitionExec → Aggregate

// Becomes 3 stages:
Stage 0: {
    id: 0,
    input_stage_ids: [],
    execution_plan: Scan → Filter,
    partition_count: 4,
}

Stage 1: {
    id: 1,
    input_stage_ids: [0],                    // Depends on Stage 0
    execution_plan: Join(t1, t2),           // Reads Stage 0 output via network
    partition_count: 4,
}

Stage 2: {
    id: 2,
    input_stage_ids: [1],
    execution_plan: Aggregate,
    partition_count: 1,
}
```

### 2. Worker-Side: ExecutionPlan Fragment Execution

**What worker receives**:
```rust
pub struct WorkerTask {
    query_id: String,
    stage_id: usize,
    partition_id: usize,
    execution_plan: Vec<u8>,          // THIS stage only (serialized)
    input_stage_addresses: Vec<String>, // e.g., ["worker-a:7777/stage_0/partition_0"]
}
```

**Worker execution model**:
```rust
pub async fn execute_stage_task(task: WorkerTask) -> Result<()> {
    // 1. Deserialize this stage's ExecutionPlan
    let stage_plan: Arc<dyn ExecutionPlan> = bincode::deserialize(&task.execution_plan)?;
    
    // 2. Setup input streams
    let mut input_streams = Vec::new();
    for input_addr in &task.input_stage_addresses {
        // Connect to upstream worker; receive RecordBatch stream
        let stream = connect_to_upstream_worker(input_addr).await?;
        input_streams.push(stream);
    }
    
    // 3. Create hybrid ExecutionPlan
    //    Input operators (normally read from disk) read from network instead
    let hybrid_plan = inject_network_inputs(stage_plan, input_streams)?;
    
    // 4. Execute plan (produces streaming RecordBatches)
    let mut output_stream = hybrid_plan.execute(task.partition_id).await?;
    
    // 5. Stream results to downstream or client
    while let Some(batch) = output_stream.next().await {
        // Send batch to:
        // - Downstream workers (if this isn't final stage)
        // - Client (via flight-proxy if this IS final stage)
        send_batch(batch).await?;
    }
    
    Ok(())
}
```

### 3. Network Streaming: RecordBatch Transport

**Mechanism**: Arrow Flight RPC or gRPC streaming

```rust
// Worker A (Stage 0): Produce stream
pub async fn stage_output_stream(
    query_id: String,
    stage_id: usize,
    partition_id: usize,
) -> Result<RecordBatchStream> {
    // Execute stage; emit RecordBatches
    while let Some(batch) = executor.next().await {
        yield batch;  // Streamed via Arrow Flight protocol
    }
}

// Worker B (Stage 1): Consume stream
pub async fn receive_upstream_stage(
    upstream_address: String,  // e.g., "worker-a:7777"
    query_id: String,
    upstream_stage_id: usize,
    partition_id: usize,
) -> Result<RecordBatchStream> {
    // Connect to upstream worker
    let client = FlightClient::connect(upstream_address).await?;
    
    // Request stream (can backpressure if consuming slow)
    let stream = client.do_get(FlightTicket {
        query_id,
        stage_id: upstream_stage_id,
        partition_id,
    }).await?;
    
    Ok(stream)
}
```

---

## Architectural Layers

### Layer 1: Server-Side Stage Extraction

**Problem**: How to break an ExecutionPlan into stages?

**Approach**: Identify RepartitionExec operators as natural stage boundaries

```
ExecutionPlan DAG:
  Scan
    ↓
  Filter
    ↓
  RepartitionExec ← STAGE BREAK HERE
    ↓
  Join
    ↓
  RepartitionExec ← STAGE BREAK HERE
    ↓
  Aggregate
```

**Algorithm**:
1. Traverse ExecutionPlan
2. When encountering RepartitionExec, mark as stage boundary
3. Everything between boundaries = one stage
4. Create ExecutionStage for each boundaries

**Implementation**:
```rust
pub fn extract_stages(plan: Arc<dyn ExecutionPlan>) -> Result<Vec<ExecutionStage>> {
    let mut stages = Vec::new();
    let mut current_stage_plan = plan.clone();
    
    // Recursively find RepartitionExec boundaries
    extract_stages_recursive(plan, &mut stages, &mut current_stage_plan)?;
    
    Ok(stages)
}

fn extract_stages_recursive(
    plan: Arc<dyn ExecutionPlan>,
    stages: &mut Vec<ExecutionStage>,
    current_stage: &mut Arc<dyn ExecutionPlan>,
) -> Result<()> {
    // Check if this is a RepartitionExec
    if is_repartition_exec(plan) {
        // Save current stage
        stages.push(ExecutionStage {
            id: stages.len(),
            input_stage_ids: vec![stages.len() - 1],  // Depends on prev stage
            execution_plan: current_stage.clone(),
            partition_count: extract_partition_count(plan)?,
        });
        
        // Start new stage with children
        *current_stage = plan.clone();
    } else {
        // Continue building current stage
        for child in plan.children() {
            extract_stages_recursive(child, stages, current_stage)?;
        }
    }
    
    Ok(())
}
```

### Layer 2: Network Communication (Inter-Stage)

**Protocol**: Arrow Flight RPC (native support for RecordBatch streaming)

```protobuf
// flight.proto
service FlightService {
  // Get stage output stream
  rpc DoGet(FlightTicket) returns (stream FlightData);
  
  // Server publishes stage availability
  rpc ListFlights(Criteria) returns (stream FlightInfo);
}

message FlightTicket {
  string query_id = 1;
  uint32 stage_id = 2;
  uint32 partition_id = 3;
}
```

**Worker A publishes**:
```
GET /query_id=abc/stage_id=0/partition_id=0
  Response: Stream<RecordBatch> (Arrow Flight format)
```

**Worker B consumes**:
```
POST /rpc/DoGet
  Ticket: {query_id: "abc", stage_id: 0, partition_id: 0}
  Response: Stream<FlightData> → convert to RecordBatch stream
```

### Layer 3: Hybrid ExecutionPlan Construction

**Problem**: Original ExecutionPlan expects to read from disk/catalog. How to make it read from network?

**Solution**: Wrapper operator that reads from network channel instead of disk

```rust
pub struct NetworkInputExec {
    upstream_stream: SendableRecordBatchStream,
    schema: SchemaRef,
}

impl ExecutionPlan for NetworkInputExec {
    fn execute(
        &self,
        partition: usize,
    ) -> Result<SendableRecordBatchStream> {
        // Simply forward upstream stream
        Ok(self.upstream_stream.clone())
    }
}

// When building hybrid plan:
// Original: Join(Scan(...), Scan(...))
// Becomes:   Join(NetworkInputExec(upstream), Scan(...))
```

**Process**:
1. Worker receives stage task
2. Identify which DataSourceExec nodes should read from network
3. Replace DataSourceExec with NetworkInputExec
4. NetworkInputExec connects to upstream worker; receives stream
5. Execute modified plan

---

## Execution Coordination

### Challenge 1: Stage Synchronization

**Problem**: Stage 1 starts before Stage 0 finishes. How do we ensure Stage 0 is ready?

**Solution**: Server tracks stage readiness; workers wait for upstream readiness signal

```
Server (Query Coordinator):
  Stage 0 assigned to Worker A
    ↓ (Worker A signals "ready to accept connections")
  Server notifies Worker B: "Stage 0 ready at worker-a:7777"
    ↓
  Worker B launches Stage 1
    └─ Immediately tries to connect to Stage 0
    └─ If Stage 0 not yet producing batches, waits (backpressure)
```

### Challenge 2: Backpressure (Flow Control)

**Problem**: Worker A produces batches faster than Worker B consumes. What happens?

**Solution**: Bounded channels + network flow control

```rust
// Worker A produces
let bounded_channel = tokio::sync::mpsc::channel(100);  // Max 100 batches buffered

while let Some(batch) = stage_output.next().await {
    // If channel full, send() blocks (backpressure)
    bounded_channel.send(batch).await?;  // ← Waits here if consumer slow
}

// Worker B consumes
while let Some(batch) = bounded_channel.recv().await {
    process_batch(batch).await?;
}
```

**Result**: Fast producer naturally throttles to consumer speed

### Challenge 3: Multi-Partition Execution

**Problem**: Stage 0 has 4 partitions; Stage 1 has 4 partitions. How do they connect?

**Solution**: Server sends full connection map to workers

```
Stage 0: Partition 0,1,2,3 (Workers A, A, B, B)
Stage 1: Partition 0,1,2,3 (Workers C, D, C, D)

Stage 0 → Stage 1 repartitioning:
  Stage0_P0 (Worker A) → sends to → Stage1_P0 (Worker C)
  Stage0_P1 (Worker A) → sends to → Stage1_P1 (Worker D)
  Stage0_P2 (Worker B) → sends to → Stage1_P2 (Worker C)
  Stage0_P3 (Worker B) → sends to → Stage1_P3 (Worker D)

(Determined by hash/round-robin repartitioning logic)
```

**Server provides mapping**:
```rust
pub struct StagePartitionMap {
    stage_id: usize,
    partition_id: usize,
    output_destinations: Vec<(String, usize, usize)>, // (worker_addr, stage_id, partition_id)
}
```

---

## Comparison: Three Architectures

### Architecture 1: Monolithic ExecutionPlan (Current Phase 2 v1)

```
Pro:
  + Simple (send one plan)
  + No network streaming complexity
  - Single worker per query
  - No cross-stage parallelism
  - Inefficient resource use
```

### Architecture 2: Distributed Streaming ExecutionPlan (Your Idea)

```
Pro:
  + Multiple workers per query
  + True parallelism across stages
  + Network streaming (no disk materialization)
  + Matches Kionas distributed design
  + Scales horizontally (more workers → more throughput)
  
Con:
  - Complex (stage extraction, network protocol)
  - Network latency between stages
  - Backpressure coordination needed
  - Server must track multiple worker connections
```

### Architecture 3: Hybrid (Compromise)

```
Use monolithic ExecutionPlan for single-node queries (fast path)
Use distributed streaming for multi-stage queries (complex path)

Pro:
  + Best of both
  + Simple queries stay fast
  - More complex to implement
```

---

## Implementation Considerations

### What Changes in Phase 2?

**Server-side**:
- ExecutionPlan extraction into stages (new logic)
- Stage dependency tracking (new)
- Task generation per stage/partition (modification)

**Worker-side**:
- Network input operators (new)
- Arrow Flight RPC server (new)
- Stream consumption from upstream (new)
- Hybrid ExecutionPlan injection (new)

**Exchange layer**:
- Replace S3 with network channels (Arrow Flight)
- Backpressure handling (new)

**NOT changing**:
- DataFusion operators themselves
- Query planning logic (DataFusion planner)
- Worker operator execution (DataFusion execute)

### Effort Estimate (Rough)

| Component | Effort | Complexity |
|-----------|--------|-----------|
| Stage extraction logic | 2-3d | Medium |
| Arrow Flight integration | 3-4d | High (network protocol) |
| Hybrid ExecutionPlan wrapper | 2-3d | Medium |
| Backpressure coordination | 2d | Medium |
| Multi-partition mapping | 2-3d | Medium |
| Testing + validation | 5-7d | High |
| **TOTAL** | **16-23d** | — |

(More complex than monolithic; but worth it for parallelism)

---

## Feasibility Analysis

### Is This Viable?

**Yes**, with caveats:

1. ✅ **Stage extraction**: Possible via RepartitionExec identification
2. ✅ **Network streaming**: Arrow Flight already supports RecordBatch streaming
3. ✅ **Hybrid plans**: DataFusion ExecutionPlan is trait-based; can extend
4. ✅ **Backpressure**: Tokio channels handle automatically
5. ⚠️ **Complexity**: Significantly more complex than monolithic approach
6. ⚠️ **Testing**: Need comprehensive integration testing

### Risks

1. **Network latency**: RecordBatches cross network → slower than local execution
   - Mitigation: Batch size tuning (larger batches = less overhead)
   
2. **Worker failure**: If Worker B (Stage 1) dies, need recovery
   - Mitigation: Checkpoint mechanism + replay
   
3. **Stage starvation**: Slow stage blocks entire query
   - Mitigation: Metrics + alerts; speculative re-execution

---

## Recommendation

### Go for Distributed Streaming ExecutionPlan

**Why**:
1. Matches your original Kionas design philosophy
2. Unlocks true horizontal scaling (more workers = more parallelism)
3. More elegant than sending monolithic plans
4. Network channels are a well-understood pattern

**Phasing**:
- **Phase 2a**: Implement monolithic (simple path first; proves concepts)
- **Phase 2b**: Add stage extraction + network streaming (distributed mode)
- **Phase 2c**: Optimize + hardening

**Decision Point**: After Phase 2a completes, decide whether to continue with distributed streaming or stay with monolithic.

---

## Next Steps for Discovery

1. **Validate stage extraction algorithm**: Does RepartitionExec boundary detection work?
2. **Prototype Arrow Flight integration**: Can we stream RecordBatches over network?
3. **Design multi-partition mapping**: How exactly do Stage 0 partitions connect to Stage 1?
4. **Plan network protocol**: Arrow Flight vs. gRPC vs. custom?
5. **Error handling**: What happens if upstream worker dies mid-stream?

---

**Status**: Discovery ready for deep-dive discussion  
**Next**: Create implementation blueprint if approach is approved
