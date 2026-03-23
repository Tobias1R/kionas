# PARALLEL Silk Road: Deep-Dive Discovery
## Stage Extraction, Arrow Flight Integration, & Multi-Partition Mapping

**Date**: March 23, 2026  
**Status**: Technical Deep-Dive (Analysis)  
**Focus**: Three critical architectural components for distributed streaming ExecutionPlan

---

## Part 1: Stage Extraction via RepartitionExec

### What is RepartitionExec?

From DataFusion documentation:
> "Maps N input partitions to M output partitions based on a Partitioning scheme."

**Key attributes**:
- `input()`: ExecutionPlan feeding into this repartition
- `partitioning()`: Strategy (Hash, RoundRobin, Range, SinglePartition, etc.)
- `preserve_order()`: Whether to maintain row ordering (expensive)

**Visual from docs**:
```
Input 0 ──┐
          ├─→ RepartitionExec ──┬──→ Output 0
Input 1 ──┘                     ├──→ Output 1
                                └──→ Output 2
```

### Why RepartitionExec = Natural Stage Boundary?

**Reason**: RepartitionExec is explicitly designed as an **exchange operator**—it redistributes data across partitions. This is exactly where we want to introduce network boundaries in distributed execution.

**Current execution** (local):
- All partitions run on same machine
- RepartitionExec uses SpillPool channels for FIFO ordering w/ spill-to-disk

**Distributed execution** (our model):
- Input partitions on Workers A, B, C
- RepartitionExec becomes network boundary (Arrow Flight)
- Output partitions on Workers D, E, F (could overlap)

### Stage Extraction Algorithm

**Goal**: Break ExecutionPlan into stages at RepartitionExec boundaries.

```rust
pub struct ExecutionStage {
    stage_id: usize,
    /// Dependency DAG: which stages this depends on
    input_stage_ids: Vec<usize>,
    
    /// ExecutionPlan fragment (from leaf to RepartitionExec or root)
    /// Does NOT include RepartitionExec itself
    execution_plan: Arc<dyn ExecutionPlan>,
    
    /// How many output partitions this stage produces
    partitions_out: usize,
    
    /// Partitioning scheme (needed by downstream stage)
    output_partitioning: Partitioning,
}

pub fn extract_stages(root: Arc<dyn ExecutionPlan>) -> Result<Vec<ExecutionStage>> {
    let mut stages = Vec::new();
    extract_stages_recursive(root, &mut stages, None)?;
    Ok(stages)
}

fn extract_stages_recursive(
    plan: Arc<dyn ExecutionPlan>,
    stages: &mut Vec<ExecutionStage>,
    parent_stage_id: Option<usize>,
) -> Result<Option<usize>> {
    // Case 1: This IS a RepartitionExec
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        // Recursively process the input to this repartition
        let input_stage_id = extract_stages_recursive(
            repartition.input()[0].clone(),
            stages,
            None,
        )?;
        
        // INPUT becomes one stage; now create OUTPUT stage
        let current_stage_id = stages.len();
        stages.push(ExecutionStage {
            stage_id: current_stage_id,
            input_stage_ids: input_stage_id.into_iter().collect(),
            execution_plan: repartition.input()[0].clone(), // Everything before repartition
            partitions_out: repartition.partitioning().partition_count(),
            output_partitioning: repartition.partitioning().clone(),
        });
        
        // Return current stage to parent
        return Ok(Some(current_stage_id));
    }
    
    // Case 2: NOT a RepartitionExec; recurse to children
    let mut child_stage_ids = Vec::new();
    for child in plan.children() {
        if let Some(child_stage_id) = extract_stages_recursive(
            child.clone(),
            stages,
            parent_stage_id,
        )? {
            child_stage_ids.push(child_stage_id);
        }
    }
    
    // Case 3: This is a leaf or final node (e.g., root without repartition)
    if stages.is_empty() && plan.children().is_empty() {
        // Entire plan is single stage (no repartitioning)
        let stage_id = stages.len();
        stages.push(ExecutionStage {
            stage_id,
            input_stage_ids: vec![],
            execution_plan: plan.clone(),
            partitions_out: 1,
            output_partitioning: Partitioning::SinglePartition,
        });
        Ok(Some(stage_id))
    } else {
        Ok(parent_stage_id)
    }
}
```

### Example: 3-Stage Query

**Original ExecutionPlan**:
```
┌──────────────────────────┐
│   Final Aggregate        │
│   (Phase 2 results)      │
└────────────┬─────────────┘
             │
    ┌────────────────────┐
    │ RepartitionExec    │  ← STAGE BREAK #2
    │ (hash-based)       │
    └────────────┬───────┘
                 │
         ┌───────────────┐
         │ HashJoin      │
         │ (inner join)  │
         └───────────────┘
                 │
    ┌────────────────────┐
    │ RepartitionExec    │  ← STAGE BREAK #1
    │ (round-robin)      │
    └────────────┬───────┘
                 │
         ┌───────────────┐
         │ Scan T1       │  (partitioned: P0, P1)
         │ Scan T2       │
         └───────────────┘
```

**Extracted Stages**:
```
Stage 0:  Scan T1 + Scan T2
  ├─ input_stage_ids: []
  ├─ partitions_out: 4 (from RepartitionExec.partitioning())
  ├─ output_partitioning: RoundRobin
  
Stage 1:  HashJoin (consumes Stage 0 output)
  ├─ input_stage_ids: [0]
  ├─ partitions_out: 4 (from next RepartitionExec)
  ├─ output_partitioning: Hash(join_key)
  
Stage 2:  Aggregate (consumes Stage 1 output)
  ├─ input_stage_ids: [1]
  ├─ partitions_out: 1 (final)
  ├─ output_partitioning: SinglePartition
```

### Key Properties

| Property | Value | Meaning |
|----------|-------|---------|
| `stage_id` | 0, 1, 2 | Unique identifier; execution order |
| `input_stage_ids` | [0], [1], [] | Dependency graph |
| `partitions_out` | 4, 4, 1 | Output parallelism per stage |
| `output_partitioning` | RoundRobin, Hash, Single | How rows are distributed to next stage's partitions |

---

## Part 2: Arrow Flight gRPC Integration

### Why Arrow Flight?

**Given**: You're using Arrow Flight gRPC.

**Perfect fit**:
- Native support for streaming `RecordBatch` sequences
- Built on gRPC (multiplexing, flow control, SSL/TLS)
- Arrow serialization already vectorized (IPC format)
- Backpressure via gRPC channel buffering

### Arrow Flight Architecture

**Service definition (conceptual)**:
```protobuf
service FlightService {
  // Standard Flight RPC
  rpc GetFlightInfo(FlightDescriptor) returns (FlightInfo);
  rpc GetSchema(FlightDescriptor) returns (SchemaResult);
  
  // Streaming (KEY for us)
  rpc DoGet(FlightTicket) returns (stream FlightData);
  rpc DoPut(stream FlightData) returns (PutResult);
}
```

### Two Integration Patterns

#### Pattern A: Flight Server on Each Worker (Publish Model)

**Worker A (Stage 0)** publishes:
```rust
#[derive(Message)]
pub struct StageOutputTicket {
    pub query_id: String,
    pub stage_id: u32,
    pub partition_id: u32,
}

// Worker A implements FlightService trait
impl FlightService for KionaFlightServer {
    async fn do_get(
        &self,
        ticket: FlightTicket,
    ) -> Result<RecordBatchStream> {
        let stage_ticket: StageOutputTicket = deserialize(ticket.ticket)?;
        
        // Execute stage partition; stream results
        let stream = self.executor.execute_stage(
            stage_ticket.query_id,
            stage_ticket.stage_id,
            stage_ticket.partition_id,
        ).await?;
        
        Ok(stream)  // Arrow Flight handles streaming
    }
}
```

**Worker B (Stage 1)** consumes:
```rust
pub async fn receive_stage_output(
    upstream_worker: String,  // "worker-a:7778"
    query_id: String,
    stage_id: u32,
    partition_id: u32,
) -> Result<SendableRecordBatchStream> {
    // Connect to upstream worker
    let mut client = FlightClient::connect(upstream_worker).await?;
    
    // Request stage output as stream
    let ticket = FlightTicket {
        ticket: serialize(StageOutputTicket {
            query_id,
            stage_id,
            partition_id,
        }),
    };
    
    let stream = client.do_get(ticket).await?;
    Ok(stream)
}
```

#### Pattern B: Centralized Flight Proxy (Your Current Model)

**Already established**: flight-proxy as intermediary

```
Worker A (Stage 0)
  ├─ Execute & stream results to flight-proxy
  
Flight-Proxy
  ├─ Aggregate Stage 0 output (4 partitions → 1 stream)
  ├─ Repartition based on output_partitioning (e.g., hash)
  ├─ Stream to Stage 1 partition destinations
  
Worker B (Stage 1)
  ├─ Receive repartitioned stream from flight-proxy
```

**For distributed streaming**: Recommend **Pattern A** (direct worker-to-worker) for this phase, because it mirrors DataFusion's local execution model naturally. Flight-proxy can handle final result delivery to client.

### Arrow Flight Data Format

**RecordBatch → FlightData conversion** (Arrow IPC):

```rust
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::FlightData;

pub fn batch_to_flight_data(batch: &RecordBatch) -> Result<FlightData> {
    // Method 1: Stream format (recommended for chunked streaming)
    let mut buffer = Vec::new();
    let mut writer = StreamWriter::new(&mut buffer);
    writer.write_batch(batch)?;
    writer.finish()?;
    
    Ok(FlightData {
        flight_descriptor: None,
        data_header: buffer.clone(),  // Arrow IPC format
        app_metadata: vec![],
        data_body: vec![],  // Body in header for simplicity
    })
}

pub fn flight_data_to_batch(data: &FlightData, schema: &Schema) -> Result<RecordBatch> {
    // Reverse: deserialize Arrow IPC format
    let reader = arrow::ipc::reader::StreamReader::try_new(
        Cursor::new(data.data_header.clone()),
    )?;
    
    reader.collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .next()
        .ok_or_else(|| "empty batch".into())
}
```

### Backpressure via Flight

**How it works**:
1. Worker A produces batches fast
2. Sends via `do_get()` stream (gRPC stream)
3. Worker B consumes via `client.do_get()`
4. If Worker B slow → gRPC channel fills up → `send()` blocks
5. Worker A naturally throttles

**Channel buffer configuration**:
```rust
// gRPC options
let mut options = ClientOptions::default();
options.tcp_keepalive = Some(std::time::Duration::from_secs(10));

// Flight client with backpressure
let client = FlightClient::new_with_options(endpoint, options)?;
```

---

## Part 3: Multi-Partition Mapping

### Problem

**Setup**:
- Stage 0: 4 partitions (workers: A, A, B, B)
- Stage 1: 4 partitions (workers: C, D, C, D)
- Stage 0 → Stage 1 partitioning: Hash(column_x)

**Question**: How do Stage 0 partitions map to Stage 1 partitions?

**Answer**: Depends on **Partitioning scheme**:

| Scheme | Mapping | Example |
|--------|---------|---------|
| **RoundRobin** | P0→P0, P1→P1, P2→P2, P3→P3 | Uniform distribution (stateless) |
| **Hash(key)** | P0→P{hash(key) % 4}, P1→P{hash(key) % 4}, ... | Grouped by key (data movement) |
| **Range(key)** | P0→P0 (rows 0-1M), P1→P0 (rows 1M-2M), ... | Sorted by key |
| **SinglePartition** | All → P0 | All rows to single partition |

### Execution Flow with Mapping

**Stage 0 Execution** (4 workers in parallel):

```
Worker A, Partition 0:
  FOR each RecordBatch in output:
    FOR each row:
      partition_idx = hash(row.column_x) % 4
      send_to(Stage1_Partition[partition_idx])
  
Worker A, Partition 1:
  FOR each RecordBatch in output:
    FOR each row:
      partition_idx = hash(row.column_x) % 4
      send_to(Stage1_Partition[partition_idx])
  
Worker B, Partition 2:
  ... same logic ...
  
Worker B, Partition 3:
  ... same logic ...
```

**Result**: All rows with same hash(column_x) end up at same Stage 1 partition (even though multiple Stage 0 partitions contributed rows).

### Marble Diagram

```
Stage 0 Outputs:
  Partition 0 (Worker A): [rows 1-1000]    → Hash applied row-wise
  Partition 1 (Worker A): [rows 1001-2000] → Hash applied row-wise
  Partition 2 (Worker B): [rows 2001-3000] → Hash applied row-wise
  Partition 3 (Worker B): [rows 3001-4000] → Hash applied row-wise

Stage 1 Inputs (after hash redistribution):
  Partition 0 (Worker C): [rows with hash%4==0] (from all Stage 0 partitions)
  Partition 1 (Worker D): [rows with hash%4==1] (from all Stage 0 partitions)
  Partition 2 (Worker C): [rows with hash%4==2] (from all Stage 0 partitions)
  Partition 3 (Worker D): [rows with hash%4==3] (from all Stage 0 partitions)
```

### Server-Side Mapping Calculation

**Sent with task to workers**:

```rust
pub struct StagePartitionExecution {
    query_id: String,
    stage_id: u32,
    partition_id: u32,
    
    /// ExecutionPlan for this stage
    execution_plan: Vec<u8>,  // bincode-serialized
    
    /// Where to send output (if not final stage)
    output_destinations: Vec<OutputDestination>,
}

pub struct OutputDestination {
    /// Stage ID this partition's output goes to
    downstream_stage_id: u32,
    
    /// Worker addresses accepting this partition's output
    worker_addresses: Vec<String>,  // e.g., ["worker-c:7778", "worker-d:7778"]
    
    /// Partitioning logic for how rows route
    partitioning: Partitioning,  // Hash, RoundRobin, etc.
}

// Example for Stage 1 Partition 0 (on Worker C):
pub struct StagePartitionExecution {
    query_id: "q123",
    stage_id: 1,
    partition_id: 0,
    execution_plan: [binary],
    output_destinations: [
        OutputDestination {
            downstream_stage_id: 2,
            worker_addresses: ["worker-f:7778"],
            partitioning: SinglePartition,  // Agg stage has 1 partition
        },
    ],
}
```

### Network Connections During Execution

**Timeline**:

```
T0: Server calculates mapping
    ├─ Stage 0 (4 partitions) → dispatches 4 tasks
    ├─ Stage 1 (4 partitions) → dispatches 4 tasks
    └─ Stage 2 (1 partition) → dispatches 1 task

T1: Workers become ready
    ├─ Workers A, B (Stage 0): ready to execute
    ├─ Workers C, D (Stage 1): wait for upstream connections
    └─ Worker F (Stage 2): wait for upstream connections

T2: Execution starts
    ├─ Worker A, P0: execute → open 4 connections (to Stage 1 P0, P1, P2, P3 destinations)
    ├─ Worker A, P1: execute → open 4 connections
    ├─ ...
    └─ Total: 4 Stage 0 partitions × 4 Stage 1 partitions = 16 network flows
       (Many-to-many; but partitioning scheme determines which actually send data)

T3: Streaming
    ├─ Worker A, P0 sends batch 0 → hash each row → route to Stage1_P{hash%4}
    ├─ Worker A, P1 sends batch 0 → hash each row → route to Stage1_P{hash%4}
    ├─ Stage 1 workers receive and process as batches arrive
    └─ Pipelined (no waiting for Stage 0 complete)

T4: Completion
    ├─ Stage 0 workers finish
    ├─ Stage 1 workers finish & start Stage 2
    └─ Final result to client
```

### Efficient Routing with MPMC Channels

**Per-partition routing** (local; no network yet):

```rust
// On downstream worker (e.g., Worker C)
pub async fn execute_stage_partition(
    task: StagePartitionExecution,
) -> Result<()> {
    let execution_plan = deserialize(task.execution_plan)?;
    let mut output_stream = execution_plan.execute(task.partition_id).await?;
    
    // Setup MPMC channels for output routing
    let mut output_channels = HashMap::new();
    for dest in &task.output_destinations {
        let (tx, rx) = tokio::sync::mpsc::channel(100);  // Bounded
        
        // Spawn task to send to downstream via Flight
        tokio::spawn(send_to_flight_endpoint(
            dest.worker_addresses.clone(),
            rx,
        ));
        
        output_channels.insert(dest.downstream_stage_id, tx);
    }
    
    // Process batches & route
    while let Some(batch) = output_stream.next().await {
        // Determine destination(s) based on partitioning scheme
        let dest_partitions = route_batch(&batch, &task.output_destinations[0].partitioning);
        
        for dest_partition in dest_partitions {
            let channel = &output_channels[&task.output_destinations[0].downstream_stage_id];
            channel.send((dest_partition, batch.clone())).await?;
        }
    }
    
    Ok(())
}

async fn send_to_flight_endpoint(
    worker_addresses: Vec<String>,
    mut rx: tokio::sync::mpsc::Receiver<(usize, RecordBatch)>,
) -> Result<()> {
    let mut client = FlightClient::connect(&worker_addresses[0]).await?;
    
    while let Some((dest_partition, batch)) = rx.recv().await {
        let flight_data = batch_to_flight_data(&batch)?;
        client.do_put(flight_data).await?;
    }
    
    Ok(())
}
```

---

## Part 4: Integration Summary

### End-to-End Execution Model

```
┌─────────────────────────────────────────────────────────────┐
│ SERVER (Query Coordinator)                                  │
│                                                              │
│  SQL → DataFusion Planner → ExecutionPlan (full DAG)        │
│                           → extract_stages()                │
│  ↓                                                           │
│  [Stage 0, Stage 1, Stage 2] ← Extracted stages            │
│  ↓                                                           │
│  Generate tasks per stage/partition                         │
│  ├─ Stage 0: 4 tasks (broadcast to Workers A, A, B, B)     │
│  ├─ Stage 1: 4 tasks (broadcast to Workers C, D, C, D)     │
│  └─ Stage 2: 1 task (broadcast to Worker F)                │
│  ↓                                                           │
│  Each task includes:                                        │
│  ├─ execution_plan (bincode)                               │
│  ├─ output_destinations (where to send results)            │
│  └─ upstream_connections (how to fetch input)              │
└─────────────────────────────────────────────────────────────┘
                         ↓
     ┌──────────────────────────────────────┐
     │ WORKER EXECUTION (Distributed)       │
     │                                      │
     │ Stage 0 (Workers A, B):              │
     │  ├─ Execute plan partition           │
     │  ├─ Stream batches                   │
     │  └─ Route via hash(key) to Stage 1   │
     │                                      │
     │ Stage 1 (Workers C, D):              │
     │  ├─ Receive batches from Stage 0     │
     │  ├─ Execute join locally             │
     │  └─ Route to Stage 2 (single worker) │
     │                                      │
     │ Stage 2 (Worker F):                  │
     │  ├─ Receive batches from Stage 1     │
     │  ├─ Execute aggregate                │
     │  └─ Stream to Flight-Proxy           │
     └──────────────────────────────────────┘
                 ↓
     ┌──────────────────────────────────────┐
     │ FLIGHT-PROXY (Result Delivery)       │
     │                                      │
     │ Receive RecordBatches (Arrow IPC)    │
     ├─ Deserialize foreach batch           │
     ├─ Convert to Arrow IPC format         │
     └─ Stream to client (low latency)      │
     └──────────────────────────────────────┘
                 ↓
            CLIENT
         (receives stream)
```

### Key Points Recap

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Stage Extraction** | RepartitionExec traversal | Break DAG into distributable pieces |
| **Network Transport** | Arrow Flight gRPC | Stream RecordBatches between workers |
| **Partition Mapping** | Partitioning schemes (Hash/RoundRobin/etc) | Route rows to correct downstream partition |
| **Backpressure** | MPMC channels + gRPC buffering | Natural throttling (producer waits if consumer slow) |
| **Fault Handling** | Error propagation in streams | Stop all downstreams if upstream fails |

---

## Technical Feasibility Assessment

### Stage Extraction

**Feasibility**: ✅ **HIGH**
- RepartitionExec is explicit  
- Traversal algorithm is straightforward
- DataFusion's ExecutionPlan trait supports `children()` & `as_any()` downcasting

**Risk**: Medium (edge case: query with no RepartitionExec = single stage; must handle)

### Arrow Flight Integration

**Feasibility**: ✅ **HIGHEST**
- You already use Arrow Flight
- RecordBatch → FlightData conversion well-documented
- gRPC backpressure automatic

**Risk**: Low (focus: error handling mid-stream)

### Multi-Partition Mapping

**Feasibility**: ✅ **HIGH**
- Partitioning enum defined in DataFusion
- MPMC channels are proven (Tokio)
- Hash partitioning standard in all query engines

**Risk**: Medium (complexity: ensuring consistent routing across stages + handling stragglers)

---

## Recommended Implementation Path

### Phase 1: Prototype (2-3 weeks)

1. Implement stage extraction on server-side
2. Test with simple 2-stage plans (Scan → Aggregate)
3. Verify RepartitionExec boundaries are correct

### Phase 2: Arrow Flight Streaming (2-3 weeks)

1. Add direct worker-to-worker Flight connections
2. Test batch serialization/deserialization
3. Validate backpressure behavior

### Phase 3: Multi-Partition Routing (2-3 weeks)

1. Implement partition routing logic
2. Test Hash partitioning (most common)
3. Validate data integrity (all rows arrive, no duplicates)

### Phase 4: End-to-End (1 week)

1. Integrate all three components
2. Performance testing
3. Deploy to production

---

## Next Steps

**For review**:
1. Does stage extraction via RepartitionExec match your mental model?
2. Any concerns with direct worker-to-worker Flight connections (vs. flight-proxy)?
3. Is Hash partitioning the primary use case, or support all schemes?

**For planning**:
1. Update Phase 2 implementation plan with this architecture
2. Create detailed design specs per component
3. Start prototype work

