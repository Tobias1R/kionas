# PARALLEL Silk Road: Phase 1 Implementation Plan
## Distributed Execution Foundation - Control Plane

**Date**: March 23, 2026 (Aggressive Redesign)  
**Status**: Planning (Breaking Changes Approved)  
**Focus**: Build distributed-first control plane; stage extraction, DAG scheduling, task model redesign

## Phase Goal

Establish the **distributed execution control plane** by implementing:
1. **Stage extraction** at RepartitionExec boundaries (from deep-dive Part 1)
2. **DAG-aware async scheduler** for independent stage dispatch
3. **Task model redesign** with output destinations (breaking change)
4. **In-memory validation** proving stage execution correctness before network layer

This phase focuses on **control plane only** (no data movement yet). Phase 2 will add Arrow Flight streaming (data plane).

**Success Criteria** (Control Plane Focused):
- ✅ Stage extraction algorithm proven on 2-3-4 stage queries
- ✅ Task model redesigned: `StagePartitionExecution { stage_id, execution_plan, output_destinations }`
- ✅ Async scheduler correctly handles stage DAG with proper dependency tracking
- ✅ Output destination mapping calculates partition routes correctly
- ✅ Full queries execute end-to-end using in-memory stage execution (no Flight)
- ✅ Results byte-identical to current execution (correctness validation)
- ✅ Ready for Phase 2: data plane implementation (Flight streaming + multi-partition routing)

## Inputs

1. **Deep-Dive Discovery**: [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](../discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)
   - Part 1: Stage Extraction via RepartitionExec (algorithm, example stages)
   - Part 2: Arrow Flight gRPC (Phase 2 focus; skipped in Phase 1)
   - Part 3: Multi-Partition Mapping (Phase 2 focus; skipped in Phase 1)
2. **Current Execution Pipeline**: [roadmaps/discover/execution_pipeline_discovery.md](../../discover/execution_pipeline_discovery.md)
3. **Roadmap**: [PARALLEL_ROADMAP.md](../PARALLEL_ROADMAP.md)
4. **Matrix Target**: [roadmaps/ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../../ROADMAP_PARALLEL_PHASE1_MATRIX.md)

## Scope Boundaries

### In Scope (Control Plane Focus)
- **Stage extraction**: Break ExecutionPlan at RepartitionExec boundaries (from deep-dive Part 1)
- **Task model redesign**: `StagePartitionExecution { stage_id, partition_id, execution_plan, output_destinations }`
- **Server-side DAG scheduler**: Async-aware topological sort + concurrent independent stage dispatch
- **Output destination mapping**: Calculate which downstream partitions receive output from each upstream partition
- **In-memory validation**: Execute full queries using extracted stages (no network; debug locally)
- **Error handling**: Stage-level failures with proper dependency cascade
- **Observability**: Metrics for stage extraction quality, DAG concurrency, validation correctness

### Out of Scope (Deferred to Phase 2)
- **Arrow Flight implementation** (data plane; Phase 2 focus)
- **Multi-partition routing logic** (Phase 2b focus)
- **Backpressure and flow control** (Phase 2 focus)
- **Network-based data movement** (all Phase 2; Phase 1 uses in-memory only)
- **Worker execution changes** (Phase 1 focuses on server-side; workers unchanged)
- **Performance optimization** (latency gains come in Phase 2 with actual distributed execution)

## Guardrails

1. **Correctness First**: All query results must be byte-identical to current execution (in-memory validation)
2. **Stage Extraction Precision**: RepartitionExec boundaries must be identified correctly; no stages over-extracted or under-extracted
3. **DAG Integrity**: Topological sort must preserve dependencies; no cycles; no forward-only DAG violations
4. **Breaking Changes Accepted**: Task model redesign is intentional; old task format incompatible with new
5. **No Network Layer Yet**: Phase 1 uses in-memory stage execution; all data stays on server (debug mode)
6. **Deterministic Routing**: Output destination mapping must be deterministic and replicable
7. **Comprehensive Tracing**: Every stage extraction, DAG traversal, and output mapping decision must be observable
8. **Forward Compatibility**: Task model must support future Flight data (fields prepared but unused in Phase 1)

## Workstreams

### Workstream 0: Task Model Redesign (Breaking Change)

**Objectives**:
- Define new `StagePartitionExecution` task format with stage identifiers and output routing
- Update server task dispatch to use new model
- Ensure proto compatibility for future phases

**Technical Approach**:
Replace monolithic task:
```rust
// OLD (Current)
pub struct Task {
    execution_plan: Vec<u8>,  // entire plan
    // other fields...
}

// NEW (Phase 1)
pub struct StagePartitionExecution {
    query_id: String,
    stage_id: u32,
    partition_id: u32,
    execution_plan: Vec<u8>,  // ONE STAGE, ONE PARTITION (smaller)
    output_destinations: Vec<OutputDestination>,  // where output goes. Keep in my the possiblity to send a flag to instruct the worker to do this by himself, since he can be the only worker available. Parallelism happen but using Multi-threading instead of multi-node.
}

pub struct OutputDestination {
    downstream_stage_id: u32,
    worker_addresses: Vec<String>,
    partitioning: Partitioning,  // Hash, RoundRobin, etc. (from DataFusion)
}
```

**Key Decision**:
- Breaking change intentional; no backward compatibility
- New task structure serialized with bincode; same as current
- Workers will deserialize new format (field-by-field)

**Tasks**:
0.1: Define proto/RPC for StagePartitionExecution
0.2: Define OutputDestination struct and Partitioning enum mapping
0.3: Update server task dispatch code to use new task format
0.4: Add unit tests for task serialization/deserialization
0.5: Update documentation; note breaking change

**Deliverables**:
- New task proto/struct definitions
- Server task dispatch refactored
- Tests proving serialization works
- Migration guide (all clusters upgrade together)

---

### Workstream 1: Stage Extraction Algorithm

**Objectives**:
- Implement RepartitionExec traversal to break ExecutionPlan into stages (from deep-dive Part 1)
- Build `ExecutionStage` struct with dependencies
- Handle edge cases (single-stage queries, multiple repartitions)

**Technical Approach** (from deep-dive):
```rust
pub struct ExecutionStage {
    stage_id: usize,
    input_stage_ids: Vec<usize>,        // dependency DAG
    execution_plan: Arc<dyn ExecutionPlan>,
    partitions_out: usize,              // output parallelism
    output_partitioning: Partitioning,  // routing scheme
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
    // Case 1: Is this a RepartitionExec? → stage boundary
    if let Some(repartition) = plan.as_any().downcast_ref::<RepartitionExec>() {
        // Recursively extract input stages
        let input_stage_id = extract_stages_recursive(
            repartition.input()[0].clone(),
            stages,
            None,
        )?;
        // Create output stage with routing info
        let current_stage = ExecutionStage {
            stage_id: stages.len(),
            input_stage_ids: vec![input_stage_id?],
            execution_plan: /* ... */,
            partitions_out: repartition.partitions(),
            output_partitioning: repartition.partitioning().clone(),
        };
        stages.push(current_stage);
        return Ok(Some(current_stage.stage_id));
    }
    
    // Case 2: Recurse to children
    let mut child_stage_ids = Vec::new();
    for child in plan.children() {
        if let Some(stage_id) = extract_stages_recursive(child, stages, None)? {
            child_stage_ids.push(stage_id);
        }
    }
    
    // Case 3: Leaf or final stage
    if stages.is_empty() && plan.children().is_empty() {
        // Single-stage query (no repartitioning)
        let stage = ExecutionStage {
            stage_id: 0,
            input_stage_ids: Vec::new(),
            execution_plan: Arc::new(plan.clone()),
            partitions_out: 1,
            output_partitioning: Partitioning::SinglePartition,
        };
        stages.push(stage);
        Ok(Some(0))
    } else {
        Ok(parent_stage_id)
    }
}
```

**Tasks**:
1.1: Implement RepartitionExec detection via `as_any().downcast_ref()`
1.2: Implement recursive traversal logic with stage boundary detection
1.3: Build ExecutionStage struct and dependency tracking
1.4: Handle edge cases: single-stage, multi-repartition, broadcast queries
1.5: Unit tests: 1-stage, 2-stage, 3-stage query extraction
1.6: Unit tests: verify output_partitioning captured correctly per stage

**Deliverables**:
- `server/src/query_planning/stage_extractor.rs` (fully functional)
- ExeuctionStage struct proven on diverse query plans
- Test suite covering 1-4 stage queries
- Documentation of extraction algorithm

---

### Workstream 2: Async DAG Scheduler

**Objectives**:
- Replace sequential scheduler with async-aware topological sort + concurrent dispatch
- Support independent-stage grouping (stages with no shared dependencies)
- Maintain strict dependency ordering for dependent stages

**Technical Approach**:
```rust
pub struct StageDAG {
    stages: Vec<ExecutionStage>,
    stage_group: HashMap<u32, u32>,  // stage_id → group_id (independent set)
}

pub fn build_stage_dag(stages: Vec<ExecutionStage>) -> Result<StageDAG> {
    // Topological sort
    let mut topo_order = Vec::new();
    let mut visited = HashSet::new();
    
    for stage in &stages {
        if !visited.contains(&stage.stage_id) {
            topo_sort_visit(&stage, &stages, &mut visited, &mut topo_order)?;
        }
    }
    
    // Group independent stages (no shared dependencies) → can dispatch concurrently
    let stage_group = group_independent_stages(&topo_order, &stages)?;
    
    Ok(StageDAG { stages, stage_group })
}

pub async fn execute_stage_dag(dag: StageDAG) -> Result<QueryResults> {
    let mut stage_results: HashMap<u32, Vec<Vec<RecordBatch>>> = HashMap::new();
    let mut current_group = 0;
    
    while let Some(group_stages) = dag.get_stage_group(current_group) {
        // Dispatch independent stages concurrently
        let mut tasks = JoinSet::new();
        
        for stage_id in group_stages {
            let upstream_results = stage_results.clone();
            tasks.spawn(async move {
                execute_stage_locally(stage_id, &upstream_results).await
            });
        }
        
        // Collect results from concurrent execution
        while let Some(result) = tasks.join_next().await {
            let (stage_id, batches) = result???;
            stage_results.insert(stage_id, batches);
        }
        
        current_group += 1;
    }
    
    Ok(stage_results)
}
```

**Tasks**:
2.1: Implement topological sort for stage dependencies
2.2: Implement stage grouping algorithm (identify independent-stage sets)
2.3: Refactor scheduler from while loop to async futures with JoinSet
2.4: Add stage state tracking (Pending → Ready → Executing → Complete)
2.5: Unit tests for topological sort (linear DAG, branching DAG, diamond DAG)
2.6: Unit tests for concurrent dispatch on independent stages

**Deliverables**:
- `server/src/query_planning/stage_dag_scheduler.rs`
- Topological sort validated on diverse DAG shapes
- Async scheduler working with JoinSet
- Tests proving concurrent dispatch on independent stages

---

### Workstream 3: Output Destination Mapping + In-Memory Validation

**Objectives**:
- Calculate output destinations per stage partition (which downstream partitions receive rows)
- Execute full queries in-memory to validate stage extraction correctness
- Prove byte-identical results vs. current execution

**Technical Approach**:

**Part A: Output Destination Mapping** (server-side):
```rust
pub fn calculate_output_destinations(
    dag: &StageDAG,
    stage: &ExecutionStage,
    partition_id: u32,
) -> Result<Vec<OutputDestination>> {
    if stage.output_partitioning == Partitioning::SinglePartition {
        // All rows go to downstream stage's partition 0
        return Ok(vec![OutputDestination {
            downstream_stage_id: /* next stage */,
            worker_addresses: vec!["localhost".to_string()],  // Phase 1: local
            partitioning: Partitioning::SinglePartition,
        }]);
    }
    
    // For other schemes (Hash, RoundRobin), populate all downstream partitions
    // (actual row routing happens in Phase 2 with Flight)
    Ok(vec![/* all downstream partitions */])
}
```

**Part B: In-Memory Query Execution**:
```rust
pub async fn execute_query_in_memory(sql: &str) -> Result<Vec<RecordBatch>> {
    // Parse and plan query (existing code)
    let plan = parse_and_plan(sql)?;
    
    // NEW: Extract stages (Phase 1)
    let stages = extract_stages(plan.clone())?;
    
    // NEW: Build DAG and execute stages locally
    let dag = build_stage_dag(stages)?;
    let stage_results = execute_stage_dag(dag).await?;
    
    // Extract final results (from leaf stage)
    let final_stage_id = /* MAX stage_id */;
    let results = stage_results.get(&final_stage_id)?;
    
    Ok(results.concat())  // Combine all partitions
}

pub async fn execute_stage_locally(
    stage_id: u32,
    stage: &ExecutionStage,
    upstream_results: &HashMap<u32, Vec<Vec<RecordBatch>>>,
) -> Result<Vec<Vec<RecordBatch>>> {
    // Deserialize stage plan
    let plan = deserialize(&stage.execution_plan)?;
    
    // Get input batches from upstream stage(s)
    let input_batches = upstream_results
        .get(&stage.input_stage_ids[0]?)
        .ok_or("upstream stage not ready")?;
    
    // Convert Vec<RecordBatch> to RecordBatchStream
    let input_stream = create_stream_from_batches(input_batches)?;
    
    // Execute plan with input stream
    let mut output_stream = plan.execute_stream(input_stream).await?;
    
    // Partition output by stage.partitions_out
    let mut partitioned_output = vec![vec![]; stage.partitions_out as usize];
    while let Some(batch) = output_stream.next().await {
        // In Phase 1, all batches go to partition 0 (or distribute if needed)
        partitioned_output[0].push(batch);
    }
    
    Ok(partitioned_output)
}
```

**Tasks**:
3.1: Implement output destination calculation for all partitioning schemes
3.2: Implement in-memory RecordBatch stream execution from stages
3.3: Implement query-level execution harness (orchestrate full stage DAG locally)
3.4: Add validation: compare in-memory results vs. current execution (byte-identical)
3.5: End-to-end test: 2-stage query (Scan → Aggregate)
3.6: End-to-end test: 3-stage query (Scan → Join → Aggregate)
3.7: Correctness matrix: single-stage, 2-stage sequential, 3-stage, broadcast queries

**Deliverables**:
- `server/src/query_planning/output_destination_mapper.rs`
- In-memory query execution (debug mode)
- Comprehensive validation tests
- Proof of byte-identical results

---

### Workstream 4: Error Handling + Observability

**Objectives**:
- Handle stage-level failures with proper dependency cascade
- Add comprehensive tracing for stage extraction, DAG scheduling, and validation
- Provide clear error messages for debugging

**Technical Approach**:
```rust
pub enum StageError {
    ExtractionError(String),  // Stage boundary detection failed
    DAGError(String),         // Dependency cycle or invalid DAG
    ExecutionError(String),   // Stage execution failed
    ValidationError(String),  // In-memory validation failed
}

pub async fn execute_stage_dag_with_recovery(dag: StageDAG) -> Result<QueryResults> {
    let mut stage_results = HashMap::new();
    
    for group in &dag.stage_groups {
        let mut tasks = JoinSet::new();
        
        for stage_id in group {
            tasks.spawn(execute_stage_locally(*stage_id, &dag));
        }
        
        while let Some(result) = tasks.join_next().await {
            match result {
                Ok(Ok((stage_id, batches))) => {
                    stage_results.insert(stage_id, batches);
                }
                Ok(Err(e)) => {
                    // Stage failed → abort all dependent stages
                    event!(Level::ERROR, "Stage {} execution failed: {:?}", stage_id, e);
                    return Err(e);  // All-or-nothing semantics
                }
                Err(e) => {
                    event!(Level::ERROR, "Stage task panicked: {:?}", e);
                    return Err(StageError::ExecutionError(e.to_string()).into());
                }
            }
        }
    }
    
    Ok(stage_results)
}
```

**Metrics**:
- `stage_extraction_count`: histogram of stages per query
- `stage_extraction_time_ms`: time to extract stages
- `stage_dag_width`: max concurrent stages in DAG
- `validation_check_count`: number of in-memory validations run
- `validation_error_rate`: % of validations that mismatched baseline

**Tasks**:
4.1: Define StageError enum and error propagation
4.2: Implement stage-level failure detection and cascade
4.3: Add event tracing for stage extraction, DAG creation, execution
4.4: Add metrics for stage pipeline metrics
4.5: Create runbook: debugging stage extraction issues
4.6: Create runbook: debugging DAG scheduling issues
4.7: Integration tests: error scenarios (invalid stage, bad DAG, execution failure)

**Deliverables**:
- Error handling framework
- Comprehensive observability (tracing + metrics)
- Two operational runbooks
- Error scenario tests

---


## Canonical Query Execution Patterns (Phase 1)

| Pattern | Execution Mode | Stage Extraction | In-Memory Validation |
|---------|---|---|---|
| **Single-stage query** | `SELECT * FROM T` | 1 stage (no repartition) | ✅ Local execution; results returned |
| **Sequential 2-stage** | `SELECT count(*) FROM T WHERE x > 5` | Scan+Filter → RepartitionExec → Aggregate | ✅ Stage 0 executes; Stage 1 consumes in-memory |
| **Sequential 3-stage** | `SELECT x, sum(y) FROM T GROUP BY x` (multi-level agg) | Scan → RepartitionExec → PartialAgg → RepartitionExec → FinalAgg | ✅ All 3 stages orchestrated; results byte-identical |
| **HashJoin + Repartition** | `SELECT * FROM T1 JOIN T2 ON T1.id = T2.id` | Scan T1 → Scan T2 → HashJoin → RepartitionExec (hash by join key) | ✅ Stage extraction captures join + downstream repartition |
| **Independent stages** (future) | `SELECT * FROM T` + `SELECT * FROM U` (no join) | 2 independent stages | ⏳ Scheduler concurrency ready; Phase 2 enables network |
| **Broadcast join** | `SELECT * FROM small JOIN T ON small.id = T.id` | Scan small → broadcast → Scan T → Join | ✅ Broadcast stage extracted; validation works |
| **Partial failure** (one partition) | Stage 0 partition fails | All stages fail (all-or-nothing) | ✅ Error handling: cascade failure down DAG |

---

## Criteria-To-Workstream Traceability

| Success Criterion | Workstream | Tasks | Evidence |
|---|---|---|---|
| Stage extraction algorithm proven | WS1 | 1.5–1.6 | Unit tests: 1-4 stage queries extracted correctly |
| Task model redesigned (breaking change) | WS0 | 0.1–0.5 | New proto struct; tests passing |
| Async DAG scheduler working | WS2 | 2.1–2.6 | Topological sort valid; concurrent dispatch on independent stages |
| Output destination mapping correct | WS3 | 3.1–3.2 | Calculation verified; all destination types covered |
| In-memory query execution end-to-end | WS3 | 3.3–3.7 | Full queries (1-3 stage) execute via extracted stages |
| Results byte-identical to baseline | WS3 | 3.4–3.7 | Validation matrix: all query patterns match |
| Error handling for stage failures | WS4 | 4.1–4.7 | Failure scenarios tested; cascade semantics proven |
| Observability: tracing + metrics | WS4 | 4.2–4.4 | Dashboards showing stage extraction, DAG width, validation |

---

## Sequence and Dependencies

```
WS0: Task Model Redesign ──────────┐
                                   ├─→ WS1: Stage Extraction
WS1: Stage Extraction ─────────────┤
                                   ├─→ WS2: Async DAG Scheduler
WS2: Async DAG Scheduler ──────────┤
                                   ├─→ WS3: Output Mapping + In-Memory Execution
WS3: Output Mapping + In-Mem ──────┤
                                   └─→ WS4: Error Handling + Observability
WS4: Error Handling ─────┐
                         └─→ Phase 1 Signoff
```

**Key Dependencies**:
- **Sequential**: WS0 → WS1 → WS2 → WS3 (building control plane foundation)
- **Parallel readiness**: WS1 and WS2 can start once WS0 complete (independent concerns)
- **Integration gate**: WS4 (error handling + observability) depends on all others
- **Compression opportunity**: WS0, WS1, WS2 can overlap in later weeks if needed (aggressive timeline)

**Timeline** (Aggressive: 3-4 weeks):
1. **Week 1** (WS0 + WS1 kickoff)
   - WS0: Task model design + proto (all tasks)
   - WS1: Stage extraction algorithm (1.1–1.3 + 1.5 start)

2. **Week 2** (WS1 completion + WS2 start)
   - WS1: Tests + edge cases (1.4–1.6)
   - WS2: Topological sort + DAG scheduling (2.1–2.4)

3. **Week 3** (WS2 completion + WS3 start)
   - WS2: Tests + edge cases (2.5–2.6)
   - WS3: Output mapping + in-memory execution harness (3.1–3.3)

4. **Week 4** (WS3 completion + WS4 finish)
   - WS3: Validation tests + correctness matrix (3.4–3.7)
   - WS4: Error handling + observability + runbooks (4.1–4.7)
   - **Phase 1 Signoff**

---

## Milestones

| Milestone | Target | Owner | Deliverables |
|------|---|---|---|
| **M1: Proto Design Complete** | Week 1 Fri | Dev | New task proto; output destination struct |
| **M2: Stage Extraction Working** | Week 2 Mon | Dev | Stage extractor module; 1-3 stage tests passing |
| **M3: DAG Scheduler Working** | Week 2 Fri | Dev | Topological sort + async dispatch; independent stage tests |
| **M4: In-Memory Execution Working** | Week 3 Fri | Dev | Full query execution via extracted stages; 2-3 stage E2E tests |
| **M5: Validation Complete** | Week 4 Mon | QA | Byte-identical results proven; all query patterns validated |
| **M6: Error Handling + Observability** | Week 4 Wed | Dev | Error framework; tracing; metrics; runbooks |
| **M7: Phase 1 Signoff** | Week 4 Fri | PM | Matrix complete; all criteria marked Done; ready for Phase 2 |

---

## Risks and Mitigations

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|-----------|
| **RepartitionExec detection misses edge cases** | Medium | High | Extensive unit tests on diverse query plans; validate with real customer queries |
| **Topological sort has cycles (invalid DAG)** | Low | High | Add cycle detection in sort; explicit DAG validation before execution |
| **Stage extraction over-fragments or under-fragments** | Medium | Medium | Validation tests comparing extracted stages vs. manual inspection; dashboard for stage count distribution |
| **In-memory validation produces false negatives** | Medium | High | Cross-check against baseline on 100+ query patterns; instrument both paths to identify divergence |
| **Task model serialization breaks deserialization** | Low | Low | Comprehensive serialization tests; version compatibility checks |
| **Output destination calculation incorrect** | Medium | High | Unit tests for Hash/RoundRobin/Range partitioning; validate routing logic against DataFusion |
| **Async scheduler introduces concurrency bugs** | Medium | High | Thread-safe architecture review; JoinSet error handling; comprehensive stress tests |
| **Error cascade logic fails to propagate properly** | Low | Medium | Manual inspection of dependency cascade; test matrix for multi-failure scenarios |
| **Observability metrics insufficient for debugging** | Medium | Medium | Comprehensive tracing via tracing crate; dashboards showing stage extraction tree |

---

## E2E Validation Scenarios (Phase 1 - In-Memory)

### Scenario 1: Simple 2-Stage Query (Scan → Aggregate)

**Query**: `SELECT COUNT(*) FROM table_1000_rows`

**Expected Flow**:
1. Server parses query → generates ExecutionPlan
2. Stage extractor finds **no RepartitionExec** → 1 stage (Scan + Aggregate)
3. Scheduler immediately executes Stage 0 (no dependencies)
4. Stage 0 produces single RecordBatch with count result
5. Result returned to client

**Validation**:
- ✅ Stage count correct (1 stage)
- ✅ Result byte-identical to current execution
- ✅ No DAG complexity

---

### Scenario 2: 2-Stage with RepartitionExec (Scan → RepartitionExec → Aggregate)

**Query**: `SELECT COUNT(*) FROM t1 WHERE x > 5`

**Setup**: Assume planner generates Scan → Filter → RepartitionExec (hash on group key) → Aggregate

**Expected Flow**:
1. Stage extractor identifies RepartitionExec boundary → 2 stages
   - Stage 0: Scan + Filter (no RepartitionExec above it)
   - Stage 1: Aggregate (next RepartitionExec mapped to final stage)
2. Scheduler creates DAG: Stage 0 → Stage 1
3. Execute Stage 0 locally → outputs Vec<RecordBatch> per partition
4. Execute Stage 1 locally → consumes Stage 0 output → final result
5. Result byte-identical to current execution

**Validation**:
- ✅ Stage extraction identifies boundary correctly
- ✅ DAG dependency correct (Stage 0 → Stage 1)
- ✅ In-memory execution produces same result
- ✅ Output destinations calculated (for Phase 2 readiness)

---

### Scenario 3: 3-Stage Multi-Level Aggregation

**Query**: `SELECT x, SUM(y) FROM t1 GROUP BY x ORDER BY x`

**Setup**: Planner generates:
```
Scan → PartialAggregate → RepartitionExec (hash by x) 
       → FinalAggregate → RepartitionExec (sort by x) 
       → Sort
```

**Expected Flow**:
1. Stage extractor finds 3 RepartitionExec boundaries → 3 stages
   - Stage 0: Scan + PartialAgg
   - Stage 1: FinalAgg
   - Stage 2: Sort
2. Scheduler: Stage 0 → Stage 1 → Stage 2 (linear DAG)
3. Execute each stage sequentially, passing outputs as inputs
4. Result byte-identical

**Validation**:
- ✅ 3 stages extracted correctly
- ✅ Linear DAG (no parallelism opportunity)
- ✅ Multi-stage result correctness

---

### Scenario 4: Independent Query Branches (Future-Ready)

**Query** (simulated): Two independent SELECT statements via federation

**Query**: `SELECT * FROM t1` ∥ `SELECT * FROM t2` (no join)

**Expected Flow** (future):
1. Stage extractor → 2 independent stages
2. Scheduler detects no dependency → groups into independent set
3. **Phase 1**: Execute sequentially (no actual parallelism yet)
4. **Phase 2**: Execute concurrently via network dispatch

**Validation** (Phase 1):
- ✅ DAG correctly identifies no dependencies
- ✅ Results correct (sequential execution matches baseline)
- ✅ Ready for Phase 2 parallel dispatch

---

### Scenario 5: Broadcast Join

**Query**: `SELECT * FROM small_t JOIN t1 ON small_t.id = t1.id`

**Setup**: Planner generates:
```
Scan(small_t) → Broadcast (1 partition)
Scan(t1) → HashJoin(broadcast) → ...
```

**Expected Flow**:
1. Stage extraction: 2 stages
   - Stage 0: Broadcast Scan(small_t)
   - Stage 1: Scan(t1) + HashJoin
2. Scheduler: Stage 0 → Stage 1
3. Execute Stage 0 → broadcast result in-memory
4. Execute Stage 1 → consumes broadcast → final result

**Validation**:
- ✅ Broadcast stage extracted as single partition
- ✅ Join result correct
- ✅ Ready for Phase 2 (broadcast may be shipped over Flight)

---

### Scenario 6: Partial Failure (Stage Execution Error)

**Setup**: Stage 1 execution encounters divide-by-zero or invalid operation

**Expected Flow**:
1. Stage 0 executes successfully
2. Stage 1 execution fails → error propagated
3. All dependent stages should not execute (all-or-nothing)
4. Error message clear: "Stage 1 execution failed: {reason}"

**Validation**:
- ✅ Error caught and propagated
- ✅ No orphaned stages
- ✅ Clear error message

---

## Implementation Checklist for Phase 1

- [ ] WS0.1: Proto definitions (StagePartitionExecution, OutputDestination)
- [ ] WS0.2-0.5: Task model refactored + tests
- [ ] WS1.1-1.3: Stage extraction algorithm + basic tests
- [ ] WS1.4-1.6: Stage extraction comprehensive tests (1-4 stage queries)
- [ ] WS2.1-2.2: Topological sort implemented + tested
- [ ] WS2.3-2.6: DAG scheduler + concurrent dispatch implementation and tests
- [ ] WS3.1-3.2: Output destination mapping + in-memory execution harness
- [ ] WS3.3-3.7: Full query execution E2E tests + validation matrix
- [ ] WS4.1-4.4: Error handling framework + tracing + metrics
- [ ] WS4.5-4.7: Observability runbooks + error scenario tests
- [ ] Code review of all modules
- [ ] Regression testing against current execution
- [ ] Documentation updated (algorithm, architecture, runbooks)
- [ ] Phase 1 Signoff Matrix completed

---

## Phase 1 → Phase 2 Handoff

**At end of Phase 1, Phase 2 receives**:
- ✅ Stage extraction algorithm (proven, tested)
- ✅ Task model with `output_destinations` field (prepared for Flight)
- ✅ Worker awareness of `stage_id`, `partition_id` (prepared for task dispatch)
- ✅ Async DAG scheduler (orchestrates distributed execution)
- ✅ Output destination calculation (ready for partition routing)

**Phase 2 adds**:
- 🔧 Arrow Flight streaming (worker-to-worker direct connections)
- 🔧 MPMC channel routing for multi-partition mapping
- 🔧 Backpressure + flow control
- 🔧 End-to-end testing with real distributed workers

**Result**: Phase 2 becomes **data movement only** (3-4 weeks); control plane already proven.

---

## Implementation Guardrails (Reinforced)

1. **Test-Driven**: Every stage extraction, DAG path, and in-memory execution covered by tests
2. **Correctness Obsession**: Byte-identical result validation is non-negotiable
3. **Observability First**: Every decision (extraction, scheduling, routing) traceable
4. **Conservative Defaults**: If anything uncertain, fail fast with clear error
5. **Breaking Change Accepted**: Old task format incompatible; all clusters upgrade together
6. **No Network Layer**: Phase 1 = in-memory debug mode; Flight comes in Phase 2
7. **Documentation**: Algorithm, architecture, and runbooks complete before signoff

---

## Phase 1 Deliverables Summary

| Deliverable | Owner | Status |
|-------------|-------|--------|
| Async scheduler with concurrent dispatch | Dev | To Be Started |
| Partition checkpoint and conditional reads | Dev | To Be Started |
| Comprehensive test suite (correctness + pipelining) | QA | To Be Started |
| Metrics and observability | Ops | To Be Started |
| Error handling and recovery logic | Dev | To Be Started |
| Phase 1 completion matrix (ROADMAP_PARALLEL_PHASE1_MATRIX.md) | PM | To Be Started |
| Runbook: Pipelined Execution Troubleshooting | Ops | To Be Started |
| Design documentation and diagrams | Arch | To Be Started |

---

## Success Criteria Checklist

- [ ] Async scheduler refactored and tested
- [ ] Partition checkpoint logic implemented
- [ ] Downstream task launch supports early start on partial upstream completion
- [ ] All existing query patterns pass regression tests
- [ ] Multi-stage query latency reduced by 15-40% (measured)
- [ ] Observability metrics in place; pipelined execution visible in traces
- [ ] Error handling graceful for all failure scenarios
- [ ] No breaking changes; backwards compatible
- [ ] Runbook and documentation complete
- [ ] Phase 1 matrix signoff achieved

---

## Evidence Locations

- **Discovery**: [../discovery/discovery-PARALLEL-PHASE1.md](../discovery/discovery-PARALLEL-PHASE1.md)
- **Design Sketches**: [../sketches/](../sketches/) (to be created)
- **Implementation PRs**: (to be linked during development)
- **Test Coverage**: [../tests/](../tests/) (to be created)
- **Phase 1 Matrix**: [../../../ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../../ROADMAP_PARALLEL_PHASE1_MATRIX.md)

---

## Next Steps

1. **Immediate**: Schedule design review meeting to align on scheduler async model and error handling
2. **Week 1**: Begin WS1 (scheduler refactoring) and WS2 (exchange logic) in parallel
3. **Week 3**: Launch WS3 (correctness testing) and WS4 (validation) once core implementations stabilize
4. **Week 5**: Complete Phase 1 signoff and prepare phase 1 matrix
5. **Post-Phase 1**: Plan Phase 2 (Streaming Exchange & Resource-Aware Scheduling)
