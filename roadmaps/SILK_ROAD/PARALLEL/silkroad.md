# PARALLEL Silk Road: Enabling Real Parallelism in Kionas

## Vision

Transform Kionas query execution from **sequential stage processing** to **truly pipelined, resource-aware distributed query execution**. Enable multi-stage queries to achieve overlapping stage execution, reducing cumulative latency by 50%+ and improving worker throughput through streaming data flow and adaptive resource allocation.

## Strategic Objective

Establish a foundation where:
- Multiple stages (or partial stages) execute concurrently
- Data flows between stages asynchronously without full materialization bottlenecks
- Workers adapt to workload skew and prevent stragglers from blocking progress
- Query latency scales with critical path rather than cumulative stage times

## Current State (Baseline)

- **Execution Model**: Sequential stages with within-stage partition parallelism (4-way default)
- **Stage Latency**: Cumulative (T0 + T1 + T2 + ...)
- **Exchange**: Full materialization to S3; blocking round-trip reads
- **Scheduling**: Single-threaded while loop; all-or-nothing completion
- **Resource Awareness**: None; blind dispatch
- **Example Impact**: 3-stage query → 70ms (30+30+10) → no overlap

## Silk Road Phases

### Phase 1: Distributed Control Plane Foundation
**Scope**: Build stage extraction + DAG orchestration + distributed task contract
**Goal**: Prove control-plane correctness before network data movement
**Duration**: 3-4 weeks
**Key Changes**:
- Stage extraction at RepartitionExec boundaries
- Async DAG scheduler with dependency-safe concurrent dispatch
- Breaking task model redesign (`StagePartitionExecution` + routing metadata)
- In-memory execution harness for byte-identical correctness validation

**Evidence**:
- [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)
- [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md)

---

### Phase 2: DataFusion Full Migration + Distributed Streaming Architecture
**Scope**: Replace entire Kionas operator stack with Apache DataFusion ExecutionPlan model; add multi-worker distributed execution with pipelined stage streaming
**Goal**: 20-30% latency improvement + enable unlimited horizontal scaling via stage-level parallelism
**Duration**: 12-14 weeks (4 integrated workstreams)
**Key Changes**:

**2a. Server-Side: DataFusion Planner + Stage Extraction** (3-4 weeks)
- Server generates optimized ExecutionPlans (DataFusion planner replaces Kionas planner)
- Stage extraction at RepartitionExec boundaries → split monolithic plans into distributable fragments
- Task model extended: execution_plan + output_destinations + input_connections

**2b. Worker-Side: Arrow Flight Streaming** (3-4 weeks)
- Workers execute stage partitions locally via DataFusion
- RecordBatches streamed directly to downstream workers via Arrow Flight gRPC
- No intermediate S3 materialization for inter-stage data (MPMC channels for backpressure)
- Native backpressure: fast producers throttle to slow consumers

**2c. Network Layer: Multi-Partition Mapping & Routing** (2-3 weeks)
- Partition-aware routing (Hash, RoundRobin, Range, SinglePartition schemes)
- Rows routed via partitioning scheme to correct downstream partition
- Efficient MPMC channel management per destination
- Multi-worker fan-out (Stage 0 P0 → Stage 1 P0,P1,P2,P3 based on hash)

**2d. Integration & Validation** (2-3 weeks)
- End-to-end testing: multi-worker, multi-stage query execution
- Performance benchmarking: latency + throughput + memory
- Deployment strategy: preview → canary → full rollout

**Why This Advanced Architecture**:
- Opportunity C (streaming micro-batches) + horizontal scaling now enabled
- **New capability**: Single query spans multiple workers in parallel (e.g., Stage 0 on Workers A,B; Stage 1 on C,D; Stage 2 on E)
- Pipelined execution: stages overlap → critical path reduces dramatically
- DataFusion provides production-grade optimizer + native streaming ExecutionPlan
- Eliminates 2000+ LOC of custom operators AND manual orchestration
- Trade-off: Higher complexity (networking, coordination) vs. unlimited scale + tech debt resolution

**Expected Results**:
- Phase 2a: Basic multi-stage execution on single worker ✓ (fallback if 2b fails)
- Phase 2b: Streaming works; Arrow Flight functional ✓ (enables pipelining)
- Phase 2c: Partition routing correct; rows distributed properly ✓ (no data loss)
- Phase 2d: All existing queries work; 15-30% latency improvement; scales to N workers ✓

**Evidence**:
- [discovery-DISTRIBUTED-STREAMING-EXECUTION.md](discovery/discovery-DISTRIBUTED-STREAMING-EXECUTION.md)
- [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)
- [plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md](plans/plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md)

---

### Phase 3: Adaptive Partitioning & Straggler Mitigation
**Scope**: Dynamic partition count adjustment; speculative duplicate execution
**Goal**: 10-15% additional improvement; handle skewed workloads
**Key Changes**:
- Runtime partition count adjustment based on data distribution
- Straggler detection via percentile-based metrics
- Speculative cloning of slow partitions
- Work-stealing between partitions

---

### Phase 4: DAG Fusion & Query Optimization
**Scope**: Merge independent stages; optimize stage boundaries
**Goal**: Eliminate unnecessary boundaries; compact DAG
**Key Changes**:
- Identify fusible stage pairs (independent or low-dependency)
- Inline small stages into larger consumers
- Reduce number of S3 round-trips
- Improved query plan optimization

---

### Phase 5: Advanced Features
**Scope**: Distributed tracing, ML-based cost modeling, multi-tenant fairness
**Key Changes**:
- End-to-end critical path tracing
- Predictive cost models for latency/throughput
- Fair scheduling across multiple concurrent queries
- Advanced debugging and profiling tools

---

## Cumulative Impact Roadmap

```
Phase  │ Latency Reduction │ Throughput │ Memory   │ Tech Debt │ Timeline  │ Scaling
───────┼──────────────────┼───────────┼──────────┼──────────┼───────────┼────────────
Base   │ —                 │ 1x        │ Baseline │ High     │ —         │ 1 worker
Ph1    │ Correctness gate  │ 1x        │ Baseline │ -20%     │ 3-4w      │ 1 worker (control plane)
Ph2*   │ +15-30% ↓         │ 1.5-2x ↑  │ -25%     │ RESOLVED │ 10-14w    │ N workers
Ph3    │ +10-15% ↓         │ 1.8x ↑    │ -10%     │ —        │ 3w        │ Adaptive
Ph4    │ +5-10% ↓          │ 2.0x ↑    │ -5%      │ —        │ 4w        │ Optimized
Ph5    │ +5% ↓ (tune)      │ 2.2x ↑    │ Optimal  │ —        │ Ongoing   │ Tuned
```

**Total Vision**: 45-65% end-to-end latency reduction; 2-2.2x throughput improvement for multi-stage workloads.

***Note**: 
- Phase 1 is a **control-plane gate** (correctness and orchestration), not a latency phase
- Phase 2 is the **strategic pivot** for true multi-worker execution and performance gain
- Combined Ph1 + Ph2 establish correctness first, then scaling and latency wins

---

## Key Architectural Changes Required

| Component | Phase | Change | Rationale |
|-----------|-------|--------|-----------|
| **Query Planner** | Ph2 | Replace Kionas with DataFusion | Native optimizer; rule-based execution |
| **Stage Extraction** | Ph2 | RepartitionExec boundary detection | Break monolithic plans into distributable stages |
| **Worker Executor** | Ph2 | Replace operators with ExecutionPlan | Vectorization + fusion + native streaming |
| **Server Scheduler** | Ph1/Ph2 | Async dispatch + stage DAG orchestration | Enable stage-level parallelism |
| **Exchange Transport** | Ph2 | Arrow Flight (gRPC streaming) worker-to-worker | Enable pipelined data flow; replace S3 |
| **Partition Routing** | Ph2 | Hash/RoundRobin/Range schemes | Route rows to correct downstream partition |
| **Exchange Buffering** | Ph2 | MPMC channels + S3 fallback | Streaming with backpressure + memory safety |
| **Network Protocol** | Ph2 | RecordBatch ↔ Arrow IPC conversion | Efficient serialization for Flight |
| **Resource Tracking** | Ph3 | Worker metrics + memory pressure detection | Prevent overload; trigger spill |
| **Partitioning** | Ph3 | Adaptive partition count | Handle skew + straggler mitigation |
| **DAG Optimizer** | Ph4 | Stage fusion logic | Compact plan; reduce network hops |
| **Observability** | Ph5 | Critical path tracing | Diagnose bottlenecks; predict latency |

---

## Dependencies Between Phases

```
Phase 1 (Control Plane: Extraction + DAG + Task Contract) ←─ PREREQUISITE
  ↓
  └─→ Phase 2 (CRITICAL: Distributed Streaming) ←─ STRATEGIC PIVOT
        ├─ 2a: DataFusion Planner
        ├─ 2b: Arrow Flight Worker-to-Worker
        └─ 2c: Multi-Partition Routing
        ↓
Phase 3 (Adaptive Partitions + Metrics)
  ├─→ Phase 4 (DAG Fusion) — can proceed in parallel post-Ph3
  └─→ Phase 5 (Advanced) — dependent on Ph3 metrics
```

**Rationale**: 
- Ph1 is foundational (stage extraction + DAG + task contract); Ph2 builds on this control plane
- **Ph2 is the strategic pivot**: Enables unlimited worker scaling and pipelined distributed data movement
- Ph2 blocked if Ph1 incomplete (needs async runtime)
- Ph3+ blocked if Ph2 incomplete (need distributed execution baseline)
- Ph1 + Ph2 together deliver correctness + horizontal scale + major latency reduction

---

## Phase 1 Detailed Overview

### Discovery & Planning Artifacts

📄 [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)
- RepartitionExec stage-boundary strategy
- Arrow Flight transport patterns (direct and proxy)
- Multi-partition mapping and routing semantics

📄 [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md)
- 5 workstreams: WS0 Task Model, WS1 Extraction, WS2 DAG Scheduler, WS3 In-Memory Validation, WS4 Error/Observability
- Breaking contract accepted (no backward compatibility)
- Control-plane signoff criteria for Phase 2 readiness

### Phase 1 Completion Matrix

📊 [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) *(to be created during implementation)*

---

## Phase 2 Detailed Overview: DataFusion Full Migration

### Strategic Rationale

**Decision**: Adopt DataFusion as primary execution engine (not just for streaming exchange).

**Why Now**: Opportunity C discovery confirmed that true streaming execution requires fundamental architecture change. Rather than maintaining parallel Kionas operators, commit to DataFusion migration (one-time cost; long-term gain).

**Key Benefits**:
- Eliminate 2000+ LOC of custom operator maintenance
- Access production-grade optimizer (rule-based; automatic)
- Vectorization + operator fusion (20-30% latency improvement)
- Automatic spill-to-disk for memory management
- Community-driven; fast feature velocity

### Discovery & Planning Artifacts

📄 [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)
- Stage extraction algorithm, Arrow Flight transport, and partition mapping foundations

📄 [plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md](plans/plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md)
- 2a planner integration + extraction integration
- 2b worker Flight data movement
- 2c partition routing and fan-out
- 2d integration, chaos testing, and performance validation

📄 [plan-DATAFUSION-MIGRATION-PHASE2.md](plans/plan-DATAFUSION-MIGRATION-PHASE2.md) *(legacy reference)*
- Maintained for historical context; superseded by integrated Phase 2 plan

### Phase 2 Completion Matrix

📊 [ROADMAP_PARALLEL_PHASE2_MATRIX.md](../../ROADMAP_PARALLEL_PHASE2_MATRIX.md) *(to be created during implementation)*

| Item | Status | Evidence | Notes |
|------|--------|----------|-------|
| Stage task contract finalized | Pending | PR#xxx | Phase 1 contract consumed by Phase 2 |
| Worker-to-worker Flight streaming operational | Pending | PR#xxx | Direct streaming between stages |
| Partition routing correctness validated | Pending | PR#xxx | Hash/RoundRobin mapping verified |
| End-to-end distributed query correctness | Pending | Test result | Byte-identical to baseline |
| Failure handling under worker loss | Pending | Chaos test | Fast-fail, no hangs |
| Performance baseline met | Pending | Benchmark report | Latency and throughput targets met |
| Observability coverage complete | Pending | Dashboard/Trace | Critical path and backpressure visible |
| Phase 2 signoff | Pending | Matrix | Mandatory criteria marked Done |

---

## Related Roadmaps & Context

### Prior Execution Work
- [ROADMAP2.md](../ROADMAP2.md) — Overall Kionas roadmap
- [roadmaps/discover/execution_pipeline_discovery.md](../discover/execution_pipeline_discovery.md) — Worker execution pipeline analysis
- [ROADMAP_EXECUTION_EP1_MATRIX.md](../ROADMAP_EXECUTION_EP1_MATRIX.md) — Prior execution phase signoff

### Complementary Initiatives
- **Constraints & Type System**: [ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](../ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)
- **DataTypes**: [ROADMAP_DATATYPES_FOUNDATION_MATRIX.md](../ROADMAP_DATATYPES_FOUNDATION_MATRIX.md)
- **Indexing**: [PHASE_INDEXING_FOUNDATION_MATRIX.md](../PHASE_INDEXING_FOUNDATION_MATRIX.md)

---

## Starting Phase 1: Getting Started

### Step 1: Review Discovery
1. Read [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)
2. Confirm RepartitionExec boundary assumptions with team
3. Confirm worker-to-worker Flight model choice (Pattern A default)

### Step 2: Design Review
1. Review [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) for WS0-WS4 sequencing
2. Lock task contract (`StagePartitionExecution`, routing metadata, execution mode hints)
3. Confirm signoff gate: in-memory byte-identical correctness before Phase 2

### Step 3: Implementation Kickoff
1. Begin WS0 (task contract) and WS1 (stage extraction)
2. Implement WS2 DAG scheduler with dependency-safe dispatch
3. Build WS3 in-memory harness and validation matrix
4. Add WS4 tracing/metrics and fail-fast error propagation

### Step 4: Continuous Integration
1. Add extraction and DAG tests to CI
2. Add byte-identical correctness matrix to CI gates
3. Track metrics: extraction count/time, DAG width, validation mismatch rate

### Step 5: Phase 1 Signoff
1. Complete [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md)
2. Mark all mandatory criteria as Done with evidence
3. Document any Deferred optional items
4. Record final signoff decision

---

## Glossary & Terms

| Term | Definition |
|------|-----------|
| **Stage** | Logical operator group executed as a single unit; maps to one or more tasks partitioned across workers |
| **Task** | Individual unit of work dispatched to a worker; scoped to partition and stage |
| **Partition** | Row-based horizontal slice of data; default 4 per stage |
| **Exchange** | Inter-stage data flow via Arrow Flight streams; S3 used only for spill/fallback safety |
| **Execution Mode Hint** | Task-level hint indicating LocalOnly or Distributed execution behavior |
| **Pipelining** | Overlapping execution of multiple stages with streamed handoff |
| **Streaming** | RecordBatch flow over Flight (gRPC + Arrow IPC), worker-to-worker |
| **DAG** | Directed acyclic graph of stages and dependencies |
| **Ready Queue** | Stages awaiting dispatch (all dependencies satisfied) |
| **Resource-Aware** | Scheduler tracks worker capacity (CPU, memory, I/O) |
| **Straggler** | Partition or worker with significantly higher latency than peers |

---

## Success Metrics for PARALLEL Roadmap

| Metric | Phase 1 Target | Phase 2-5 Vision |
|--------|---------------|--------------------|
| **Multi-stage query latency** | Correctness gate (N/A) | -50% overall |
| **Query throughput** | Baseline maintained | +100% (2x) |
| **Worker utilization** | Baseline maintained | +40% |
| **Peak memory per stage** | Baseline maintained | -30% |
| **P99 query latency** | Baseline maintained | -40% |
| **Scheduler overhead** | <2% | <1% |
| **Observability coverage** | All stages traced | Critical path tracked |

---

## Document Index

```
roadmaps/SILK_ROAD/PARALLEL/
├── silkroad.md (this file)
├── discovery/
│   ├── discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md
│   └── discovery-PARALLEL-PHASE1.md (historical)
├── plans/
│   ├── plan-PARALLEL-PHASE1.md
│   ├── plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md
│   └── plan-DATAFUSION-MIGRATION-PHASE2.md (legacy)
├── sketches/ (to be created)
│   ├── stage_extraction_design.md
│   ├── dag_scheduler_design.md
│   └── error_handling_design.md
└── tests/ (to be created)
  ├── stage_extractor_tests.rs
  ├── dag_scheduler_tests.rs
  └── distributed_e2e_correctness_tests.rs
```

---

## How to Navigate This Roadmap

1. **For Quick Overview**: Read this file (silkroad.md)
2. **For Technical Details**: See [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)
3. **For Phase 1 Plan**: See [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md)
4. **For Phase 2 Plan**: See [plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md](plans/plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md)
5. **For Current Status**: Check [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) and Phase 2 matrix when created
6. **For Code Changes**: Follow PRs linked in phase matrices

---

## Contact & Questions

For questions about the PARALLEL roadmap:
- **Architecture**: See discovery and plan documents for decision rationale
- **Implementation**: Refer to phase matrix for task assignments
- **Progress**: Check phase matrix for current status
- **Blockers**: Escalate through Risks & Mitigations section in plan

---

**Roadmap Version**: 1.1  
**Created**: March 2026  
**Status**: Discovery & Planning - Phase 1 + Integrated Phase 2  
**Next Review**: Post-Phase 1 Signoff
