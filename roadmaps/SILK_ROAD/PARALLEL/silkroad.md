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

### Phase 1: Pipelined Stage Scheduler & Partial Exchange
**Scope**: Enable concurrent stage dispatch and early downstream startup
**Goal**: 20-40% latency reduction for multi-stage queries
**Duration**: ~5 weeks
**Key Changes**:
- Async scheduler with concurrent independent-stage dispatch
- Partition-level checkpoint tracking for early downstream launches
- Downstream tasks can begin consuming exchange data as upstream partitions complete
- Correctness validation and comprehensive testing

**Evidence**:
- [discovery-PARALLEL-PHASE1.md](PARALLEL/discovery/discovery-PARALLEL-PHASE1.md)
- [plan-PARALLEL-PHASE1.md](PARALLEL/plans/plan-PARALLEL-PHASE1.md)

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
Ph1    │ 10-15% ↓          │ 1.1x ↑    │ -5%      │ -10%     │ 5w        │ 1 worker (async)
Ph2*   │ +15-30% ↓         │ 1.5-2x ↑  │ -25%     │ RESOLVED │ 12-14w    │ N workers!
Ph3    │ +10-15% ↓         │ 1.8x ↑    │ -10%     │ —        │ 3w        │ Adaptive
Ph4    │ +5-10% ↓          │ 2.0x ↑    │ -5%      │ —        │ 4w        │ Optimized
Ph5    │ +5% ↓ (tune)      │ 2.2x ↑    │ Optimal  │ —        │ Ongoing   │ Tuned
```

**Total Vision**: 45-65% end-to-end latency reduction; 2-2.2x throughput improvement for multi-stage workloads.

***Note**: 
- Phase 2 is the **strategic pivot** — enables unlimited worker scaling (Phase 1 was single-worker optimization)
- Phase 2 resolves ~2000 LOC technical debt AND enables true parallelism (not just pipelining)
- Combined Ph1 + Ph2 latency reduction: **30-45%** for typical multi-stage queries (MUST DO BOTH)

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
Phase 1 (Scheduler + Checkpoints) ←─ PREREQUISITE
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
- Ph1 is foundational (async scheduler); Ph2 builds on async infrastructure
- **Ph2 is the strategic pivot**: Enables unlimited worker scaling (not possible in Ph1 single-worker model)
- Ph2 blocked if Ph1 incomplete (needs async runtime)
- Ph3+ blocked if Ph2 incomplete (need distributed execution baseline)
- Ph1 + Ph2 together deliver 30-45% latency reduction + horizontal scale

---

## Phase 1 Detailed Overview

### Discovery & Planning Artifacts

📄 [discovery-PARALLEL-PHASE1.md](PARALLEL/discovery/discovery-PARALLEL-PHASE1.md)
- Current sequential scheduler bottleneck analysis
- Parallelism opportunities enumerated
- Architectural components mapped
- Constraints preventing parallelism documented

📄 [plan-PARALLEL-PHASE1.md](PARALLEL/plans/plan-PARALLEL-PHASE1.md)
- 4 workstreams: Async Scheduler, Partial Exchange, Correctness Testing, Error Handling
- 16 concrete tasks with deliverables
- Risk assessment and mitigation strategies
- E2E scenarios and success criteria

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

📄 [discovery-DATAFUSION-MIGRATION-PHASE2.md](PARALLEL/discovery/discovery-DATAFUSION-MIGRATION-PHASE2.md)
- Current Kionas architecture overview (to be replaced)
- DataFusion architecture and ExecutionPlan model
- Server-side changes (query planning)
- Worker-side changes (execution + streaming results)
- Exchange integration (MPMC channels + S3 fallback)
- Flight-proxy streaming integration (critical section)
- Risk assessment and deployment strategy

📄 [plan-DATAFUSION-MIGRATION-PHASE2.md](PARALLEL/plans/plan-DATAFUSION-MIGRATION-PHASE2.md)
- 4 workstreams:
  - WS1: Server-side ExecutionPlan generation (2 weeks)
  - WS2: Worker-side execution + flight streaming (2 weeks)
  - WS3: Exchange repartitioning + channel integration (1 week)
  - WS4: Correctness testing + performance validation (1 week)
- 22 concrete tasks with effort estimates
- Task summary table and timeline
- Success metrics and deployment strategy

### Phase 2 Completion Matrix

📊 [ROADMAP_PARALLEL_PHASE2_MATRIX.md](../../ROADMAP_PARALLEL_PHASE2_MATRIX.md) *(to be created during implementation)*

| Item | Status | Evidence | Notes |
|------|--------|----------|-------|
| Async scheduler refactored | Pending | PR#xxx | Replace while loop with futures |
| Partition checkpoints implemented | Pending | PR#xxx | Enable early downstream launch |
| Correctness test suite complete | Pending | PR#xxx | All existing queries pass |
| Metrics and observability added | Pending | PR#xxx | Stage dispatch concurrency tracked |
| Multi-stage query tested | Pending | Test result | 3-stage query latency reduced 20-40% |
| Error handling complete | Pending | PR#xxx | Graceful degradation on failures |
| Regression tests pass | Pending | CI run | No breaking changes |
| Phase 1 signoff | Pending | Matrix | All mandatory criteria Done |

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
1. Read [discovery-PARALLEL-PHASE1.md](PARALLEL/discovery/discovery-PARALLEL-PHASE1.md) to understand current bottlenecks
2. Review embedded evidence links to scheduler code
3. Understand the parallelism opportunities (A-E)

### Step 2: Design Review
1. Review [plan-PARALLEL-PHASE1.md](PARALLEL/plans/plan-PARALLEL-PHASE1.md) high-level approach
2. Discuss architectural decisions (especially async patterns and error handling)
3. Confirm workstream grouping and task sequencing

### Step 3: Implementation Kickoff
1. Begin WS1 (Async Scheduler Refactoring): Design topological sort and ready queue
2. Begin WS2 (Partial Exchange): Design partition checkpoint tracking
3. Create detailed design sketches in [PARALLEL/sketches/](PARALLEL/sketches/)
4. Link design sketches in plan document

### Step 4: Continuous Integration
1. Add scheduler tests to CI
2. Add pipelining-specific correctness tests
3. Track metrics: stage concurrency, partition wait times

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
| **Exchange** | Inter-stage data flow; currently materialized to S3 |
| **Checkpoint** | Partition completion signaled when task complete and artifact available |
| **Pipelining** | Overlapping execution of downstream stages before upstream fully complete |
| **Streaming** | Micro-batch or record-at-a-time data flow (Phase 2+) |
| **DAG** | Directed acyclic graph of stages and dependencies |
| **Ready Queue** | Stages awaiting dispatch (all dependencies satisfied) |
| **Resource-Aware** | Scheduler tracks worker capacity (CPU, memory, I/O) |
| **Straggler** | Partition or worker with significantly higher latency than peers |

---

## Success Metrics for PARALLEL Roadmap

| Metric | Phase 1 Target | Phase 2-5 Vision |
|--------|---------------|--------------------|
| **Multi-stage query latency** | -25% (3-stage baseline) | -50% overall |
| **Query throughput** | +20% | +100% (2x) |
| **Worker utilization** | +15% | +40% |
| **Peak memory per stage** | -5% | -30% |
| **P99 query latency** | -20% | -40% |
| **Scheduler overhead** | <2% | <1% |
| **Observability coverage** | All stages traced | Critical path tracked |

---

## Document Index

```
roadmaps/SILK_ROAD/PARALLEL/
├── silkroad.md (this file)
├── discovery/
│   └── discovery-PARALLEL-PHASE1.md
├── plans/
│   └── plan-PARALLEL-PHASE1.md
├── sketches/ (to be created)
│   ├── async_scheduler_design.md
│   ├── partition_checkpoint_design.md
│   └── error_handling_design.md
└── tests/ (to be created)
    ├── scheduler_tests.rs
    ├── exchange_pipelining_tests.rs
    └── e2e_correctness_tests.rs
```

---

## How to Navigate This Roadmap

1. **For Quick Overview**: Read this file (silkroad.md)
2. **For Technical Details**: See [discovery-PARALLEL-PHASE1.md](PARALLEL/discovery/discovery-PARALLEL-PHASE1.md)
3. **For Implementation Plan**: See [plan-PARALLEL-PHASE1.md](PARALLEL/plans/plan-PARALLEL-PHASE1.md)
4. **For Current Status**: Check [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) (created during Phase 1)
5. **For Code Changes**: Follow PRs linked in phase matrix

---

## Contact & Questions

For questions about the PARALLEL roadmap:
- **Architecture**: See discovery and plan documents for decision rationale
- **Implementation**: Refer to phase matrix for task assignments
- **Progress**: Check phase matrix for current status
- **Blockers**: Escalate through Risks & Mitigations section in plan

---

**Roadmap Version**: 1.0  
**Created**: March 2026  
**Status**: Discovery & Planning - Phase 1  
**Next Review**: Post-Phase 1 Signoff
