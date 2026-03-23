# PARALLEL Silk Road: Discovery & Planning Summary

**Date**: March 22, 2026  
**Status**: ✓ Discovery Complete | ✓ Phase 1 Planning Complete | ⏳ Implementation Ready

---

## Executive Summary

This document summarizes the discovery and planning artifacts created for the **PARALLEL Silk Road**, a strategic initiative to enable real parallelism in Kionas query execution.

### Key Finding: Sequential Stage Scheduler is Bottleneck

The current Kionas execution model processes query stages **sequentially** due to a blocking while loop in [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs#L270). Multi-stage queries experience cumulative latency:

```
Query: 3-stage aggregation
Current:  Stage0 (30ms) → Stage1 (30ms) → Stage2 (10ms) = 70ms total
Potential: Overlapped execution = ~50ms (30% improvement)
Phase 5: Fully optimized = ~25ms (65% improvement)
```

### Strategic Opportunity: Pipelined Execution

By enabling downstream stages to start before upstream stages complete, Kionas can:
- **Reduce query latency** by 20-50% for multi-stage queries
- **Improve worker throughput** by 50-100% through better resource utilization
- **Enable adaptive scheduling** for workload skew and stragglers

---

## Artifacts Created

### 1. **Discovery Document**: [discovery-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/discovery/discovery-PARALLEL-PHASE1.md)

**What**: Detailed investigation of current sequential scheduler and parallelism opportunities

**Key Sections**:
- Current execution model and bottleneck analysis
- 5 parallelism opportunities identified (A-E):
  - **A**: Concurrent independent stages
  - **B**: Pipelined downstream stages (partial exchange)
  - **C**: Streaming exchange (micro-batches)
  - **D**: Adaptive partition count
  - **E**: Resource-aware scheduling
- Architectural components mapped with evidence
- Current execution patterns analyzed
- Constraints documented (12 constraints across 4 layers)

**Evidence**: All claims linked to specific source code files and functions

---

### 2. **Phase 1 Plan**: [plan-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PARALLEL-PHASE1.md)

**What**: Detailed implementation roadmap for Phase 1 (Pipelined Stage Scheduler & Partial Exchange)

**Key Sections**:
- **4 Workstreams**:
  - WS1: Async Scheduler Refactoring (T1.1–T1.5)
  - WS2: Partial Exchange Consumption (T2.1–T2.5)
  - WS3: Correctness & Integration Testing (T3.1–T3.6)
  - WS4: Dependency Validation & Error Handling (T4.1–T4.4)
- **16 Concrete Tasks** with deliverables
- **Canonical Mode Decision Table**: How different query patterns behave
- **E2E Scenarios**: 4 detailed end-to-end examples with expected flows
- **Risk Assessment**: 6 risks with mitigation strategies
- **Timeline**: 5-week execution plan with milestones

**Success Criteria**: 20-40% latency reduction for multi-stage queries

---

### 3. **DataFusion Migration Discovery**: [discovery-DATAFUSION-MIGRATION-PHASE2.md](roadmaps/SILK_ROAD/PARALLEL/discovery/discovery-DATAFUSION-MIGRATION-PHASE2.md) (NEW)

**What**: Strategic investigation of replacing entire Kionas operator stack with Apache DataFusion

**Status**: Phase 1 prerequisites established; Phase 2 charts new direction (DataFusion-first)

**Key Sections**:
- Executive summary: Why full DataFusion migration makes sense now
- Current Kionas architecture (to be replaced)
- DataFusion architecture (ExecutionPlan, SendableRecordBatchStream, vectorization)
- Architecture changes by layer (server planner, worker executor, exchange, flight integration)
- **Critical**: Flight-proxy streaming integration (RecordBatch → Arrow IPC format)
- Implementation phases (2a: planning, 2b: execution, 2c: exchange, 2d: testing)
- Risk assessment and mitigation
- Comparative analysis: maintaining parallel operators vs. DataFusion adoption

**Key Insight**: DataFusion's native streaming execution eliminates need for custom operator pipeline; one-time migration cost justified by long-term maintainability + 20-30% perf gain

---

### 4. **Distributed Streaming Architecture Discovery**: [discovery-DISTRIBUTED-STREAMING-EXECUTION.md](roadmaps/SILK_ROAD/PARALLEL/discovery/discovery-DISTRIBUTED-STREAMING-EXECUTION.md) (NEW - March 23)

**What**: Strategic deep-dive on breaking ExecutionPlans into stages for true multi-worker parallelism

**Key Insight**: Rather than sending entire ExecutionPlan to one worker, **break it into stages at RepartitionExec boundaries** and send stage fragments to different workers with network streaming between them.

**Key Sections**:
- Problem with monolithic approach (single-worker bottleneck)
- Distributed streaming model (Worker A: Stage 0 → Worker B: Stage 1 → Worker C: Stage 2 with pipelined execution)
- Benefits: True parallelism, no waiting for upstream completion, scales horizontally
- 3 critical components: Stage extraction, Network channels, Multi-partition mapping
- Feasibility analysis: HIGH on all fronts

**Key Result**: Enables **unlimited worker scaling** for single query (vs. monolithic: 1 worker per query)

---

### 5. **Deep-Dive: Stage Extraction + Arrow Flight + Partition Mapping**: [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](roadmaps/SILK_ROAD/PARALLEL/discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md) (NEW - March 23)

**What**: Technical deep-dive into 3 architectural components for distributed streaming

**Key Sections**:

**Part 1: Stage Extraction via RepartitionExec**
- How RepartitionExec marks natural stage boundaries
- Stage extraction algorithm (pseudocode)
- Example: 3-stage query decomposition
- Feasibility: HIGH (straightforward traversal)

**Part 2: Arrow Flight gRPC Integration**
- Why Arrow Flight is perfect fit (you're already using it!)
- Two patterns: Direct worker-to-worker vs. centralized proxy
- Recommendation: Pattern A (direct) for Phase 2
- RecordBatch → Arrow IPC format conversion
- Automatic backpressure via gRPC channels

**Part 3: Multi-Partition Mapping**
- How Stage 0's 4 partitions map to Stage 1's 4 partitions
- Partitioning schemes: Hash, RoundRobin, Range, SinglePartition
- Server-side mapping calculation
- Network connection timeline
- MPMC channel routing implementation

**Part 4: End-to-End Integration**
- Complete execution flow diagram
- Key decision matrix
- Feasibility & risk assessment per component

---

### 6. **Integrated Phase 2 Plan**: [plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md) (NEW - March 23)

**What**: REVISED Phase 2 plan coupling DataFusion migration WITH distributed streaming architecture

**Status**: Strategic pivot confirmed; this is the new direction (parallelism is a MUST)

**Key Sections**:

**Architecture Alignment**:
- How new plan subsumes original plan (extends, not replaces)
- Overlap analysis: All original Phase 2 components extended/enhanced
- Unified approach: Planner + Stage Extraction + Arrow Flight + Partition Routing

**Implementation Phases** (12-14 weeks total):

- **Phase 2a: DataFusion Planner + Stage Extraction** (3-4 weeks, 6-7 person-days)
  - WS 2a.1: Server-side DataFusion integration (2 weeks)
  - WS 2a.2: Stage extraction algorithm (2 weeks)  
  - WS 2a.3: Task generation with mapping (1 week)

- **Phase 2b: Arrow Flight Worker-to-Worker** (3-4 weeks, 6-7 person-days)
  - WS 2b.1: Flight server/client per worker
  - WS 2b.2: RecordBatch ↔ Arrow IPC conversion
  - WS 2b.3: Stage partition execution + streaming
  - WS 2b.4: Backpressure + flow control

- **Phase 2c: Multi-Partition Mapping & Routing** (2-3 weeks, 5-6 person-days)
  - WS 2c.1: Partition routing logic (Hash, RoundRobin, Range, SinglePartition)
  - WS 2c.2: Multi-worker partition distribution

- **Phase 2d: End-to-End Integration** (2-3 weeks, 5-6 person-days)
  - WS 2d.1: Full pipeline testing
  - WS 2d.2: Performance benchmarking
  - WS 2d.3: Documentation & deployment

**Success Criteria** (7 mandatory items for signoff):
1. Query correctness (byte-identical results)
2. Multi-worker execution (3+ workers per query)
3. Streaming integration (no buffering)
4. Partition mapping (hash/rr/range working)
5. Performance (no regression; target 15-30% improvement)
6. Fault handling (graceful degradation on failure)
7. Code quality (complexity < 25, clippy clean)

**Timeline**: 12-14 weeks, ~25-28 person-days across 4-5 engineers (critical path: 2a → 2b → 2c → 2d)

**Decision Points**: 
- Worker-to-worker direct (Pattern A) OR proxy-based (Pattern B)? → **Recommend A**
- S3 fallback threshold? → **Only on >90% memory pressure**
- Partitioning priority? → **Hash first (80%), then RoundRobin, then Range**

---

### 7. **Strategic Roadmap Update**: [silkroad.md](roadmaps/SILK_ROAD/PARALLEL/silkroad.md) (UPDATED)

**What**: Updated 5-phase vision incorporating new distributed streaming architecture

**Key Changes**:
- Phase 2 now emphasized as **CRITICAL STRATEGIC PIVOT** (enables unlimited scaling)
- Duration revised: 6w → 12-14w (complexity increase justified by scale benefit)
- Cumulative impact updated: 45-65% latency reduction now from Ph1+Ph2 combined
- Tech debt resolution now explicit (2000+ LOC eliminated)
- **New column**: "Scaling" dimension showing progression from 1 worker → N workers

---

### 8. **DataFusion Migration Plan**: [plan-DATAFUSION-MIGRATION-PHASE2.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-DATAFUSION-MIGRATION-PHASE2.md) (ARCHIVED)
  - WS2: Worker-side execution + flight streaming adapter (6 tasks, 2 weeks)
  - WS3: Exchange repartitioning + MPMC channels (6 tasks, 1 week)
  - WS4: Correctness & performance testing (6 tasks, 1 week)
- **22 Concrete Tasks** with detailed effort estimates
- **Task Summary Table**: Organized by workstream with owner assignments
- **Implementation Timeline**: 6-week end-to-end plan
- **Success Metrics**: 7 completion criteria for phase signoff
- **Deployment Strategy**: Preview → Canary → Full rollout

**Success Criteria**: All queries via DataFusion; 20-30% latency improvement; streaming works end-to-end; technical debt resolved

---

### 5. **Silk Road Overview**: [silkroad.md](roadmaps/SILK_ROAD/PARALLEL/silkroad.md)

**What**: Strategic vision document for full PARALLEL initiative (5 phases)

**Updated Content**:
- **Phase 1**: Async scheduler + partition checkpoints (20-40% improvement, 5w)
- **Phase 2**: DataFusion Full Migration (15-25% more, 6w, resolves tech debt)
- **Phase 3**: Resource-aware scheduling + adaptive partitioning (10-15% more, 3w)
- **Phase 4**: DAG fusion + advanced optimization (5-10% more, 4w)
- **Phase 5**: Critical path observability + advanced tuning (ongoing)
- **Cumulative**: 45-65% latency reduction; 2x throughput + maintenance burden eliminated

---

### 6. **Phase 1 Completion Matrix**: [ROADMAP_PARALLEL_PHASE1_MATRIX.md](roadmaps/ROADMAP_PARALLEL_PHASE1_MATRIX.md)

**What**: Tracking document for Phase 1 implementation progress

**Key Content**:
- **27 Completion Items**: Mapped to workstreams and tasks
- **8 Mandatory Criteria** (signoff gate):
  - A1: Async scheduler refactored
  - A2: Partition checkpoint logic
  - A3: Downstream early launch
  - A4: Correctness validation
  - A5: 20-40% latency reduction achieved
  - A6: Error handling complete
  - A7: Observability metrics added
  - A8: No breaking changes
- **Optional Hardening**: 5 deferred items mapped to Phase 2-5
- **Workstream Summary**: High-level deliverables per workstream
- **Gate Checklist**: Pre-signoff verification

---

## Folder Structure Created

```
roadmaps/SILK_ROAD/PARALLEL/
├── silkroad.md                          ✓ Created
├── discovery/
│   └── discovery-PARALLEL-PHASE1.md    ✓ Created
└── plans/
    └── plan-PARALLEL-PHASE1.md         ✓ Created

roadmaps/
└── ROADMAP_PARALLEL_PHASE1_MATRIX.md   ✓ Created
```

**To Create During Implementation**:
```
roadmaps/SILK_ROAD/PARALLEL/
├── sketches/
│   ├── async_scheduler_design.md       ⏳ Design phase
│   ├── partition_checkpoint_design.md  ⏳ Design phase
│   └── error_handling_design.md        ⏳ Design phase
├── tests/
│   ├── scheduler_tests.rs              ⏳ Implementation
│   ├── exchange_pipelining_tests.rs    ⏳ Implementation
│   └── e2e_correctness_tests.rs        ⏳ Implementation
├── benchmarks/
│   └── pipelined_e2e_latency.md        ⏳ Performance validation
├── runbooks/
│   └── pipelining_troubleshoot.md      ⏳ Ops guide
└── docs/
    └── pipelined_execution.md          ⏳ Technical documentation
```

---

## Key Discovery Findings

### Finding 1: Blocking While Loop is Primary Bottleneck
```rust
// Current pattern (helpers.rs ~L270)
while !ready_stages.is_empty() || !all_done {
    stage = ready_stages.pop();           // Wait for deps
    dispatch_stage_partitions(stage);     // Parallel within stage
    wait_for_all_partitions(stage);       // ← BLOCKS HERE
    mark_stage_complete(stage);           // 
}
```

**Impact**: No stage can start until ALL previous stages fully complete

**Solution**: Async dispatch with ready queue; support partial/progressive upstream completion

---

### Finding 2: Exchange is All-or-Nothing
Current pattern:
1. Upstream writes **all** partitions to S3
2. Downstream reads **all** upstream partitions before proceeding
3. Then begins downstream operators

**Opportunity**: 
1. Downstream can read partition 0 as soon as available (within ~100ms)
2. Begin pipelined execution while remaining upstream partitions still writing
3. ~15-20% latency win for typical 2-stage queries

---

### Finding 3: 5 Distinct Parallelism Opportunities
| Opp | Scope | Phase | Est. Impact |
|-----|-------|-------|------------|
| **A** | Independent stages | Ph1-2 | Depends on query DAG |
| **B** | Pipelined downstream | Ph1 | 20-40% cumulative |
| **C** | Streaming exchange | Ph2 | +10-20% |
| **D** | Adaptive partitioning | Ph3 | +10-15% |
| **E** | Resource awareness | Ph2-3 | +5-10% |

---

### Finding 4: Constraints Span Multiple Layers

**Server Scheduler**:
- Blocking while loop; no concurrent stages
- All-or-nothing completion semantics
- Static task graph; no dynamic adaptation

**Exchange**:
- Full materialization required before downstream reads
- Deterministic paths (no work-stealing)
- Synchronous round-trips

**Worker**:
- Sequential upstream reads (must wait for all)
- Tight coupling to specific operator stage
- No partial consumption support

**Resource Layer**:
- No capacity tracking
- Unbounded task queues
- No straggler mitigation

**=> Phase 1 addresses Server + Exchange; Phase 2-5 address Worker + Resource layers**

---

## Phase 1 High-Level Approach

### Workstream 1: Async Scheduler
**Goal**: Enable concurrent dispatch of independent stages

**Approach**:
- Replace while loop with async futures
- Implement topological sort + ready queue
- Independent stages execute concurrently; dependent stages still respect ordering
- Track stage transitions atomically

**Deliverables**:
- Refactored scheduler function
- Design documentation
- Unit/integration tests
- Scheduler timeline metrics

---

### Workstream 2: Partial Exchange
**Goal**: Enable downstream to read available partitions early

**Approach**:
- Define partition checkpoint: "complete when task finishes and artifact exists"
- Downstream task checks S3 for upstream partition availability
- Conditional polling with exponential backoff
- Can launch when 1+ upstream partition is available

**Deliverables**:
- Partition checkpoint tracking
- Conditional exchange reader
- Downstream launch logic update
- E2E pipelined query test

---

### Workstream 3: Testing & Correctness
**Goal**: Verify all query patterns work correctly with pipelining

**Approach**:
- Comprehensive correctness matrix (13 scenarios)
- Cross-check results vs sequential baseline
- Regression test entire query suite
- Failure injection testing

**Deliverables**:
- Test harness
- Correctness validation report
- Regression results
- 100% pass rate

---

### Workstream 4: Error Handling
**Goal**: Gracefully handle dependency and timeout failures

**Approach**:
- Explicit dependency validation
- Structured error types for scheduler
- Configurable timeouts with clear error messages
- Fallback to sequential if safety violated

**Deliverables**:
- Error taxonomy
- Timeout/retry logic
- Clear diagnostics runbook
- Safety fallback logic

---

## Timeline: Phase 1 (5 Weeks)

| Week | Focus | Milestones |
|------|-------|-----------|
| **1** | Design & kickoff | Design review passed; WS1/WS2 begin |
| **2** | Core implementation | WS1 scheduler refactored; WS2 checkpoints done |
| **3** | Testing & validation | Correctness baseline; regression tests |
| **4** | Error handling & E2E | Error scenarios tested; pipelined query runs |
| **5** | Signoff | Matrix complete; all criteria Done; phase approval |

---

## Success Criteria: Phase 1 Signoff

All of the following must be **Done** (not Deferred):

1. ✓ Async scheduler refactored and tested
2. ✓ Partition checkpoint logic implemented
3. ✓ Downstream early launch working
4. ✓ All existing query patterns pass (regression)
5. ✓ Multi-stage query shows 20-40% latency reduction
6. ✓ Error handling graceful for failures
7. ✓ Observability metrics in place
8. ✓ No breaking changes; backwards compatible

**Measurement**: Performance benchmark showing baseline vs pipelined latency for 3-stage aggregation query

---

## Next Steps

### Immediate (Week 1)
1. ✓ Review discovery + plan artifacts (this document)
2. Schedule design review with team
3. Align on architectural decisions (async patterns, error handling model)
4. Begin detailed design sketches (WS1 scheduler, WS2 checkpoints)

### Week 1-2
1. Implement WS1 core (async scheduler loop, ready queue, topological sort)
2. Implement WS2 core (partition checkpoint tracking, conditional reads)
3. Add scheduler tests and metrics
4. Create first E2E test (single pipelined query)

### Week 3-5
1. Comprehensive correctness testing
2. Error handling + recovery logic
3. Performance validation
4. Phase 1 matrix signoff

### Post-Phase 1
1. Plan Phase 2 (Streaming Exchange + Resource Awareness)
2. Share learnings with team
3. Begin Phase 2 implementation

---

## Related Documents

### Discovery & Context
- [discovery-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/discovery/discovery-PARALLEL-PHASE1.md) — Detailed technical analysis
- [plan-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PARALLEL-PHASE1.md) — Implementation plan
- [silkroad.md](roadmaps/SILK_ROAD/PARALLEL/silkroad.md) — Full 5-phase vision

### Execution Pipeline (Prior Context)
- [roadmaps/discover/execution_pipeline_discovery.md](roadmaps/discover/execution_pipeline_discovery.md) — Worker-side execution analysis
- [roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md](roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md) — Prior execution phase

### Progress Tracking
- [ROADMAP_PARALLEL_PHASE1_MATRIX.md](roadmaps/ROADMAP_PARALLEL_PHASE1_MATRIX.md) — Phase 1 completion tracking

---

## Glossary

| Term | Definition |
|------|-----------|
| **Pipelining** | Overlapping execution of downstream stages before upstream fully complete |
| **Partition Checkpoint** | Milestone when upstream partition is written and available for downstream reads |
| **Ready Queue** | Stages awaiting dispatch (all dependencies satisfied) |
| **Async Scheduler** | Non-blocking scheduler using futures for concurrent task dispatch |
| **Streaming Exchange** | Micro-batch or record-at-a-time data flow (Phase 2+) |
| **DAG Fusion** | Merging of independent/low-dependency stages to reduce boundaries |
| **Straggler** | Partition/worker with significantly higher latency than peers |
| **All-or-Nothing** | Semantics where entire stage fails if any partition fails |

---

## Contact & Questions

For questions about PARALLEL roadmap:

- **Why parallelism matters**: See "Executive Summary" above
- **How we'll do it**: See [plan-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PARALLEL-PHASE1.md)
- **What we discovered**: See [discovery-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/discovery/discovery-PARALLEL-PHASE1.md)
- **Progress tracking**: See [ROADMAP_PARALLEL_PHASE1_MATRIX.md](roadmaps/ROADMAP_PARALLEL_PHASE1_MATRIX.md)
- **Full vision**: See [silkroad.md](roadmaps/SILK_ROAD/PARALLEL/silkroad.md)

---

**Document**: PARALLEL Silk Road: Discovery & Planning Summary  
**Version**: 1.0  
**Status**: ✓ Complete (Discovery & Planning Phase)  
**Last Updated**: March 22, 2026  
**Ready for**: Phase 1 Implementation Kickoff
