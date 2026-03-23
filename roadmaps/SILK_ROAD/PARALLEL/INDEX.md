# PARALLEL Silk Road: Complete Artifact Index

**Status**: ✓ Discovery & Planning Updated | Revamp Planning Active (Control Plane + Distributed Data Plane)

---

## 📁 Directory Structure

```
roadmaps/SILK_ROAD/PARALLEL/
├── README.md                                    ← Start here (Quick overview & summary)
├── silkroad.md                                  ← Strategic vision for all 5 phases
├── OPPORTUNITY-C-SUMMARY.md                     ← Opportunity C integration + Phase 2 planning
├── discovery/
│   ├── discovery-PARALLEL-PHASE1.md            ← Technical deep-dive: bottlenecks & opportunities
│   └── discovery-OPPORTUNITY-C-DATAFUSION.md   ← DataFusion streaming model investigation (NEW)
└── plans/
  ├── plan-PARALLEL-PHASE1.md                 ← Phase 1 control-plane roadmap (WS0-WS4)
  ├── plan-DATAFUSION-MIGRATION-PHASE2.md     ← Legacy Phase 2 migration plan (superseded)
  └── plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md ← Active integrated Phase 2 plan

roadmaps/
└── ROADMAP_PARALLEL_PHASE1_MATRIX.md           ← Phase 1 completion tracking template
```

---

## 📄 Artifacts Created

### Tier 1: Start Here 🚀

#### **[README.md](README.md)** (This Series)
- **Purpose**: Quick overview of discovery & planning
- **Length**: ~5 min read
- **Contains**: 
  - Executive summary of key findings
  - Bottleneck identification
  - Artifacts overview
  - Next steps
- **Audience**: Project managers, team leads, anyone wanting quick context

---

### Tier 2: Strategic Vision 📊

#### **[silkroad.md](silkroad.md)**
- **Purpose**: Full strategic vision for PARALLEL roadmap (all 5 phases)
- **Length**: ~10 min read
- **Contains**:
  - Vision statement
  - 5-phase roadmap with cumulative impact
  - Phase dependencies and sequencing
  - Key architectural changes per phase
  - Success metrics for each phase
  - Related roadmaps and context
- **Audience**: Architects, tech leads, strategic planners
- **Key Insight**: Shows how Phase 1 fits into larger 5-phase plan for 50%+ latency improvement

---

### Tier 3: Technical Deep-Dive 🔍

#### **[discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md)**
- **Purpose**: Primary technical foundation for distributed execution redesign
- **Length**: ~20-30 min read (technical)
- **Contains**:
  - Stage extraction at RepartitionExec boundaries
  - Arrow Flight worker-to-worker transport models
  - Multi-partition mapping and routing semantics
  - End-to-end integration model for distributed stage execution
- **Audience**: Architects, leads, implementation developers
- **Key Insight**: Distributed execution must be stage-based and stream-first, not monolithic-plan dispatch

#### **[discovery-PARALLEL-PHASE1.md](discovery/discovery-PARALLEL-PHASE1.md)**
- **Purpose**: Historical baseline bottleneck analysis
- **Status**: Superseded by deep-dive-led redesign

---

#### **[plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md)**
- **Purpose**: Detailed Phase 1 control-plane implementation roadmap
- **Length**: ~30 min read (comprehensive)
- **Contains**:
  - Breaking task contract redesign (WS0)
  - Stage extraction and DAG scheduler implementation
  - In-memory correctness harness and signoff matrix
  - Error propagation + observability package
- **Audience**: Implementation teams, project managers, QA leads
- **Key Insight**: Phase 1 is a correctness gate for distributed execution, not a legacy pipelining phase

---

### Tier 3b: DataFusion Full Migration (Phase 2) 🚀

#### **[discovery/discovery-DATAFUSION-MIGRATION-PHASE2.md](discovery/discovery-DATAFUSION-MIGRATION-PHASE2.md)** (NEW)
- **Purpose**: Strategic blueprint for replacing entire Kionas operator stack with Apache DataFusion
- **Length**: ~30 min read (architectural)
- **Contains**:
  - Executive summary: Why full DataFusion migration makes sense now
  - Rationale: Trade-off analysis (maintenance vs. community-driven solution)
  - Current Kionas architecture (to be replaced)
  - DataFusion architecture (ExecutionPlan, SendableRecordBatchStream, vectorization)
  - Architecture changes by layer (server planner, worker executor, exchange, flight)
  - **Critical section**: Flight-proxy streaming integration (RecordBatch → Arrow IPC)
  - Implementation phases (2a: planning, 2b: execution, 2c: exchange, 2d: testing)
  - Risk assessment (serialization, custom operators, integration)
  - Deployment strategy (preview → canary → full)
- **Audience**: Architects, tech leads, decision-makers
- **Key Insight**: Eliminates need to maintain parallel operators; true streaming native
- **Status**: Complete; ready for Phase 2 kickoff

#### **[plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md](plans/plan-PHASE2-INTEGRATED-DATAFUSION-DISTRIBUTED.md)** (ACTIVE)
- **Purpose**: Integrated distributed data-plane roadmap after Phase 1 signoff
- **Length**: ~35-45 min read
- **Contains**:
  - 2a planner integration + stage extraction integration
  - 2b Arrow Flight worker-to-worker streaming
  - 2c partition routing and fan-out orchestration
  - 2d integration, chaos testing, and performance validation
- **Audience**: Implementation teams, project managers, QA leads
- **Key Insight**: Forward-only distributed architecture; no rollback to monolith

#### **[plan-DATAFUSION-MIGRATION-PHASE2.md](plans/plan-DATAFUSION-MIGRATION-PHASE2.md)** (LEGACY)
- **Purpose**: Detailed Phase 2 implementation roadmap (DataFusion migration)
- **Length**: ~40 min read (comprehensive execution guide)
- **Contains**:
  - 4 Workstreams (22 concrete tasks):
    - WS1: Server ExecutionPlan generation (5 tasks, 2 weeks)
    - WS2: Worker execution + flight streaming adapter (6 tasks, 2 weeks)
    - WS3: Exchange repartitioning + MPMC channels (6 tasks, 1 week)
    - WS4: Testing & performance validation (6 tasks, 1 week)
  - Task summary table with effort estimates (person-days per task)
  - Implementation timeline (detailed 6-week schedule)
  - Success metrics (7 completion criteria for signoff)
  - Key decisions (serialization, planner location, channel capacity)
  - Deployment strategy (phases: preview, canary, full rollout)
- **Audience**: Implementation teams, project managers, QA leads
- **Key Insight**: 26 person-days; fundamental execution model shift; Phase 1 prerequisite
- **Status**: Kept for reference; superseded by integrated Phase 2 plan

### Tier 3c: Opportunity C Archive (Historical) 📦

#### **[OPPORTUNITY-C-SUMMARY.md](OPPORTUNITY-C-SUMMARY.md)**
- **Purpose** (Historical): Initial Opportunity C investigation via DataFusion insights
- **Status**: Superseded by Phase 2 full DataFusion migration plan
- **Reference Value**: Documents streaming exchange exploration; concepts incorporated into Ph2
- **Audience**: Reference only (historical context)

#### **[discovery/discovery-OPPORTUNITY-C-DATAFUSION.md](discovery/discovery-OPPORTUNITY-C-DATAFUSION.md)**
- **Purpose** (Historical): Deep-dive on DataFusion streaming for Opportunity C
- **Status**: Superseded by Phase 2 discovery; concepts and flight integration pattern critical for Ph2
- **Reference Value**: Technical foundation for streaming concepts
- **Audience**: Reference (flight-proxy streaming adapter logic remains relevant)

---

### Tier 4: Progress Tracking 📈

#### **[ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md)**
- **Purpose**: Phase 1 completion tracking and signoff template
- **Length**: ~15 min read
- **Contains**:
  - 27 completion items mapped to workstreams
  - 8 mandatory criteria for signoff gate
  - 5 optional hardening items (deferred to Ph2-5)
  - Workstream summary table
  - Timeline with milestones
  - Pre-signoff gate checklist
  - Evidence artifact locations
  - Approval signatures section
- **Audience**: Project manager, QA lead, architects (signoff authority)
- **Key Use**: Update matrix weekly during Phase 1 implementation
- **Signoff Decision**: "All A1-A8 criteria marked Done" = phase complete

---

## 🎯 Quick Navigation by Role

### 👨‍💼 Project Manager
1. Start: [README.md](README.md) — get context
2. Review: [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) — workstreams & timeline
3. Track: [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) — update weekly

### 🏗️ Architect / Tech Lead
1. Start: [silkroad.md](silkroad.md) — understand strategy
2. Deep-dive: [discovery-PARALLEL-PHASE1.md](discovery/discovery-PARALLEL-PHASE1.md) — technical analysis
3. Review: [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) — implementation approach
4. Design: Create sketches in `sketches/` folder (referenced in plan)

### 👨‍💻 Implementation Developer
1. Start: [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) — tasks & approach
2. Reference: [discovery-PARALLEL-PHASE1.md](discovery/discovery-PARALLEL-PHASE1.md) — background & constraints
3. Code: Implement WS1/WS2 workstreams (scheduler & exchange)
4. Test: Create tests in `tests/` folder

### 🧪 QA / Test Engineer
1. Start: [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) — Workstream 3
2. Reference: E2E scenarios section (4 scenarios listed)
3. Build: Correctness test matrix and regression tests
4. Track: Update matrix with test results

### 📊 Operations / SRE
1. Start: [README.md](README.md) — get overview
2. Review: [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) — Workstream 3 (metrics)
3. Prepare: Runbook for pipelining troubleshooting (to create during Ph1)
4. Monitor: Scheduler concurrency and partition wait time metrics

### 🔮 Future Phase Planners (Phase 2+)
1. Start: [silkroad.md](silkroad.md) — understand full 5-phase vision
2. Research: [OPPORTUNITY-C-SUMMARY.md](OPPORTUNITY-C-SUMMARY.md) — Phase 2 approach
3. Deep-dive: [discovery-OPPORTUNITY-C-DATAFUSION.md](discovery/discovery-OPPORTUNITY-C-DATAFUSION.md) — Technical details
4. Plan: Create Phase 2 implementation plan after Phase 1 review

---

## 🔗 Key Cross-References

### To Existing Work
- **Execution Pipeline**: [roadmaps/discover/execution_pipeline_discovery.md](../discover/execution_pipeline_discovery.md)
- **Prior Execution Phase**: [roadmaps/ROADMAP_EXECUTION_EP1_MATRIX.md](../ROADMAP_EXECUTION_EP1_MATRIX.md)
- **Overall Roadmap**: [roadmaps/ROADMAP2.md](../ROADMAP2.md)

### Code Evidence (From Discovery)
- **Scheduler (Bottleneck)**: [server/src/statement_handler/shared/helpers.rs](server/src/statement_handler/shared/helpers.rs#L270)
- **DAG Compilation**: [server/src/statement_handler/shared/distributed_dag.rs](server/src/statement_handler/shared/distributed_dag.rs#L69)
- **Query Planning**: [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs#L635)
- **Exchange Reading (Worker)**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs#L1678)

---

## 📋 Documentation Checklist

### ✓ Already Created
- [x] Strategic Silk Road overview (5-phase vision)
- [x] Deep-dive discovery (stage extraction + Flight + partition mapping)
- [x] Phase 1 control-plane plan (WS0-WS4)
- [x] Integrated Phase 2 distributed plan
- [x] Phase 1 completion matrix (tracking template)
- [x] README with quick overview

### ⏳ To Create During Phase 1 Implementation

**Design Sketches** (Week 1):
- [ ] `sketches/stage_extraction_design.md` — RepartitionExec boundaries and examples
- [ ] `sketches/dag_scheduler_design.md` — DAG orchestration and failure propagation
- [ ] `sketches/error_handling_design.md` — Error taxonomy and recovery paths

**Tests** (Week 1-4):
- [ ] `tests/stage_extractor_tests.rs` — Stage extraction test suite
- [ ] `tests/dag_scheduler_tests.rs` — DAG scheduling and dependency tests
- [ ] `tests/distributed_e2e_correctness_tests.rs` — End-to-end correctness validation

**Operational Guides** (Week 4):
- [ ] `runbooks/stage_execution_troubleshoot.md` — Control-plane troubleshooting runbook
- [ ] `docs/distributed_control_plane.md` — Technical documentation
- [ ] `benchmarks/phase1_correctness_gate.md` — Validation and acceptance report

---

## 🚀 Getting Started: Next Actions

### Week 1 Tasks

1. **Lock Contracts**
  - Finalize WS0 structs and enums in [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md)
  - Confirm cluster-wide breaking rollout strategy

2. **Kickoff Core Implementation**
  - Start WS1 stage extraction implementation
  - Start WS2 DAG scheduler skeleton

3. **Create Design Sketches**
  - Create [sketches/stage_extraction_design.md](sketches/stage_extraction_design.md)
  - Create [sketches/dag_scheduler_design.md](sketches/dag_scheduler_design.md)
  - Create [sketches/error_handling_design.md](sketches/error_handling_design.md)

4. **Set Up Tracking**
  - Update [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) with owners and due dates
  - Create GitHub issues for WS0-WS4 tasks
  - Set weekly matrix review cadence

---

## 📊 Metrics to Track During Phase 1

| Metric | Target | Baseline |
|--------|--------|----------|
| **Stage Extraction Correctness** | 100% expected stage boundaries | New |
| **Regression Pass Rate** | 100% | — |
| **Code Coverage** (extraction + scheduler) | >85% | — |
| **Scheduler Overhead** | <2% | — |
| **Validation Mismatch Rate** | 0% (byte-identical) | New |
| **Error Scenario Pass Rate** | 100% | — |

---

## 🎓 Key Learnings from Discovery

### Stage Boundaries Drive Distribution
RepartitionExec boundaries provide the most reliable split points for distributed stage tasks.

### Control Plane First Lowers Risk
Stage extraction + DAG execution validated in-memory reduces Phase 2 networking risk.

### Forward-Only Architecture
The redesign is intentionally breaking and avoids legacy fallback to monolithic task execution.

---

## ❓ FAQ

**Q: Why discovery only? No code changes?**  
A: Discovery phase establishes understanding and plan before implementation. Code changes come in Phase 1 implementation (Week 1-5).

**Q: How long is Phase 1?**  
A: 3-4 weeks estimated with aggressive WS overlap.

**Q: Will this break existing queries?**  
A: Query semantics must remain correct, but infrastructure contracts are intentionally breaking.

**Q: What if Phase 1 doesn't achieve 20-40% latency improvement?**  
A: Phase 1 is a correctness gate, not a latency optimization phase.

**Q: Can Phase 2 start before Phase 1 ends?**  
A: Planning yes; implementation starts after Phase 1 signoff criteria are met.

**Q: What about worker-side changes?**  
A: Worker data movement changes are Phase 2 scope (Flight + routing).

---

## 📞 Contact & Support

For questions or clarifications:

| Question | Reference |
|----------|-----------|
| "What's the overall vision?" | [silkroad.md](silkroad.md) |
| "Why is parallelism needed?" | [README.md](README.md) Executive Summary |
| "How will we implement it?" | [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) |
| "What exactly is the architecture pivot?" | [discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md](discovery/discovery-DEEP-DIVE-STAGE-EXTRACTION-FLIGHT-MAPPING.md) |
| "Where do I track progress?" | [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) |
| "What are the risks?" | [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md#risks-and-mitigations) |

---

## 📈 Document Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Mar 22, 2026 | Initial creation: discovery & planning complete |
| 1.1 | Mar 23, 2026 | Aggressive redesign alignment: control-plane-first Phase 1 + integrated Phase 2 |

---

**Ready for**: Phase 1 Implementation Kickoff  
**Next Step**: Lock WS0 contract and start WS1/WS2  
**Timeline**: Week 1 kickoff → Week 4 phase signoff
