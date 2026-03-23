# PARALLEL Silk Road: Complete Artifact Index

**Status**: ✓ Discovery & Planning Complete | No Code Changes (Discovery Phase Only)

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
    └── plan-PARALLEL-PHASE1.md                 ← Implementation roadmap: 4 workstreams, 16 tasks

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

#### **[discovery-PARALLEL-PHASE1.md](discovery/discovery-PARALLEL-PHASE1.md)**
- **Purpose**: Detailed technical investigation of current bottleneck and parallelism opportunities
- **Length**: ~20 min read (technical)
- **Contains**:
  - Current sequential scheduler bottleneck analysis
  - Why parallelism matters (quantified)
  - Executive summary with visual timeline comparison
  - 5 parallelism opportunities identified (A-E)
  - Architectural components mapped with code references
  - Current execution patterns analyzed
  - 12 constraints preventing parallelism (organized by layer)
  - Architectural decision points for design
  - All claims linked to source code evidence
- **Audience**: Architects, leads, implementation developers
- **Key Insight**: `run_stage_groups_for_input()` in helpers.rs is primary bottleneck

---

#### **[plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md)**
- **Purpose**: Detailed Phase 1 implementation roadmap
- **Length**: ~30 min read (comprehensive)
- **Contains**:
  - Phase 1 goal (20-40% latency reduction)
  - 4 workstreams with objectives, approach, tasks:
    - WS1: Async Scheduler (5 tasks)
    - WS2: Partial Exchange (5 tasks)
    - WS3: Correctness Testing (6 tasks)
    - WS4: Error Handling (4 tasks)
  - Canonical mode decision table (how different query types behave)
  - Criteria-to-workstream traceability matrix
  - Sequence, dependencies, and milestones
  - 6 identified risks with mitigations
  - 4 E2E scenarios with expected flows
  - Implementation guardrails
  - Success criteria checklist
- **Audience**: Implementation teams, project managers, QA leads
- **Key Insight**: 16 concrete tasks; 5-week timeline; parallel workstreams

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

#### **[plan-DATAFUSION-MIGRATION-PHASE2.md](plans/plan-DATAFUSION-MIGRATION-PHASE2.md)** (NEW)
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
- **Status**: Ready for Phase 2 implementation kickoff

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
- [x] Phase 1 discovery document (technical analysis)
- [x] Phase 1 detailed plan (4 workstreams, 16 tasks)
- [x] Phase 1 completion matrix (tracking template)
- [x] README with quick overview

### ⏳ To Create During Phase 1 Implementation

**Design Sketches** (Week 1):
- [ ] `sketches/async_scheduler_design.md` — Scheduler architecture with diagrams
- [ ] `sketches/partition_checkpoint_design.md` — Checkpoint model and state machine
- [ ] `sketches/error_handling_design.md` — Error taxonomy and recovery paths

**Tests** (Week 1-4):
- [ ] `tests/scheduler_correctness_tests.rs` — Unit tests for scheduler
- [ ] `tests/exchange_pipelining_tests.rs` — Exchange partial-read tests
- [ ] `tests/e2e_correctness_tests.rs` — End-to-end correctness validation

**Operational Guides** (Week 4-5):
- [ ] `runbooks/pipelining_troubleshoot.md` — Troubleshooting runbook
- [ ] `docs/pipelined_execution.md` — Technical documentation
- [ ] `benchmarks/pipelined_e2e_latency.md` — Performance benchmark report

---

## 🚀 Getting Started: Next Actions

### Week 1 Tasks

1. **Review Artifacts** (30 min)
   - Read [README.md](README.md) for overview
   - Skim [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) for workstreams

2. **Team Alignment** (1-2 hours)
   - Schedule design review meeting with architects/devs
   - Review [discovery-PARALLEL-PHASE1.md](discovery/discovery-PARALLEL-PHASE1.md) together
   - Align on key decisions (async patterns, error handling)

3. **Design Sketches** (4-8 hours)
   - Architect creates [sketches/async_scheduler_design.md](sketches/async_scheduler_design.md)
   - Architect creates [sketches/partition_checkpoint_design.md](sketches/partition_checkpoint_design.md)
   - Architect creates [sketches/error_handling_design.md](sketches/error_handling_design.md)

4. **Task Assignment**
   - Assign WS1 tasks (scheduler) to 1-2 devs
   - Assign WS2 tasks (exchange) to 1-2 devs
   - Assign WS3 tasks (testing) to QA lead
   - Assign WS4 tasks (error handling) to lead dev

5. **Set Up Tracking**
   - Update [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) with owner names
   - Create GitHub issues for each task (16 total)
   - Set up weekly sync to review matrix progress

---

## 📊 Metrics to Track During Phase 1

| Metric | Target | Baseline |
|--------|--------|----------|
| **Query Latency** (3-stage agg) | -20% to -40% | 70ms → 42-56ms |
| **Regression Pass Rate** | 100% | — |
| **Code Coverage** (scheduler) | >85% | — |
| **Scheduler Overhead** | <2% | — |
| **Error Scenario Pass Rate** | 100% | — |

---

## 🎓 Key Learnings from Discovery

### Bottleneck #1: Sequential Stage Scheduler
**Current**: `while !done { stage = ready.pop(); run_partitions(stage); BLOCK_WAIT_ALL; }`  
**Future**: Async futures with concurrent independent stages + partial upstream awareness

### Opportunity #1: Partition-Level Checkpoints
**Current**: Downstream waits for ALL upstream partitions before starting  
**Future**: Downstream can start reading partition 0 while partition 1-3 still processing

### Constraint #1: All-or-Nothing Semantics
**Current** (Good): Any failure fails entire stage (simple, correct)  
**Future** (Maintain): Still per-stage; but partial progress possible without partial success

### Scale: Multi-Stage Queries
**3-Stage Query** shows best opportunity: cumulative stages → overlapped windows

---

## ❓ FAQ

**Q: Why discovery only? No code changes?**  
A: Discovery phase establishes understanding and plan before implementation. Code changes come in Phase 1 implementation (Week 1-5).

**Q: How long is Phase 1?**  
A: ~5 weeks estimated. Workstreams can be parallelized; dependencies tracked in plan.

**Q: Will this break existing queries?**  
A: No. Guardrail: all existing query patterns must pass regression tests unchanged.

**Q: What if Phase 1 doesn't achieve 20-40% latency improvement?**  
A: Still valuable (foundation); opportunity for tuning in Phase 2-3. Conservative fallback to sequential if needed.

**Q: Can Phase 2 start before Phase 1 ends?**  
A: Planning yes; implementation no. Phase 1 core (scheduler + checkpoints) must be done first.

**Q: What about worker-side changes?**  
A: Out of scope for Phase 1. Focus: server scheduler + exchange model only.

---

## 📞 Contact & Support

For questions or clarifications:

| Question | Reference |
|----------|-----------|
| "What's the overall vision?" | [silkroad.md](silkroad.md) |
| "Why is parallelism needed?" | [README.md](README.md) Executive Summary |
| "How will we implement it?" | [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md) |
| "What exactly is the bottleneck?" | [discovery-PARALLEL-PHASE1.md](discovery/discovery-PARALLEL-PHASE1.md) |
| "Where do I track progress?" | [ROADMAP_PARALLEL_PHASE1_MATRIX.md](../../ROADMAP_PARALLEL_PHASE1_MATRIX.md) |
| "What are the risks?" | [plan-PARALLEL-PHASE1.md](plans/plan-PARALLEL-PHASE1.md#risks-and-mitigations) |

---

## 📈 Document Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Mar 22, 2026 | Initial creation: discovery & planning complete |

---

**Ready for**: Phase 1 Implementation Kickoff  
**Next Step**: Schedule design review meeting  
**Timeline**: Week 1 kickoff → Week 5 phase signoff
