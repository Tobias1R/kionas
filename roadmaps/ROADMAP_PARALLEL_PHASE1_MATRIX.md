# ROADMAP_PARALLEL_PHASE1_MATRIX

## Phase 1: Pipelined Stage Scheduler & Partial Exchange

### Scope
Implement async stage scheduler supporting concurrent dispatch of independent stages and enable downstream stages to consume upstream exchange data as it becomes available (partition-level checkpoints). This establishes the foundation for overlapping multi-stage query execution.

**Baseline**: Sequential stage scheduler with all-or-nothing completion  
**Target**: Pipelined scheduler with early downstream startup and 20-40% latency reduction  
**Duration**: ~5 weeks  
**Phases Reference**: [plan-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PARALLEL-PHASE1.md)

---

## Completion Matrix

| # | Item | Workstream | Status | Evidence | Notes |
|---|------|-----------|--------|----------|-------|
| 1 | Async scheduler architecture designed | WS1 | Not Started | Design doc: [sketches/async_scheduler_design.md](roadmaps/SILK_ROAD/PARALLEL/sketches/async_scheduler_design.md) | Topological sort + ready queue + async dispatch |
| 2 | Topological sort implemented for DAG | WS1 | Not Started | PR: [xxx](mailto:xxx) | Validates dependency graph; no cycles |
| 3 | Async scheduler main loop refactored | WS1 | Not Started | PR: [xxx](mailto:xxx) | Replaces while loop with futures; concurrent independent stages |
| 4 | Scheduler state transitions tracked | WS1 | Not Started | PR: [xxx](mailto:xxx) | Pending → Ready → Dispatched → Executing → Complete |
| 5 | Stage dispatch concurrency metrics added | WS1 | Not Started | PR: [xxx](mailto:xxx) | `scheduler_stage_dispatch_concurrency` histogram |
| 6 | Partition checkpoint design completed | WS2 | Not Started | Design doc: [sketches/partition_checkpoint_design.md](roadmaps/SILK_ROAD/PARALLEL/sketches/partition_checkpoint_design.md) | Defines "partition ready" semantics |
| 7 | Partition availability polling implemented | WS2 | Not Started | PR: [xxx](mailto:xxx) | Downstream checks S3 for upstream partition existence |
| 8 | Downstream task launch criteria updated | WS2 | Not Started | PR: [xxx](mailto:xxx) | Can launch when 1+ upstream partition available |
| 9 | Exchange reader handles partial consumption | WS2 | Not Started | PR: [xxx](mailto:xxx) | Graceful retry if partition not yet available |
| 10 | Partition checkpoint observability added | WS2 | Not Started | PR: [xxx](mailto:xxx) | Logs, spans, metrics for wait times |
| 11 | Correctness test matrix defined | WS3 | Not Started | Test plan: [tests/correctness_test_matrix.md](roadmaps/SILK_ROAD/PARALLEL/tests/correctness_test_matrix.md) | Covers: single-stage, multi-stage sequential, fanout, broadcast |
| 12 | Correctness baseline tests implemented | WS3 | Not Started | PR: [xxx](mailto:xxx) | Cross-check pipelined results vs sequential baseline |
| 13 | Single-stage query correctness verified | WS3 | Not Started | CI: [xxx](mailto:xxx) | No changes expected (not multi-stage) |
| 14 | Sequential 2-stage query tested | WS3 | Not Started | CI: [xxx](mailto:xxx) | Stage 1 can launch early; results identical to baseline |
| 15 | Multi-level 3-stage aggregation tested | WS3 | Not Started | CI: [xxx](mailto:xxx) | Longest cumulative latency gains from pipelining |
| 16 | Broadcast join scenario tested | WS3 | Not Started | CI: [xxx](mailto:xxx) | Fanout pattern with early read |
| 17 | Partition failure handling tested | WS3 | Not Started | CI: [xxx](mailto:xxx) | Downstream fails gracefully if upstream partition unavailable |
| 18 | Regression test suite passes | WS3 | Not Started | CI: [xxx](mailto:xxx) | All existing query patterns still work unchanged |
| 19 | Error handling design completed | WS4 | Not Started | Design doc: [sketches/error_handling_design.md](roadmaps/SILK_ROAD/PARALLEL/sketches/error_handling_design.md) | Error taxonomy; recovery strategies |
| 20 | Dependency validation pre-flight checks added | WS4 | Not Started | PR: [xxx](mailto:xxx) | No cycle detection; all dependencies explicit |
| 21 | Partition timeout handling implemented | WS4 | Not Started | PR: [xxx](mailto:xxx) | Configurable timeout; structured error if partition unavailable |
| 22 | Structured error types for scheduler | WS4 | Not Started | PR: [xxx](mailto:xxx) | `SchedulerError::DependencyNotMet`, `SchedulerError::PartitionTimeout` |
| 23 | Error diagnostics and runbooks provided | WS4 | Not Started | Doc: [runbooks/pipelining_troubleshoot.md](roadmaps/SILK_ROAD/PARALLEL/runbooks/pipelining_troubleshoot.md) | Clear remediation paths |
| 24 | Fallback to sequential if safety violated | WS4 | Not Started | PR: [xxx](mailto:xxx) | Conservative: fail or revert if pipelining invariants broken |
| 25 | E2E pipelined query (3-stage) confirms latency reduction | Integration | Not Started | Perf test: [benchmarks/pipelined_e2e_latency.md](roadmaps/SILK_ROAD/PARALLEL/benchmarks/pipelined_e2e_latency.md) | Measure: 20-40% reduction |
| 26 | Backwards compatibility verified | Integration | Not Started | CI: [xxx](mailto:xxx) | Older clients/workers don't break |
| 27 | Documentation: "Pipelined Execution in Kionas" | Integration | Not Started | Doc: [docs/pipelined_execution.md](roadmaps/SILK_ROAD/PARALLEL/docs/pipelined_execution.md) | Design, configuration, troubleshooting |

---

## Mandatory Criteria (Must be Done for Phase 1 Signoff)

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| A1 | Async scheduler refactored and tested | Not Started | PR + CI pass |
| A2 | Partition checkpoint logic implemented | Not Started | PR + functional tests |
| A3 | Downstream early launch supported | Not Started | E2E test demonstrates <br /> stage 1 begins before stage 0 complete |
| A4 | Correctness: all existing query patterns pass | Not Started | Regression test suite 100% pass |
| A5 | Multi-stage query latency reduced 20-40% | Not Started | Perf benchmark report |
| A6 | Error handling graceful for all failure modes | Not Started | Error scenario test matrix pass |
| A7 | Observability metrics in place | Not Started | Metrics available in traces/logs |
| A8 | No breaking changes | Not Started | Backwards compatibility test pass |

**Signoff Criteria**: All A1-A8 marked **Done** with concrete evidence.

---

## Optional Hardening (Non-Blocking Deferred Items)

| # | Item | Rationale | Phase |
|---|------|-----------|-------|
| O1 | Advanced straggler detection (percentile-based) | Not needed for Phase 1; can defer to Phase 3 | Ph3 |
| O2 | Streaming exchange (micro-batch buffers) | Larger scope; Phase 2 focus | Ph2 |
| O3 | Resource-aware backpressure | Requires worker metrics; Phase 2 dependency | Ph2 |
| O4 | DAG fusion optimization | Separate concern; Phase 4 focus | Ph4 |
| O5 | Multi-query fairness scheduling | Advanced feature; Phase 5 focus | Ph5 |

**Phase 1 Focus**: Core pipelining foundation. Optional items deferred with clear phase assignments.

---

## Workstream Summary

| Workstream | Objective | Tasks | Deliverables | Owner |
|-----------|-----------|-------|--------------|-------|
| **WS1: Async Scheduler** | Replace sequential loop with concurrent dispatch | T1.1–T1.5 | Refactored scheduler, metrics, tests | Dev |
| **WS2: Partial Exchange** | Enable early reads of available partitions | T2.1–T2.5 | Checkpoint logic, conditional reads, E2E test | Dev |
| **WS3: Correctness** | Verify all query patterns work correctly | T3.1–T3.6 | Test suite, regression results, docs | QA |
| **WS4: Error Handling** | Gracefully handle dependencies and timeouts | T4.1–T4.4 | Error taxonomy, recovery, runbook | Dev |

---

## Phase 1 Timeline & Milestones

| Week | Milestones | Status |
|------|-----------|--------|
| **1** | Design review passed; WS1 core kickoff | Not Started |
| **2** | WS1 main loop refactored; WS2 checkpoint logic complete | Not Started |
| **3** | Correctness baseline established; regression pass | Not Started |
| **4** | Error handling complete; E2E pipelined query runs | Not Started |
| **5** | Phase 1 matrix complete; all A1-A8 Done; phase signoff | Not Started |

---

## Gate Checklist (Before Phase 1 Signoff)

### Mandatory Gate Items
- [ ] All A1-A8 criteria marked **Done** (not Deferred/Blocked)
- [ ] Each Done item includes concrete evidence reference (PR, test result, metric)
- [ ] Regression test suite: 100% pass rate
- [ ] E2E pipelined query demonstrates 20-40% latency reduction
- [ ] Error handling scenarios tested and passing
- [ ] Code review approved by architecture owners
- [ ] Documentation (runbook + design) complete and linked

### Recommended Gate Items
- [ ] Performance report showing baseline vs pipelined latency
- [ ] Observability dashboard for scheduler concurrency
- [ ] Team training / knowledge transfer complete
- [ ] Backwards compatibility verified with older server/worker versions

### Blockers for Signoff (None Expected Phase 1)
- [ ] (None; all blockers should be resolved or explicitly Deferred with rationale)

---

## Signoff Decision

| Item | Decision | Justification |
|------|----------|---------------|
| **Phase 1 Status** | Pending | Awaiting implementation completion and mandatory gate pass |
| **Proceed to Phase 2?** | TBD | Conditional on Phase 1 completion and latency targets met |
| **Go-Live Date** | TBD | Post-signoff; include in next release cycle |
| **Known Risks** | See [plan-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PARALLEL-PHASE1.md#risks-and-mitigations) | Concurrency bugs, scheduler overhead, exchange contention |
| **Rollback Plan** | Fallback to sequential scheduler if critical bugs discovered | Conservative: safety over performance |

---

## Evidence & Artifact Locations

| Artifact | Path | Status |
|----------|------|--------|
| Discovery Doc | [discovery-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/discovery/discovery-PARALLEL-PHASE1.md) | ✓ Complete |
| Plan Doc | [plan-PARALLEL-PHASE1.md](roadmaps/SILK_ROAD/PARALLEL/plans/plan-PARALLEL-PHASE1.md) | ✓ Complete |
| Design Sketches | [sketches/](roadmaps/SILK_ROAD/PARALLEL/sketches/) | ⏳ To Create |
| Test Suite | [tests/](roadmaps/SILK_ROAD/PARALLEL/tests/) | ⏳ To Create |
| Implementation PRs | CI/CD | ⏳ To Create |
| Performance Report | [benchmarks/](roadmaps/SILK_ROAD/PARALLEL/benchmarks/) | ⏳ To Create |
| Runbook | [runbooks/pipelining_troubleshoot.md](roadmaps/SILK_ROAD/PARALLEL/runbooks/pipelining_troubleshoot.md) | ⏳ To Create |
| Ops Documentation | [docs/pipelined_execution.md](roadmaps/SILK_ROAD/PARALLEL/docs/pipelined_execution.md) | ⏳ To Create |

---

## Phase 1 Signoff Approval

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Project Manager | — | — | — |
| Architect | — | — | — |
| Dev Lead | — | — | — |
| QA Lead | — | — | — |

---

## Notes & Comments

- Phase 1 is foundational; all future parallelism phases depend on successful scheduler refactoring
- Conservative approach: fallback to sequential if pipelining safety violated
- Metrics and observability critical for performance validation
- Backwards compatibility must be maintained; no client protocol changes
- Phase 2 planning can begin once Phase 1 is 50% complete

---

**Matrix Version**: 1.0  
**Created**: March 2026  
**Last Updated**: —  
**Next Review**: After WS1 completion (Week 2)
