## Plan: Roadmap 2 - Query Cycle Expansion

Roadmap 2 continues the validated loop client -> server -> worker -> flight -> client, now focused on incremental query semantics and planner maturity. The goal is to deliver features vertically through the whole loop while preserving stable contracts and phase-gated signoff.

**Steps**
1. Phase 1: Replan roadmap and setup
1.1 Create a dependency-aware roadmap file for the next execution cycle.
1.2 Define phase boundaries for query semantics, planner hardening, and mutation deferral.
1.3 Establish matrix gating requirements for every phase before signoff.
[X] DONE

2. Phase 2: Discovery and architecture baseline
2.1 Capture blockers and requirements for ORDER BY, LIMIT, window functions, and QUALIFY.
2.2 Capture planner and execution constraints across parser, logical model, physical model, validator, and worker runtime.
2.3 List long-horizon planner strategy components (statistics, indexing, cost model, optimizer passes) as discovery items only.
2.4 Record architecture decisions and rationale as first-class artifacts for implementation phases.

### Discovery (Phase 2)
- Main blocker: advanced operators are represented in planning catalogs but constrained by current validation and/or execution capability.
- Technical deep dive: [roadmaps/dive-in/roadmap2_divein_phase2_main_blocker.md](roadmaps/dive-in/roadmap2_divein_phase2_main_blocker.md)
- ORDER BY gap: accepted at query surface, but not yet enforced consistently in execution output ordering.
- Window/QUALIFY gap: requires frame semantics and expression/runtime support not yet active in the current execution path.
- UPDATE/DELETE gap: intentionally deferred until mutation and transaction criteria are explicitly gated.

### Planner Strategy Components (Discovery Only)
- Statistics collection and persistence.
- Index awareness and index-driven access paths.
- Cost model scaffolding and heuristic fallback.
- Optimizer pipeline boundaries and explainability hooks.

### Architecture Decisions (Phase 2)
- Vertical-slice delivery: each feature ships across client/server/worker/flight as one slice.
- Scope control: phases 7-11 exclude UPDATE/DELETE and focus on query semantics plus planner hardening.
- Sequencing: ORDER BY before window/QUALIFY to deliver fast correctness wins and de-risk later slices.
- Closure gate: no phase signoff without a completed matrix with mandatory criteria marked Done.

3. Phase 3: ORDER BY vertical slice
3.1 Preserve ORDER BY semantics from parse/model/plan to worker execution.
3.2 Ensure ordered output correctness for deterministic result validation.
3.3 Add explain and diagnostics to show ordering decisions.
[X] DONE

4. Phase 4: LIMIT and semantics hardening
4.1 Add LIMIT support in the same end-to-end path.
4.2 Add regression coverage for ORDER BY + LIMIT interactions.
4.3 Harden validation and error taxonomy for unsupported query shapes.
4.4 When possible use different mod or files. Create new ones often. dont concentrate too much logic on the same file. 

[DISCOVER INTERMISSION]
Better sql predicates & operators to support next phases(planner, executor)

5. Phase 5: Window foundation
5.1 Introduce minimal supported window semantics with explicit constraints.
5.2 Add planner/runtime contracts for partition and frame metadata.
5.3 Add observability hooks for window execution diagnostics.

6. Phase 6: QUALIFY
6.1 Add QUALIFY evaluation on top of window outputs.
6.2 Ensure stable filtering semantics and actionable validation errors.
6.3 Add explainability for post-window filtering behavior.

7. Phase 7: Query part II
7.1 Expand supported query semantics iteratively behind capability gates.
7.2 Keep backward compatibility on response and handle contracts.

8. Phase 8: Query part III
8.1 Complete remaining targeted query semantics for this roadmap line.
8.2 Resolve deferred hardening items from prior query phases.

9. Phase 9: Reliability and observability hardening
9.1 Add lifecycle telemetry and audit traces across dispatch and retrieval.
9.2 Add resilience tests for partial failures and retries.

10. Phase 10: Server session expansion
10.1 Expand session context to support richer query-state requirements.
10.2 Keep auth and scope contracts stable with bounded compatibility changes.

11. Phase 11: Transition gate to mutation roadmap
11.1 Define explicit prerequisites for UPDATE/DELETE track.
11.2 Produce a handoff matrix for mutation phases.

**Parallelism and Dependencies**
1. Phase 1 blocks Phase 2.
2. Phase 2 blocks Phases 3 through 6 because model and architecture decisions must be frozen first.
3. Phase 3 blocks Phase 4.
4. Phase 5 blocks Phase 6.
5. Phases 7 and 8 can overlap partially after Phase 6 with strict matrix-gated milestones.
6. Phase 11 depends on completion outcomes from Phases 7 through 10.

**Critical Path**
1. Phase 1 -> Phase 2 -> Phase 3 -> Phase 4 -> Phase 5 -> Phase 6.

**Contributor Tracks**
1. Planner/model track: query model, logical/physical plan contracts, validation gates.
2. Worker execution track: runtime operators, deterministic output semantics, diagnostics.
3. Flight/proxy track: handle contracts, retrieval compatibility, metadata continuity.
4. Reliability track: telemetry, audits, resilience and regression guardrails.

**Relevant Files**
- c:/code/kionas/kionas/src/sql/query_model.rs - query-model extraction and canonical envelope.
- c:/code/kionas/kionas/src/planner/logical_plan.rs - logical plan representation.
- c:/code/kionas/kionas/src/planner/physical_plan.rs - physical operator catalog.
- c:/code/kionas/kionas/src/planner/physical_validate.rs - operator capability and validation gates.
- c:/code/kionas/server/src/statement_handler/query_select.rs - query dispatch and stage preparation.
- c:/code/kionas/worker/src/services/query_execution.rs - worker execution pipeline.
- c:/code/kionas/worker/src/flight/server.rs - worker flight retrieval path.
- c:/code/kionas/flight_proxy/src/main.rs - proxy routing and metadata forwarding.
- c:/code/kionas/client/src/main.rs - handle decode and retrieval path.

**Verification**
1. Keep phase matrices as hard closure gates for each phase signoff.
2. For each semantics slice, validate end-to-end behavior with deterministic expected output.
3. Maintain repository quality gates for implementation phases.
4. Track deferred items explicitly in each phase matrix.

**Decisions**
- Start with ORDER BY as the first semantics implementation slice.
- Keep phases 7-11 focused on query semantics and planner hardening.
- Defer UPDATE/DELETE to a dedicated mutation roadmap after transition gating.
