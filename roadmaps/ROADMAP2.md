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
[X] DONE

[DISCOVER INTERMISSION]
- Who we should implement first? GROUP or JOIN?
- How is our planner current situation to start breaking down SCAN+FILTER+JOIN
- Can JOIN be implement vertically now?
[X] DONE

### Intermission Resolution
- Decision: implement JOIN before GROUP.
- Planner readiness: SCAN and FILTER are vertically ready; JOIN is cataloged but still blocked by model extraction, logical representation, physical translation, validation gates, and worker execution.
- Vertical feasibility: JOIN can be implemented now as a constrained vertical slice (INNER equi-join first), followed by GROUP as a separate phase on top of the JOIN-ready planner/runtime foundation.
- Technical deep dive: [roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md](roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md)
- Closure matrix: [roadmaps/ROADMAP2_INTERMISSION_4-5_MATRIX.md](roadmaps/ROADMAP2_INTERMISSION_4-5_MATRIX.md)

5. Phase 5: JOIN foundation
5.1 Introduce minimal JOIN semantics with explicit constraints (INNER equi-join first).
5.2 Add planner/runtime contracts for multi-relation modeling and JOIN operator execution.
5.3 Add explain and diagnostics visibility for JOIN strategy and predicates.
[X] DONE

[INTERMISSION CLEANUPS - WORKER]
- Breakdown the modules worker\src\services\query_execution.rs worker\src\services\query_join.rs and worker\src\services\query.rs
Lets discuss a new place for this code. They shouldnt be here in services

### Intermission Cleanup Resolution (Worker)
- Decision: move query runtime ownership from `services` to a new top-level `execution` module under worker.
- Initial implementation started with execution scaffold and first module moves (query/join).
- Technical deep dive: [roadmaps/dive-in/roadmap2_divein_intermission_worker_cleanup.md](roadmaps/dive-in/roadmap2_divein_intermission_worker_cleanup.md)
- Closure matrix: [roadmaps/ROADMAP2_INTERMISSION_WORKER_CLEANUP_MATRIX.md](roadmaps/ROADMAP2_INTERMISSION_WORKER_CLEANUP_MATRIX.md)
[X] DONE


6. Phase 6: GROUP foundation
6.1 Introduce GROUP semantics with explicit constraints and deterministic aggregate behavior.
6.2 Add planner/runtime contracts for aggregate partial/final metadata.
6.3 Add observability hooks for aggregation execution diagnostics.

### Phase 6 Implementation
- Plan: [roadmaps/ROADMAP2/plans/plan-roadmap2Phase6GroupFoundation.prompt.md](roadmaps/ROADMAP2/plans/plan-roadmap2Phase6GroupFoundation.prompt.md)
- Closure matrix: [roadmaps/ROADMAP2_PHASE6_MATRIX.md](roadmaps/ROADMAP2_PHASE6_MATRIX.md)
[X] DONE

## SILK ROAD
[roadmaps/SILK_ROAD/silkroad.md](roadmaps/SILK_ROAD/silkroad.md)
[ROAD TO INDEXING]
[ROAD TO CONSTRAINTS]
[RICH DATATYPES]
[GARBAGE COLLECTION]: Introducing the Janitor - a background process for cleaning up old metadata and optimizing storage.

### Current SILK ROAD Artifacts
- INDEXING: [roadmaps/SILK_ROAD/INDEXING/ROADMAP_INDEXING.md](roadmaps/SILK_ROAD/INDEXING/ROADMAP_INDEXING.md)
- CONSTRAINTS: [roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md](roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md)
- CONSTRAINTS FOUNDATION discovery: [roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md)
- CONSTRAINTS FOUNDATION matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)
- UI: [roadmaps/SILK_ROAD/UI/ROADMAP_UI.md](roadmaps/SILK_ROAD/UI/ROADMAP_UI.md)
- UI FOUNDATION discovery: [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md)
- UI FOUNDATION plan: [roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md)
- UI FOUNDATION matrix: [roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md)

[INTERMISSION Code Cleanup - Worker]
- Decision: move DDL task handlers from `worker/src/services` to `worker/src/transactions/ddl` to align ownership with transaction execution.
- Evidence (new module):
	- [worker/src/transactions/ddl/mod.rs](worker/src/transactions/ddl/mod.rs)
	- [worker/src/transactions/ddl/create_database.rs](worker/src/transactions/ddl/create_database.rs)
	- [worker/src/transactions/ddl/create_schema.rs](worker/src/transactions/ddl/create_schema.rs)
	- [worker/src/transactions/ddl/create_table.rs](worker/src/transactions/ddl/create_table.rs)
- Evidence (dispatch rewiring): [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs)
- Legacy modules removed from services:
	- worker/src/services/create_database.rs
	- worker/src/services/create_schema.rs
	- worker/src/services/create_table.rs
[X] DONE

[INTERMISSION Code Cleanup - Server]
- src/core seems to be dead code. Confirm and remove if so.
- Move statement-specific handlers from `server/src/statement_handler` to more focused modules (e.g. `server/src/statement_handler/ddl`, `server/src/statement_handler/dml`).
- move all tests to server/src/tests/

### Intermission Cleanup Resolution (Server)
- Decision: remove dead `server/src/core`, group statement handlers by ownership, and move inline tests into `server/src/tests`.
- Evidence (grouped handler modules):
	- [server/src/statement_handler/ddl/mod.rs](server/src/statement_handler/ddl/mod.rs)
	- [server/src/statement_handler/dml/mod.rs](server/src/statement_handler/dml/mod.rs)
	- [server/src/statement_handler/query/mod.rs](server/src/statement_handler/query/mod.rs)
	- [server/src/statement_handler/shared/mod.rs](server/src/statement_handler/shared/mod.rs)
	- [server/src/statement_handler/utility/mod.rs](server/src/statement_handler/utility/mod.rs)
- Evidence (dispatch rewiring): [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs)
- Evidence (tests moved):
	- [server/src/tests/tasks_mod_tests.rs](server/src/tests/tasks_mod_tests.rs)
	- [server/src/tests/services_warehouse_service_server_tests.rs](server/src/tests/services_warehouse_service_server_tests.rs)
	- [server/src/tests/statement_handler_shared_distributed_dag_tests.rs](server/src/tests/statement_handler_shared_distributed_dag_tests.rs)
	- [server/src/tests/statement_handler_query_select_tests.rs](server/src/tests/statement_handler_query_select_tests.rs)
- Closure matrix: [roadmaps/ROADMAP2_INTERMISSION_SERVER_CLEANUP_MATRIX.md](roadmaps/ROADMAP2_INTERMISSION_SERVER_CLEANUP_MATRIX.md)
[X] DONE

[INTERMISSION Code Cleanup - Worker - Tests]
- move all tests to worker/src/tests/

### Intermission Cleanup Resolution (Worker - Tests)
- Decision: relocate inline worker test modules into `worker/src/tests` with path-based test module wiring.
- Evidence (tests moved):
	- [worker/src/tests/authz_tests.rs](worker/src/tests/authz_tests.rs)
	- [worker/src/tests/storage_exchange_tests.rs](worker/src/tests/storage_exchange_tests.rs)
	- [worker/src/tests/execution_query_tests.rs](worker/src/tests/execution_query_tests.rs)
	- [worker/src/tests/execution_pipeline_tests.rs](worker/src/tests/execution_pipeline_tests.rs)
	- [worker/src/tests/execution_aggregate_mod_tests.rs](worker/src/tests/execution_aggregate_mod_tests.rs)
	- [worker/src/tests/flight_server_tests.rs](worker/src/tests/flight_server_tests.rs)
	- [worker/src/tests/services_query_execution_tests.rs](worker/src/tests/services_query_execution_tests.rs)
	- [worker/src/tests/transactions_maestro_uri_tests.rs](worker/src/tests/transactions_maestro_uri_tests.rs)
	- [worker/src/tests/transactions_maestro_insert_tests.rs](worker/src/tests/transactions_maestro_insert_tests.rs)
- Evidence (source rewiring):
	- [worker/src/authz.rs](worker/src/authz.rs)
	- [worker/src/storage/exchange.rs](worker/src/storage/exchange.rs)
	- [worker/src/execution/query.rs](worker/src/execution/query.rs)
	- [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs)
	- [worker/src/execution/aggregate/mod.rs](worker/src/execution/aggregate/mod.rs)
	- [worker/src/flight/server.rs](worker/src/flight/server.rs)
	- [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)
	- [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs)
- Validation note: quality gates passed with strict linting and compile checks (`cargo fmt --all`, `cargo check -p worker`, `cargo clippy -p worker --all-targets --all-features -- -D warnings`) after resolving dependency-side clippy blockers.
- Closure matrix: [roadmaps/ROADMAP2_INTERMISSION_WORKER_TESTS_CLEANUP_MATRIX.md](roadmaps/ROADMAP2_INTERMISSION_WORKER_TESTS_CLEANUP_MATRIX.md)
[X] DONE

[INTERMISSION Code Cleanup - Kionas] - Deferred
- Refactor planner modules to group by logical/physical and operator categories.
- move all tests to kionas/src/tests/



7. Phase 7: Window foundation
7.1 Introduce minimal supported window semantics with explicit constraints.
7.2 Add planner/runtime contracts for partition and frame metadata.
7.3 Add observability hooks for window execution diagnostics.

8. Phase 8: QUALIFY
8.1 Add QUALIFY evaluation on top of window outputs.
8.2 Ensure stable filtering semantics and actionable validation errors.
8.3 Add explainability for post-window filtering behavior.

9. Phase 9: Query part II
9.1 Expand supported query semantics iteratively behind capability gates.
9.2 Keep backward compatibility on response and handle contracts.

10. Phase 10: Query part III
10.1 Complete remaining targeted query semantics for this roadmap line.
10.2 Resolve deferred hardening items from prior query phases.

11. Phase 11: Reliability and observability hardening
11.1 Add lifecycle telemetry and audit traces across dispatch and retrieval.
11.2 Add resilience tests for partial failures and retries.

12. Phase 12: Server session expansion
12.1 Expand session context to support richer query-state requirements.
12.2 Keep auth and scope contracts stable with bounded compatibility changes.

13. Phase 13: Transition gate to mutation roadmap
13.1 Define explicit prerequisites for UPDATE/DELETE track.
13.2 Produce a handoff matrix for mutation phases.

**Parallelism and Dependencies**
1. Phase 1 blocks Phase 2.
2. Phase 2 blocks Phases 3 through 8 because model and architecture decisions must be frozen first.
3. Phase 3 blocks Phase 4.
4. Phase 4 blocks Phase 5.
5. Phase 5 blocks Phase 6.
6. Phase 7 blocks Phase 8.
7. Phases 9 and 10 can overlap partially after Phase 8 with strict matrix-gated milestones.
8. Phase 13 depends on completion outcomes from Phases 9 through 12.

**Critical Path**
1. Phase 1 -> Phase 2 -> Phase 3 -> Phase 4 -> Phase 5 -> Phase 6 -> Phase 7 -> Phase 8.

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
- Resolve intermission with JOIN-first sequencing, then GROUP, then Window/QUALIFY.
- Keep phases 9-13 focused on query semantics completion, reliability hardening, and transition gating.
- Defer UPDATE/DELETE to a dedicated mutation roadmap after transition gating.
