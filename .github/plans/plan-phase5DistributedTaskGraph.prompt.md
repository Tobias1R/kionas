## Plan: Phase 5 Distributed Task Graph

Deliver Phase 5 as two slices to reduce risk and preserve incremental signoff: 5A establishes distributed plan primitives, stage DAG compilation, and stage-aware coordination protocol; 5B delivers the first end-to-end distributed use-case (parallel scan plus merge) using storage-mediated exchange. This matches your decisions to defer joins and avoid direct worker-to-worker exchange in this phase.

**Steps**
1. Phase 5A: Foundations and Contracts (blocks 5B)
1.1 Add distributed plan primitives in kionas planner layer: stage model, stage edges, and partition specification types. Depends on current physical plan model in kionas/src/planner/physical_plan.rs.
1.2 Extend physical translation path to emit stage-oriented distributed plan for supported first slice shapes, while preserving existing single-stage path for non-distributed behavior. Depends on 1.1.
1.3 Add distributed plan validation rules (stage graph sanity, dependency acyclicity, partition spec constraints, capability checks) in planner validation path. Depends on 1.1 and 1.2.
1.4 Add server-side DAG compiler that transforms distributed plan into stage task groups with explicit dependencies and stage metadata in params. Depends on 1.1 to 1.3.
1.5 Extend task lifecycle model to include stage metadata and stage-level completion bookkeeping while keeping backward-compatible single-task updates. Depends on 1.4.
1.6 Extend interops task-update protocol and server handler to support stage progress metadata (optional fields only for compatibility). Depends on 1.5.
1.7 Introduce stage scheduler orchestration in server statement flow: dispatch ready stages, wait for stage completion, then unlock dependent stages. Depends on 1.4 to 1.6.

2. Phase 5B: First Distributed Use-Case (depends on 5A)
2.1 Implement storage-mediated exchange buffers for inter-stage data transfer (write/read partitioned stage artifacts). Depends on 1.7.
2.2 Add worker-side stage execution path for parallel scan plus merge using stage metadata and exchange buffer inputs/outputs. Depends on 2.1.
2.3 Implement stage completion signaling from worker to server through interops updates so downstream stage scheduling is dependency-safe. Depends on 2.2 and 1.6.
2.4 Wire query dispatch to choose distributed path for first supported use-case while preserving current non-distributed path for all others. Depends on 2.2 and 1.7.
2.5 Keep joins and broader distributed operators explicitly deferred; unsupported distributed shapes must fail with existing business-style capability semantics. Depends on 2.4.

3. Cross-Cutting Hardening (parallelizable after 5A core)
3.1 Add deterministic IDs and idempotency guards for stage-partition tasks to avoid duplicate progress on retries. Parallel with 2.1 and 2.2.
3.2 Add structured logging and trace points for stage transitions, dependency unlocks, and partition completion counts. Parallel with 2.2 and 2.3.
3.3 Add bounded timeouts and graceful failure propagation so blocked stages fail fast with actionable status. Parallel with 2.3.

4. Signoff Artifacts and Governance
4.1 Create roadmaps/ROADMAP_PHASE5A_MATRIX.md for 6.1 to 6.3 foundations and contract readiness.
4.2 Create roadmaps/ROADMAP_PHASE5B_MATRIX.md for 6.4 first distributed use-case validation.
4.3 Create roadmaps/ROADMAP_PHASE5_MATRIX.md consolidated acceptance matrix referencing 5A and 5B evidence.
4.4 Update roadmaps/ROADMAP.md only when consolidated 5A and 5B signoff is accepted.

**Relevant files**
- kionas/src/planner/physical_plan.rs — extend operator-level model into stage-aware distributed primitives.
- kionas/src/planner/physical_translate.rs — compile logical plan into distributed stage graph for first supported shape.
- kionas/src/planner/physical_validate.rs — enforce distributed capability and graph validation.
- server/src/statement_handler/query_select.rs — route supported queries into distributed compilation and execution path.
- server/src/statement_handler/helpers.rs — task dispatch helper extension to stage groups and dependency-aware scheduling.
- server/src/tasks/mod.rs — stage metadata, stage lifecycle bookkeeping, and task update idempotency anchor.
- server/src/services/interops_service_server.rs — process stage-aware completion updates.
- kionas/proto/interops_service.proto — optional stage progress fields for backward-compatible coordination.
- worker/src/transactions/maestro.rs — introduce stage-aware execution entrypoints for query operation.
- worker/src/services/query_execution.rs — parallel scan and merge stage execution logic.
- worker/src/state/mod.rs — stage-partition result location and progress state support.
- worker/src/storage/staging.rs — pattern source for storage-mediated exchange semantics.
- roadmaps/ROADMAP_PHASE_MATRIX_TEMPLATE.md — matrix structure and status vocabulary reference.

**Verification**
1. Planner validation checks:
1.1 Distributed plan compile emits stage graph and valid dependency edges for supported first shape.
1.2 Unsupported distributed shapes fail with deterministic business capability semantics.
2. Server scheduler checks:
2.1 Downstream stage is never dispatched before upstream completion is recorded.
2.2 Stage failure blocks dependent stages and surfaces actionable error state.
3. Worker execution checks:
3.1 Parallel scan stage writes partitioned exchange artifacts.
3.2 Merge stage consumes exchange artifacts and emits final result metadata and location.
4. End-to-end manual flow (container-based):
4.1 Query dispatch creates distributed stage tasks.
4.2 Interops updates show stage progress and completion transitions.
4.3 Client retrieves final result through existing retrieval path without regression.
5. Quality gates for touched scope:
5.1 cargo fmt --all
5.2 cargo clippy --all-targets --all-features -- -D warnings
5.3 cargo check

**Decisions**
- Split Phase 5 into 5A and 5B.
- First distributed use-case is limited to parallel scan plus merge.
- Inter-stage exchange uses storage-mediated routing first.
- Joins and broader distributed operators are excluded from this phase and deferred.

**Further Considerations**
1. Decide whether stage metadata should be persisted beyond memory for crash-recovery in this phase or deferred to reliability phase.
2. Decide whether to include a minimal stage-level metrics payload now or defer all performance telemetry to Phase 7.
3. Decide whether matrix acceptance for 5A requires one synthetic integration run or only unit plus manual orchestration evidence.
