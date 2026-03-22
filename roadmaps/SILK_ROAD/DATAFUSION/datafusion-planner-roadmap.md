
# DataFusion Planner Replacement Roadmap

## Scope
This roadmap replaces all custom SQL planning and custom physical execution for SELECT queries with DataFusion.

Key decisions:
- No fallback path.
- No backward-compatibility bridge for old planner payloads.
- Server plans with DataFusion logical and physical planning only.
- Server translates DataFusion physical plan into Kionas physical execution tasks.
- Worker executes Kionas task operators only (SCAN/FILTER/HASHJOIN/AGG/SORT/LIMIT/MATERIALIZE).
- Worker never receives SQL text.

## Discovery And Planning Artifacts
- Discovery: [discovery/discovery-DATAFUSION-PHASE1.md](discovery/discovery-DATAFUSION-PHASE1.md)
- Plan: [plans/plan-DATAFUSION-PHASE1.md](plans/plan-DATAFUSION-PHASE1.md)

## Phase 1: End-To-End DataFusion Cutover

### Objectives
1. Replace custom logical planning with DataFusion logical planning on server.
2. Replace custom physical planning with DataFusion physical planning on server.
3. Translate DataFusion physical plan into Kionas distributed task operators on server.
4. Keep worker runtime on Kionas operator execution contract and remove DataFusion envelope dependency.
5. Keep distributed orchestration flow but align stage semantics to DataFusion plan boundaries.
6. Preserve Flight result delivery behavior after runtime cutover.

### Mandatory Completion Criteria
1. Server query path generates DataFusion logical plan for supported SELECT scope.
2. Server optimizer path executes DataFusion optimization passes and emits explain diagnostics.
3. Server generates DataFusion physical plan and translates it to Kionas task operator payloads.
4. Worker transport contract carries Kionas task payloads only (no DataFusion envelope dependency).
5. Worker deserializes Kionas task operators for query tasks.
6. Worker executes query tasks through Kionas operator runtime.
7. Distributed task/stage decomposition operates on DataFusion plan semantics.
8. Flight retrieval paths continue to resolve and stream materialized query results.
9. Old custom planning and custom operator execution paths are removed from query flow.
10. Quality gates pass for changed crates:
   - cargo fmt --all
   - cargo clippy --all-targets --all-features -- -D warnings
   - cargo check

### Optional Hardening
1. Add richer explain snapshots for before/after optimization and physical stage boundaries.
2. Add additional distributed stress scenarios for join and aggregate heavy workloads.

### Signoff Matrix
- Phase matrix: [roadmaps/ROADMAP_DATAFUSION_PHASE1_MATRIX.md](../../ROADMAP_DATAFUSION_PHASE1_MATRIX.md)