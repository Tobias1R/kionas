# Discovery: DataFusion Phase 1

## What
This discovery defines the exact replacement boundaries to move from custom planning and custom query execution to DataFusion-only planning and execution.

## Why
The current custom planner/runtime duplicates functionality DataFusion already provides and increases maintenance, correctness, and evolution risk.

## Explicit Scope Decisions
1. Full replacement of custom logical and physical planning for SELECT path.
2. Full replacement of custom worker execution path for query operators.
3. Contract change now for DataFusion plan transport.
4. No fallback and no dual-path execution.

## Current Architecture Snapshot
1. Server currently builds a custom logical plan and custom physical plan from SQL payload models.
2. Server dispatches query tasks to workers with JSON payload contracts based on custom physical operators.
3. Worker currently parses custom operator payloads and executes custom pipeline stages.
4. Distributed task orchestration exists and must be adapted to DataFusion plan boundaries.
5. Flight result retrieval path exists and must stay functionally intact.

## Technical Findings
1. Server replacement boundary:
   - SQL parse and model handoff currently rooted in query dispatch envelope creation.
   - Custom translate and physical translate planner modules are current implementation points.
2. Contract replacement boundary:
   - Worker task protobuf currently carries payload in generic input string and optional filter predicate structure.
   - Worker contract remains Kionas operator-task based; DataFusion plan transport is not required.
3. Worker replacement boundary:
   - Runtime extraction logic currently expects Kionas physical operators.
   - Query execution runtime currently applies Kionas operator pipeline.
   - Server must translate DataFusion physical plan into this operator model.
4. Distributed boundary:
   - Existing stage DAG orchestration should remain as orchestration layer.
   - Stage decomposition semantics must align to DataFusion exchanges and execution plan boundaries.
5. Correctness boundary:
   - Type coercion and deterministic behavior from previous phases must remain explicit and validated.

## Risks And Constraints
1. Contract break is intentional; all active components must migrate together.
2. Distributed stage adaptation may surface semantic differences in join/aggregate exchange placement.
3. Explain/diagnostics output format may change and needs explicit acceptance criteria.
4. Worker materialization and Flight retrieval integration must be validated after executor cutover.

## Non-Goals For This Phase
1. No support expansion beyond current SELECT feature envelope unless needed for replacement correctness.
2. No work on unrelated DDL/DML planner paths in this roadmap slice.

## Output Of Discovery
1. Implementation plan file: [plans/plan-DATAFUSION-PHASE1.md](../plans/plan-DATAFUSION-PHASE1.md)
2. Roadmap anchor: [datafusion-planner-roadmap.md](../datafusion-planner-roadmap.md)
3. Phase matrix scaffold: [roadmaps/ROADMAP_DATAFUSION_PHASE1_MATRIX.md](../../../ROADMAP_DATAFUSION_PHASE1_MATRIX.md)
