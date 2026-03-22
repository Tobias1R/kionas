# Plan: DataFusion Phase 1

## Goal
Deliver an end-to-end DataFusion-only query path for SELECT execution across server planning, transport, worker execution, and distributed orchestration.

## Guardrails
1. No fallback to legacy planner or legacy operator runtime.
2. No dual-read or dual-write payload strategy.
3. Query execution path accepts DataFusion envelope only.

## Workstreams

### Workstream A: Server Planning Replacement
1. Replace custom planner entry in query dispatch flow with DataFusion logical plan generation.
2. Run DataFusion optimizer pipeline and retain diagnostics needed for operability.
3. Produce DataFusion physical plan as canonical server output for query tasks.

Deliverables:
1. Query dispatch path emits DataFusion plan envelope.
2. Explain/diagnostic artifacts for logical and physical planning are available.

### Workstream B: Transport Contract Redesign
1. Keep worker task transport focused on Kionas task operators and params.
2. Remove DataFusion envelope dependency from query transport contract.
3. Define clear error semantics for invalid/missing plan envelope.

Deliverables:
1. Protobuf contract compiled and consumed by server and worker.
2. Server emits translated Kionas operator payloads for query tasks.

### Workstream C: Worker Runtime Replacement
1. Keep runtime extraction centered on Kionas operator payload contract.
2. Execute Kionas task operators produced by DataFusion-to-Kionas translator.
3. Keep materialization outputs compatible with Flight retrieval flow.

Deliverables:
1. Worker query runtime operates on Kionas operator tasks only.
2. Query result materialization still feeds Flight paths successfully.

### Workstream D: Distributed Adaptation
1. Map existing stage/task orchestration to DataFusion execution boundaries.
2. Align partition/exchange semantics with DataFusion physical plan behavior.
3. Verify stage dependency correctness and deterministic orchestration behavior.

Deliverables:
1. Distributed stage compilation and scheduling use DataFusion plan semantics.
2. Multi-stage query flows complete with expected outputs.

### Workstream E: Validation And Signoff
1. Validate filter, projection, join, aggregate, sort, and limit correctness.
2. Validate strict typing/coercion policy behavior under DataFusion path.
3. Validate Flight endpoint retrieval and stream behavior after cutover.
4. Run required quality gates and record evidence.

Deliverables:
1. Phase matrix updated with evidence for every mandatory criterion.
2. Phase signoff decision recorded.

## Sequence And Dependencies
1. A and B start immediately.
2. C starts once B contract is available.
3. D starts once A and C produce stable execution artifacts.
4. E runs continuously and closes at signoff.

## Acceptance Criteria
1. Legacy planner and legacy query execution are not used in SELECT path.
2. Server and worker communicate using the new DataFusion contract only.
3. End-to-end distributed SELECT execution completes through Flight retrieval.
4. All mandatory matrix items are marked Done with evidence.

## Evidence Locations
1. Roadmap: [datafusion-planner-roadmap.md](../datafusion-planner-roadmap.md)
2. Discovery: [discovery/discovery-DATAFUSION-PHASE1.md](../discovery/discovery-DATAFUSION-PHASE1.md)
3. Matrix: [roadmaps/ROADMAP_DATAFUSION_PHASE1_MATRIX.md](../../../ROADMAP_DATAFUSION_PHASE1_MATRIX.md)
