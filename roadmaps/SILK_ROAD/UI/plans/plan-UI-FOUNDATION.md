# Plan: UI FOUNDATION

## Objective
Deliver a single operational dashboard page with a separate UI backend and polling transport, providing visibility into server statistics, sessions, tokens, and workers.

## Status
Planned.

## Pre-Considerations
- Discovery decisions in [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md) must be frozen before implementation.
- Add tests, but do not execute them.
- Add matrix evidence links for all mandatory criteria.
- Redis availability path is planned in [roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md).
## Implementation Phases

### Phase A: Contracts and architecture
1. Define UI backend role and boundaries relative to existing services.
2. Define dashboard snapshot schema and per-domain sections.
3. Define response metadata fields (generated_at, data_freshness, partial_failure).
4. Define error taxonomy and fallback behavior for unavailable upstream domains.

### Phase B: UI backend endpoints
1. Implement endpoint for aggregated dashboard snapshot.
2. Add bounded timeouts and partial-response handling when one upstream source fails.
3. Add lightweight health endpoint for frontend bootstrap checks.
4. Keep all routes read-only in this phase.

### Phase C: Web dashboard
1. Build single-page layout for summary cards and domain panels.
2. Implement polling loop with configurable interval and pause/resume behavior.
3. Render loading, stale, empty, and degraded states clearly.
4. Ensure desktop and mobile layouts are usable and readable.

### Phase D: Verification and matrix closure
1. Validate dashboard loads and refreshes without manual interaction.
2. Validate each domain panel under normal and degraded backend conditions.
3. Validate stale-data indicators and partial-failure rendering.
4. Complete matrix evidence for all mandatory criteria before signoff.

## Dependencies
1. Discovery decisions in [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md) must be frozen before implementation.
2. Matrix gating in [roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md) is mandatory before phase closure.
3. Redis Dive-IN plan [roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md) must be completed for final backend data-availability design.
4. Phase dependency order is A -> B -> C -> D.

## Explicit Exclusions
1. No authentication or authorization in FOUNDATION.
2. No websocket or streaming transport in FOUNDATION.
3. No mutating administrative actions in FOUNDATION.
4. No multi-page information architecture in FOUNDATION.
