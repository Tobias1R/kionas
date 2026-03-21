# SILK_ROAD Discovery: UI FOUNDATION

## Objective
Define the first SILK_ROAD UI path for operational visibility, choosing architecture and transport decisions that can ship quickly without destabilizing server and worker runtime behavior.

## Decisions Confirmed
1. Runtime shape: web UI plus a separate UI backend service.
2. Data transport: polling endpoints every N seconds (no websocket in this phase).
3. Product scope: single dashboard page.
4. Dashboard data domains: server statistics, sessions, tokens, workers.
5. Security boundary for this phase: no authentication.
6. This artifact is planning/discovery only. No production code changes are included.

## Problem Statement
Current observability is mostly terminal/log driven. Contributors and operators need a consolidated dashboard to understand runtime health, active sessions, issued tokens, and worker fleet state without scanning multiple services and logs.

## System Surfaces Under Analysis
1. Server runtime metrics and status surfaces.
2. Session and token lifecycle visibility surfaces.
3. Worker health and registration surfaces.
4. UI backend aggregation seams for cross-service snapshots.
5. Frontend polling and state refresh behavior.

## Option Analysis

### Option A: Serve UI from existing server binary
Pros:
1. Fewer deployable units.
2. Simpler local startup.

Cons:
1. Couples UI cadence to server release cadence.
2. Increases server blast radius for frontend-related failures.
3. Raises risk of framework and dependency pressure in the server crate.

### Option B: Separate UI backend and web UI (selected)
Pros:
1. Clean separation between core query/control runtime and dashboard concerns.
2. Independent iteration speed for API shaping and frontend UX.
3. Lower risk to server stability from UI changes.

Cons:
1. Additional service to deploy.
2. Requires explicit contract management between UI backend and core services.

## Recommendation
Proceed with Option B for FOUNDATION: separate UI backend plus web frontend, with polling-based data refresh and a single dashboard page.

## FOUNDATION Scope (In)
1. Define backend endpoint contracts for dashboard snapshot domains.
2. Implement periodic polling model and stale-data handling semantics.
3. Render one dashboard page with at-a-glance cards/tables for server stats, sessions, tokens, and workers.
4. Provide clear loading, empty, and degraded states.
5. Establish matrix-ready evidence expectations for signoff.

## FOUNDATION Scope (Out)
1. Authentication and authorization.
2. Multi-page navigation and deep drill-down workflows.
3. Mutating operations (revoke token, kill session, restart worker).
4. Websocket/streaming transport.

## Risks and Mitigations
1. Risk: polling burst load on core services.
   Mitigation: bounded poll interval, request timeout, and backend fan-in aggregation.
2. Risk: inconsistent snapshot timestamps across domains.
   Mitigation: include generated_at metadata and stale indicators in response payload.
3. Risk: no-auth dashboard exposure.
   Mitigation: keep scope non-sensitive and establish explicit follow-up auth phase.

## Handoff Targets
- Roadmap: [roadmaps/SILK_ROAD/UI/ROADMAP_UI.md](roadmaps/SILK_ROAD/UI/ROADMAP_UI.md)
- Plan: [roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md)
- Redis Dive-IN Plan: [roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md)
- Matrix: [roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md)


## Dive-IN
### Redis integration for UI backend data availability
- How to use Redis to make server/session/token/worker information available to the UI backend without destabilizing existing service boundaries. We need some loop logic to populate Redis with the relevant information, and then the UI backend can read from Redis to serve the dashboard snapshot endpoint. This allows us to decouple the UI backend from direct dependencies on the core services while still providing timely data for the dashboard.
- We have consul on our cluster! Maybe show some cluster configuration on this dashboard.

### Redis Dive-IN decisions
1. Publisher ownership: server-owned publisher loop.
2. Redis model: per-domain JSON keys.
3. Freshness target: 10-15 seconds.
4. Consul scope: read-only cluster summary panel in FOUNDATION.
5. Exposure scope for FOUNDATION: public endpoint allowed.
