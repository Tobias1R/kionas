# Plan: UI REDIS DIVE-IN FOUNDATION

## Objective
Define and de-risk a Redis-backed data availability path for the UI backend so dashboard reads are decoupled from core service fan-out, with a server-owned publisher loop, per-domain Redis keys, and balanced freshness targets.

## Status
Planned.

## Architecture Decisions (Locked)
1. Publisher ownership: server-owned publisher loop.
2. Redis data model: per-domain JSON keys.
3. Freshness target: 10-15 seconds.
4. Consul scope: read-only cluster summary in FOUNDATION.
5. Exposure choice for FOUNDATION: public endpoint allowed.


## Implementation Phases

### Phase A: Domain ownership and extraction seams
1. Freeze source-of-truth ownership per dashboard domain.
2. Map extraction seams for stats, sessions, tokens, workers, and Consul summary.
3. Confirm UI backend remains Redis-read-only in FOUNDATION.
4. Define schema versioning expectations for forward compatibility.

### Phase B: Redis key contracts
1. Define per-domain keys:
   - kionas:ui:dashboard:server_stats
   - kionas:ui:dashboard:sessions
   - kionas:ui:dashboard:tokens
   - kionas:ui:dashboard:workers
   - kionas:ui:dashboard:consul_cluster_summary
2. Define shared envelope fields:
   - generated_at
   - freshness_target_seconds
   - source_component
   - partial_failure
   - data_version
3. Define key TTL policy and stale-read behavior.
4. Define payload size and max_items truncation policy for large domains.

### Phase C: Publisher loop strategy
1. Design server-owned periodic publisher with independent domain writes.
2. Define startup warm write and cadence jitter behavior.
3. Define fault-isolation behavior so one domain failure does not block others.
4. Define observability fields for publisher health.
5. Lets call this mod janitor(server/src/janitor/). There are more to come here! We currently have a loop in the server bootstrap that make an api available with some metrics. Lets replace that with a janitor module that will be responsible for this and other periodic tasks in the future. No need for this metrics api anymore. we can use sys to provide memory and cpu usage metrics to the dashboard.

### Phase D: UI backend snapshot assembly
1. Define Redis read path for /dashboard/snapshot.
2. Define partial-response behavior for missing/invalid domain keys.
3. Support include_domains and max_items request parameters.
4. Define stale-indicator semantics using generated_at and freshness target.

### Phase E: Consul cluster summary panel
1. Define read-only summary contract sourced from cluster config semantics.
2. Include nodes, master endpoint, storage backend type, and refresh timestamp.
3. Exclude secrets and mutating Consul operations.
4. Define degraded behavior when Consul summary key is absent.

### Phase F: Verification and matrix closure
1. Validate schema contract for all domain keys.
2. Validate freshness and stale-indicator behavior at 10-15 second cadence.
3. Validate partial-response behavior when one domain key is unavailable.
4. Add evidence links and update completion matrix statuses.

## Dependencies
1. Discovery decisions in [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md) must remain frozen.
2. FOUNDATION plan in [roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md) remains the parent plan.
3. Matrix gating in [roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md) is mandatory before signoff.

## Relevant Files
- [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md)
- [roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md)
- [roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md)
- [server/src/session.rs](server/src/session.rs)
- [server/src/auth/persistence.rs](server/src/auth/persistence.rs)
- [server/src/warehouse/state.rs](server/src/warehouse/state.rs)
- [server/src/workers_pool.rs](server/src/workers_pool.rs)
- [kionas/src/constants.rs](kionas/src/constants.rs)
- [configs/cluster.json](configs/cluster.json)
- [docker/docker-compose.yaml](docker/docker-compose.yaml)

## Explicit Exclusions
1. No authentication or authorization in this dive-in phase.
2. No websocket/stream transport in this dive-in phase.
3. No mutating administrative endpoints.
4. No historical warehouse of dashboard snapshots.
