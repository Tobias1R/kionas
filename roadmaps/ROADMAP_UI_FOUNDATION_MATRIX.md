# Phase UI FOUNDATION Completion Matrix

## Scope
Discovery, architecture definition, and first implementation slice planning for a polling-based, single-page operational dashboard with a separate UI backend, as defined in [roadmaps/SILK_ROAD/UI/ROADMAP_UI.md](roadmaps/SILK_ROAD/UI/ROADMAP_UI.md).

## Completion Matrix
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Mandatory criterion 1: UI discovery created with architecture and transport decisions | Done | [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md) | Confirms separate backend, polling, single page, no-auth scope. |
| Mandatory criterion 2: UI roadmap created and linked | Done | [roadmaps/SILK_ROAD/UI/ROADMAP_UI.md](roadmaps/SILK_ROAD/UI/ROADMAP_UI.md) | FOUNDATION/HARDENING/EXTENSION phases scaffolded. |
| Mandatory criterion 3: FOUNDATION implementation plan created and linked | Done | [roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md) | Execution sequence and exclusions are explicit. |
| Mandatory criterion 4: Redis Dive-IN plan created with architecture decisions | Done | [roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-REDIS-DIVEIN-FOUNDATION.md), [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md) | Publisher ownership, Redis model, freshness, Consul scope, and exposure decision are frozen. |
| Mandatory criterion 5: Backend dashboard snapshot endpoint implemented | Not Started | N/A | To be completed during implementation. |
| Mandatory criterion 6: Polling dashboard page implemented | Not Started | N/A | To be completed during implementation. |
| Mandatory criterion 7: Stale/partial failure states implemented and validated | Not Started | N/A | To be completed during implementation. |
| Mandatory criterion 8: Single-page domain coverage validated (stats/sessions/tokens/workers) | Not Started | N/A | To be completed during implementation. |
| Optional hardening 1: Adaptive polling and backoff strategy | Deferred | N/A | Non-blocking for foundation signoff. |
| Optional hardening 2: Historical sparkline and trend cache | Deferred | N/A | Non-blocking for foundation signoff. |

Status values:
- Done
- Deferred
- Blocked
- Not Started

## Signoff Decision
- Phase signoff: Pending
- Blocking items:
  - Mandatory criterion 5 is not Done.
  - Mandatory criterion 6 is not Done.
  - Mandatory criterion 7 is not Done.
  - Mandatory criterion 8 is not Done.

## Gate Checklist
1. All mandatory criteria are marked Done.
2. Each Done item includes concrete evidence reference.
3. Deferred items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Expected environment variables for implementation phase:
  - UI_DASHBOARD_POLL_INTERVAL_SECONDS: polling interval used by web UI refresh loop.
  - UI_BACKEND_UPSTREAM_TIMEOUT_MS: per-upstream timeout for UI backend fan-in calls.
- Expected parameters for implementation phase:
  - include_domains query parameter on dashboard snapshot endpoint to request specific panels.
  - max_items parameter for bounded sessions/tokens/worker lists.
