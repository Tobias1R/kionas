# Intermission 4-5 Completion Matrix

## Scope
Intermission scope from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Who should be implemented first: GROUP or JOIN?
- How is the planner current situation to break down SCAN+FILTER+JOIN?
- Can JOIN be implemented vertically now?

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Sequencing decision (GROUP vs JOIN) is explicit | Done | [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md), [roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md](roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md) | Decision locked as JOIN-first with constrained first slice. |
| Planner readiness inventory for SCAN+FILTER+JOIN is documented | Done | [roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md](roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md), [kionas/src/planner/logical_plan.rs](kionas/src/planner/logical_plan.rs), [kionas/src/planner/physical_translate.rs](kionas/src/planner/physical_translate.rs), [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Documents active capabilities and current JOIN blockers by layer. |
| Vertical JOIN feasibility conclusion is documented with constraints | Done | [roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md](roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md) | JOIN is feasible now with INNER equi-join first and strict gates. |
| JOIN-first implementation handoff is decomposed by vertical layers | Done | [roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md](roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md) | Includes query model, planner, validation, runtime, and signoff tracks. |
| ROADMAP phase sequence is updated to reflect intermission decision | Done | [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md) | Phase sequence now places JOIN and GROUP before Window/QUALIFY. |
| Optional hardening: distributed JOIN strategy deep dive (broadcast vs shuffle heuristics) | Deferred | N/A | Non-blocking for intermission signoff. |
| Optional hardening: advanced JOIN semantics (outer/non-equi/multi-join optimization) | Deferred | N/A | Non-blocking for intermission signoff. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Intermission 4-5 signoff: `Accepted`
- Blocking items:
  - None

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected for this intermission:
  - None required for discovery signoff.
- Parameters expected for this intermission:
  - JOIN-first sequencing for implementation phases.
  - First JOIN capability gate: constrained to INNER equi-join.
