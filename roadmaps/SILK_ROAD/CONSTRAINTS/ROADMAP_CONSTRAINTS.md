# ROADMAP: SILK_ROAD CONSTRAINTS

## Scope
Deliver constraint-aware correctness in incremental phases, starting with enforcement foundations (NOT NULL and constraint metadata lifecycle), then expanding to UNIQUE/PRIMARY KEY/CHECK/FK semantics and planner-aware optimizations.

## Phase Plan

### Phase FOUNDATION
- Objective: Establish constraints foundation with normalized metadata contract and NOT NULL enforcement in INSERT flow.
- Status: Closed (Accepted).
- Evidence matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)
- Discovery: [roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md)
- Implementation plan: [roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md)
- Completion matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)

### Phase ENFORCEMENT
- Objective: Expand enforcement beyond current INSERT NOT NULL slice (UPDATE path coverage, richer constraint taxonomy, and unified violation semantics).
- Status: Ready for planning.

### Phase OPTIMIZATION
- Objective: Use trusted constraints for planner/runtime simplifications and diagnostics.
- Status: Not started.

## Signoff Rule
Each phase requires a completion matrix with mandatory criteria marked Done and evidence links before phase signoff.
