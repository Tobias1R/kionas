# ROADMAP: SILK_ROAD CONSTRAINTS

## Scope
Deliver constraint-aware correctness in incremental phases, starting with the completed foundation slice (NOT NULL and constraint metadata lifecycle), then expanding to UNIQUE/PRIMARY KEY/CHECK/FK semantics and planner-aware optimizations.

## Phase Plan

### Phase FOUNDATION
- Objective: Establish constraints foundation with normalized metadata contract and NOT NULL enforcement in INSERT flow.
- Status: Closed (Accepted).
- Evidence matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)
- Discovery: [roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md)
- Implementation plan: [roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/plans/plan-CONSTRAINTS-FOUNDATION.md)
- Completion matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)

### Phase OPTIMIZATION
- Objective: Continue post-foundation constraints expansion (including broader validation coverage and richer taxonomy) and use trusted constraints for planner/runtime simplifications and diagnostics.
- Status: Not started.

## Signoff Rule
Each phase requires a completion matrix with mandatory criteria marked Done and evidence links before phase signoff.
