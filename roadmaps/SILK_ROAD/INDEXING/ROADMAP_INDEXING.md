# ROADMAP: SILK_ROAD INDEXING

## Scope
Deliver indexing-oriented query acceleration incrementally, beginning with predicate pruning and file skipping in scan execution, while preserving deterministic correctness and fallback safety.

## Phase Plan

### Phase FOUNDATION
- Objective: Prove metadata-driven file skipping with strict correctness fallback.
- Discovery: [roadmaps/SILK_ROAD/INDEXING/discovery/discovery-INDEXING-FOUNDATION.md](roadmaps/SILK_ROAD/INDEXING/discovery/discovery-INDEXING-FOUNDATION.md)
- Planned implementation file (next iteration): [roadmaps/SILK_ROAD/INDEXING/plans/plan-INDEXING-FOUNDATION.md](roadmaps/SILK_ROAD/INDEXING/plans/plan-INDEXING-FOUNDATION.md)
- Completion matrix: [roadmaps/ROADMAP_PHASE_INDEXING_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_PHASE_INDEXING_FOUNDATION_MATRIX.md)

### Phase INDEX-CATALOG
- Objective: Add explicit index descriptor catalog and metadata lifecycle.
- Status: Not started.

### Phase ACCESS-PATHS
- Objective: Introduce additional access-path operators and selection logic.
- Status: Not started.

## Signoff Rule
Each phase requires a completion matrix with mandatory criteria marked Done and evidence links before phase signoff.
