
## Silk Road Methodology
We reach a point on the project where we can explore new frontiers and take on more ambitious challenges. To guide our efforts, we adopt the "Silk Road" methodology, a strategic approach that emphasizes exploration, adaptability, and incremental progress. The Silk Road methodology encourages us to venture into uncharted territories while maintaining a focus on delivering value and learning from our experiences.

## Road to Indexing
What does it take to build an indexing subsystem? We will explore the requirements, design considerations, and implementation steps needed to create a robust indexing solution that can enhance query performance and support advanced query features.
Deltatable indexing
Jump files during scan
Fast Access Paths

Current roadmap: [roadmaps/SILK_ROAD/INDEXING/ROADMAP_INDEXING.md](roadmaps/SILK_ROAD/INDEXING/ROADMAP_INDEXING.md)

Foundation discovery: [roadmaps/SILK_ROAD/INDEXING/discovery/discovery-INDEXING-FOUNDATION.md](roadmaps/SILK_ROAD/INDEXING/discovery/discovery-INDEXING-FOUNDATION.md)

Foundation matrix: [roadmaps/ROADMAP_PHASE_INDEXING_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_PHASE_INDEXING_FOUNDATION_MATRIX.md)

## Road to Constraints

Current roadmap: [roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md](roadmaps/SILK_ROAD/CONSTRAINTS/ROADMAP_CONSTRAINTS.md)

Foundation discovery: [roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md](roadmaps/SILK_ROAD/CONSTRAINTS/discovery/discovery-CONSTRAINTS-FOUNDATION.md)

Foundation matrix: [roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_CONSTRAINTS_FOUNDATION_MATRIX.md)

## Road to Rich Datatypes

Current roadmap: [roadmaps/SILK_ROAD/DATATYPES/ROADMAP_DATATYPES.md](roadmaps/SILK_ROAD/DATATYPES/ROADMAP_DATATYPES.md)

Foundation discovery: [roadmaps/SILK_ROAD/DATATYPES/discovery/discovery-DATATYPES-FOUNDATION.md](roadmaps/SILK_ROAD/DATATYPES/discovery/discovery-DATATYPES-FOUNDATION.md)

Foundation plan: [roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION.md](roadmaps/SILK_ROAD/DATATYPES/plans/plan-DATATYPES-FOUNDATION.md)

Foundation matrix: [roadmaps/ROADMAP_DATATYPES_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_DATATYPES_FOUNDATION_MATRIX.md)

## Road to Structured Predicates (Predicate Algebra)

**What:** Refactor from raw SQL string predicates (re-parsed at worker) to structured, typed PredicateExpr objects sent from server.

**Why:** Current design causes parsing edge cases (BETWEEN+IN combinations failing), lacks type safety, and prevents clean operator extensibility.

**Impact:** Eliminates entire class of filter parsing bugs; enables safe addition of new operators; improves performance (1 parse instead of 2).

Navigation: [roadmaps/SILK_ROAD/PREDICATE_ALGEBRA/README.md](roadmaps/SILK_ROAD/PREDICATE_ALGEBRA/README.md)

Phase 1 discovery: [roadmaps/SILK_ROAD/PREDICATE_ALGEBRA/discovery/discovery-predicate-algebra-phase1.md](roadmaps/SILK_ROAD/PREDICATE_ALGEBRA/discovery/discovery-predicate-algebra-phase1.md)

Phase 1 plan (5 phases total): [roadmaps/SILK_ROAD/PREDICATE_ALGEBRA/plans/plan-predicate-algebra-phase1.md](roadmaps/SILK_ROAD/PREDICATE_ALGEBRA/plans/plan-predicate-algebra-phase1.md)

## Road to UI

Current roadmap: [roadmaps/SILK_ROAD/UI/ROADMAP_UI.md](roadmaps/SILK_ROAD/UI/ROADMAP_UI.md)

Foundation discovery: [roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/discovery/discovery-UI-FOUNDATION.md)

Foundation plan: [roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md](roadmaps/SILK_ROAD/UI/plans/plan-UI-FOUNDATION.md)

Foundation matrix: [roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md](roadmaps/ROADMAP_UI_FOUNDATION_MATRIX.md)



