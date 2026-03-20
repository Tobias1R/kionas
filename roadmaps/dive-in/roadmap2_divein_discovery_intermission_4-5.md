# Roadmap2 Intermission Discovery (Phase 4-5)

## Scope
Intermission questions from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Who should be implemented first: GROUP or JOIN?
- What is the current planner situation to break down SCAN+FILTER+JOIN?
- Can JOIN be implemented vertically now?

## Executive Decision
- Implement JOIN first, then GROUP.
- Start with a constrained JOIN slice: INNER equi-join only.
- Keep existing single-table query path intact while JOIN path is introduced.

## Direct Answers
1. Who first: GROUP or JOIN?
- JOIN first.
- Reason: GROUP on top of multi-relation queries is safer after JOIN contracts are established across model, planner, validation, and runtime.

2. Planner current situation for SCAN+FILTER+JOIN breakdown?
- SCAN and FILTER are operational end-to-end.
- JOIN operators exist in physical catalog but are currently blocked in vertical flow.
- Current planner path is still single-relation and linear for non-join queries.

3. Can JOIN be implemented vertically now?
- Yes, with constrained scope (INNER equi-join first) and strict capability gates.

## Evidence Snapshot
- Query-model entry currently rejects JOIN at extraction level and needs join metadata extraction support.
- Logical plan remains relation-centric and must gain a JOIN-capable representation.
- Physical translation emits a fixed pipeline and must branch for JOIN queries.
- Physical and worker validation currently treat JOIN as unsupported and must shift to conditional allow with invariants.
- Worker execution currently has no active JOIN runtime operation path.

## Implementation Track (JOIN-First)
1. Query model track
- Add JOIN metadata extraction for FROM/JOIN clauses.
- Preserve backward compatibility for single-table SELECT.

2. Planner/model track
- Add JOIN-capable logical representation.
- Extend physical translation to emit JOIN operator path for join queries only.
- Extend explain output to include JOIN strategy and key diagnostics.

3. Validation track
- Replace hard rejection with constrained acceptance for INNER equi-join.
- Enforce strict key and operator ordering invariants.

4. Worker execution track
- Add JOIN runtime path for constrained shape.
- Preserve existing ORDER BY/LIMIT/materialize semantics after JOIN output.

5. Signoff track
- Use mandatory matrix gating for this intermission slice.
- Mark signoff only when model, planner, validator, runtime, and diagnostics are all evidenced.

## Scope Boundaries (First JOIN Slice)
- Included:
	- Single JOIN between two relations.
	- INNER equi-join predicate.
	- End-to-end explain and diagnostics visibility.
- Deferred:
	- Outer joins.
	- Non-equi join predicates.
	- Multi-join ordering optimization and broad cost-based planning.
	- Broad predicate-language expansion not required for INNER equi-join slice.

## Risks And Mitigations
- Risk: JOIN changes destabilize current single-table path.
	- Mitigation: This is a breaking change! If have to break, it will break!
- Risk: Validation drift between planner and worker.
	- Mitigation: Mirror invariant rules and add negative tests at both layers.

## Exit Criteria
- Query model can represent constrained JOIN requests.
- Logical and physical plans include JOIN path with deterministic explain output.
- Planner and worker validators accept only constrained JOIN shape and reject unsupported variants.
- Worker executes constrained JOIN path and returns stable results through Flight retrieval.
- Matrix for this intermission is completed with all mandatory items marked Done.

