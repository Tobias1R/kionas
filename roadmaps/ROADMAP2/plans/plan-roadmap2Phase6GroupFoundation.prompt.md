## Plan: ROADMAP2 Phase 6 GROUP Foundation

Phase 6 should ship as a strict vertical slice, reusing the proven JOIN rollout pattern: model extraction -> logical/physical contracts -> distributed split -> worker execution -> diagnostics -> matrix signoff.
Scope decisions captured from you are now embedded: include COUNT(*), SUM, MIN, MAX, AVG; SQL-consistent NULL semantics; order guarantees only with ORDER BY; diagnostics in explain text plus worker tracing counters.

**Steps**
1. Phase A: Contract and gate setup
1. Freeze Phase 6 constraints in roadmap execution notes and enforce matrix-gated signoff.
2. Define explicit unsupported-shape failures (for out-of-scope aggregate cases) with actionable planner errors.
3. Lock determinism rule: grouped result order is only guaranteed when ORDER BY is present.

2. Phase B: Query model and logical contract (depends on Phase A)
1. Extend select-model extraction to carry GROUP metadata from SQL AST.
2. Add typed logical aggregate contracts (grouping keys + aggregate expressions), mirroring existing typed-spec style.
3. Extend logical plan to represent grouping and aggregate metadata.
4. Add tests for GROUP extraction and constrained validation behavior.

3. Phase C: Physical planning and distributed contracts (depends on Phase B)
1. Replace aggregate placeholders with typed partial/final metadata specs.
2. Emit AggregatePartial and AggregateFinal in valid operator order during physical translation.
3. Update physical validation to allow constrained aggregates and enforce ordering invariants.
4. Extend distributed planning to split aggregate pipelines into partial/final stages with grouping-key partition metadata.
5. Add planner tests for emission, rejection/acceptance, and distributed split invariants.

4. Phase D: Worker runtime and aggregate execution (depends on Phase C)
1. Extend runtime-plan extraction to parse aggregate specs and phase metadata.
2. Implement aggregate execution in worker execution modules (not services), including partial accumulation and final merge.
3. Integrate aggregate execution into pipeline ordering before downstream projection/sort/limit.
4. Enforce NULL semantics:
5. COUNT(*) includes rows.
6. SUM/MIN/MAX/AVG ignore NULL input values.
7. AVG finalization uses stable sum/count state and returns NULL for empty non-null groups.
8. Add runtime tests for partial/final correctness, NULL semantics, and deterministic behavior policy.

5. Phase E: Observability and closure (can overlap with late Phase D)
1. Extend planner explain rendering for aggregate operators and group/function summaries.
2. Add worker tracing counters for aggregation diagnostics (input rows, groups formed, output rows, stage phase).
3. Verify diagnostics remain backward-compatible with existing response/retrieval contracts.
4. Complete Phase 6 completion matrix with evidence and Done/Deferred closure status before signoff.

**Relevant files**
- c:/code/kionas/roadmaps/ROADMAP2.md
- c:/code/kionas/roadmaps/ROADMAP2_PHASE5_MATRIX.md
- c:/code/kionas/roadmaps/ROADMAP_PHASE_MATRIX_TEMPLATE.md
- c:/code/kionas/kionas/src/sql/query_model.rs
- c:/code/kionas/kionas/src/planner/logical_plan.rs
- c:/code/kionas/kionas/src/planner/join_spec.rs
- c:/code/kionas/kionas/src/planner/translate.rs
- c:/code/kionas/kionas/src/planner/physical_plan.rs
- c:/code/kionas/kionas/src/planner/physical_translate.rs
- c:/code/kionas/kionas/src/planner/physical_validate.rs
- c:/code/kionas/kionas/src/planner/distributed_plan.rs
- c:/code/kionas/kionas/src/planner/explain.rs
- c:/code/kionas/worker/src/execution/planner.rs
- c:/code/kionas/worker/src/execution/pipeline.rs
- c:/code/kionas/worker/src/execution/join.rs
- c:/code/kionas/worker/src/execution/artifacts.rs
- c:/code/kionas/worker/src/services/query_execution.rs

**Verification**
1. Planner/model tests validate GROUP extraction, physical emission, and aggregate ordering constraints.
2. Distributed-plan tests validate partial/final decomposition and grouping-key partition wiring.
3. Worker tests validate partial state accumulation, final merge/finalization, and SQL-consistent NULL behavior.
4. End-to-end checks validate:
5. GROUP without ORDER BY for content correctness only.
6. GROUP with ORDER BY for deterministic row-order correctness.
7. Diagnostics checks validate aggregate explain strings and worker tracing counters.
8. Implementation quality gates: cargo fmt --all, cargo clippy --all-targets --all-features -- -D warnings, cargo check (with Windows Docker fallback for server package if native toolchain fails).

**Decisions**
- Included scope: GROUP foundation with COUNT(*), SUM, MIN, MAX, AVG.
- Included policy: SQL-consistent NULL behavior now.
- Determinism policy: explicit ORDER BY required for order guarantees.
- Diagnostics baseline: planner explain text + worker tracing counters.
- Excluded from Phase 6: HAVING, advanced aggregates (percentile/stddev), non-trivial aggregate expression rewrites, mutation semantics.
- Dont expand existing modules strictly when necessary to contain changes within the Phase 6 scope and avoid premature refactors. Create new modules for aggregate execution if needed to keep code organized without impacting existing paths. For this plan, we should have one file per windows function we are dealing(COUNT, SUM, MIN, MAX, AVG) and we can call them aggregate_count.rs, aggregate_sum.rs, etc. This way we can keep the code organized and avoid making changes to existing files that are not directly related to the new functionality we are adding.