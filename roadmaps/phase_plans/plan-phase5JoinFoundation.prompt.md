## Plan: Phase 5 JOIN Foundation (Distributed, Multi-Key Equi)

Phase 5 will deliver a constrained but distributed-capable JOIN vertical slice across query model, planner, validation, worker execution, and diagnostics. Scope is INNER equi-join with multi-key predicates, implemented without regressing existing single-table SCAN/FILTER/PROJECTION/SORT/LIMIT behavior.

**Steps**
1. Phase A: Scope Lock And Matrix Scaffolding
1.1 Create `c:/code/kionas/roadmaps/ROADMAP2_PHASE5_MATRIX.md` with mandatory criteria mapped to Phase 5 items in `c:/code/kionas/roadmaps/ROADMAP2.md`.
1.2 Lock capability gates for this phase: distributed JOIN enabled, INNER equi-join only, multi-key predicate support, single JOIN between two relations.
1.3 Record deferred hardening upfront: outer joins, non-equi predicates, multi-join tree optimization, full cost-based planning.

2. Phase B: Query Model JOIN Extraction
2.1 Create a new mod for this logic and wire in `SelectQueryModel` in `c:/code/kionas/kionas/src/sql/query_model.rs` with JOIN metadata structures (right relation, join type, join key pairs).
2.2 Replace current JOIN hard reject (`UnsupportedJoin`) with constrained extraction and validation for INNER equi-join only.
2.3 Keep single-table path fully backward compatible; non-join queries must produce identical payloads.
2.4 Add stable query-model errors for unsupported JOIN shapes (outer/non-equi/ambiguous keys).
2.5 Add tests to a tests module for accepted and rejected JOIN query shapes. *parallel with 2.4*

3. Phase C: Logical Plan JOIN Representation
3.1 Introduce JOIN-capable logical structures in `c:/code/kionas/kionas/src/planner/logical_plan.rs` (left relation, right relation, join spec, join keys) But keep the logic in a separate module.
3.2 Update translation in `c:/code/kionas/kionas/src/planner/translate.rs` to route between non-join logical plan and join logical plan.
3.3 Preserve deterministic SQL-to-logical parity and existing LIMIT/SORT field behavior for both paths.
3.4 Add logical translation tests to a tests module for JOIN and non-JOIN parity. *depends on 3.1/3.2*

4. Phase D: Physical Translation And Distributed Staging
4.1 Create a new file for this logic and wire in `c:/code/kionas/kionas/src/planner/physical_plan.rs` JOIN operators to carry explicit join type and multi-key pair metadata. 
4.2 Update `c:/code/kionas/kionas/src/planner/physical_translate.rs` to emit JOIN pipeline for join queries while keeping current linear pipeline for non-join queries.
4.3 Emit distributed stages in `c:/code/kionas/kionas/src/planner/distributed_plan.rs` and `c:/code/kionas/kionas/src/planner/physical_translate.rs` for JOIN path using exchange operators and explicit stage dependencies.
4.4 Define deterministic exchange strategy for Phase 5 (single selected strategy or documented heuristic) and encode it in physical diagnostics.
4.5 Add translation tests to a tests module validating operator order, stage dependencies, and payload determinism.

5. Phase E: Validation Gate Expansion
5.1 Replace JOIN unsupported hard-stops in `c:/code/kionas/kionas/src/planner/physical_validate.rs` with constrained allow rules for INNER equi multi-key JOIN.
5.2 Add invariants: one JOIN operator max in phase scope, valid key references on both sides, JOIN placement relative to projection/sort/limit, and exchange operator consistency.
5.3 Update distributed validation in `c:/code/kionas/kionas/src/planner/distributed_validate.rs` for JOIN stage dependency correctness.
5.4 Mirror the same constraints in worker payload validation at `c:/code/kionas/worker/src/services/query.rs`.
5.5 Add negative tests to a tests module for disallowed JOIN shapes and invalid pipeline ordering.

6. Phase F: Worker Distributed JOIN Runtime
6.1 Create new file for this logic and wire in runtime plan extraction in `c:/code/kionas/worker/src/services/query_execution.rs` to parse JOIN and exchange operators.
6.2 Implement constrained JOIN execution path (INNER equi multi-key) with deterministic row construction and null/equality handling.
6.3 Integrate stage input loading for JOIN sides from exchange artifacts and maintain deterministic stage execution order.
6.4 Preserve existing post-join sort/limit/materialize semantics by reusing current operator order contracts.
6.5 Add runtime tests to a tests module for happy path joins, empty-side behavior, unmatched keys, duplicate-key amplification, and key type mismatch validation.

7. Phase G: Explainability And Diagnostics
7.1 Extend explain output in `c:/code/kionas/kionas/src/planner/explain.rs` to render JOIN operator details (join type, key pairs, exchange strategy).
7.2 Ensure payload diagnostics in `c:/code/kionas/kionas/src/sql/query_model.rs` include JOIN and distributed pipeline text.
7.3 Add tests to a tests module asserting deterministic explain/diagnostic output for JOIN queries.

8. Phase H: Integration Verification And Signoff
8.1 Manual E2E query set for matrix evidence:
- INNER JOIN single-key baseline
- INNER JOIN multi-key equality
- JOIN + WHERE + ORDER BY
- JOIN + ORDER BY + LIMIT/OFFSET
- no-match JOIN returning zero rows
- rejected non-equi JOIN
- rejected outer JOIN
8.2 Run quality gates after implementation:
- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo check` (respect existing Windows/docker project convention when used)
8.3 Add tests to a tests module  required by phase but execute test suite according current project policy.
8.4 Complete `c:/code/kionas/roadmaps/ROADMAP2_PHASE5_MATRIX.md` with evidence links and mark signoff only when all mandatory criteria are Done.

**Relevant files**
- `c:/code/kionas/roadmaps/ROADMAP2.md` — Phase 5 scope and sequencing authority.
- `c:/code/kionas/roadmaps/dive-in/roadmap2_divein_discovery_intermission_4-5.md` — intermission decision basis and scope constraints.
- `c:/code/kionas/roadmaps/ROADMAP2_PHASE5_MATRIX.md` — Phase 5 completion gate artifact to create.
- `c:/code/kionas/kionas/src/sql/query_model.rs` — JOIN extraction boundary and payload diagnostics.
- `c:/code/kionas/kionas/src/planner/logical_plan.rs` — logical JOIN representation.
- `c:/code/kionas/kionas/src/planner/translate.rs` — query model to logical routing.
- `c:/code/kionas/kionas/src/planner/physical_plan.rs` — JOIN operator contract and metadata.
- `c:/code/kionas/kionas/src/planner/physical_translate.rs` — JOIN physical pipeline emission.
- `c:/code/kionas/kionas/src/planner/physical_validate.rs` — planner-side JOIN and pipeline validation.
- `c:/code/kionas/kionas/src/planner/distributed_plan.rs` — stage graph for distributed JOIN path.
- `c:/code/kionas/kionas/src/planner/distributed_validate.rs` — distributed DAG validation invariants.
- `c:/code/kionas/kionas/src/planner/explain.rs` — explainability for JOIN diagnostics.
- `c:/code/kionas/worker/src/services/query.rs` — worker payload/operator validation mirror.
- `c:/code/kionas/worker/src/services/query_execution.rs` — distributed runtime JOIN execution.

**Verification**
1. Every new JOIN capability has dual validation: planner-side and worker-side.
2. Non-join regression check confirms legacy SELECT path remains unchanged.
3. Distributed plan validation confirms acyclic stage dependencies and exchange/operator consistency.
4. Runtime evidence includes both successful JOIN execution and rejection of out-of-scope JOIN shapes.
5. Matrix gate verifies all mandatory criteria are Done with concrete evidence.

**Decisions**
- Sequence: JOIN first, GROUP second (from intermission).
- Phase 5 includes distributed JOIN path, not only local single-stage execution.
- Supported JOIN shape for Phase 5: INNER equi-join with multi-key predicates.
- Excluded in Phase 5: outer/non-equi joins, multi-join optimization, broad predicate-language expansion.

**Further Considerations**
1. Exchange strategy default for Phase 5 should be locked explicitly (single strategy vs heuristic) before coding starts to avoid planner/runtime divergence.
2. If distributed JOIN complexity threatens phase schedule, maintain vertical integrity by delivering one deterministic distributed strategy first and defer alternatives.
3. GROUP phase should reuse finalized JOIN payload/operator contracts to avoid rework in Phase 6.
