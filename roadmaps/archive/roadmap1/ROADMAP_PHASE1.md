## Plan: Phase 1 AST to Logical Plan Foundation

Deliver a Phase 1-only foundation that converts minimal SELECT AST into a typed, validated logical plan in the shared kionas crate, then serializes that plan as the canonical v2 query payload consumed by existing server query dispatch. This phase intentionally excludes worker execution changes and focuses on correctness, stability, explainability, and contributor-ready boundaries.

## Scope
- In scope: minimal SELECT only (single table scan, projection list, optional WHERE filter).
- In scope: hard switch to v2 payload for query path.
- In scope: typed Rust logical model plus serde JSON serialization.
- Out of scope: joins, aggregates, GROUP BY, ORDER BY, worker execution behavior, Flight retrieval changes.

## Milestones
1. Scope Lock and Contracts
- Freeze the accepted SQL subset for this phase.
- Freeze payload contract version and required fields for v2 logical-plan envelope.
- Freeze explicit rejection behavior for unsupported SQL shapes.

2. Shared Planner Module in kionas
- Create planner module boundary in kionas/src/planner.
- Add typed logical model for plan root, relation node, projection expressions, and predicate expressions.
- Add planner error taxonomy with actionable messages.
- Implement AST to logical translation for minimal SELECT.
- Add deterministic normalization rules for namespace defaults, identifier casing, and stable projection ordering.
- Add explain outputs (human-readable text and stable JSON).

3. Server Integration
- Replace ad hoc query payload construction in statement handler with planner-produced logical plan v2 payload.
- Keep existing statement routing and task dispatch flow unchanged.
- Preserve existing outcome response shape and query handle mapping.
- Keep query logs compact; no full AST or oversized payload at info level.

4. Validation and Tests
- Add planner unit tests for success and rejection paths.
- Add server query handler tests that assert v2 logical payload emission and dispatch continuity.
- Add deterministic explain snapshot-style tests.
- Add compatibility assertions for unchanged dispatch metadata beyond payload-body transition.

5. Completion Gate
- Minimal SELECT reliably translates to typed logical plan and emits v2 payload through existing query dispatch.
- Unsupported SQL is rejected early with clear validation-grade messages.
- Explain output is available and deterministic for debugging.
- No worker-side code changes are required to close Phase 1.

## Work Breakdown
1. Module scaffolding
- Add kionas/src/planner/mod.rs
- Add kionas/src/planner/logical_plan.rs
- Add kionas/src/planner/translate.rs
- Add kionas/src/planner/validate.rs
- Add kionas/src/planner/explain.rs
- Add kionas/src/planner/error.rs
- Export planner from kionas/src/lib.rs

2. Translator slice
- Map DataFusion AST query body to logical relation node.
- Map projection list to typed expression variants.
- Map WHERE predicate into typed filter expression variants.
- Reject unsupported clauses with explicit planner errors.

3. Validation slice
- Validate one relation source for Phase 1.
- Validate expression subset and operand compatibility.
- Normalize identifiers and namespace defaults deterministically.

4. Server seam
- Call planner API inside server statement_handler query path.
- Serialize validated logical plan to v2 payload.
- Continue dispatch via existing operation=query path.

5. Tests and quality gates
- Planner unit tests.
- Server integration tests for payload and dispatch behavior.
- cargo fmt --all
- cargo clippy --all-targets --all-features -- -D warnings
- cargo check
- cargo test -- --test-threads=1

## Deliverables
- Dedicated planner module in shared kionas crate.
- v2 logical plan payload as canonical query dispatch contract.
- Deterministic explain output for troubleshooting.
- Test coverage proving correct translation, normalization, and rejection behavior.
- No behavioral regressions for existing create/insert/query dispatch baseline.

## Risks and Mitigations
- Risk: Translation drift between parser assumptions and server payload usage.
- Mitigation: Keep single planner API as only payload producer.

- Risk: Non-deterministic explain or JSON serialization causing flaky tests.
- Mitigation: Enforce stable ordering and canonical normalization in validation.

- Risk: Scope creep into physical planning or worker execution.
- Mitigation: Hard out-of-scope guardrails and explicit unsupported-feature errors.

## Relevant Files
- kionas/src/lib.rs
- kionas/src/parser/sql.rs
- kionas/src/planner/mod.rs
- kionas/src/planner/logical_plan.rs
- kionas/src/planner/translate.rs
- kionas/src/planner/validate.rs
- kionas/src/planner/explain.rs
- kionas/src/planner/error.rs
- server/src/statement_handler/query_select.rs
- server/src/statement_handler/mod.rs
- server/src/statement_handler/helpers.rs

## Acceptance Criteria
1. Given supported minimal SELECT, planner returns typed logical plan and server dispatches v2 payload.
2. Given unsupported SQL feature, planner returns actionable validation error and dispatch is not attempted.
3. Explain text and JSON outputs are deterministic across runs.
4. Repository quality gates pass for modified crates.
