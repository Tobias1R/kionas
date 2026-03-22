# EP-4 Completion Matrix

## Scope
EP-4 scope from [roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md](roadmaps/SILK_ROAD/DATAFUSION/ROADMAP_EXECUTION.md):
- Expanded join and aggregate runtime envelope with deterministic correctness.
- Null/type edge-case verification for supported semantics.
- Clear unsupported-semantics diagnostics and non-regression verification.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| Targeted join/aggregate capabilities are explicitly enumerated and verified | Done | [roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP4.md](roadmaps/SILK_ROAD/DATAFUSION/discovery/discovery-EXECUTION-EP4.md); [roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md](roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md); [worker/src/tests/services_query_execution_tests.rs](worker/src/tests/services_query_execution_tests.rs); [worker/src/tests/execution_join_tests.rs](worker/src/tests/execution_join_tests.rs); [worker/src/tests/execution_aggregate_mod_tests.rs](worker/src/tests/execution_aggregate_mod_tests.rs) | EP-4 join/aggregate capability envelope is enumerated in discovery/plan artifacts and verified through alias-handling, join envelope, aggregate semantics, and diagnostics tests across worker lib/bin targets. |
| Null/type edge-case behavior is tested and documented in matrix evidence | Done | [roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md](roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md); [worker/src/tests/execution_aggregate_mod_tests.rs](worker/src/tests/execution_aggregate_mod_tests.rs) | Workstream B now includes passing null-semantics and type-diagnostics tests (`aggregate_count_and_sum_apply_null_semantics_per_group`, `aggregate_rejects_non_numeric_input_type_for_sum`) on worker lib/bin targets. |
| Unsupported semantics fail clearly with actionable diagnostics | Done | [roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md](roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md#L207); [worker/src/tests/execution_join_tests.rs](worker/src/tests/execution_join_tests.rs#L122); [worker/src/tests/execution_join_tests.rs](worker/src/tests/execution_join_tests.rs#L133); [worker/src/tests/execution_aggregate_mod_tests.rs](worker/src/tests/execution_aggregate_mod_tests.rs#L87) | Join and aggregate unsupported-semantics diagnostics are now evidenced by passing negative-path tests (`join_rejects_spec_with_empty_keys`, `join_rejects_spec_with_empty_right_relation`, `aggregate_rejects_predicate_input_expression`). |
| Added runtime support does not regress existing covered scenarios | Done | [roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md](roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md); [worker/src/tests/execution_join_tests.rs](worker/src/tests/execution_join_tests.rs); [roadmaps/ROADMAP_EXECUTION_EP3_MATRIX.md](roadmaps/ROADMAP_EXECUTION_EP3_MATRIX.md) | Covered join non-regression scenarios are now evidenced by passing `join_keeps_deterministic_match_order`, `join_renames_duplicate_right_columns`, and `join_with_no_matches_returns_empty_batch_with_expected_schema` on worker lib/bin targets. |
| Quality gate evidence captured (fmt, clippy, check) | Done | [roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md](roadmaps/SILK_ROAD/DATAFUSION/plans/plan-EXECUTION-EP4.md) | Full-workspace EP-4 quality-gate commands are complete: `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo check` all passed. |
| Optional hardening: compatibility matrix for query-shape coverage | Deferred | N/A | Non-blocking for EP-4 mandatory signoff. |
| Optional hardening: shadow-run checks for selected aggregate boundary scenarios | Deferred | N/A | Non-blocking for EP-4 mandatory signoff. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Phase signoff: `Done`
- Blocking items:
  - None.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected in this phase:
  - Existing runtime configuration variables are used as-is unless EP-4 runtime validation requires explicit phase-run variables, to be listed during implementation.
- Parameters expected in this phase:
  - Join and aggregate phase-run parameters and validation toggles will be documented during implementation once capability slices are locked.
  - Test-filter and scenario-selection parameters for EP-4 evidence capture will be documented with concrete command references.
