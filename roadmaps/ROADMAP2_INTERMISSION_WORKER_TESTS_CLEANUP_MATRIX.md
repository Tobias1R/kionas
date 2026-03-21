# Intermission Worker Tests Cleanup Completion Matrix

## Scope
Intermission scope from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Move all worker tests to worker/src/tests.

## Completion Matrix
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Inventory all inline worker test modules | Done | [worker/src/execution/query.rs](worker/src/execution/query.rs), [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs), [worker/src/flight/server.rs](worker/src/flight/server.rs), [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs), [worker/src/authz.rs](worker/src/authz.rs), [worker/src/storage/exchange.rs](worker/src/storage/exchange.rs), [worker/src/execution/aggregate/mod.rs](worker/src/execution/aggregate/mod.rs) | Inventory captured the full migration scope before relocation. |
| Create worker/src/tests structure and relocate test modules | Done | [worker/src/tests/authz_tests.rs](worker/src/tests/authz_tests.rs), [worker/src/tests/storage_exchange_tests.rs](worker/src/tests/storage_exchange_tests.rs), [worker/src/tests/execution_query_tests.rs](worker/src/tests/execution_query_tests.rs), [worker/src/tests/execution_pipeline_tests.rs](worker/src/tests/execution_pipeline_tests.rs), [worker/src/tests/execution_aggregate_mod_tests.rs](worker/src/tests/execution_aggregate_mod_tests.rs), [worker/src/tests/flight_server_tests.rs](worker/src/tests/flight_server_tests.rs), [worker/src/tests/services_query_execution_tests.rs](worker/src/tests/services_query_execution_tests.rs), [worker/src/tests/transactions_maestro_uri_tests.rs](worker/src/tests/transactions_maestro_uri_tests.rs), [worker/src/tests/transactions_maestro_insert_tests.rs](worker/src/tests/transactions_maestro_insert_tests.rs) | Test files are now centralized under worker/src/tests. |
| Replace inline test blocks with path-based modules wired to worker/src/tests | Done | [worker/src/authz.rs](worker/src/authz.rs), [worker/src/storage/exchange.rs](worker/src/storage/exchange.rs), [worker/src/execution/query.rs](worker/src/execution/query.rs), [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs), [worker/src/execution/aggregate/mod.rs](worker/src/execution/aggregate/mod.rs), [worker/src/flight/server.rs](worker/src/flight/server.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs), [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs) | All inline `mod tests` blocks were replaced with `#[path = ...]` test modules. |
| Compile and lint validation after test relocation | Done | `cargo fmt --all; cargo check -p worker; cargo clippy -p worker --all-targets --all-features -- -D warnings`; `cargo clippy -p kionas --all-targets --all-features --no-deps -- -D warnings` | Worker and dependency lint gates now pass after resolving strict clippy blockers. |
| Optional hardening: split oversized test modules by domain under worker/src/tests | Deferred | N/A | Non-blocking for initial relocation. |
| Optional hardening: unify shared test fixtures into worker/src/tests/support | Deferred | N/A | Non-blocking and can be done after migration lands. |

Status values:
- `Done`
- `Deferred`
- `Blocked`
- `Not Started`

## Signoff Decision
- Intermission signoff: `Approved`
- Blocking items:
  - None. All mandatory criteria are marked `Done`.

## Gate Checklist
1. All mandatory criteria are marked `Done`.
2. Each `Done` item includes concrete evidence reference.
3. `Deferred` items include rationale and are explicitly non-blocking.
4. Final signoff decision is recorded in this file.

## Environment and Parameters
- Environment variables expected for this intermission:
  - None new. Existing worker runtime variables remain unchanged.

- Parameters expected for this intermission:
  - Structural test relocation only; no behavior changes to worker execution paths.
  - Existing worker contracts and query semantics must remain unchanged.
