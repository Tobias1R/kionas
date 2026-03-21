# Intermission Server Cleanup Completion Matrix

## Scope
Intermission scope from [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md):
- Confirm and remove dead code under server/src/core.
- Move statement-specific handlers from server/src/statement_handler into focused modules (ddl, dml, query, shared, utility).
- Move server inline tests to server/src/tests.

## Completion Matrix
| Item | Status | Evidence | Notes |
|---|---|---|---|
| Dead core module is removed | Done | [server/src/main.rs](server/src/main.rs), [server/src](server/src) | core module import remains commented and server/src/core has no remaining files. |
| Statement handlers are reorganized by ownership | Done | [server/src/statement_handler/mod.rs](server/src/statement_handler/mod.rs), [server/src/statement_handler/ddl/mod.rs](server/src/statement_handler/ddl/mod.rs), [server/src/statement_handler/dml/mod.rs](server/src/statement_handler/dml/mod.rs), [server/src/statement_handler/query/mod.rs](server/src/statement_handler/query/mod.rs), [server/src/statement_handler/shared/mod.rs](server/src/statement_handler/shared/mod.rs), [server/src/statement_handler/utility/mod.rs](server/src/statement_handler/utility/mod.rs) | Dispatch and imports are rewired to grouped modules. |
| Server inline tests are moved to server/src/tests | Done | [server/src/tests/tasks_mod_tests.rs](server/src/tests/tasks_mod_tests.rs), [server/src/tests/services_warehouse_service_server_tests.rs](server/src/tests/services_warehouse_service_server_tests.rs), [server/src/tests/statement_handler_shared_distributed_dag_tests.rs](server/src/tests/statement_handler_shared_distributed_dag_tests.rs), [server/src/tests/statement_handler_query_select_tests.rs](server/src/tests/statement_handler_query_select_tests.rs) | Original inline test blocks replaced by path-based test modules. |
| Optional hardening: remove transition comments/legacy references in roadmap Relevant Files section | Deferred | N/A | Non-blocking documentation cleanup. |
| Optional hardening: workspace-wide strict clippy remediation outside server scope | Deferred | N/A | Non-blocking for this intermission; failures are in non-server crates. |

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
  - None new. Existing server/runtime variables remain unchanged.

- Parameters expected for this intermission:
  - Structural cleanup only; no query semantics changes.
  - Compatibility preserved by dispatch rewiring within server/src/statement_handler/mod.rs.
