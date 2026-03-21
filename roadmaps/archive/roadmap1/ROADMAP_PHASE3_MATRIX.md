# Phase 3 Completion Matrix

## Scope
Phase 3 in [ROADMAP.md](ROADMAP.md) focuses on local query execution on worker, deterministic artifacts, retrieval compatibility, and contract stability.

## Completion Matrix

| Item | Status | Evidence | Notes |
|---|---|---|---|
| 4.1 Local worker execution replaces query stub | Done | [worker/src/services/query.rs](worker/src/services/query.rs), [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Query path now executes scan, optional filter, projection, and materialization pipeline for supported shapes. |
| 4.2 Deterministic query artifacts per task | Done | [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs), [worker/src/flight/server.rs](worker/src/flight/server.rs) | Artifacts written under task-scoped staging path with deterministic keying. |
| 4.3 Result metadata persisted in stable sidecar shape | Done | [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) | Sidecar includes row count, source batch count, parquet file count, schema columns, and artifact integrity metadata. |
| 4.4 Keep create and insert behavior stable | Done | [worker/src/services/query.rs](worker/src/services/query.rs), [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs) | Existing operation routing and behavior retained; query changes isolated to query flow. |
| Retrieval determinism positive path | Done | Runtime evidence from user verification logs in this phase | Repeated DoGet for same handle returned deterministic payload count and fingerprint. |
| Retrieval integrity negative path (tamper detection) | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs), runtime evidence from user tamper test logs | Tampered metadata checksum correctly rejected with failed precondition and checksum mismatch. |
| Validation contract stability for unsupported query shapes | Done | [server/src/statement_handler/query_select.rs](server/src/statement_handler/query_select.rs), [server/src/services/warehouse_service_server.rs](server/src/services/warehouse_service_server.rs) | Unsupported JOIN still maps to BUSINESS_UNSUPPORTED_QUERY_SHAPE. |
| Metadata schema strictness checks (name, type, nullability, order) | Done | [worker/src/flight/server.rs](worker/src/flight/server.rs) | Retrieval validates exact schema metadata alignment against decoded batches. |
| Repair or reset helper for intentionally tampered task artifacts | Deferred | N/A | Optional developer convenience utility; not required for Phase 3 acceptance. |
| Metadata authenticity signing (for example HMAC or signature) | Deferred | N/A | Optional hardening beyond consistency and checksum checks. |

## Signoff Decision
Phase 3 is accepted for current roadmap scope with mandatory criteria marked Done.

## Standardized Gate Rule
For every roadmap-derived implementation plan:
1. Produce this matrix format with status and evidence.
2. Require all mandatory items to be Done before signoff.
3. List non-blocking hardening items as Deferred with clear rationale.
