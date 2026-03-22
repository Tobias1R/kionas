## Plan: DataFusion Providers Module (Catalog + Schema + Table)

Scope is now locked to your selections:
1. ListingTable is the real scan provider.
2. MemTable is test and fallback only.
3. StreamingTable is deferred.
4. This phase includes SELECT plus INSERT and DDL integration.
5. Providers will take name like KionasCatalogProvider, KionasSchemaProvider, KionasTableProvider to distinguish from DataFusion in code and diagnostics.

### Steps
1. Phase 0, boundary lock: keep provider ownership fully in server; keep worker contract unchanged (worker receives only Kionas tasks produced from DataFusion execution-plan intent).
2. Phase 1, provider domain layer: create a shared provider metadata model (relation, columns, location, nullability, types), plus one shared identifier normalization utility used by planner, SELECT, INSERT, and DDL.
3. Phase 1, metastore resolver: centralize table and schema fetch logic into one resolver service with optional short-lived cache and explicit invalidation hook points for DDL.
- Any connection to metastore must be pooled by deadpool.
4. Phase 2, provider builders: implement a provider context builder that registers catalog, schema, and table providers into SessionContext deterministically; ListingTable is primary, MemTable is explicit fallback path only.
5. Phase 2, type mapping: add a single metastore-type to Arrow/DataFusion mapping policy so provider schema and INSERT constraint/type hints share the same source of truth.
6. Phase 3, planner wiring: refactor planner engine to consume provider context APIs instead of assembling MemoryCatalogProvider and MemorySchemaProvider inline.
7. Phase 4, SELECT integration: replace direct relation column loading in SELECT flow with resolver-driven relation sets and one provider context per query; reuse resolved metadata for diagnostics and relation column payloads.
8. Phase 4, INSERT integration: replace INSERT-specific metadata lookup internals with the shared resolver model while preserving current insert_payload_json and constraint/type-hint behavior.
9. Phase 4, DDL integration: route identifier normalization and namespace existence checks through shared provider utilities for create database, create schema, and create table handlers.
10. Phase 5, validation and signoff: add dedicated provider tests (normalization, type mapping, ListingTable construction, context registration, fallback semantics), then run fmt, clippy, and check, and update roadmap matrix evidence.

### Parallelism and Dependencies
1. Phase 1 and Phase 2 can run in parallel after boundary lock.
2. Phase 3 depends on Phase 1 and Phase 2.
3. Phase 4 depends on Phase 3.
4. Phase 5 depends on completed Phase 4 wiring.

### Relevant files
- [server/src/planner/engine.rs](server/src/planner/engine.rs)
- [server/src/planner/mod.rs](server/src/planner/mod.rs)
- [server/src/statement_handler/query/select.rs](server/src/statement_handler/query/select.rs)
- [server/src/statement_handler/dml/insert.rs](server/src/statement_handler/dml/insert.rs)
- [server/src/statement_handler/ddl/create_table.rs](server/src/statement_handler/ddl/create_table.rs)
- [server/src/statement_handler/ddl/create_schema.rs](server/src/statement_handler/ddl/create_schema.rs)
- [server/src/statement_handler/ddl/create_database.rs](server/src/statement_handler/ddl/create_database.rs)
- [server/src/services/metastore_client.rs](server/src/services/metastore_client.rs)
- [kionas/proto/metastore_service.proto](kionas/proto/metastore_service.proto)
- [roadmaps/ROADMAP_DATAFUSION_PHASE1_MATRIX.md](roadmaps/ROADMAP_DATAFUSION_PHASE1_MATRIX.md)

### Verification
1. Static: planner no longer contains duplicated provider registration and duplicated identifier normalization helpers.
2. Functional SELECT: metastore-backed relations plan via ListingTable and still translate to Kionas worker tasks.
3. Functional INSERT: constraint and type-hint contracts remain stable and are sourced from shared resolver metadata.
4. Functional DDL: namespace and object validation behavior remains deterministic with clearer error taxonomy.
5. Regression: diagnostics payload remains present; worker transport remains DataFusion-free.
6. Quality gates: cargo fmt --all, cargo clippy --all-targets --all-features -- -D warnings, cargo check.

Plan has been saved and synced in session memory at /memories/session/plan.md.
