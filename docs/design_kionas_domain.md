Kionas Domain Model — Investigation Notes

Goal
- Define core ecosystem objects and business rules to drive statement handling, task construction, metastore persistence, and worker dispatch.

Core objects

1) KionasCatalog
- Responsibility: logical catalog of schemas and catalogs across the cluster.
- Key fields: id (uuid), name, owner, created_at, properties (map).
- Business rules: name uniqueness, owner permissions, immutable id, validation for reserved names.
- Actions: create_catalog(), drop_catalog(), rename_catalog(), list_tables().
- Produces: metastore requests (CreateCatalogRequest), may generate follow-up Tasks (e.g., create remote storage).

2) KionasTable
- Responsibility: logical table representation and metadata (columns, engine, location, partitions).
- Key fields: id, name, catalog/schema, columns[], partitioning, engine, properties.
- Business rules: validate column types, ensure engine supports features, idempotent CREATE if-if-not-exists semantics.
- Actions: create_table(), drop_table(), alter_table(), add_column().
- Produces: TaskRequests for workers (e.g., initialize table files), Metastore requests for persistence.

3) KionasWarehouse
- Responsibility: runtime execution target (worker instance/pool) with connection, TLS, and capacity metadata.
- Key fields: id, name, host, port, ca_cert, last_heartbeat, capacity metrics.
- Business rules: selection strategy (affinity, round-robin, least-loaded), TLS validation, pool lifecycle.
- Actions: register_worker(), heartbeat(), allocate_task().

Design principles / responsibilities
- Single source of truth: metastore persists canonical metadata; workers provide execution results (locations).
- Separation: domain objects own validation and mapping logic; TaskManager owns lifecycle and state; statement_handler orchestrates domain + tasks.
- Idempotency: all create/drop flows should be safe to re-run; operations should detect and short-circuit no-ops.
- Observability: domain operations log intent, inputs, and generated Task IDs and Metastore requests.

Interfaces and hooks
- Define trait ResourceDomain with methods:
  - validate(&self) -> Result<()>
  - build_task_request(&self) -> TaskRequest
  - build_metastore_request(&self) -> MetastoreRequest
  - post_apply(&self, TaskResponse, MetastoreResponse)
  - original_ast<T>(&self, ast: <T>)

- statement_handler flow (refactor):
  1) Parse SQL → build domain object (`KionasTable::from_ast(create_table_ast)`)
  2) domain.validate()
  3) task_id = TaskManager.create_task(...)
  4) conn = acquire pooled worker
  5) send TaskRequest (domain.build_task_request())
  6) wait for TaskUpdate via notifier
  7) if succeeded → domain.build_metastore_request() → MetastoreClient.execute()
  8) domain.post_apply(...) for any follow-ups

Examples
- CREATE SCHEMA
  - Build `KionasCatalog` with name/owner
  - No heavy worker task required in simple cases, but still model as domain operation -> may produce lightweight Task or direct metastore call depending on rules.

- CREATE TABLE
  - Build `KionasTable` with name, columns
  - Validate columns/engine
  - Build TaskRequest to worker to allocate storage or register schema on worker
  - After worker success, call Metastore Execute to persist table metadata
  - If both succeed, commit, else rollback

Next steps (recommended implementation plan)
1) Draft Rust structs and traits under `server/src/domain/` (KionasCatalog, KionasTable, KionasWarehouse, ResourceDomain trait).
2) Refactor `statement_handler` to use domain builders and call a `DomainService` that encapsulates the flow above.
3) Add unit tests for validation rules and idempotency.
4) Add integration tests covering full flow (task -> worker -> task_update -> metastore).
5) Document mapping from AST → domain objects in `docs/`.

Files to add/modify
- server/src/domain/mod.rs
- server/src/domain/catalog.rs
- server/src/domain/table.rs
- server/src/domain/warehouse.rs
- server/src/services/domain_service.rs (or incorporate into statement_handler/helpers)
- docs/design_kionas_domain.md (this file)

Questions to clarify
- Expected semantics for `IF NOT EXISTS` / `OR REPLACE` across domain objects.
- Which operations must touch workers vs only metastore.
- Access control model (owners/roles) and where to enforce checks.

Created: docs/design_kionas_domain.md
