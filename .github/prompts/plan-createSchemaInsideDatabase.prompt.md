## Plan: Server-Owned CREATE SCHEMA Inside Database

Implement a server-first CREATE SCHEMA flow for hierarchical namespaces (`catalog_or_database.schema.object`) that validates parent database existence and schema non-existence in metastore, dispatches a typed worker task to initialize schema folder state under the database path, persists metastore metadata only after worker success, and returns deterministic validation/infra outcomes.

**Steps**
1. Discovery and baseline lock
- Confirm the current CREATE SCHEMA branch is still inline in statement handler and only uses `schema_name` while ignoring the rest of AST fields.
- Confirm current metastore schema contract only accepts `schema_name` and does not model parent database/catalog.
- Confirm worker has `create_database` execution but no `create_schema` operation branch yet.

2. Namespace model and AST handling contract
- Treat CREATE SCHEMA target as hierarchical namespace where schema belongs to a parent database/catalog.
- Define deterministic extraction rules from `SchemaName` for inputs like `db.schema`
- Preserve all CREATE SCHEMA AST fields in the worker payload for forward compatibility (`if_not_exists`, `with`, `options`, `default_collate_spec`, `clone`, etc.), even if initial behavior uses a subset.

3. Server orchestration module (source of truth)
- Move CREATE SCHEMA orchestration out of the large statement handler match into a dedicated module under statement_handler, mirroring CREATE DATABASE architecture.
- Validate normalized database/schema names.
- Query metastore for parent database first:
- Missing parent database => validation error `DATABASE_NOT_FOUND`.
- Query metastore for schema existence next:
- Existing schema => validation error `SCHEMA_ALREADY_EXISTS` (or success-skip when IF NOT EXISTS).
- Unexpected metastore failures => infra error and stop before worker dispatch.

4. Worker task dispatch design
- Keep SQL interpretation in server only; worker receives typed operation payload (`operation=create_schema`).
- Build payload from full AST plus canonicalized namespace parts so worker can deterministically derive storage path.
- Ensure path contract is explicit: schema marker/folder must live under parent database path, not at root.

5. Worker create_schema physical initialization
- Add worker handling for `create_schema` operation.
- Parse typed payload and validate path invariants (`database` + `schema` are present and valid).
- Create idempotent schema marker under database path in object storage (and local fallback path).
- Return deterministic success location; return explicit error for invalid payload/path invariants.

6. Metastore persistence sequencing
- Persist metastore CreateSchema only after worker success.
- During create race between pre-check and create, map conflict to `SCHEMA_ALREADY_EXISTS` deterministically.
- If metastore write fails after worker success, return explicit partial-failure infra message and log correlation fields (session, database, schema, worker location).
- Persist all params from AST in metastore for future-proofing, even if initial implementation only uses a subset.

7. Response contract and mapping
- Reuse structured statement outcome pattern already used for CREATE DATABASE (`RESULT|category|code|message`).
- Ensure QueryResponse always carries meaningful message and deterministic status/error_code:
- Success => `status=OK`.
- Validation (`DATABASE_NOT_FOUND`, `SCHEMA_ALREADY_EXISTS`, invalid name) => business error code.
- Infra/transport/protocol failures => infra error code.

8. Concurrency and race handling
- Maintain check-then-create sequence for user clarity.
- Treat metastore uniqueness conflict on CreateSchema as expected race, not generic infra failure.
- Keep worker schema path creation idempotent so duplicate dispatch attempts do not corrupt storage state.

9. Verification plan[User will execute manually]
- Case A: first `create schema db1.sc1;` => worker schema folder/marker created under `db1` + metastore row created + OK response.
- Case B: second same command => no duplicate side effects + deterministic `SCHEMA_ALREADY_EXISTS` response.
- Case C: `create schema missing_db.sc1;` => validation `DATABASE_NOT_FOUND`, no worker dispatch.
- Case D: worker failure => metastore not written.
- Case E: simulated metastore conflict after worker success => deterministic duplicate response.

**Relevant files**
- c:/code/kionas/server/src/statement_handler/mod.rs — replace inline CREATE SCHEMA orchestration with module delegation
- c:/code/kionas/server/src/statement_handler/helpers.rs — reuse existing task dispatch helpers
- c:/code/kionas/server/src/statement_handler — add dedicated CREATE SCHEMA orchestration module following CREATE DATABASE structure
- c:/code/kionas/server/src/services/warehouse_service_server.rs — reuse existing structured outcome mapping
- c:/code/kionas/kionas/proto/metastore_service.proto — extend/create schema contract with parent database context
- c:/code/kionas/metastore/src/services/metastore_service.rs — route updated schema actions
- c:/code/kionas/metastore/src/services/actions/mod.rs — register updated/added schema actions
- c:/code/kionas/metastore/src/services/provider/postgres.rs — implement parent-aware schema get/create and conflict semantics
- c:/code/kionas/worker/src/transactions/maestro.rs — add `create_schema` operation execution branch
- c:/code/kionas/worker/src/services/mod.rs — register create_schema service module
- c:/code/kionas/worker/src/services/worker_service_server.rs — keep task endpoint delegating through maestro path
- c:/code/kionas/worker/src/services — add schema task executor mirroring create_database marker model

**Verification**
1. Compile checks:
- `cargo check -p worker`
- `cargo check -p metastore`
- `cargo check -p server` (or Docker-based command on Windows if local rdkafka toolchain blocks)
2. Lint/format checks:
- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings` (may expose unrelated existing workspace warnings)
3. Integration checks:
- submit CREATE SCHEMA statements via warehouse query endpoint and assert response codes/messages and storage/metastore side effects
4. Log validation:
- verify correlation across server orchestration, worker execution, and metastore persistence for each case

**Decisions**
- Included: parent database validation, full-AST payload preservation, worker physical schema initialization, deterministic duplicate/not-found mapping.
```rust
 CreateSchema {
        /// `<schema name> | AUTHORIZATION <schema authorization identifier>  | <schema name>  AUTHORIZATION <schema authorization identifier>`
        schema_name: SchemaName,
        if_not_exists: bool,
        /// Schema properties.        
        with: Option<Vec<SqlOption>>,
        /// Schema options.        
        options: Option<Vec<SqlOption>>,
        /// Default collation specification for the schema.        
        default_collate_spec: Option<Expr>,
        /// Clones a schema       
        clone: Option<ObjectName>,
    },
```
- Excluded: broad parser grammar redesign and cross-object namespace redesign beyond CREATE SCHEMA flow.
- Rule: worker executes typed task payload only; server remains SQL interpretation authority.

**Further considerations**
1. Backward compatibility for existing metastore schema records:
- Option A: migrate to explicit `(database_name, schema_name)` model
- Option B: transitional dual-read with default database fallback
2. Multipart namespace extraction policy:
- Option A: use last two parts as `database.schema`
- Option B: require explicit two-part form and reject ambiguous names
