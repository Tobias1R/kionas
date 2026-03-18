## Plan: Server-Owned CREATE DATABASE Flow

Implement a server-first CREATE DATABASE flow that validates existence in metastore, instructs worker to create physical folder state, persists metastore metadata only after successful worker task completion, and returns deterministic duplicate/validation errors.

**Steps**
1. Discovery and baseline mapping
- Confirm current parser branch for `Statement::CreateDatabase` in `server/src/statement_handler/mod.rs` and currently returned message/status behavior in `server/src/services/warehouse_service_server.rs`.
- Confirm worker task handling for operation `create_database` in `worker/src/services/worker_service_server.rs` and whether path creation is implemented or simulated.
- Confirm metastore API semantics for `GetDatabase` and `CreateDatabase` in `kionas/proto/metastore_service.proto` and corresponding handlers.
- We have to use all components of Statement::CreateDatabase in the AST to ensure we can support all features in the future, even if we don't implement them all in the first iteration. This means we need to include fields like `if_not_exists`, `location`, `or_replace`, etc. in our initial implementation, even if we only use a subset of them for the first version of the feature. This will allow us to easily add support for additional features later without having to change the AST structure.
```rust
/// ```sql
    /// CREATE DATABASE
    /// ```   
    CreateDatabase {
        db_name: ObjectName,
        if_not_exists: bool,
        location: Option<String>,
        managed_location: Option<String>,
        or_replace: bool,
        transient: bool,
        clone: Option<ObjectName>,
        data_retention_time_in_days: Option<u64>,
        max_data_extension_time_in_days: Option<u64>,
        external_volume: Option<String>,
        catalog: Option<String>,
        replace_invalid_characters: Option<bool>,
        default_ddl_collation: Option<String>,
        storage_serialization_policy: Option<StorageSerializationPolicy>,
        comment: Option<String>,
        catalog_sync: Option<String>,
        catalog_sync_namespace_mode: Option<CatalogSyncNamespaceMode>,
        catalog_sync_namespace_flatten_delimiter: Option<String>,
        with_tags: Option<Vec<Tag>>,
        with_contacts: Option<Vec<ContactEntry>>,
    },
```
- persist the full AST in the worker task payload for future-proofing, even if we only use `db_name` and `if_not_exists` for the initial implementation. This way we can easily add support for additional features later without having to change the worker task payload structure.

2. Server validation first (source of truth)
- Add a `Statement::CreateDatabase` branch in `server/src/statement_handler/mod.rs`.
- Resolve normalized database name from AST `ObjectName`.
- Call metastore `GetDatabase` before dispatching worker task:
- If database exists: return explicit invalid response message (`database already exists`) and do not dispatch worker task.
- If database not found: continue.
- If metastore call fails unexpectedly: return infrastructure error and stop.

3. Server dispatches worker folder task (not SQL command)
- Keep SQL interpretation in server only; worker receives operation task, not SQL DDL.
- Build worker task using `helpers::run_task_for_input(...)` with `operation = "create_database"` and payload containing canonical database params from AST `CreateDatabase`.
- Ensure payload is structured so worker can deterministically derive target path under storage root.

4. Worker physical initialization for database
- Implement worker handling for `create_database` operation in `worker/src/services/worker_service_server.rs` (or delegated module) to create database folder/marker in object storage.
- Operation should be idempotent at storage layer but return explicit failure if required metadata/path invariants are violated.
- Return task status and location to server through existing response path.

5. Persist metastore only after worker success
- In server `CreateDatabase` flow, call metastore `CreateDatabase` only if worker task succeeds.
- On metastore create conflict/race (between pre-check and create): map to `database already exists` response.
- On metastore failure after worker success: return clear partial-failure message and log with correlation ids.

6. Response contract and error mapping
- Update `server/src/services/warehouse_service_server.rs` to always propagate handler result to `QueryResponse.message`.
- Map outcomes explicitly:
- Success => `status=OK`, descriptive message.
- Validation failure (`already exists`, invalid name) => `status=ERROR`, business error code.
- Transport/infrastructure failure => `status=ERROR`, infra error code.

7. Concurrency and race handling
- Guard against concurrent `CREATE DATABASE xyz` by relying on metastore uniqueness and conflict mapping.
- Keep check-then-create sequence but treat metastore create conflict as expected race, not generic failure.

8. Verification plan
- Case A: First `create database xyz;` => worker folder created + metastore entry created + OK response.
- Case B: Second `create database xyz;` => no worker create dispatch, ERROR response `database already exists`.
- Case C: Worker failure => metastore not written.
- Case D: Simulated metastore conflict after worker success => ERROR response with duplicate message.

**Relevant files**
- `c:/code/kionas/ai/feature/create_database.md` — plan document to hold final refined version.
- `c:/code/kionas/server/src/statement_handler/mod.rs` — add/create `CreateDatabase` orchestration.
- `c:/code/kionas/server/src/statement_handler/helpers.rs` — worker task dispatch helpers.
- `c:/code/kionas/server/src/services/warehouse_service_server.rs` — response message/status mapping.
- `c:/code/kionas/worker/src/services/worker_service_server.rs` — `create_database` task execution.
- `c:/code/kionas/kionas/proto/metastore_service.proto` — metastore contract (`GetDatabase`, `CreateDatabase`).
- `c:/code/kionas/scripts/metastore/init.sql` — uniqueness assumptions and database metadata backing checks.

**Verification**
1. Compile checks: `cargo check -p server`, `cargo check -p worker`, `cargo check -p metastore`.
2. Integration checks:
- User sends `create database xyz;` to server endpoint.
3. Log validation:
- Confirm correlation logs across server dispatch, worker execution, and metastore write.

**Decisions**
- Included: server-owned validation and orchestration, worker physical folder task, duplicate detection behavior.
- Excluded: SQL parser grammar changes, broad catalog semantics beyond current database/database abstraction.
- Rule: worker should not interpret SQL for database creation; it only executes typed operation task payload from server.
  - Task should include all relevant AST fields for future-proofing, even if initial implementation only uses a subset.
- The logic of validating and creating a task should stay in a new mod inside `server/src/statement_handler/` to keep the main handler file clean and focused on orchestration. The worker should only be responsible for executing the task based on the payload it receives, without any SQL interpretation logic.