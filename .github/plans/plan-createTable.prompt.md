## Plan: Server-Owned CREATE TABLE Orchestration

Refactor CREATE TABLE to the same server-owned architecture used by CREATE DATABASE/SCHEMA: strict namespace validation, metastore pre-checks, typed worker payloads, idempotent worker initialization, and deterministic outcome mapping. This removes the current inline flow in statement_handler/mod.rs, avoids blind duplicate creates, and preserves rich AST parameters end-to-end.

**Steps**
1. Phase 1: Baseline alignment and scope lock
1. Confirm current CREATE TABLE branch in server statement handler is inline and currently sends SQL text plus limited metastore fields.
2. Confirm worker CREATE TABLE path still relies on SQL re-parse/legacy path instead of dedicated create_table service execution.
3. Confirm metastore get/create table contract/provider behavior for existence and duplicate semantics.
4. Lock accepted rules from discussion: strict 3-part namespace required (`database.schema.table`), IF NOT EXISTS returns success-skipped, include metastore contract updates.

2. Phase 2: Server orchestration extraction (blocks later phases)
1. Add a dedicated server handler module at server/src/statement_handler/create_table.rs mirroring create_database/create_schema structure.
2. Define borrowed AST wrapper struct for CREATE TABLE with full relevant fields needed for forward compatibility.
3. Implement canonical namespace extraction and validation for strict 3-part names.
4. Implement outcome formatter using RESULT|category|code|message.
5. Implement metastore pre-check sequence before worker dispatch:
- validate parent database exists
- validate parent schema exists
- query table existence
- map existing table to TABLE_ALREADY_EXISTS or TABLE_ALREADY_EXISTS_SKIPPED when IF NOT EXISTS is set
6. Build typed worker payload from normalized namespace + AST-derived params (no SQL interpretation in worker).
7. Dispatch worker operation `create_table` with typed payload.
8. Persist metastore create_table only after worker success.
9. Map duplicate conflict on metastore write to TABLE_ALREADY_EXISTS deterministically; map protocol/transport errors to INFRA codes with partial-failure detail.

3. Phase 3: Statement handler integration
1. Update server/src/statement_handler/mod.rs to declare module create_table.
2. Replace inline Statement::CreateTable branch with delegation to create_table::handle_create_table(...), passing borrowed AST fields.
```rust
/// CREATE TABLE statement.
#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "visitor", derive(Visit, VisitMut))]
pub struct CreateTable {
    pub or_replace: bool,
    pub temporary: bool,
    pub external: bool,
    pub dynamic: bool,
    pub global: Option<bool>,
    pub if_not_exists: bool,
    pub transient: bool,
    pub volatile: bool,
    pub iceberg: bool,
    /// Table name
    #[cfg_attr(feature = "visitor", visit(with = "visit_relation"))]
    pub name: ObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    pub constraints: Vec<TableConstraint>,
    pub hive_distribution: HiveDistributionStyle,
    pub hive_formats: Option<HiveFormat>,
    pub table_options: CreateTableOptions,
    pub file_format: Option<FileFormat>,
    pub location: Option<String>,
    pub query: Option<Box<Query>>,
    pub without_rowid: bool,
    pub like: Option<CreateTableLikeKind>,
    pub clone: Option<ObjectName>,
    pub version: Option<TableVersion>,
    // For Hive dialect, the table comment is after the column definitions without `=`,
    // so the `comment` field is optional and different than the comment field in the general options list.
    // [Hive](https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable)
    pub comment: Option<CommentDef>,
    pub on_commit: Option<OnCommit>,
    /// ClickHouse "ON CLUSTER" clause:
    /// <https://clickhouse.com/docs/en/sql-reference/distributed-ddl/>
    pub on_cluster: Option<Ident>,
    /// ClickHouse "PRIMARY KEY " clause.
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/table/>
    pub primary_key: Option<Box<Expr>>,
    /// ClickHouse "ORDER BY " clause. Note that omitted ORDER BY is different
    /// than empty (represented as ()), the latter meaning "no sorting".
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/table/>
    pub order_by: Option<OneOrManyWithParens<Expr>>,
    /// BigQuery: A partition expression for the table.
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression>
    pub partition_by: Option<Box<Expr>>,
    /// BigQuery: Table clustering column list.
    /// <https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#table_option_list>
    /// Snowflake: Table clustering list which contains base column, expressions on base columns.
    /// <https://docs.snowflake.com/en/user-guide/tables-clustering-keys#defining-a-clustering-key-for-a-table>
    pub cluster_by: Option<WrappedCollection<Vec<Expr>>>,
    /// Hive: Table clustering column list.
    /// <https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-CreateTable>
    pub clustered_by: Option<ClusteredBy>,
    /// Postgres `INHERITs` clause, which contains the list of tables from which
    /// the new table inherits.
    /// <https://www.postgresql.org/docs/current/ddl-inherit.html>
    /// <https://www.postgresql.org/docs/current/sql-createtable.html#SQL-CREATETABLE-PARMS-INHERITS>
    pub inherits: Option<Vec<ObjectName>>,
    /// SQLite "STRICT" clause.
    /// if the "STRICT" table-option keyword is added to the end, after the closing ")",
    /// then strict typing rules apply to that table.
    pub strict: bool,
    /// Snowflake "COPY GRANTS" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub copy_grants: bool,
    /// Snowflake "ENABLE_SCHEMA_EVOLUTION" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub enable_schema_evolution: Option<bool>,
    /// Snowflake "CHANGE_TRACKING" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub change_tracking: Option<bool>,
    /// Snowflake "DATA_RETENTION_TIME_IN_DAYS" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub data_retention_time_in_days: Option<u64>,
    /// Snowflake "MAX_DATA_EXTENSION_TIME_IN_DAYS" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub max_data_extension_time_in_days: Option<u64>,
    /// Snowflake "DEFAULT_DDL_COLLATION" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub default_ddl_collation: Option<String>,
    /// Snowflake "WITH AGGREGATION POLICY" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub with_aggregation_policy: Option<ObjectName>,
    /// Snowflake "WITH ROW ACCESS POLICY" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub with_row_access_policy: Option<RowAccessPolicy>,
    /// Snowflake "WITH TAG" clause
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-table>
    pub with_tags: Option<Vec<Tag>>,
    /// Snowflake "EXTERNAL_VOLUME" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub external_volume: Option<String>,
    /// Snowflake "BASE_LOCATION" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub base_location: Option<String>,
    /// Snowflake "CATALOG" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub catalog: Option<String>,
    /// Snowflake "CATALOG_SYNC" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub catalog_sync: Option<String>,
    /// Snowflake "STORAGE_SERIALIZATION_POLICY" clause for Iceberg tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-iceberg-table>
    pub storage_serialization_policy: Option<StorageSerializationPolicy>,
    /// Snowflake "TARGET_LAG" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub target_lag: Option<String>,
    /// Snowflake "WAREHOUSE" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub warehouse: Option<Ident>,
    /// Snowflake "REFRESH_MODE" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub refresh_mode: Option<RefreshModeKind>,
    /// Snowflake "INITIALIZE" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub initialize: Option<InitializeKind>,
    /// Snowflake "REQUIRE USER" clause for dybamic tables
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-dynamic-table>
    pub require_user: bool,
}
```
3. Keep QueryResponse mapping unchanged by continuing to return structured RESULT outcomes.

4. Phase 4: Worker create_table service parity (parallel with Phase 5 after payload contract is fixed)
1. Add worker/src/services/create_table.rs mirroring create_database/create_schema service style.
2. Parse typed payload JSON and validate required namespace keys (`database`, `schema`, `table`) and naming rules.
3. Validate parent path invariants and derive deterministic storage path under database/schema.
4. Implement idempotent marker behavior for table initialization and deterministic result_location.
5. Perform physical table initialization (delta path creation) from typed payload-derived schema/options.
6. Return explicit invalid-payload/path errors for deterministic server mapping.
7. Register module in worker/src/services/mod.rs.
8. Add `operation == "create_table"` branch in worker/src/transactions/maestro.rs that delegates to create_table service, matching create_database/create_schema pattern.
9. Remove/retire legacy SQL re-parse CREATE TABLE execution path in maestro prepare flow once new path is active.

5. Phase 5: Metastore contract and provider updates (parallel with Phase 4; blocks final integration)
1. Extend kionas/proto/metastore_service.proto create/get table requests with explicit parent context (database_name + schema_name + table_name alignment) and any required metadata fields for persisted table params.
2. Regenerate affected Rust types through existing build flow.
3. Update server call sites building CreateTableRequest/GetTableRequest to match new fields.
4. Implement metastore provider get_table/create_table logic in metastore/src/services/provider/postgres.rs with parent-aware lookup and deterministic not-found/duplicate semantics.
5. Ensure uniqueness/conflict behavior is deterministic for check-then-create races (map unique conflict to duplicate).
6. Preserve backwards compatibility strategy for existing rows where possible (transitional read behavior if needed).

6. Phase 6: Consistency hardening and cleanup
1. Ensure all CREATE TABLE paths use typed payload flow only; no worker SQL parsing authority for CREATE TABLE.
2. Ensure structured outcome codes cover success, validation, infra, and partial-failure states.
3. Verify logging includes correlation fields (session/database/schema/table/worker location) on success and partial-failure paths.
4. Remove dead helpers that become unused after migration.

7. Phase 7: Verification and acceptance
1. Compile checks:
- cargo check -p worker
- cargo check -p metastore
- cargo check -p server (Docker fallback on Windows if local toolchain blocks)
2. Quality gates:
- cargo fmt --all
- cargo clippy --all-targets --all-features -- -D warnings
3. Targeted runtime validation:
- Case A: first create table database.schema.table => worker initializes + metastore row + SUCCESS TABLE_CREATED
- Case B: second same command => deterministic TABLE_ALREADY_EXISTS (or TABLE_ALREADY_EXISTS_SKIPPED with IF NOT EXISTS)
- Case C: missing parent database => DATABASE_NOT_FOUND and no worker dispatch
- Case D: missing parent schema => SCHEMA_NOT_FOUND and no worker dispatch
- Case E: worker failure => metastore not written
- Case F: metastore conflict after worker success => deterministic duplicate mapping
4. Log validation:
- confirm correlated logs across server orchestration, worker execution, and metastore persistence.

**Relevant files**
- c:/code/kionas/server/src/statement_handler/mod.rs — replace inline CREATE TABLE branch with dedicated module delegation
- c:/code/kionas/server/src/statement_handler/create_table.rs — new server-owned CREATE TABLE orchestrator
- c:/code/kionas/server/src/statement_handler/helpers.rs — reuse worker dispatch helpers
- c:/code/kionas/server/src/services/warehouse_service_server.rs — reuse structured RESULT mapping
- c:/code/kionas/kionas/proto/metastore_service.proto — extend get/create table contracts with explicit parent context
- c:/code/kionas/metastore/src/services/metastore_service.rs — ensure action routing remains aligned
- c:/code/kionas/metastore/src/services/actions/mod.rs — register/verify get_table + create_table action paths
- c:/code/kionas/metastore/src/services/actions/create_table.rs — provider delegation stays consistent with updated request fields
- c:/code/kionas/metastore/src/services/actions/get_table.rs — provider delegation for pre-check path
- c:/code/kionas/metastore/src/services/provider/postgres.rs — implement parent-aware get/create table and deterministic conflict mapping
- c:/code/kionas/worker/src/services/mod.rs — register create_table service module
- c:/code/kionas/worker/src/services/create_table.rs — new typed task executor for CREATE TABLE
- c:/code/kionas/worker/src/transactions/maestro.rs — add create_table operation delegation and retire legacy inline path

**Verification**
1. Build and lint gates pass for touched crates and workspace commands noted above.
2. Structured response mapping returns deterministic status/error_code for duplicate/not-found/infra cases.
3. Worker no longer parses CREATE TABLE SQL for execution authority; payload is server-normalized typed JSON.
4. Metastore rows include expected parent context and persisted table params.

**Decisions**
- Strict namespace policy: require explicit 3-part database.schema.table for CREATE TABLE in this phase.
- IF NOT EXISTS policy: return SUCCESS with TABLE_ALREADY_EXISTS_SKIPPED when target exists.
- Scope includes metastore contract updates for explicit parent context.
- Included: server-owned validation/orchestration, typed worker payload, deterministic race mapping, parity with CREATE DATABASE/SCHEMA patterns.
- Excluded: broad parser grammar redesign beyond CREATE TABLE path.

**Further Considerations**
1. Backward compatibility migration strategy for existing table metadata rows lacking explicit parent context should be defined before rollout.
2. Decide whether to persist a table-level ast_payload_json for forward compatibility symmetry with CREATE SCHEMA.
3. Decide whether to keep Delta-only existence as secondary signal or enforce marker-first idempotency as primary signal for CREATE TABLE.
