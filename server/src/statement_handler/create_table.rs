use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::{
    ColumnOption, CreateTable as SqlCreateTable, ObjectName,
};
use kionas::planner::validate_constraint_contract;
use kionas::planner::validate_datatype_contract;
use kionas::sql::constraints::{
    TableConstraintContract, build_constraint_contract_from_create_table,
};
use kionas::sql::datatypes::{TableDatatypeContract, build_datatype_contract_from_create_table};

const OUTCOME_PREFIX: &str = "RESULT";

/// What: Borrowed CREATE TABLE AST view used by server orchestration.
///
/// Inputs:
/// - `create_table`: Parsed CREATE TABLE AST.
///
/// Output:
/// - Structured borrowed data for validation and payload generation.
///
/// Details:
/// - The orchestrator preserves full AST context in the worker/metastore payload for
///   forward compatibility while enforcing deterministic namespace and duplicate behavior.
pub(crate) struct CreateTableAst<'a> {
    pub(crate) create_table: &'a SqlCreateTable,
}

/// What: Encode a structured statement outcome for API mapping.
///
/// Inputs:
/// - `category`: `SUCCESS`, `VALIDATION`, or `INFRA`.
/// - `code`: Stable machine-readable code.
/// - `message`: User-facing message.
///
/// Output:
/// - Encoded outcome string in `RESULT|category|code|message` format.
fn format_outcome(category: &str, code: &str, message: impl Into<String>) -> String {
    format!(
        "{}|{}|{}|{}",
        OUTCOME_PREFIX,
        category,
        code,
        message.into()
    )
}

/// What: Normalize identifier text for deterministic comparisons.
///
/// Inputs:
/// - `raw`: Identifier text, possibly quoted.
///
/// Output:
/// - Canonical lowercase identifier.
fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

/// What: Validate canonical namespace segment names.
///
/// Inputs:
/// - `name`: Canonical namespace segment.
///
/// Output:
/// - `true` when the segment is valid.
fn is_valid_namespace_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

/// What: Identify not-found semantics from provider messages.
///
/// Inputs:
/// - `msg`: Raw provider/metastore message.
///
/// Output:
/// - `true` when message denotes missing resource.
fn is_not_found_message(msg: &str) -> bool {
    msg.to_ascii_lowercase().contains("not found")
}

/// What: Identify duplicate/conflict semantics from provider messages.
///
/// Inputs:
/// - `msg`: Raw provider/metastore message.
///
/// Output:
/// - `true` when message denotes uniqueness conflict.
fn is_duplicate_message(msg: &str) -> bool {
    let lower = msg.to_ascii_lowercase();
    lower.contains("duplicate") || lower.contains("already exists") || lower.contains("unique")
}

/// What: Extract strict `database.schema.table` namespace from object name.
///
/// Inputs:
/// - `name`: SQL object name from CREATE TABLE.
///
/// Output:
/// - Canonical `(database, schema, table)` tuple.
///
/// Details:
/// - This implementation intentionally requires exactly 3 segments.
fn extract_table_namespace(name: &ObjectName) -> Result<(String, String, String), String> {
    let rendered = name.to_string();
    let parts = rendered
        .split('.')
        .map(normalize_identifier)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();

    if parts.len() != 3 {
        return Err(
            "CREATE TABLE requires explicit namespace in form <database>.<schema>.<table>"
                .to_string(),
        );
    }

    Ok((parts[0].clone(), parts[1].clone(), parts[2].clone()))
}

/// What: Convert AST column definitions to metastore column schema values.
///
/// Inputs:
/// - `create_table`: Parsed CREATE TABLE AST.
///
/// Output:
/// - Column schema list with deterministic nullability.
fn metastore_columns(create_table: &SqlCreateTable) -> Vec<ms::ColumnSchema> {
    create_table
        .columns
        .iter()
        .map(|col| {
            let not_null = col
                .options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull));
            ms::ColumnSchema {
                name: col.name.to_string(),
                data_type: col.data_type.to_string(),
                nullable: !not_null,
            }
        })
        .collect::<Vec<_>>()
}

/// What: Build typed CREATE TABLE payload for worker execution.
///
/// Inputs:
/// - `ast`: CREATE TABLE AST wrapper.
/// - `database_name`: Canonical database name.
/// - `schema_name`: Canonical schema name.
/// - `table_name`: Canonical table name.
///
/// Output:
/// - JSON payload string preserving AST details.
fn build_worker_payload(
    ast: &CreateTableAst<'_>,
    database_name: &str,
    schema_name: &str,
    table_name: &str,
    constraint_contract: &TableConstraintContract,
    datatype_contract: &TableDatatypeContract,
) -> String {
    let create = ast.create_table;
    let mut payload = serde_json::Map::new();
    payload.insert(
        "statement".to_string(),
        serde_json::Value::String("CreateTable".to_string()),
    );

    let mut namespace = serde_json::Map::new();
    namespace.insert(
        "raw".to_string(),
        serde_json::Value::String(create.name.to_string()),
    );
    namespace.insert(
        "database".to_string(),
        serde_json::Value::String(database_name.to_string()),
    );
    namespace.insert(
        "schema".to_string(),
        serde_json::Value::String(schema_name.to_string()),
    );
    namespace.insert(
        "table".to_string(),
        serde_json::Value::String(table_name.to_string()),
    );
    payload.insert(
        "namespace".to_string(),
        serde_json::Value::Object(namespace),
    );

    let columns = create
        .columns
        .iter()
        .map(|col| {
            let not_null = col
                .options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull));
            serde_json::json!({
                "name": col.name.to_string(),
                "data_type": col.data_type.to_string(),
                "nullable": !not_null,
            })
        })
        .collect::<Vec<_>>();
    payload.insert("columns".to_string(), serde_json::Value::Array(columns));

    let insert_val = |map: &mut serde_json::Map<String, serde_json::Value>,
                      key: &str,
                      value: serde_json::Value| {
        map.insert(key.to_string(), value);
    };

    insert_val(
        &mut payload,
        "or_replace",
        serde_json::Value::Bool(create.or_replace),
    );
    insert_val(
        &mut payload,
        "temporary",
        serde_json::Value::Bool(create.temporary),
    );
    insert_val(
        &mut payload,
        "external",
        serde_json::Value::Bool(create.external),
    );
    insert_val(
        &mut payload,
        "dynamic",
        serde_json::Value::Bool(create.dynamic),
    );
    insert_val(
        &mut payload,
        "global",
        serde_json::to_value(create.global).unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "if_not_exists",
        serde_json::Value::Bool(create.if_not_exists),
    );
    insert_val(
        &mut payload,
        "transient",
        serde_json::Value::Bool(create.transient),
    );
    insert_val(
        &mut payload,
        "volatile",
        serde_json::Value::Bool(create.volatile),
    );
    insert_val(
        &mut payload,
        "iceberg",
        serde_json::Value::Bool(create.iceberg),
    );
    insert_val(
        &mut payload,
        "constraints",
        serde_json::to_value(constraint_contract).unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "datatypes",
        serde_json::to_value(datatype_contract).unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "hive_distribution",
        serde_json::Value::String(format!("{:?}", create.hive_distribution)),
    );
    insert_val(
        &mut payload,
        "hive_formats",
        create
            .hive_formats
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "table_options",
        serde_json::Value::String(format!("{:?}", create.table_options)),
    );
    insert_val(
        &mut payload,
        "file_format",
        create
            .file_format
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "location",
        create
            .location
            .as_ref()
            .map(|v| serde_json::Value::String(v.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "query",
        create
            .query
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "without_rowid",
        serde_json::Value::Bool(create.without_rowid),
    );
    insert_val(
        &mut payload,
        "like",
        create
            .like
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "clone",
        create
            .clone
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "version",
        create
            .version
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "comment",
        create
            .comment
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "on_commit",
        create
            .on_commit
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "on_cluster",
        create
            .on_cluster
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "primary_key",
        create
            .primary_key
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "order_by",
        create
            .order_by
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "partition_by",
        create
            .partition_by
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "cluster_by",
        create
            .cluster_by
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "clustered_by",
        create
            .clustered_by
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "inherits",
        create
            .inherits
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "strict",
        serde_json::Value::Bool(create.strict),
    );
    insert_val(
        &mut payload,
        "copy_grants",
        serde_json::Value::Bool(create.copy_grants),
    );
    insert_val(
        &mut payload,
        "enable_schema_evolution",
        serde_json::to_value(create.enable_schema_evolution).unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "change_tracking",
        serde_json::to_value(create.change_tracking).unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "data_retention_time_in_days",
        serde_json::to_value(create.data_retention_time_in_days).unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "max_data_extension_time_in_days",
        serde_json::to_value(create.max_data_extension_time_in_days)
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "default_ddl_collation",
        create
            .default_ddl_collation
            .as_ref()
            .map(|v| serde_json::Value::String(v.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "with_aggregation_policy",
        create
            .with_aggregation_policy
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "with_row_access_policy",
        create
            .with_row_access_policy
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "with_tags",
        create
            .with_tags
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "external_volume",
        create
            .external_volume
            .as_ref()
            .map(|v| serde_json::Value::String(v.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "base_location",
        create
            .base_location
            .as_ref()
            .map(|v| serde_json::Value::String(v.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "catalog",
        create
            .catalog
            .as_ref()
            .map(|v| serde_json::Value::String(v.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "catalog_sync",
        create
            .catalog_sync
            .as_ref()
            .map(|v| serde_json::Value::String(v.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "storage_serialization_policy",
        create
            .storage_serialization_policy
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "target_lag",
        create
            .target_lag
            .as_ref()
            .map(|v| serde_json::Value::String(v.clone()))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "warehouse",
        create
            .warehouse
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "refresh_mode",
        create
            .refresh_mode
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "initialize",
        create
            .initialize
            .as_ref()
            .map(|v| serde_json::Value::String(format!("{:?}", v)))
            .unwrap_or(serde_json::Value::Null),
    );
    insert_val(
        &mut payload,
        "require_user",
        serde_json::Value::Bool(create.require_user),
    );

    serde_json::Value::Object(payload).to_string()
}

/// What: Orchestrate server-owned CREATE TABLE flow.
///
/// Inputs:
/// - `shared_data`: Shared server state.
/// - `session_id`: Current session id for routing and correlation.
/// - `ast`: Borrowed CREATE TABLE AST wrapper.
///
/// Output:
/// - Encoded result outcome in `RESULT|category|code|message` format.
pub(crate) async fn handle_create_table(
    shared_data: &SharedData,
    session_id: &str,
    ast: CreateTableAst<'_>,
) -> String {
    let constraint_contract = build_constraint_contract_from_create_table(ast.create_table);
    if let Err(e) = validate_constraint_contract(&constraint_contract) {
        return format_outcome(
            "VALIDATION",
            "INVALID_CONSTRAINT_CONTRACT",
            format!("invalid constraint metadata for CREATE TABLE: {}", e),
        );
    }

    let datatype_contract = build_datatype_contract_from_create_table(ast.create_table);
    if let Err(e) = validate_datatype_contract(&datatype_contract) {
        return format_outcome(
            "VALIDATION",
            "INVALID_DATATYPE_CONTRACT",
            format!("invalid datatype metadata for CREATE TABLE: {}", e),
        );
    }

    let (database_name, schema_name, table_name) =
        match extract_table_namespace(&ast.create_table.name) {
            Ok(parts) => parts,
            Err(e) => return format_outcome("VALIDATION", "INVALID_TABLE_NAMESPACE", e),
        };

    for (name, code) in [
        (&database_name, "INVALID_DATABASE_NAME"),
        (&schema_name, "INVALID_SCHEMA_NAME"),
        (&table_name, "INVALID_TABLE_NAME"),
    ] {
        if !is_valid_namespace_name(name) {
            return format_outcome(
                "VALIDATION",
                code,
                format!("invalid name '{}': only [a-zA-Z0-9_-] are allowed", name),
            );
        }
    }

    let mut metastore_client = match MetastoreClient::connect_with_shared(shared_data).await {
        Ok(client) => client,
        Err(e) => {
            return format_outcome(
                "INFRA",
                "METASTORE_CONNECT_FAILED",
                format!("failed to connect to metastore: {}", e),
            );
        }
    };

    let get_db_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::GetDatabase(
            ms::GetDatabaseRequest {
                database_name: database_name.clone(),
            },
        )),
    };
    match metastore_client.execute(get_db_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::GetDatabaseResponse(r)) => {
                if !r.success {
                    if is_not_found_message(&r.message) {
                        return format_outcome(
                            "VALIDATION",
                            "DATABASE_NOT_FOUND",
                            format!("database not found: {}", database_name),
                        );
                    }
                    return format_outcome(
                        "INFRA",
                        "METASTORE_GET_DATABASE_FAILED",
                        format!(
                            "failed to validate database existence in metastore: {}",
                            r.message
                        ),
                    );
                }
            }
            _ => {
                return format_outcome(
                    "INFRA",
                    "METASTORE_PROTOCOL_ERROR",
                    "metastore returned an unexpected response for get_database",
                );
            }
        },
        Err(e) => {
            return format_outcome(
                "INFRA",
                "METASTORE_GET_DATABASE_FAILED",
                format!("failed to validate database existence in metastore: {}", e),
            );
        }
    }

    let get_schema_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::GetSchema(
            ms::GetSchemaRequest {
                schema_name: schema_name.clone(),
                database_name: database_name.clone(),
            },
        )),
    };
    match metastore_client.execute(get_schema_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::GetSchemaResponse(r)) => {
                if !r.success {
                    if is_not_found_message(&r.message) {
                        return format_outcome(
                            "VALIDATION",
                            "SCHEMA_NOT_FOUND",
                            format!("schema not found: {}.{}", database_name, schema_name),
                        );
                    }
                    return format_outcome(
                        "INFRA",
                        "METASTORE_GET_SCHEMA_FAILED",
                        format!(
                            "failed to validate schema existence in metastore: {}",
                            r.message
                        ),
                    );
                }
            }
            _ => {
                return format_outcome(
                    "INFRA",
                    "METASTORE_PROTOCOL_ERROR",
                    "metastore returned an unexpected response for get_schema",
                );
            }
        },
        Err(e) => {
            return format_outcome(
                "INFRA",
                "METASTORE_GET_SCHEMA_FAILED",
                format!("failed to validate schema existence in metastore: {}", e),
            );
        }
    }

    let get_table_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::GetTable(
            ms::GetTableRequest {
                database_name: database_name.clone(),
                schema_name: schema_name.clone(),
                table_name: table_name.clone(),
                location: String::new(),
            },
        )),
    };
    let table_exists = match metastore_client.execute(get_table_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::GetTableResponse(r)) => {
                if r.success {
                    true
                } else if is_not_found_message(&r.message) {
                    false
                } else {
                    return format_outcome(
                        "INFRA",
                        "METASTORE_GET_TABLE_FAILED",
                        format!(
                            "failed to validate table existence in metastore: {}",
                            r.message
                        ),
                    );
                }
            }
            _ => {
                return format_outcome(
                    "INFRA",
                    "METASTORE_PROTOCOL_ERROR",
                    "metastore returned an unexpected response for get_table",
                );
            }
        },
        Err(e) => {
            return format_outcome(
                "INFRA",
                "METASTORE_GET_TABLE_FAILED",
                format!("failed to validate table existence in metastore: {}", e),
            );
        }
    };

    if table_exists {
        if ast.create_table.if_not_exists {
            return format_outcome(
                "SUCCESS",
                "TABLE_ALREADY_EXISTS_SKIPPED",
                format!(
                    "table already exists: {}.{}.{} (skipped because IF NOT EXISTS was set)",
                    database_name, schema_name, table_name
                ),
            );
        }
        return format_outcome(
            "VALIDATION",
            "TABLE_ALREADY_EXISTS",
            format!(
                "table already exists: {}.{}.{}",
                database_name, schema_name, table_name
            ),
        );
    }

    let worker_payload = build_worker_payload(
        &ast,
        &database_name,
        &schema_name,
        &table_name,
        &constraint_contract,
        &datatype_contract,
    );
    let worker_result_location = match helpers::run_task_for_input(
        shared_data,
        session_id,
        "create_table",
        worker_payload.clone(),
        120,
    )
    .await
    {
        Ok(location) => location,
        Err(e) => {
            return format_outcome(
                "INFRA",
                "WORKER_CREATE_TABLE_FAILED",
                format!("worker failed to initialize table storage: {}", e),
            );
        }
    };

    let create_req = ms::MetastoreRequest {
        action: Some(ms::metastore_request::Action::CreateTable(
            ms::CreateTableRequest {
                database_name: database_name.clone(),
                schema_name: schema_name.clone(),
                table_name: table_name.clone(),
                engine: "delta".to_string(),
                columns: metastore_columns(ast.create_table),
                location: worker_result_location.clone(),
                ast_payload_json: worker_payload,
            },
        )),
    };

    match metastore_client.execute(create_req).await {
        Ok(resp) => match resp.result {
            Some(ms::metastore_response::Result::CreateTableResponse(r)) => {
                if r.success {
                    log::info!(
                        "create_table success: session_id={} database={} schema={} table={} worker_location={}",
                        session_id,
                        database_name,
                        schema_name,
                        table_name,
                        worker_result_location
                    );
                    format_outcome(
                        "SUCCESS",
                        "TABLE_CREATED",
                        format!(
                            "table created successfully: {}.{}.{} (location: {})",
                            database_name, schema_name, table_name, worker_result_location
                        ),
                    )
                } else if is_duplicate_message(&r.message) {
                    format_outcome(
                        "VALIDATION",
                        "TABLE_ALREADY_EXISTS",
                        format!(
                            "table already exists: {}.{}.{}",
                            database_name, schema_name, table_name
                        ),
                    )
                } else {
                    log::error!(
                        "create_table partial failure: session_id={} database={} schema={} table={} worker_location={} metastore_error={}",
                        session_id,
                        database_name,
                        schema_name,
                        table_name,
                        worker_result_location,
                        r.message
                    );
                    format_outcome(
                        "INFRA",
                        "METASTORE_CREATE_AFTER_WORKER_FAILED",
                        format!(
                            "table storage initialized but metastore write failed: {}",
                            r.message
                        ),
                    )
                }
            }
            _ => format_outcome(
                "INFRA",
                "METASTORE_PROTOCOL_ERROR",
                "metastore returned an unexpected response for create_table",
            ),
        },
        Err(e) => {
            log::error!(
                "create_table partial failure: session_id={} database={} schema={} table={} worker_location={} metastore_error={}",
                session_id,
                database_name,
                schema_name,
                table_name,
                worker_result_location,
                e
            );
            format_outcome(
                "INFRA",
                "METASTORE_CREATE_AFTER_WORKER_FAILED",
                format!(
                    "table storage initialized but metastore write failed: {}",
                    e
                ),
            )
        }
    }
}
