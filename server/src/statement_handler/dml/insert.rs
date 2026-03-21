use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::statement_handler::shared::helpers;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::{Insert, Statement};
use std::collections::HashMap;

const OUTCOME_PREFIX: &str = "RESULT";

fn format_outcome(category: &str, code: &str, message: impl Into<String>) -> String {
    format!(
        "{}|{}|{}|{}",
        OUTCOME_PREFIX,
        category,
        code,
        message.into()
    )
}

fn map_insert_dispatch_error(err: &str) -> (&'static str, &'static str) {
    let lower = err.to_ascii_lowercase();
    if lower.contains("temporal_literal_invalid") {
        return ("VALIDATION", "TEMPORAL_LITERAL_INVALID");
    }
    if lower.contains("datetime_timezone_not_allowed") {
        return ("VALIDATION", "DATETIME_TIMEZONE_NOT_ALLOWED");
    }
    if lower.contains("decimal_coercion_failed") {
        return ("VALIDATION", "DECIMAL_COERCION_FAILED");
    }
    if lower.contains("insert_type_hints_malformed") {
        return ("VALIDATION", "INSERT_TYPE_HINTS_MALFORMED");
    }
    if lower.contains("not null constraint violated") {
        return ("VALIDATION", "CONSTRAINT_NOT_NULL_VIOLATION");
    }
    if lower.contains("missing not null column") {
        return ("VALIDATION", "CONSTRAINT_NOT_NULL_COLUMNS_MISSING");
    }
    if lower.contains("table not found") {
        return ("VALIDATION", "TABLE_NOT_FOUND");
    }
    ("INFRA", "INSERT_DISPATCH_FAILED")
}

fn normalize_insert_error_message(code: &str, err: &str) -> String {
    match code {
        "TEMPORAL_LITERAL_INVALID" => err.to_string(),
        "DATETIME_TIMEZONE_NOT_ALLOWED" => err.to_string(),
        "DECIMAL_COERCION_FAILED" => err.to_string(),
        "INSERT_TYPE_HINTS_MALFORMED" => err.to_string(),
        "CONSTRAINT_NOT_NULL_VIOLATION" => err.to_string(),
        "CONSTRAINT_NOT_NULL_COLUMNS_MISSING" => err.to_string(),
        "TABLE_NOT_FOUND" => {
            if let Some(idx) = err.to_ascii_lowercase().find("table not found") {
                return err[idx..].to_string();
            }
            "table not found".to_string()
        }
        "INSERT_DISPATCH_FAILED" => format!("insert dispatch failed: {}", err),
        _ => err.to_string(),
    }
}

/// What: Normalize identifier text for deterministic metadata lookups.
///
/// Inputs:
/// - `raw`: Identifier text that may include quoting.
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

/// What: Canonicalize insert table text into a deterministic name.
///
/// Inputs:
/// - `raw_table_name`: SQL table reference from INSERT.
/// - `default_schema`: Session default schema/database marker.
///
/// Output:
/// - Canonical table name used for worker task params.
///
/// Details:
/// - Preserves existing behavior for single-part names by mapping to
///   `deltalake.<default_schema>.<table>`.
fn canonicalize_insert_table_name(raw_table_name: &str, default_schema: &str) -> String {
    let cleaned = raw_table_name.trim().trim_matches('"').trim_matches('`');
    if cleaned.is_empty() {
        return cleaned.to_string();
    }
    if cleaned.contains('.') {
        cleaned.to_string()
    } else {
        format!("deltalake.{}.{}", default_schema, cleaned)
    }
}

/// What: Parse canonical table name into three namespace parts.
///
/// Inputs:
/// - `table_name`: Canonical table name string.
///
/// Output:
/// - `(database, schema, table)` tuple when exactly three parts are present.
fn parse_table_namespace(table_name: &str) -> Option<(String, String, String)> {
    let parts = table_name
        .split('.')
        .map(normalize_identifier)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    Some((parts[0].clone(), parts[1].clone(), parts[2].clone()))
}

/// What: Load ordered table columns and NOT NULL columns from metastore metadata.
///
/// Inputs:
/// - `shared_data`: Shared server state.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
///
/// Output:
/// - `(table_columns, not_null_columns, column_type_hints)` with canonical column names.
///
/// Details:
/// - The lookup is strict for known three-part namespaces and returns actionable errors.
async fn load_table_constraint_columns(
    shared_data: &SharedData,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<(Vec<String>, Vec<String>, HashMap<String, String>), String> {
    let mut client = MetastoreClient::connect_with_shared(shared_data)
        .await
        .map_err(|e| {
            format!(
                "failed to connect to metastore for INSERT constraints: {}",
                e
            )
        })?;

    let response = client
        .execute(ms::MetastoreRequest {
            action: Some(ms::metastore_request::Action::GetTable(
                ms::GetTableRequest {
                    database_name: database.to_string(),
                    schema_name: schema.to_string(),
                    table_name: table.to_string(),
                    location: String::new(),
                },
            )),
        })
        .await
        .map_err(|e| {
            format!(
                "failed to load table metadata for INSERT constraints {}.{}.{}: {}",
                database, schema, table, e
            )
        })?;

    let result = response.result.ok_or_else(|| {
        format!(
            "metastore returned empty response for INSERT constraints {}.{}.{}",
            database, schema, table
        )
    })?;

    let get_table = match result {
        ms::metastore_response::Result::GetTableResponse(value) => value,
        _ => {
            return Err(format!(
                "metastore returned unexpected response for INSERT constraints {}.{}.{}",
                database, schema, table
            ));
        }
    };

    if !get_table.success {
        return Err(format!(
            "metastore table lookup failed for INSERT constraints {}.{}.{}: {}",
            database, schema, table, get_table.message
        ));
    }

    let metadata = get_table.metadata.ok_or_else(|| {
        format!(
            "metastore table lookup missing metadata for INSERT constraints {}.{}.{}",
            database, schema, table
        )
    })?;

    let mut table_columns = metadata
        .columns
        .iter()
        .map(|column| normalize_identifier(&column.name))
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    table_columns.dedup();

    let mut required_columns = metadata
        .columns
        .iter()
        .filter(|column| !column.nullable)
        .map(|column| normalize_identifier(&column.name))
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();
    required_columns.sort();
    required_columns.dedup();

    let mut column_type_hints = HashMap::new();
    for column in &metadata.columns {
        let key = normalize_identifier(&column.name);
        if !key.is_empty() {
            column_type_hints.insert(key, column.data_type.clone());
        }
    }

    Ok((table_columns, required_columns, column_type_hints))
}

/// What: Handle INSERT by attaching constraint metadata for worker-side enforcement.
///
/// Inputs:
/// - `shared_data`: Shared server state.
/// - `session_id`: Session identifier.
/// - `stmt`: Original INSERT statement.
/// - `insert_stmt`: Borrowed insert AST node.
///
/// Output:
/// - User-facing dispatch result message.
///
/// Details:
/// - Keeps existing task dispatch behavior while augmenting params with
///   normalized NOT NULL contract data.
pub(crate) async fn handle_insert_statement(
    shared_data: &SharedData,
    session_id: &str,
    stmt: &Statement,
    insert_stmt: &Insert,
) -> String {
    let payload = stmt.to_string();
    let default_schema = {
        let state = shared_data.lock().await;
        match state
            .session_manager
            .get_session(session_id.to_string())
            .await
        {
            Some(session) => session.get_use_database(),
            None => "default".to_string(),
        }
    };

    let mut params = HashMap::new();
    let table_name =
        canonicalize_insert_table_name(&insert_stmt.table.to_string(), &default_schema);
    params.insert("table_name".to_string(), table_name.clone());

    if let Some((database, schema, table)) = parse_table_namespace(&table_name) {
        match load_table_constraint_columns(shared_data, &database, &schema, &table).await {
            Ok((table_columns, required_columns, column_type_hints)) => {
                params.insert("constraint_contract_version".to_string(), "1".to_string());
                params.insert("datatype_contract_version".to_string(), "1".to_string());
                params.insert(
                    "table_columns_json".to_string(),
                    serde_json::to_string(&table_columns).unwrap_or_else(|_| "[]".to_string()),
                );
                params.insert(
                    "not_null_columns_json".to_string(),
                    serde_json::to_string(&required_columns).unwrap_or_else(|_| "[]".to_string()),
                );
                params.insert(
                    "column_type_hints_json".to_string(),
                    serde_json::to_string(&column_type_hints).unwrap_or_else(|_| "{}".to_string()),
                );
            }
            Err(e) => {
                let err = e.to_string();
                let (category, code) = map_insert_dispatch_error(&err);
                return format_outcome(category, code, normalize_insert_error_message(code, &err));
            }
        }
    }

    match helpers::run_task_for_input_with_params(
        shared_data,
        session_id,
        "insert",
        payload,
        params,
        None,
        30,
    )
    .await
    {
        Ok(result_location) => format_outcome(
            "SUCCESS",
            "INSERT_DISPATCHED",
            format!("insert dispatched successfully: {}", result_location),
        ),
        Err(e) => {
            let err = e.to_string();
            let (category, code) = map_insert_dispatch_error(&err);
            format_outcome(category, code, normalize_insert_error_message(code, &err))
        }
    }
}
