use crate::parser::datafusion_sql::sqlparser::ast::{
    Expr, Insert, SetExpr, Statement, Value as SqlValue,
};
use crate::providers::{KionasMetastoreResolver, normalize_identifier};
use crate::statement_handler::shared::helpers;
use crate::warehouse::state::SharedData;
use serde::Serialize;
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

fn parse_structured_outcome(err: &str) -> Option<(&str, &str, &str)> {
    let mut parts = err.splitn(4, '|');
    let prefix = parts.next()?;
    if prefix != OUTCOME_PREFIX {
        return None;
    }
    let category = parts.next()?;
    let code = parts.next()?;
    let message = parts.next().unwrap_or_default();
    Some((category, code, message))
}

fn map_insert_dispatch_error(err: &str) -> (&'static str, &'static str) {
    if let Some((category, _code, _message)) = parse_structured_outcome(err)
        && matches!(
            category,
            "VALIDATION" | "CONSTRAINT" | "EXECUTION" | "INFRA"
        )
    {
        return match category {
            "VALIDATION" => ("VALIDATION", "VALIDATION_INSERT_DISPATCH_FAILED"),
            "CONSTRAINT" => ("CONSTRAINT", "CONSTRAINT_INSERT_DISPATCH_FAILED"),
            "EXECUTION" => ("EXECUTION", "EXECUTION_INSERT_DISPATCH_FAILED"),
            _ => ("INFRA", "INFRA_INSERT_DISPATCH_FAILED"),
        };
    }

    let rules: [(&str, &str, &str); 8] = [
        (
            "temporal_literal_invalid",
            "VALIDATION",
            "VALIDATION_TEMPORAL_LITERAL_INVALID",
        ),
        (
            "datetime_timezone_not_allowed",
            "VALIDATION",
            "VALIDATION_DATETIME_TIMEZONE_NOT_ALLOWED",
        ),
        (
            "decimal_coercion_failed",
            "VALIDATION",
            "VALIDATION_DECIMAL_COERCION_FAILED",
        ),
        (
            "insert_type_hints_malformed",
            "VALIDATION",
            "VALIDATION_INSERT_TYPE_HINTS_MALFORMED",
        ),
        (
            "not null constraint violated",
            "CONSTRAINT",
            "CONSTRAINT_NOT_NULL_VIOLATION",
        ),
        (
            "missing not null column",
            "CONSTRAINT",
            "CONSTRAINT_NOT_NULL_COLUMNS_MISSING",
        ),
        (
            "table not found",
            "VALIDATION",
            "VALIDATION_TABLE_NOT_FOUND",
        ),
        (
            "register_object_store",
            "INFRA",
            "INFRA_INSERT_OBJECT_STORE_UNAVAILABLE",
        ),
    ];

    let lower = err.to_ascii_lowercase();
    for (needle, category, code) in rules {
        if lower.contains(needle) {
            return (category, code);
        }
    }

    ("INFRA", "INFRA_INSERT_DISPATCH_FAILED")
}

fn normalize_insert_error_message(code: &str, err: &str) -> String {
    match code {
        "VALIDATION_TEMPORAL_LITERAL_INVALID"
        | "VALIDATION_DATETIME_TIMEZONE_NOT_ALLOWED"
        | "VALIDATION_DECIMAL_COERCION_FAILED"
        | "VALIDATION_INSERT_TYPE_HINTS_MALFORMED"
        | "CONSTRAINT_NOT_NULL_VIOLATION"
        | "CONSTRAINT_NOT_NULL_COLUMNS_MISSING"
        | "INFRA_INSERT_OBJECT_STORE_UNAVAILABLE" => err.to_string(),
        "VALIDATION_TABLE_NOT_FOUND" => {
            if let Some(idx) = err.to_ascii_lowercase().find("table not found") {
                return err[idx..].to_string();
            }
            "table not found".to_string()
        }
        "VALIDATION_INSERT_DISPATCH_FAILED"
        | "CONSTRAINT_INSERT_DISPATCH_FAILED"
        | "EXECUTION_INSERT_DISPATCH_FAILED"
        | "INFRA_INSERT_DISPATCH_FAILED" => {
            if let Some((_category, _code, message)) = parse_structured_outcome(err) {
                return message.to_string();
            }
            format!("insert dispatch failed: {}", err)
        }
        _ => err.to_string(),
    }
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

#[derive(Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
enum InsertScalarPayload {
    Int(i64),
    Bool(bool),
    Str(String),
    Null,
}

#[derive(Serialize)]
struct InsertPayload {
    table_name: String,
    columns: Vec<String>,
    rows: Vec<Vec<InsertScalarPayload>>,
}

/// What: Build the worker INSERT payload contract from server-side SQL AST.
///
/// Inputs:
/// - `insert_stmt`: Insert AST node from parsed SQL statement.
/// - `table_name`: Canonical table name computed by the server.
///
/// Output:
/// - JSON text for `insert_payload_json` task param.
///
/// Details:
/// - Supports only `INSERT ... VALUES (...)` forms.
/// - Preserves scalar intent across int, bool, string, and null values.
fn build_insert_payload_json(insert_stmt: &Insert, table_name: &str) -> Result<String, String> {
    let source = insert_stmt
        .source
        .as_ref()
        .ok_or_else(|| "INSERT source is missing".to_string())?;

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => values,
        _ => {
            return Err(
                "INSERT source is not VALUES; only VALUES inserts are supported in worker"
                    .to_string(),
            );
        }
    };

    if values.rows.is_empty() {
        return Err("INSERT VALUES has no rows".to_string());
    }

    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let mut parsed_row = Vec::with_capacity(row.len());
        for expr in row {
            let scalar = match expr {
                Expr::Value(vws) => match &vws.value {
                    SqlValue::Number(n, _) => n
                        .parse::<i64>()
                        .map(InsertScalarPayload::Int)
                        .unwrap_or_else(|_| InsertScalarPayload::Str(n.clone())),
                    SqlValue::Boolean(b) => InsertScalarPayload::Bool(*b),
                    SqlValue::Null => InsertScalarPayload::Null,
                    other => InsertScalarPayload::Str(other.to_string()),
                },
                other => InsertScalarPayload::Str(other.to_string()),
            };
            parsed_row.push(scalar);
        }
        rows.push(parsed_row);
    }

    let column_count = rows.first().map(std::vec::Vec::len).unwrap_or(0);
    if column_count == 0 {
        return Err("INSERT VALUES produced zero columns".to_string());
    }
    if rows.iter().any(|r| r.len() != column_count) {
        return Err("INSERT VALUES row width mismatch".to_string());
    }

    let columns = if insert_stmt.columns.is_empty() {
        (1..=column_count).map(|i| format!("c{}", i)).collect()
    } else {
        insert_stmt
            .columns
            .iter()
            .map(ToString::to_string)
            .collect()
    };

    serde_json::to_string(&InsertPayload {
        table_name: table_name.to_string(),
        columns,
        rows,
    })
    .map_err(|e| format!("failed to serialize INSERT payload: {}", e))
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
    let resolver = KionasMetastoreResolver::new(shared_data.clone(), 8)?;
    let metadata = resolver.resolve_relation(database, schema, table).await?;

    let mut table_columns = metadata.column_names();
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
    let insert_payload_json = match build_insert_payload_json(insert_stmt, &table_name) {
        Ok(value) => value,
        Err(e) => {
            let err = e.to_string();
            let (category, code) = map_insert_dispatch_error(&err);
            return format_outcome(category, code, normalize_insert_error_message(code, &err));
        }
    };
    params.insert("insert_payload_json".to_string(), insert_payload_json);

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

#[cfg(test)]
#[path = "../../tests/statement_handler_dml_insert_tests.rs"]
mod tests;
