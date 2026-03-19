use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::{
    Query as SqlQuery, Select, SetExpr, Statement, TableFactor,
};
use serde_json::json;
use std::collections::HashMap;

const OUTCOME_PREFIX: &str = "RESULT";

/// What: A borrowed view over a parsed SQL query statement used for server-side planning.
///
/// Inputs:
/// - `query`: Parsed SQL query AST.
///
/// Output:
/// - Lightweight borrowed wrapper consumed by validation and payload builders.
///
/// Details:
/// - This phase only supports a minimal single-table SELECT subset.
pub(crate) struct SelectQueryAst<'a> {
    pub(crate) query: &'a SqlQuery,
}

/// What: Encode a structured statement outcome for API response mapping.
///
/// Inputs:
/// - `category`: `SUCCESS`, `VALIDATION`, or `INFRA`.
/// - `code`: Stable machine-readable code.
/// - `message`: User-facing message.
///
/// Output:
/// - Encoded outcome string in the format `RESULT|<category>|<code>|<message>`.
fn format_outcome(category: &str, code: &str, message: impl Into<String>) -> String {
    format!(
        "{}|{}|{}|{}",
        OUTCOME_PREFIX,
        category,
        code,
        message.into()
    )
}

/// What: Normalize identifier text for deterministic comparisons and payload content.
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

/// What: Parse a fully qualified object name into normalized segments.
///
/// Inputs:
/// - `raw`: Stringified object name.
///
/// Output:
/// - Non-empty normalized path segments.
fn parse_object_parts(raw: &str) -> Vec<String> {
    raw.split('.')
        .map(normalize_identifier)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
}

/// What: Resolve canonical namespace `(database, schema, table)` for the single table source.
///
/// Inputs:
/// - `raw_table_name`: SQL string form of the source table.
/// - `default_database`: Session default database fallback.
/// - `default_schema`: Session default schema fallback.
///
/// Output:
/// - Canonical namespace tuple.
///
/// Details:
/// - Accepts 1-part, 2-part, or 3-part table references and fills missing parts from defaults.
fn canonicalize_table_namespace(
    raw_table_name: &str,
    default_database: &str,
    default_schema: &str,
) -> (String, String, String) {
    let parts = parse_object_parts(raw_table_name);
    match parts.as_slice() {
        [table] => (
            normalize_identifier(default_database),
            normalize_identifier(default_schema),
            table.clone(),
        ),
        [schema, table] => (
            normalize_identifier(default_database),
            schema.clone(),
            table.clone(),
        ),
        [database, schema, table] => (database.clone(), schema.clone(), table.clone()),
        _ => (
            normalize_identifier(default_database),
            normalize_identifier(default_schema),
            normalize_identifier(raw_table_name),
        ),
    }
}

/// What: Validate and extract the minimal SELECT shape required for phase-1 query support.
///
/// Inputs:
/// - `query`: Parsed SQL query AST.
///
/// Output:
/// - Borrowed `Select` AST on success.
///
/// Details:
/// - Only accepts a plain `SELECT ... FROM <single-table> [WHERE ...]` shape.
fn extract_minimal_select(query: &SqlQuery) -> Result<&Select, String> {
    let select = match query.body.as_ref() {
        SetExpr::Select(select) => select.as_ref(),
        _ => return Err(
            "only SELECT queries are supported in this phase (set operations are not supported)"
                .to_string(),
        ),
    };

    if select.from.len() != 1 {
        return Err("query must reference exactly one table in FROM".to_string());
    }

    let from = &select.from[0];
    if !from.joins.is_empty() {
        return Err("JOIN is not supported in this phase".to_string());
    }

    Ok(select)
}

/// What: Build a canonical JSON payload for worker query execution.
///
/// Inputs:
/// - `query`: Parsed SQL query AST.
/// - `session_id`: Session identifier for traceability.
/// - `default_database`: Session default database.
/// - `default_schema`: Session default schema.
///
/// Output:
/// - Serialized canonical query payload as JSON string.
///
/// Details:
/// - Payload is versioned to allow non-breaking expansion in future planning phases.
fn build_canonical_query_payload(
    query: &SqlQuery,
    session_id: &str,
    default_database: &str,
    default_schema: &str,
) -> Result<String, String> {
    let select = extract_minimal_select(query)?;

    let from = &select.from[0];
    let table_name = match &from.relation {
        TableFactor::Table { name, .. } => name.to_string(),
        _ => return Err("only direct table references are supported in this phase".to_string()),
    };

    let (database, schema, table) =
        canonicalize_table_namespace(&table_name, default_database, default_schema);

    let projection = select
        .projection
        .iter()
        .map(std::string::ToString::to_string)
        .collect::<Vec<_>>();

    let payload = json!({
        "version": 1,
        "statement": "Select",
        "session_id": session_id,
        "namespace": {
            "database": database,
            "schema": schema,
            "table": table,
            "raw": table_name,
        },
        "projection": projection,
        "selection": select.selection.as_ref().map(std::string::ToString::to_string),
        "sql": query.to_string(),
    });

    Ok(payload.to_string())
}

/// What: Extract canonical namespace from a canonical query payload.
///
/// Inputs:
/// - `payload`: Serialized canonical query payload JSON.
///
/// Output:
/// - Canonical `(database, schema, table)` tuple.
///
/// Details:
/// - Returns an error when required namespace fields are missing or non-string.
fn extract_namespace_from_payload(payload: &str) -> Result<(String, String, String), String> {
    let parsed: serde_json::Value = serde_json::from_str(payload)
        .map_err(|e| format!("failed to parse canonical query payload: {}", e))?;
    let namespace = parsed
        .get("namespace")
        .ok_or_else(|| "canonical query payload is missing namespace".to_string())?;

    let read_string = |key: &str| {
        namespace
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(std::string::ToString::to_string)
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| format!("canonical query payload missing namespace field '{}'", key))
    };

    let database = read_string("database")?;
    let schema = read_string("schema")?;
    let table = read_string("table")?;
    Ok((database, schema, table))
}

/// What: Handle SELECT query statements for server-side AST preparation.
///
/// Inputs:
/// - `shared_data`: Shared server state used to resolve session defaults.
/// - `session_id`: Current session id.
/// - `ast`: Borrowed query AST wrapper.
///
/// Output:
/// - Encoded statement outcome string in the format `RESULT|<category>|<code>|<message>`.
///
/// Details:
/// - This initial phase validates minimal SELECT shape and generates canonical payload.
/// - Worker dispatch for query execution is introduced in a subsequent phase.
pub(crate) async fn handle_select_query(
    shared_data: &SharedData,
    session_id: &str,
    ast: SelectQueryAst<'_>,
) -> String {
    let (default_database, default_schema) = {
        let state = shared_data.lock().await;
        match state
            .session_manager
            .get_session(session_id.to_string())
            .await
        {
            Some(session) => {
                let db = session.get_use_database();
                (
                    if db.trim().is_empty() {
                        "default".to_string()
                    } else {
                        db
                    },
                    "public".to_string(),
                )
            }
            None => ("default".to_string(), "public".to_string()),
        }
    };

    match build_canonical_query_payload(ast.query, session_id, &default_database, &default_schema) {
        Ok(payload) => {
            let (database, schema, table) = match extract_namespace_from_payload(&payload) {
                Ok(parts) => parts,
                Err(e) => {
                    return format_outcome("INFRA", "QUERY_PAYLOAD_NAMESPACE_MISSING", e);
                }
            };

            let mut params = HashMap::new();
            params.insert("database_name".to_string(), database.clone());
            params.insert("schema_name".to_string(), schema.clone());
            params.insert("table_name".to_string(), table.clone());
            params.insert("query_kind".to_string(), "select".to_string());

            let worker_result_location = match helpers::run_task_for_input_with_params(
                shared_data,
                session_id,
                "query",
                payload.clone(),
                params,
                120,
            )
            .await
            {
                Ok(location) => location,
                Err(e) => {
                    return format_outcome(
                        "INFRA",
                        "WORKER_QUERY_FAILED",
                        format!("worker query dispatch failed: {}", e),
                    );
                }
            };

            log::info!(
                "query_select dispatched: session_id={} database={} schema={} table={} worker_result_location={}",
                session_id,
                database,
                schema,
                table,
                worker_result_location
            );
            format_outcome(
                "SUCCESS",
                "QUERY_DISPATCHED",
                format!(
                    "query dispatched successfully for {}.{}.{} (location: {})",
                    database, schema, table, worker_result_location
                ),
            )
        }
        Err(e) => format_outcome("VALIDATION", "UNSUPPORTED_QUERY_SHAPE", e),
    }
}

/// What: Build a SELECT AST wrapper from a generic SQL statement.
///
/// Inputs:
/// - `stmt`: Parsed SQL statement.
///
/// Output:
/// - `SelectQueryAst` for query statements.
///
/// Details:
/// - Returns an error when the statement is not a query statement.
pub(crate) fn select_ast_from_statement(stmt: &Statement) -> Result<SelectQueryAst<'_>, String> {
    match stmt {
        Statement::Query(query) => Ok(SelectQueryAst { query }),
        _ => Err("statement is not a query".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        build_canonical_query_payload, extract_minimal_select, extract_namespace_from_payload,
    };
    use kionas::parser::sql::parse_query;

    #[test]
    fn minimal_select_payload_builds() {
        let statements = parse_query("SELECT id, name FROM sales.public.users WHERE active = true")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            kionas::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let payload = build_canonical_query_payload(query, "s1", "sales", "public")
            .expect("payload should build");
        assert!(payload.contains("\"statement\":\"Select\""));
        assert!(payload.contains("\"database\":\"sales\""));
        assert!(payload.contains("\"table\":\"users\""));
    }

    #[test]
    fn rejects_multi_table_shape() {
        let statements = parse_query("SELECT * FROM a, b").expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            kionas::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let err = extract_minimal_select(query).expect_err("should reject multi-table select");
        assert!(err.contains("exactly one table"));
    }

    #[test]
    fn extracts_namespace_from_payload() {
        let statements =
            parse_query("SELECT id FROM sales.public.users").expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            kionas::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let payload = build_canonical_query_payload(query, "s1", "sales", "public")
            .expect("payload should build");
        let (database, schema, table) =
            extract_namespace_from_payload(&payload).expect("namespace should be present");

        assert_eq!(database, "sales");
        assert_eq!(schema, "public");
        assert_eq!(table, "users");
    }
}
