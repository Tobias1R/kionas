use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::{Query as SqlQuery, Statement};
use kionas::sql::query_model::{
    QueryModelError, build_select_query_dispatch_envelope, validation_code_for_query_error,
};
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

    match build_select_query_dispatch_envelope(
        ast.query,
        session_id,
        &default_database,
        &default_schema,
    ) {
        Ok(canonical_query) => {
            let payload = canonical_query.payload;
            let database = canonical_query.database;
            let schema = canonical_query.schema;
            let table = canonical_query.table;

            let mut params = HashMap::new();
            params.insert("database_name".to_string(), database.clone());
            params.insert("schema_name".to_string(), schema.clone());
            params.insert("table_name".to_string(), table.clone());
            params.insert("query_kind".to_string(), "select".to_string());

            let worker_result_location = match helpers::run_task_for_input_with_params(
                shared_data,
                session_id,
                "query",
                payload,
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
        Err(e) => format_outcome(
            "VALIDATION",
            validation_code_for_query_error(&e),
            e.to_string(),
        ),
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
    use kionas::parser::sql::parse_query;
    use kionas::sql::query_model::{
        QueryModelError, VALIDATION_CODE_UNSUPPORTED_OPERATOR,
        VALIDATION_CODE_UNSUPPORTED_PIPELINE, VALIDATION_CODE_UNSUPPORTED_PREDICATE,
        build_select_query_dispatch_envelope, validation_code_for_query_error,
    };

    #[test]
    fn minimal_select_payload_builds() {
        let statements = parse_query("SELECT id, name FROM sales.public.users WHERE active = true")
            .expect("statement should parse");
        let statement = statements.first().expect("statement expected");
        let query = match statement {
            kionas::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
            _ => panic!("expected query statement"),
        };

        let canonical = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect("payload should build");
        let payload = canonical.payload;
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

        let err = build_select_query_dispatch_envelope(query, "s1", "default", "public")
            .expect_err("should reject multi-table select");
        assert!(err.to_string().contains("exactly one table"));
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

        let canonical = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
            .expect("payload should build");
        let database = canonical.database;
        let schema = canonical.schema;
        let table = canonical.table;

        assert_eq!(database, "sales");
        assert_eq!(schema, "public");
        assert_eq!(table, "users");
    }

    #[test]
    fn maps_capability_error_codes() {
        assert_eq!(
            validation_code_for_query_error(&QueryModelError::InvalidPhysicalPipeline(
                "pipeline must end with materialize".to_string(),
            )),
            VALIDATION_CODE_UNSUPPORTED_PIPELINE
        );
        assert_eq!(
            validation_code_for_query_error(&QueryModelError::UnsupportedPhysicalOperator(
                "HashJoin".to_string(),
            )),
            VALIDATION_CODE_UNSUPPORTED_OPERATOR
        );
        assert_eq!(
            validation_code_for_query_error(&QueryModelError::UnsupportedPredicate(
                "LIKE".to_string(),
            )),
            VALIDATION_CODE_UNSUPPORTED_PREDICATE
        );
    }
}
