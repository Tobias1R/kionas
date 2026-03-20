use crate::services::request_context::RequestContext;
use crate::statement_handler::distributed_dag;
use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::{Query as SqlQuery, Statement};
use kionas::planner::PhysicalPlan;
use kionas::planner::{distributed_from_physical_plan, validate_distributed_physical_plan};
use kionas::sql::query_model::{
    build_select_query_dispatch_envelope, validation_code_for_query_error,
};
use serde_json::Value;
use uuid::Uuid;

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
    ctx: &RequestContext,
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

            let payload_value: Value = match serde_json::from_str(&payload) {
                Ok(value) => value,
                Err(e) => {
                    return format_outcome(
                        "INFRA",
                        "WORKER_QUERY_FAILED",
                        format!("failed to parse canonical query payload: {}", e),
                    );
                }
            };

            let physical_value = match payload_value.get("physical_plan") {
                Some(value) => value.clone(),
                None => {
                    return format_outcome(
                        "INFRA",
                        "WORKER_QUERY_FAILED",
                        "canonical query payload missing physical_plan",
                    );
                }
            };

            let physical_plan: PhysicalPlan = match serde_json::from_value(physical_value) {
                Ok(plan) => plan,
                Err(e) => {
                    return format_outcome(
                        "INFRA",
                        "WORKER_QUERY_FAILED",
                        format!("failed to decode physical_plan from payload: {}", e),
                    );
                }
            };

            let distributed_plan = distributed_from_physical_plan(&physical_plan);
            if let Err(e) = validate_distributed_physical_plan(&distributed_plan) {
                return format_outcome(
                    "INFRA",
                    "WORKER_QUERY_FAILED",
                    format!("distributed plan validation failed: {}", e),
                );
            }

            let mut stage_groups =
                match distributed_dag::compile_stage_task_groups(&distributed_plan, "query") {
                    Ok(groups) => groups,
                    Err(e) => {
                        return format_outcome(
                            "INFRA",
                            "WORKER_QUERY_FAILED",
                            format!("failed to compile distributed stage groups: {}", e),
                        );
                    }
                };

            let query_run_id = Uuid::new_v4().to_string();

            for group in &mut stage_groups {
                group
                    .params
                    .insert("database_name".to_string(), database.clone());
                group
                    .params
                    .insert("schema_name".to_string(), schema.clone());
                group.params.insert("table_name".to_string(), table.clone());
                group
                    .params
                    .insert("query_kind".to_string(), "select".to_string());
                group
                    .params
                    .insert("query_run_id".to_string(), query_run_id.clone());
            }

            let auth_scope = format!("select:{}.{}.{}", database, schema, table);
            let dispatch_auth_ctx = helpers::DispatchAuthContext {
                rbac_user: ctx.rbac_user.clone(),
                rbac_role: ctx.role.clone(),
                scope: auth_scope,
                query_id: ctx.query_id.clone(),
            };

            let worker_result_location = match helpers::run_stage_groups_for_input(
                shared_data,
                session_id,
                &stage_groups,
                Some(&dispatch_auth_ctx),
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
