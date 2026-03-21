use crate::services::metastore_client::MetastoreClient;
use crate::services::metastore_client::metastore_service as ms;
use crate::services::request_context::RequestContext;
use crate::statement_handler::distributed_dag;
use crate::statement_handler::helpers;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::{Query as SqlQuery, Statement};
use kionas::planner::{PhysicalOperator, PhysicalPlan};
use kionas::planner::{distributed_from_physical_plan, validate_distributed_physical_plan};
use kionas::sql::query_model::{
    build_select_query_dispatch_envelope, validation_code_for_query_error,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
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

/// What: Build a stable relation key used by worker-side relation column mapping.
///
/// Inputs:
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
///
/// Output:
/// - Lowercase key in `<database>.<schema>.<table>` form.
fn relation_key(database: &str, schema: &str, table: &str) -> String {
    format!(
        "{}.{}.{}",
        database.to_ascii_lowercase(),
        schema.to_ascii_lowercase(),
        table.to_ascii_lowercase()
    )
}

/// What: Read canonical table columns from metastore for one relation namespace.
///
/// Inputs:
/// - `shared_data`: Shared server state used to open metastore client.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
///
/// Output:
/// - Ordered canonical column names when metadata is available.
async fn load_relation_columns(
    shared_data: &SharedData,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, String> {
    let mut client = MetastoreClient::connect_with_shared(shared_data)
        .await
        .map_err(|e| {
            format!(
                "failed to connect to metastore for relation metadata: {}",
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
                "failed to fetch table metadata from metastore for {}.{}.{}: {}",
                database, schema, table, e
            )
        })?;

    let table_result = response.result.ok_or_else(|| {
        format!(
            "metastore returned empty get_table result for {}.{}.{}",
            database, schema, table
        )
    })?;

    let get_table_response = match table_result {
        ms::metastore_response::Result::GetTableResponse(value) => value,
        _ => {
            return Err(format!(
                "metastore returned unexpected response for get_table {}.{}.{}",
                database, schema, table
            ));
        }
    };

    if !get_table_response.success {
        return Err(format!(
            "metastore get_table failed for {}.{}.{}: {}",
            database, schema, table, get_table_response.message
        ));
    }

    let metadata = get_table_response.metadata.ok_or_else(|| {
        format!(
            "metastore get_table missing metadata for {}.{}.{}",
            database, schema, table
        )
    })?;

    let columns = metadata
        .columns
        .iter()
        .map(|column| column.name.trim().to_ascii_lowercase())
        .filter(|name| !name.is_empty())
        .collect::<Vec<_>>();

    if columns.is_empty() {
        return Err(format!(
            "metastore get_table returned empty columns for {}.{}.{}",
            database, schema, table
        ));
    }

    Ok(columns)
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

            let mut relation_set = BTreeSet::<(String, String, String)>::new();
            relation_set.insert((database.clone(), schema.clone(), table.clone()));
            for operator in &physical_plan.operators {
                if let PhysicalOperator::HashJoin { spec } = operator {
                    relation_set.insert((
                        spec.right_relation.database.clone(),
                        spec.right_relation.schema.clone(),
                        spec.right_relation.table.clone(),
                    ));
                }
            }

            let mut relation_columns = BTreeMap::<String, Vec<String>>::new();
            for (relation_db, relation_schema, relation_table) in relation_set {
                match load_relation_columns(
                    shared_data,
                    relation_db.as_str(),
                    relation_schema.as_str(),
                    relation_table.as_str(),
                )
                .await
                {
                    Ok(columns) => {
                        relation_columns.insert(
                            relation_key(
                                relation_db.as_str(),
                                relation_schema.as_str(),
                                relation_table.as_str(),
                            ),
                            columns,
                        );
                    }
                    Err(err) => {
                        log::warn!(
                            "query_select relation metadata unavailable for {}.{}.{}: {}",
                            relation_db,
                            relation_schema,
                            relation_table,
                            err
                        );
                    }
                }
            }

            let relation_columns_json = if relation_columns.is_empty() {
                None
            } else {
                serde_json::to_string(&relation_columns).ok()
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
                if let Some(mapping_json) = relation_columns_json.as_ref() {
                    group
                        .params
                        .insert("relation_columns_json".to_string(), mapping_json.clone());
                }
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
