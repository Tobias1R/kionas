use crate::parser::datafusion_sql::sqlparser::ast::{Query as SqlQuery, Statement};
use crate::planner::{
    DataFusionPlanArtifacts, build_datafusion_plan_artifacts_with_providers,
    translate_datafusion_to_kionas_physical_plan_with_providers,
};
use crate::providers::{KionasMetastoreResolver, KionasRelationMetadata};
use crate::services::request_context::RequestContext;
use crate::statement_handler::shared::distributed_dag;
use crate::statement_handler::shared::helpers;
use crate::warehouse::state::SharedData;
use deltalake::open_table_with_storage_options;
use kionas::planner::render_predicate_expr;
use kionas::planner::{PhysicalExpr, PhysicalOperator, PhysicalPlan};
use kionas::planner::{distributed_from_physical_plan, validate_distributed_physical_plan};
use kionas::sql::query_model::{
    VALIDATION_CODE_UNSUPPORTED_OPERATOR, VALIDATION_CODE_UNSUPPORTED_PIPELINE,
    VALIDATION_CODE_UNSUPPORTED_PREDICATE, VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
    build_select_query_model, validation_code_for_query_error,
};
use kionas::{config, parse_env_vars};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::collections::{BTreeMap, BTreeSet};
use url::Url;
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

fn validation_code_for_planner_error(err: &kionas::planner::PlannerError) -> &'static str {
    match err {
        kionas::planner::PlannerError::InvalidPhysicalPipeline(_) => {
            VALIDATION_CODE_UNSUPPORTED_PIPELINE
        }
        kionas::planner::PlannerError::UnsupportedPhysicalOperator(_) => {
            VALIDATION_CODE_UNSUPPORTED_OPERATOR
        }
        kionas::planner::PlannerError::UnsupportedPredicate(_) => {
            VALIDATION_CODE_UNSUPPORTED_PREDICATE
        }
        _ => VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
    }
}

fn is_relation_resolution_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (lower.contains("table") && lower.contains("not found"))
        || lower.contains("no suitable object store found")
        || lower.contains("register_object_store")
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

#[derive(Clone)]
struct PredicateComparisonAst {
    column: String,
    op: String,
    value: String,
}

#[derive(Clone)]
enum PredicateAst {
    Comparison(PredicateComparisonAst),
    And(Vec<PredicateAst>),
}

/// What: Parse one comparison term from a SQL predicate fragment.
///
/// Inputs:
/// - `term`: Trimmed predicate term expected to contain one comparison operator.
///
/// Output:
/// - Parsed comparison AST when the term matches supported shape.
fn parse_comparison_term(term: &str) -> Option<PredicateComparisonAst> {
    let operators = ["<=", ">=", "!=", "=", "<", ">"];

    for op in operators {
        if let Some(index) = term.find(op) {
            let left = term[..index].trim();
            let right = term[index + op.len()..].trim();
            if left.is_empty() || right.is_empty() {
                return None;
            }

            return Some(PredicateComparisonAst {
                column: left.to_string(),
                op: op.to_string(),
                value: right.to_string(),
            });
        }
    }

    None
}

/// What: Parse a simple FOUNDATION predicate AST with AND combinations.
///
/// Inputs:
/// - `predicate_sql`: Filter predicate SQL string from physical filter operator.
///
/// Output:
/// - Parsed predicate AST when all terms match supported comparison shape.
fn parse_foundation_predicate_ast(predicate_sql: &str) -> Option<PredicateAst> {
    let trimmed = predicate_sql.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut terms = Vec::<String>::new();
    let mut current = Vec::<String>::new();
    for token in trimmed.split_whitespace() {
        if token.eq_ignore_ascii_case("OR") {
            return None;
        }
        if token.eq_ignore_ascii_case("AND") {
            if current.is_empty() {
                return None;
            }
            terms.push(current.join(" "));
            current.clear();
            continue;
        }
        current.push(token.to_string());
    }
    if !current.is_empty() {
        terms.push(current.join(" "));
    }

    if terms.is_empty() {
        return None;
    }

    let mut parsed_terms = Vec::with_capacity(terms.len());
    for term in &terms {
        let parsed = parse_comparison_term(term.as_str())?;
        parsed_terms.push(PredicateAst::Comparison(parsed));
    }

    if parsed_terms.len() == 1 {
        parsed_terms.into_iter().next()
    } else {
        Some(PredicateAst::And(parsed_terms))
    }
}

fn predicate_ast_to_json(ast: &PredicateAst) -> serde_json::Value {
    match ast {
        PredicateAst::Comparison(node) => json!({
            "kind": "comparison",
            "column": node.column,
            "op": node.op,
            "value": node.value,
        }),
        PredicateAst::And(terms) => json!({
            "kind": "and",
            "terms": terms.iter().map(predicate_ast_to_json).collect::<Vec<_>>(),
        }),
    }
}

/// What: Build deltalake storage options from cluster storage configuration.
///
/// Inputs:
/// - `storage`: Cluster storage config from shared kionas config model.
///
/// Output:
/// - Deltalake storage options map.
fn delta_storage_options_from_cluster(storage: &config::StorageConfig) -> HashMap<String, String> {
    let mut options = HashMap::new();

    let endpoint = parse_env_vars(&storage.endpoint);
    let region = parse_env_vars(&storage.region);
    let access_key = parse_env_vars(&storage.access_key);
    let secret_key = parse_env_vars(&storage.secret_key);

    if !region.trim().is_empty() {
        options.insert("aws_region".to_string(), region.trim().to_string());
    }
    if !access_key.trim().is_empty() {
        options.insert(
            "aws_access_key_id".to_string(),
            access_key.trim().to_string(),
        );
    }
    if !secret_key.trim().is_empty() {
        options.insert(
            "aws_secret_access_key".to_string(),
            secret_key.trim().to_string(),
        );
    }
    if !endpoint.trim().is_empty() {
        options.insert("aws_endpoint".to_string(), endpoint.trim().to_string());
        options.insert("aws_endpoint_url".to_string(), endpoint.trim().to_string());
        if endpoint.trim().starts_with("http://") {
            options.insert("aws_allow_http".to_string(), "true".to_string());
            options.insert("allow_http".to_string(), "true".to_string());
        }
    }

    options
}

/// What: Read canonical table location from metastore for one relation namespace.
///
/// Inputs:
/// - `shared_data`: Shared server state used to open metastore client.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
///
/// Output:
/// - Canonical table location URI used for Delta snapshot resolution.
async fn load_relation_location(
    shared_data: &SharedData,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<String, String> {
    let resolver = KionasMetastoreResolver::new(shared_data.clone(), 8)?;
    let relation = resolver.resolve_relation(database, schema, table).await?;
    let location = relation.location.unwrap_or_default().trim().to_string();
    if location.is_empty() {
        return Err(format!(
            "metastore get_table returned empty location for {}.{}.{}",
            database, schema, table
        ));
    }

    Ok(location)
}

/// What: Resolve scan delta version pin from Delta snapshot metadata.
///
/// Inputs:
/// - `shared_data`: Shared server state for config and metastore access.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
///
/// Output:
/// - Delta table snapshot version pin when available.
async fn resolve_scan_delta_version_pin(
    shared_data: &SharedData,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<u64, String> {
    let table_location = load_relation_location(shared_data, database, schema, table).await?;

    let consul_host = {
        let state = shared_data.lock().await;
        state
            .config
            .as_ref()
            .map(|cfg| cfg.consul_host.clone())
            .unwrap_or_default()
    };

    let cluster_config = config::load_cluster_config(Some(consul_host.as_str()))
        .await
        .map_err(|e| format!("failed to load cluster config for delta pin lookup: {}", e))?;
    let storage_options = delta_storage_options_from_cluster(&cluster_config.storage);

    let table_url = Url::parse(&table_location).map_err(|e| {
        format!(
            "invalid table location URI for delta pin lookup {}.{}.{} ({}): {}",
            database, schema, table, table_location, e
        )
    })?;

    let delta_table = open_table_with_storage_options(table_url, storage_options)
        .await
        .map_err(|e| {
            format!(
                "failed to open delta table for pin lookup {}.{}.{} ({}): {}",
                database, schema, table, table_location, e
            )
        })?;
    let version = delta_table.version().ok_or_else(|| {
        format!(
            "delta table has no committed snapshot version for {}.{}.{} ({})",
            database, schema, table, table_location
        )
    })?;
    u64::try_from(version).map_err(|_| {
        format!(
            "delta table version is negative for {}.{}.{} ({}): {}",
            database, schema, table, table_location, version
        )
    })
}

/// What: Build compact scan pruning hints from the physical plan filter operators.
///
/// Inputs:
/// - `physical_plan`: Canonical physical plan emitted by planner translation.
///
/// Output:
/// - Optional serialized JSON pruning hint payload for worker scan runtime.
///
/// Details:
/// - FOUNDATION emits lightweight hints only; worker still owns eligibility and fallback.
fn build_scan_pruning_hints_json(physical_plan: &PhysicalPlan) -> Option<String> {
    let mut predicate_sql = None;

    for operator in &physical_plan.operators {
        if let PhysicalOperator::Filter { predicate } = operator {
            let raw = match predicate {
                PhysicalExpr::Raw { sql } => sql.trim().to_string(),
                PhysicalExpr::ColumnRef { name } => name.trim().to_string(),
                PhysicalExpr::Predicate { predicate } => render_predicate_expr(predicate),
            };
            if !raw.is_empty() {
                predicate_sql = Some(raw);
                break;
            }
        }
    }

    predicate_sql.and_then(|sql| {
        let parsed_ast = parse_foundation_predicate_ast(&sql);
        let eligible = parsed_ast.is_some();
        let reason = if eligible {
            "eligible_foundation_and_comparisons"
        } else {
            "unsupported_predicate_shape"
        };

        serde_json::to_string(&json!({
            "hint_version": 2,
            "source": "foundation_server",
            "predicate_sql": sql,
            "eligible": eligible,
            "reason": reason,
            "predicate_ast": parsed_ast.as_ref().map(predicate_ast_to_json),
        }))
        .ok()
    })
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
    let resolver = KionasMetastoreResolver::new(shared_data.clone(), 8)?;
    let relation = resolver.resolve_relation(database, schema, table).await?;
    let columns = relation.column_names();

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

    let canonical_query =
        match build_select_query_model(ast.query, session_id, &default_database, &default_schema) {
            Ok(value) => value,
            Err(e) => {
                return format_outcome(
                    "VALIDATION",
                    validation_code_for_query_error(&e),
                    e.to_string(),
                );
            }
        };

    let database = canonical_query.database;
    let schema = canonical_query.schema;
    let table = canonical_query.table;
    let model = canonical_query.model;

    let mut planning_relation_set = BTreeSet::<(String, String, String)>::new();
    planning_relation_set.insert((database.clone(), schema.clone(), table.clone()));
    for join in &model.joins {
        planning_relation_set.insert((
            join.right.database.clone(),
            join.right.schema.clone(),
            join.right.table.clone(),
        ));
    }

    let resolver = match KionasMetastoreResolver::new(shared_data.clone(), 8) {
        Ok(value) => value,
        Err(error) => {
            return format_outcome(
                "INFRA",
                "WORKER_QUERY_FAILED",
                format!("failed to initialize metastore resolver: {}", error),
            );
        }
    };

    let mut planner_relation_inputs = Vec::<KionasRelationMetadata>::new();
    let mut relation_columns_by_relation = BTreeMap::<String, Vec<String>>::new();
    for (relation_db, relation_schema, relation_table) in &planning_relation_set {
        match resolver
            .resolve_relation(
                relation_db.as_str(),
                relation_schema.as_str(),
                relation_table.as_str(),
            )
            .await
        {
            Ok(metadata) => {
                let columns = metadata.column_names();
                relation_columns_by_relation.insert(
                    relation_key(
                        relation_db.as_str(),
                        relation_schema.as_str(),
                        relation_table.as_str(),
                    ),
                    columns.clone(),
                );
                planner_relation_inputs.push(metadata);
            }
            Err(err) => {
                log::warn!(
                    "query_select relation metadata unavailable for planner {}.{}.{}: {}",
                    relation_db,
                    relation_schema,
                    relation_table,
                    err
                );
            }
        }
    }

    let physical_plan = match translate_datafusion_to_kionas_physical_plan_with_providers(
        &model,
        &planner_relation_inputs,
    )
    .await
    {
        Ok(plan) => plan,
        Err(e) => {
            return format_outcome(
                "VALIDATION",
                validation_code_for_planner_error(&e),
                e.to_string(),
            );
        }
    };
    let physical_plan_text = kionas::planner::explain::explain_physical_plan(&physical_plan);

    let datafusion_artifacts =
        match build_datafusion_plan_artifacts_with_providers(&model.sql, &planner_relation_inputs)
            .await
        {
            Ok(artifacts) => artifacts,
            Err(err) => {
                let message = err.to_string();
                if is_relation_resolution_error(&message) {
                    DataFusionPlanArtifacts {
                        logical_plan_text: format!(
                            "DataFusion logical diagnostics unavailable: {}",
                            message
                        ),
                        optimized_logical_plan_text: format!(
                            "DataFusion optimized logical diagnostics unavailable: {}",
                            message
                        ),
                        physical_plan_text: physical_plan_text.clone(),
                    }
                } else {
                    return format_outcome(
                        "VALIDATION",
                        VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
                        format!("failed to build logical plan: {}", message),
                    );
                }
            }
        };

    let physical_pipeline = physical_plan
        .operators
        .iter()
        .map(|op| op.canonical_name().to_string())
        .collect::<Vec<_>>();
    let payload = json!({
        "version": model.version,
        "statement": model.statement,
        "session_id": model.session_id,
        "namespace": model.namespace,
        "projection": model.projection,
        "selection": model.selection,
        "joins": model.joins,
        "group_by": model.group_by,
        "order_by": model.order_by,
        "limit": model.limit,
        "offset": model.offset,
        "sql": model.sql,
        "logical_plan": {
            "engine": "datafusion",
            "text": datafusion_artifacts.logical_plan_text,
        },
        "physical_plan": physical_plan,
        "diagnostics": {
            "logical_plan_text": datafusion_artifacts.logical_plan_text,
            "logical_plan_optimized_text": datafusion_artifacts.optimized_logical_plan_text,
            "physical_plan_text": datafusion_artifacts.physical_plan_text,
            "physical_plan_legacy_text": physical_plan_text,
            "physical_pipeline": physical_pipeline,
        },
    })
    .to_string();

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
        let key = relation_key(
            relation_db.as_str(),
            relation_schema.as_str(),
            relation_table.as_str(),
        );
        if let Some(existing) = relation_columns_by_relation.get(key.as_str()) {
            relation_columns.insert(key, existing.clone());
            continue;
        }

        match load_relation_columns(
            shared_data,
            relation_db.as_str(),
            relation_schema.as_str(),
            relation_table.as_str(),
        )
        .await
        {
            Ok(columns) => {
                relation_columns.insert(key, columns);
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
    let scan_pruning_hints_json = build_scan_pruning_hints_json(&physical_plan);
    let scan_hints_eligible = scan_pruning_hints_json
        .as_ref()
        .and_then(|value| serde_json::from_str::<Value>(value).ok())
        .and_then(|value| value.get("eligible").and_then(Value::as_bool))
        .unwrap_or(false);
    let scan_mode = if scan_hints_eligible {
        "metadata_pruned"
    } else {
        "full"
    };
    let scan_delta_version_pin = match resolve_scan_delta_version_pin(
        shared_data,
        database.as_str(),
        schema.as_str(),
        table.as_str(),
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            log::warn!(
                "query_select failed to resolve delta snapshot version pin for {}.{}.{}: {} (emitting sentinel scan_delta_version_pin=0)",
                database,
                schema,
                table,
                err
            );
            0
        }
    };

    if scan_mode == "metadata_pruned" && scan_delta_version_pin == 0 {
        log::warn!(
            "query_select metadata_pruned mode enabled without concrete delta version pin; emitting sentinel scan_delta_version_pin=0"
        );
    }

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
        // Phase FOUNDATION contract: scan metadata is always explicit,
        // even when runtime falls back to full scan behavior.
        group
            .params
            .insert("scan_mode".to_string(), scan_mode.to_string());
        group.params.insert(
            "scan_delta_version_pin".to_string(),
            scan_delta_version_pin.to_string(),
        );
        if let Some(scan_hints_json) = scan_pruning_hints_json.as_ref() {
            group.params.insert(
                "scan_pruning_hints_json".to_string(),
                scan_hints_json.clone(),
            );
        }
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
#[path = "../../tests/statement_handler_query_select_tests.rs"]
mod tests;
