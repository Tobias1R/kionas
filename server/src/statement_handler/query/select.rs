use crate::parser::datafusion_sql::sqlparser::ast::{Query as SqlQuery, Statement};
use crate::planner::{DataFusionPlanArtifacts, DataFusionQueryPlanner};
use crate::providers::{KionasMetastoreResolver, KionasRelationMetadata};
use crate::services::request_context::RequestContext;
use crate::statement_handler::shared::distributed_dag;
use crate::statement_handler::shared::helpers;
use crate::warehouse::state::SharedData;
use deltalake::open_table_with_storage_options;
use kionas::planner::DistributedPhysicalPlan;
use kionas::planner::render_predicate_expr;
use kionas::planner::{PhysicalExpr, PhysicalOperator, PhysicalPlan, StageDependency};
use kionas::planner::{distributed_from_physical_plan, validate_distributed_physical_plan};
use kionas::sql::query_model::{
    SelectQueryModel, VALIDATION_CODE_UNSUPPORTED_OPERATOR, VALIDATION_CODE_UNSUPPORTED_PIPELINE,
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
const OUTCOME_CATEGORY_SUCCESS: &str = "SUCCESS";
const OUTCOME_CATEGORY_VALIDATION: &str = "VALIDATION";
const OUTCOME_CATEGORY_INFRA: &str = "INFRA";
const OUTCOME_CODE_WORKER_QUERY_FAILED: &str = "WORKER_QUERY_FAILED";
const OUTCOME_CODE_QUERY_DISPATCHED: &str = "QUERY_DISPATCHED";
const SQL_LOG_PREVIEW_CHARS: usize = 512;
const DEFAULT_DATABASE_NAME: &str = "default";
const DEFAULT_SCHEMA_NAME: &str = "public";
const SELECT_PLAN_ENGINE_DATAFUSION: &str = "datafusion";
const DISTRIBUTED_STAGE_OPERATION_QUERY: &str = "query";
const SCAN_MODE_METADATA_PRUNED: &str = "metadata_pruned";
const SCAN_MODE_FULL: &str = "full";
const METASTORE_RESOLVER_CONCURRENCY: usize = 8;
const DISPATCH_TIMEOUT_SECS: u64 = 120;
const SCAN_DELTA_VERSION_PIN_SENTINEL: u64 = 0;
const SCAN_DELTA_VERSION_PIN_SENTINEL_WARNING: &str = "emitting sentinel scan_delta_version_pin";
const STAGE_PARAM_DATABASE_NAME: &str = "database_name";
const STAGE_PARAM_SCHEMA_NAME: &str = "schema_name";
const STAGE_PARAM_TABLE_NAME: &str = "table_name";
const STAGE_PARAM_QUERY_KIND: &str = "query_kind";
const STAGE_PARAM_QUERY_RUN_ID: &str = "query_run_id";
const STAGE_PARAM_SCAN_MODE: &str = "scan_mode";
const STAGE_PARAM_SCAN_DELTA_VERSION_PIN: &str = "scan_delta_version_pin";
const STAGE_PARAM_SCAN_PRUNING_HINTS_JSON: &str = "scan_pruning_hints_json";
const STAGE_PARAM_RELATION_COLUMNS_JSON: &str = "relation_columns_json";
const STAGE_PARAM_QUERY_KIND_SELECT: &str = "select";
const DELTA_STORAGE_AWS_REGION: &str = "aws_region";
const DELTA_STORAGE_AWS_ACCESS_KEY_ID: &str = "aws_access_key_id";
const DELTA_STORAGE_AWS_SECRET_ACCESS_KEY: &str = "aws_secret_access_key";
const DELTA_STORAGE_AWS_ENDPOINT: &str = "aws_endpoint";
const DELTA_STORAGE_AWS_ENDPOINT_URL: &str = "aws_endpoint_url";
const DELTA_STORAGE_AWS_ALLOW_HTTP: &str = "aws_allow_http";
const DELTA_STORAGE_ALLOW_HTTP: &str = "allow_http";
const DELTA_STORAGE_TRUE: &str = "true";
const SCAN_HINT_VERSION: u8 = 2;
const SCAN_HINT_SOURCE: &str = "foundation_server";
const SCAN_HINT_REASON_ELIGIBLE: &str = "eligible_foundation_and_comparisons";
const SCAN_HINT_REASON_UNSUPPORTED: &str = "unsupported_predicate_shape";
const DATAFUSION_LOGICAL_DIAGNOSTICS_UNAVAILABLE: &str =
    "DataFusion logical diagnostics unavailable";
const DATAFUSION_OPTIMIZED_LOGICAL_DIAGNOSTICS_UNAVAILABLE: &str =
    "DataFusion optimized logical diagnostics unavailable";
const DATAFUSION_BUILD_LOGICAL_PLAN_FAILED: &str = "failed to build logical plan";
const RELATION_METADATA_UNAVAILABLE_FOR_PLANNER: &str =
    "query_select relation metadata unavailable for planner";
const RELATION_METADATA_UNAVAILABLE_FOR_RUNTIME: &str =
    "query_select relation metadata unavailable for";

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

/// What: Build a normalized infrastructure outcome for worker-query execution failures.
///
/// Inputs:
/// - `message`: Failure detail to surface in the encoded statement outcome.
///
/// Output:
/// - Encoded `INFRA|WORKER_QUERY_FAILED` outcome payload.
fn format_worker_query_failed_outcome(message: impl Into<String>) -> String {
    format_outcome(
        OUTCOME_CATEGORY_INFRA,
        OUTCOME_CODE_WORKER_QUERY_FAILED,
        message,
    )
}

/// What: Build a normalized validation outcome envelope.
///
/// Inputs:
/// - `code`: Stable machine-readable validation code.
/// - `message`: Validation detail to surface in the encoded statement outcome.
///
/// Output:
/// - Encoded `VALIDATION|<code>` outcome payload.
fn format_validation_outcome(code: &str, message: impl Into<String>) -> String {
    format_outcome(OUTCOME_CATEGORY_VALIDATION, code, message)
}

/// What: Build a standardized prefixed diagnostic message.
///
/// Inputs:
/// - `prefix`: Stable diagnostic message prefix.
/// - `message`: Runtime detail appended after the prefix.
///
/// Output:
/// - Combined message in `<prefix>: <message>` format.
fn format_prefixed_diagnostic_message(prefix: &str, message: &str) -> String {
    format!("{}: {}", prefix, message)
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

/// What: Build a compact SQL preview string for observability logs.
///
/// Inputs:
/// - `sql`: Original SQL text that may include long whitespace sequences.
///
/// Output:
/// - Single-line SQL preview capped to `SQL_LOG_PREVIEW_CHARS` characters.
///
/// Details:
/// - Collapses whitespace to reduce log noise.
/// - Appends truncation metadata when the compacted SQL exceeds the preview cap.
fn compact_whitespace_for_logs(sql: &str) -> String {
    let mut compact = String::with_capacity(sql.len());
    let mut previous_was_whitespace = false;

    for ch in sql.chars() {
        if ch.is_whitespace() {
            if !previous_was_whitespace && !compact.is_empty() {
                compact.push(' ');
            }
            previous_was_whitespace = true;
        } else {
            compact.push(ch);
            previous_was_whitespace = false;
        }
    }

    if compact.ends_with(' ') {
        compact.pop();
    }

    compact
}

fn sql_preview_for_logs(sql: &str) -> String {
    let compact = compact_whitespace_for_logs(sql);
    let compact_len = compact.chars().count();

    if compact_len <= SQL_LOG_PREVIEW_CHARS {
        return compact;
    }

    let preview = compact
        .chars()
        .take(SQL_LOG_PREVIEW_CHARS)
        .collect::<String>();
    format!(
        "{}... [truncated {} chars]",
        preview,
        compact_len.saturating_sub(SQL_LOG_PREVIEW_CHARS)
    )
}

struct RoutingObservabilityContext<'a> {
    routing_source: &'a str,
    runtime_address_count: usize,
    effective_address_count: usize,
    env_fallback_applied: bool,
    fallback_kind: &'a str,
    fallback_active: bool,
    distributed_stage_count: usize,
}

/// What: Build routing observability context from runtime/effective worker-pool metadata.
///
/// Inputs:
/// - `routing_source`: Runtime source label used to resolve worker addresses.
/// - `runtime_address_count`: Number of runtime worker addresses before fallback.
/// - `effective_address_count`: Number of effective worker addresses used for routing.
/// - `effective_pool_source`: Effective routing pool source (`runtime`, `env`, `default`).
/// - `distributed_stage_count`: Number of distributed stages in compiled plan.
///
/// Output:
/// - Fully populated routing observability context used for logs and stage params.
///
/// Details:
/// - Preserves existing fallback activation contract: active only when fallback is applied on multi-stage plans.
fn build_routing_observability_context<'a>(
    routing_source: &'a str,
    runtime_address_count: usize,
    effective_address_count: usize,
    effective_pool_source: &'a str,
    distributed_stage_count: usize,
) -> RoutingObservabilityContext<'a> {
    let env_fallback_applied =
        effective_pool_source != distributed_dag::ROUTING_POOL_SOURCE_RUNTIME;
    let fallback_active = env_fallback_applied && distributed_stage_count > 1;

    RoutingObservabilityContext {
        routing_source,
        runtime_address_count,
        effective_address_count,
        env_fallback_applied,
        fallback_kind: effective_pool_source,
        fallback_active,
        distributed_stage_count,
    }
}

/// What: Emit routing observability logs for distributed worker address selection.
///
/// Inputs:
/// - `routing_source`: Runtime source used to resolve worker addresses.
/// - `runtime_address_count`: Number of resolved runtime worker addresses before fallback.
/// - `effective_address_count`: Number of worker addresses used for routing after fallback.
/// - `env_fallback_applied`: Whether env/default fallback supplied the effective routing pool.
/// - `fallback_kind`: Effective routing pool source classification (`runtime`, `env`, `default`).
/// - `fallback_active`: Whether fallback routing is active for a multi-stage query.
/// - `distributed_stage_count`: Number of distributed stages in the compiled plan.
/// - `sql`: Original SQL text used for context in warning logs.
///
/// Output:
/// - Side effect only. Emits info and warning logs for distributed routing diagnostics.
///
/// Details:
/// - Emits a warning only when fallback routing is active for multi-stage plans.
fn log_distributed_routing_observability(context: &RoutingObservabilityContext, sql: &str) {
    log::info!(
        "query_select distributed_routing_worker_pool: source={} runtime_addresses={} effective_addresses={} env_fallback_applied={} fallback_kind={} fallback_active={}",
        context.routing_source,
        context.runtime_address_count,
        context.effective_address_count,
        context.env_fallback_applied,
        context.fallback_kind,
        context.fallback_active
    );

    if context.fallback_active {
        log::warn!(
            "query_select distributed_routing_fallback_active: stage_count={} source={} runtime_addresses={} effective_addresses={} env_fallback_applied={} fallback_kind={} sql={}",
            context.distributed_stage_count,
            context.routing_source,
            context.runtime_address_count,
            context.effective_address_count,
            context.env_fallback_applied,
            context.fallback_kind,
            sql
        );
    }
}

/// What: Attach routing observability fields to each distributed stage group.
///
/// Inputs:
/// - `stage_groups`: Mutable stage groups to annotate.
/// - `routing_source`: Runtime source used to resolve worker addresses.
/// - `runtime_address_count`: Number of resolved runtime worker addresses before fallback.
/// - `effective_address_count`: Number of worker addresses used for routing after fallback.
/// - `env_fallback_applied`: Whether env/default fallback supplied the effective routing pool.
/// - `fallback_kind`: Effective routing pool source classification (`runtime`, `env`, `default`).
/// - `fallback_active`: Whether fallback routing is active for a multi-stage query.
///
/// Output:
/// - Side effect only. Each stage group receives stable routing observability params.
///
/// Details:
/// - Uses stable key names so downstream worker diagnostics can rely on a fixed contract.
fn attach_distributed_routing_observability_to_stage_groups(
    stage_groups: &mut [distributed_dag::StageTaskGroup],
    context: &RoutingObservabilityContext,
) {
    let routing_worker_source_key = distributed_dag::ROUTING_WORKER_SOURCE_PARAM.to_string();
    let routing_worker_count_key = distributed_dag::ROUTING_WORKER_COUNT_PARAM.to_string();
    let routing_runtime_worker_count_key =
        distributed_dag::ROUTING_RUNTIME_WORKER_COUNT_PARAM.to_string();
    let routing_effective_worker_count_key =
        distributed_dag::ROUTING_EFFECTIVE_WORKER_COUNT_PARAM.to_string();
    let routing_env_fallback_applied_key =
        distributed_dag::ROUTING_ENV_FALLBACK_APPLIED_PARAM.to_string();
    let routing_fallback_kind_key = distributed_dag::ROUTING_FALLBACK_KIND_PARAM.to_string();
    let routing_fallback_active_key = distributed_dag::ROUTING_FALLBACK_ACTIVE_PARAM.to_string();
    let routing_source = context.routing_source.to_string();
    let runtime_address_count = context.runtime_address_count.to_string();
    let effective_address_count = context.effective_address_count.to_string();
    let env_fallback_applied = context.env_fallback_applied.to_string();
    let fallback_kind = context.fallback_kind.to_string();
    let fallback_active = context.fallback_active.to_string();

    for group in stage_groups {
        group
            .params
            .insert(routing_worker_source_key.clone(), routing_source.clone());
        group.params.insert(
            routing_worker_count_key.clone(),
            effective_address_count.clone(),
        );
        group.params.insert(
            routing_runtime_worker_count_key.clone(),
            runtime_address_count.clone(),
        );
        group.params.insert(
            routing_effective_worker_count_key.clone(),
            effective_address_count.clone(),
        );
        group.params.insert(
            routing_env_fallback_applied_key.clone(),
            env_fallback_applied.clone(),
        );
        group
            .params
            .insert(routing_fallback_kind_key.clone(), fallback_kind.clone());
        group
            .params
            .insert(routing_fallback_active_key.clone(), fallback_active.clone());
    }
}

struct RuntimeWorkerAddressResolution {
    addresses: Vec<String>,
    source: &'static str,
}

/// What: Resolve runtime worker addresses with source diagnostics for distributed routing metadata.
///
/// Inputs:
/// - `shared_data`: Shared server state with registered workers and warehouses.
///
/// Output:
/// - Address resolution containing deduplicated `host:port` list and selected source label.
///
/// Details:
/// - Source is `workers` when worker entries are present.
/// - Source is `warehouses` when worker entries are empty but warehouses are present.
/// - Source is `fallback` when neither runtime source provides addresses.
async fn resolve_runtime_worker_address_resolution(
    shared_data: &SharedData,
) -> RuntimeWorkerAddressResolution {
    let shared = shared_data.lock().await;
    let workers = shared.workers.lock().await;
    let warehouses = shared.warehouses.lock().await;

    let mut addresses = workers
        .values()
        .map(|entry| {
            format!(
                "{}:{}",
                entry.warehouse.get_host(),
                entry.warehouse.get_port()
            )
        })
        .collect::<Vec<_>>();

    let source = if !addresses.is_empty() {
        distributed_dag::ROUTING_RUNTIME_SOURCE_WORKERS
    } else {
        addresses.extend(
            warehouses
                .values()
                .map(|warehouse| format!("{}:{}", warehouse.get_host(), warehouse.get_port())),
        );

        if addresses.is_empty() {
            distributed_dag::ROUTING_RUNTIME_SOURCE_FALLBACK
        } else {
            distributed_dag::ROUTING_RUNTIME_SOURCE_WAREHOUSES
        }
    };

    addresses.sort();
    addresses.dedup();

    RuntimeWorkerAddressResolution { addresses, source }
}

/// What: Resolve effective query routing worker pool with its observability context.
///
/// Inputs:
/// - `shared_data`: Shared server state containing runtime workers and warehouses.
/// - `distributed_stage_count`: Distributed stage count used for fallback-activation semantics.
///
/// Output:
/// - Tuple with effective routing worker pool and derived routing observability context.
///
/// Details:
/// - Preserves runtime->effective pool source contracts and fallback activation behavior.
async fn resolve_query_routing_context(
    shared_data: &SharedData,
    distributed_stage_count: usize,
) -> (Vec<String>, RoutingObservabilityContext<'static>) {
    let RuntimeWorkerAddressResolution {
        addresses: runtime_worker_addresses,
        source: routing_source,
    } = resolve_runtime_worker_address_resolution(shared_data).await;

    let (effective_routing_worker_pool, effective_pool_source) =
        distributed_dag::resolve_effective_routing_worker_pool_with_source(
            &runtime_worker_addresses,
        );

    let routing_observability = build_routing_observability_context(
        routing_source,
        runtime_worker_addresses.len(),
        effective_routing_worker_pool.len(),
        effective_pool_source,
        distributed_stage_count,
    );

    (effective_routing_worker_pool, routing_observability)
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

/// What: Parse a SQL predicate expression into FOUNDATION-compatible predicate AST.
///
/// Inputs:
/// - `predicate_sql`: Predicate SQL string emitted by planner diagnostics.
///
/// Output:
/// - Parsed predicate AST when expression shape is supported by FOUNDATION hints.
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

/// What: Replace distributed stage dependencies with extractor-derived topology when compatible.
///
/// Inputs:
/// - `plan`: Distributed plan produced from the Kionas physical plan.
/// - `stage_extraction`: DataFusion stage extraction diagnostics from planner engine.
///
/// Output:
/// - Distributed plan whose dependency edges mirror DataFusion stage extraction.
///
/// Details:
/// - Applies only when extracted stage ids and stage count align with the distributed plan.
/// - Falls back to original plan shape if any id mismatch is detected.
fn apply_stage_extraction_topology(
    mut plan: DistributedPhysicalPlan,
    stage_extraction: &crate::planner::DataFusionStageExtractionDiagnostics,
) -> DistributedPhysicalPlan {
    if stage_extraction.stage_count != plan.stages.len() {
        return plan;
    }

    let stage_ids = plan
        .stages
        .iter()
        .map(|stage| stage.stage_id)
        .collect::<BTreeSet<_>>();

    if stage_extraction
        .stages
        .iter()
        .any(|stage| !stage_ids.contains(&stage.stage_id))
    {
        return plan;
    }

    let extracted_partitioning_by_stage = stage_extraction
        .stages
        .iter()
        .map(|stage| {
            (
                stage.stage_id,
                (
                    stage.output_partitioning.as_str(),
                    stage.output_partitioning_keys.clone(),
                    stage.partitions_out as u32,
                ),
            )
        })
        .collect::<BTreeMap<_, _>>();

    for stage in &mut plan.stages {
        if let Some((partitioning_kind, output_partitioning_keys, extracted_partitions_out)) =
            extracted_partitioning_by_stage.get(&stage.stage_id)
        {
            stage.partition_spec = match *partitioning_kind {
                "single" => kionas::planner::PartitionSpec::Single,
                "hash" => {
                    if !output_partitioning_keys.is_empty() {
                        kionas::planner::PartitionSpec::Hash {
                            keys: output_partitioning_keys.clone(),
                        }
                    } else {
                        match &stage.partition_spec {
                            kionas::planner::PartitionSpec::Hash { .. } => {
                                stage.partition_spec.clone()
                            }
                            _ => kionas::planner::PartitionSpec::Hash {
                                keys: vec!["__datafusion_hash_partition".to_string()],
                            },
                        }
                    }
                }
                "round_robin_batch" => kionas::planner::PartitionSpec::Single,
                _ => stage.partition_spec.clone(),
            };

            stage.output_partition_count = Some((*extracted_partitions_out).max(1));
        }
    }

    let mut dependency_edges = BTreeSet::<(u32, u32)>::new();
    for stage in &stage_extraction.stages {
        for input_stage_id in &stage.input_stage_ids {
            if !stage_ids.contains(input_stage_id) || *input_stage_id == stage.stage_id {
                return plan;
            }
            dependency_edges.insert((*input_stage_id, stage.stage_id));
        }
    }

    plan.dependencies = dependency_edges
        .into_iter()
        .map(|(from_stage_id, to_stage_id)| StageDependency {
            from_stage_id,
            to_stage_id,
        })
        .collect::<Vec<_>>();

    plan
}

///
/// Output:
/// - Mutates `params` to include stable observability telemetry keys.
#[cfg(test)]
fn attach_distributed_observability_params(
    params: &mut HashMap<String, String>,
    dag_metrics_json: &str,
    stage_extraction_mismatch: bool,
    datafusion_stage_count: usize,
    distributed_stage_count: usize,
) {
    params.insert(
        distributed_dag::OBS_DAG_METRICS_JSON_PARAM.to_string(),
        dag_metrics_json.to_string(),
    );
    params.insert(
        distributed_dag::OBS_PLAN_VALIDATION_STATUS_PARAM.to_string(),
        distributed_dag::OBS_PLAN_VALIDATION_STATUS_PASSED.to_string(),
    );
    params.insert(
        distributed_dag::OBS_STAGE_EXTRACTION_MISMATCH_PARAM.to_string(),
        stage_extraction_mismatch.to_string(),
    );
    params.insert(
        distributed_dag::OBS_DATAFUSION_STAGE_COUNT_PARAM.to_string(),
        datafusion_stage_count.to_string(),
    );
    params.insert(
        distributed_dag::OBS_DISTRIBUTED_STAGE_COUNT_PARAM.to_string(),
        distributed_stage_count.to_string(),
    );
}

/// What: Attach distributed observability telemetry to every compiled stage task group.
///
/// Inputs:
/// - `stage_groups`: Mutable stage task groups prepared for worker dispatch.
/// - `dag_metrics_json`: Serialized DAG metrics payload.
/// - `stage_extraction_mismatch`: Whether extractor and distributed stage counts diverged.
/// - `datafusion_stage_count`: Stage count produced by DataFusion extractor.
/// - `distributed_stage_count`: Stage count used by distributed plan compilation.
///
/// Output:
/// - Mutates each stage group's params to include stable observability telemetry keys.
///
/// Details:
/// - Keeps telemetry insertion deterministic across all stage groups in one dispatch.
fn attach_observability_to_stage_groups(
    stage_groups: &mut [distributed_dag::StageTaskGroup],
    dag_metrics_json: &str,
    stage_extraction_mismatch: bool,
    datafusion_stage_count: usize,
    distributed_stage_count: usize,
) {
    let obs_dag_metrics_json_key = distributed_dag::OBS_DAG_METRICS_JSON_PARAM.to_string();
    let obs_plan_validation_status_key =
        distributed_dag::OBS_PLAN_VALIDATION_STATUS_PARAM.to_string();
    let obs_stage_extraction_mismatch_key =
        distributed_dag::OBS_STAGE_EXTRACTION_MISMATCH_PARAM.to_string();
    let obs_datafusion_stage_count_key =
        distributed_dag::OBS_DATAFUSION_STAGE_COUNT_PARAM.to_string();
    let obs_distributed_stage_count_key =
        distributed_dag::OBS_DISTRIBUTED_STAGE_COUNT_PARAM.to_string();
    let obs_plan_validation_status = distributed_dag::OBS_PLAN_VALIDATION_STATUS_PASSED.to_string();
    let obs_dag_metrics_json = dag_metrics_json.to_string();
    let obs_stage_extraction_mismatch = stage_extraction_mismatch.to_string();
    let obs_datafusion_stage_count = datafusion_stage_count.to_string();
    let obs_distributed_stage_count = distributed_stage_count.to_string();

    for group in stage_groups {
        group.params.insert(
            obs_dag_metrics_json_key.clone(),
            obs_dag_metrics_json.clone(),
        );
        group.params.insert(
            obs_plan_validation_status_key.clone(),
            obs_plan_validation_status.clone(),
        );
        group.params.insert(
            obs_stage_extraction_mismatch_key.clone(),
            obs_stage_extraction_mismatch.clone(),
        );
        group.params.insert(
            obs_datafusion_stage_count_key.clone(),
            obs_datafusion_stage_count.clone(),
        );
        group.params.insert(
            obs_distributed_stage_count_key.clone(),
            obs_distributed_stage_count.clone(),
        );
    }
}

/// What: Compute, log, and serialize distributed DAG metrics for one compiled plan.
///
/// Inputs:
/// - `distributed_plan`: Distributed physical plan used to compile stage groups.
/// - `stage_groups`: Compiled stage groups used for DAG metrics derivation.
///
/// Output:
/// - Serialized DAG metrics JSON payload for observability stage params.
///
/// Details:
/// - Preserves existing compute-failure and metrics-log message contracts.
/// - Uses a stable fallback JSON payload when serialization fails.
fn compute_dag_metrics_json(
    distributed_plan: &DistributedPhysicalPlan,
    stage_groups: &[distributed_dag::StageTaskGroup],
) -> Result<String, String> {
    let dag_metrics =
        distributed_dag::compute_distributed_dag_metrics(distributed_plan, stage_groups)
            .map_err(|e| format!("failed to compute distributed dag metrics: {}", e))?;

    log::info!(
        "query_select distributed_dag_metrics: stage_count={} dependency_count={} wave_count={} max_wave_width={} total_partitions={}",
        dag_metrics.stage_count,
        dag_metrics.dependency_count,
        dag_metrics.wave_count,
        dag_metrics.max_wave_width,
        dag_metrics.total_partitions
    );

    Ok(serde_json::to_string(&dag_metrics).unwrap_or_else(|_| {
        "{\"error\":\"failed_to_serialize_distributed_dag_metrics\"}".to_string()
    }))
}

/// What: Emit routing observability logs and resolve DAG metrics payload for stage params.
///
/// Inputs:
/// - `distributed_plan`: Distributed physical plan used for metrics computation.
/// - `stage_groups`: Compiled stage groups used for metrics computation.
/// - `routing_observability`: Routing observability context for runtime/effective pool diagnostics.
/// - `sql_log_preview`: Compact SQL preview used by fallback-routing warning logs.
///
/// Output:
/// - Serialized DAG metrics payload or normalized metrics failure message.
fn resolve_routing_and_dag_metrics_json(
    distributed_plan: &DistributedPhysicalPlan,
    stage_groups: &[distributed_dag::StageTaskGroup],
    routing_observability: &RoutingObservabilityContext,
    sql_log_preview: &str,
) -> Result<String, String> {
    log_distributed_routing_observability(routing_observability, sql_log_preview);
    compute_dag_metrics_json(distributed_plan, stage_groups)
}

/// What: Compile distributed stage task groups using the resolved routing worker pool.
///
/// Inputs:
/// - `distributed_plan`: Validated distributed physical plan used for stage compilation.
/// - `routing_worker_pool`: Effective worker addresses selected for stage routing.
///
/// Output:
/// - Compiled stage task groups ready for observability enrichment and dispatch.
///
/// Details:
/// - Preserves existing stage-group compilation failure message contract.
fn compile_query_stage_groups(
    distributed_plan: &DistributedPhysicalPlan,
    routing_worker_pool: &[String],
) -> Result<Vec<distributed_dag::StageTaskGroup>, String> {
    distributed_dag::compile_stage_task_groups_with_worker_pool(
        distributed_plan,
        DISTRIBUTED_STAGE_OPERATION_QUERY,
        routing_worker_pool,
    )
    .map_err(|e| format!("failed to compile distributed stage groups: {}", e))
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

    let region_trim = region.trim();
    if !region_trim.is_empty() {
        options.insert(
            DELTA_STORAGE_AWS_REGION.to_string(),
            region_trim.to_string(),
        );
    }
    let access_key_trim = access_key.trim();
    if !access_key_trim.is_empty() {
        options.insert(
            DELTA_STORAGE_AWS_ACCESS_KEY_ID.to_string(),
            access_key_trim.to_string(),
        );
    }
    let secret_key_trim = secret_key.trim();
    if !secret_key_trim.is_empty() {
        options.insert(
            DELTA_STORAGE_AWS_SECRET_ACCESS_KEY.to_string(),
            secret_key_trim.to_string(),
        );
    }
    let endpoint_trim = endpoint.trim();
    if !endpoint_trim.is_empty() {
        options.insert(
            DELTA_STORAGE_AWS_ENDPOINT.to_string(),
            endpoint_trim.to_string(),
        );
        options.insert(
            DELTA_STORAGE_AWS_ENDPOINT_URL.to_string(),
            endpoint_trim.to_string(),
        );
        if endpoint_trim.starts_with("http://") {
            options.insert(
                DELTA_STORAGE_AWS_ALLOW_HTTP.to_string(),
                DELTA_STORAGE_TRUE.to_string(),
            );
            options.insert(
                DELTA_STORAGE_ALLOW_HTTP.to_string(),
                DELTA_STORAGE_TRUE.to_string(),
            );
        }
    }

    options
}

/// What: Build a metastore resolver with standardized initialization settings.
///
/// Inputs:
/// - `shared_data`: Shared server state used to construct metastore access context.
///
/// Output:
/// - Initialized `KionasMetastoreResolver` or a normalized initialization error string.
///
/// Details:
/// - Uses shared `METASTORE_RESOLVER_CONCURRENCY` constant.
/// - Normalizes resolver-construction failures to a stable error-message shape.
fn build_metastore_resolver(shared_data: &SharedData) -> Result<KionasMetastoreResolver, String> {
    KionasMetastoreResolver::new(shared_data.clone(), METASTORE_RESOLVER_CONCURRENCY)
        .map_err(|error| format!("failed to initialize metastore resolver: {}", error))
}

/// What: Resolve canonical relation metadata from metastore for one namespace tuple.
///
/// Inputs:
/// - `shared_data`: Shared server state used to construct metastore resolver context.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
///
/// Output:
/// - Fully populated relation metadata or a normalized resolver error string.
///
/// Details:
/// - Uses the shared resolver-construction settings and preserves metastore-origin error payloads.
async fn resolve_relation_metadata(
    shared_data: &SharedData,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<KionasRelationMetadata, String> {
    let resolver = build_metastore_resolver(shared_data)?;
    resolver.resolve_relation(database, schema, table).await
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
    let relation = resolve_relation_metadata(shared_data, database, schema, table).await?;
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
/// - Tuple with optional serialized JSON pruning hint payload and eligibility flag.
///
/// Details:
/// - FOUNDATION emits lightweight hints only; worker still owns eligibility and fallback.
fn build_scan_pruning_hints_json(physical_plan: &PhysicalPlan) -> (Option<String>, bool) {
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

    let Some(sql) = predicate_sql else {
        return (None, false);
    };

    let parsed_ast = parse_foundation_predicate_ast(&sql);
    let eligible = parsed_ast.is_some();
    let reason = if eligible {
        SCAN_HINT_REASON_ELIGIBLE
    } else {
        SCAN_HINT_REASON_UNSUPPORTED
    };

    let payload = serde_json::to_string(&json!({
        "hint_version": SCAN_HINT_VERSION,
        "source": SCAN_HINT_SOURCE,
        "predicate_sql": sql,
        "eligible": eligible,
        "reason": reason,
        "predicate_ast": parsed_ast.as_ref().map(predicate_ast_to_json),
    }))
    .ok();

    let effective_eligible = payload.is_some() && eligible;
    (payload, effective_eligible)
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
    let relation = resolve_relation_metadata(shared_data, database, schema, table).await?;
    let columns = relation.column_names();

    if columns.is_empty() {
        return Err(format!(
            "metastore get_table returned empty columns for {}.{}.{}",
            database, schema, table
        ));
    }

    Ok(columns)
}

/// What: Resolve default database and schema for the active session context.
///
/// Inputs:
/// - `shared_data`: Shared server state that owns the session manager.
/// - `session_id`: Current session id used to look up persisted namespace state.
///
/// Output:
/// - Tuple with effective `(database, schema)` defaults for query-model construction.
///
/// Details:
/// - Falls back to `DEFAULT_DATABASE_NAME`/`DEFAULT_SCHEMA_NAME` when session is missing.
/// - Treats empty `USE` database in existing session as unset and applies default database.
async fn resolve_session_namespace_defaults(
    shared_data: &SharedData,
    session_id: &str,
) -> (String, String) {
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
                    DEFAULT_DATABASE_NAME.to_string()
                } else {
                    db
                },
                DEFAULT_SCHEMA_NAME.to_string(),
            )
        }
        None => (
            DEFAULT_DATABASE_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        ),
    }
}

/// What: Resolve planner relation metadata inputs and relation-column mapping for SELECT planning.
///
/// Inputs:
/// - `resolver`: Initialized metastore resolver used for relation lookups.
/// - `planning_relation_set`: Canonical relation namespace tuples required by the query model.
///
/// Output:
/// - Tuple with planner relation metadata vector and relation-key to column-name mapping.
///
/// Details:
/// - Missing relation metadata is logged and skipped to preserve existing planner fallback behavior.
/// - Relation-column keys are emitted in canonical lowercase `database.schema.table` form.
async fn load_planner_relation_metadata(
    resolver: &KionasMetastoreResolver,
    planning_relation_set: &BTreeSet<(String, String, String)>,
) -> (Vec<KionasRelationMetadata>, BTreeMap<String, Vec<String>>) {
    let mut planner_relation_inputs = Vec::<KionasRelationMetadata>::new();
    let mut relation_columns_by_relation = BTreeMap::<String, Vec<String>>::new();

    for (relation_db, relation_schema, relation_table) in planning_relation_set {
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
                    "{} {}.{}.{}: {}",
                    RELATION_METADATA_UNAVAILABLE_FOR_PLANNER,
                    relation_db,
                    relation_schema,
                    relation_table,
                    err
                );
            }
        }
    }

    (planner_relation_inputs, relation_columns_by_relation)
}

/// What: Resolve planner relation metadata inputs from shared server state.
///
/// Inputs:
/// - `shared_data`: Shared server state used to build metastore resolver.
/// - `planning_relation_set`: Canonical relation namespace tuples required by planner.
///
/// Output:
/// - Planner relation metadata vector and relation-key to columns mapping.
///
/// Details:
/// - Preserves resolver initialization error contract and planner metadata fallback behavior.
async fn resolve_planner_relation_inputs(
    shared_data: &SharedData,
    planning_relation_set: &BTreeSet<(String, String, String)>,
) -> Result<(Vec<KionasRelationMetadata>, BTreeMap<String, Vec<String>>), String> {
    let resolver = build_metastore_resolver(shared_data)?;
    Ok(load_planner_relation_metadata(&resolver, planning_relation_set).await)
}

/// What: Collect planning-time relation namespaces required by the query semantic model.
///
/// Inputs:
/// - `database`: Canonical base database name from query model.
/// - `schema`: Canonical base schema name from query model.
/// - `table`: Canonical base table name from query model.
/// - `model`: Canonical SELECT semantic model containing join metadata.
///
/// Output:
/// - Sorted set of distinct relation namespace tuples used for planner metadata resolution.
///
/// Details:
/// - Includes the base relation and right-side relations declared in query joins.
fn collect_planning_relation_set(
    database: &str,
    schema: &str,
    table: &str,
    model: &SelectQueryModel,
) -> BTreeSet<(String, String, String)> {
    let mut planning_relation_set = BTreeSet::<(String, String, String)>::new();
    planning_relation_set.insert((database.to_string(), schema.to_string(), table.to_string()));

    for join in &model.joins {
        planning_relation_set.insert((
            join.right.database.clone(),
            join.right.schema.clone(),
            join.right.table.clone(),
        ));
    }

    planning_relation_set
}

/// What: Collect runtime relation namespaces required for relation-column hydration.
///
/// Inputs:
/// - `database`: Canonical base database name from query model.
/// - `schema`: Canonical base schema name from query model.
/// - `table`: Canonical base table name from query model.
/// - `physical_plan`: Canonical physical plan used for dispatch preparation.
///
/// Output:
/// - Sorted set of distinct relation namespace tuples used for runtime metadata resolution.
///
/// Details:
/// - Includes the base relation and right-side hash-join relations referenced by the plan.
fn collect_runtime_relation_set(
    database: &str,
    schema: &str,
    table: &str,
    physical_plan: &PhysicalPlan,
) -> BTreeSet<(String, String, String)> {
    let mut relation_set = BTreeSet::<(String, String, String)>::new();
    relation_set.insert((database.to_string(), schema.to_string(), table.to_string()));

    for operator in &physical_plan.operators {
        if let PhysicalOperator::HashJoin { spec } = operator {
            relation_set.insert((
                spec.right_relation.database.clone(),
                spec.right_relation.schema.clone(),
                spec.right_relation.table.clone(),
            ));
        }
    }

    relation_set
}

/// What: Hydrate runtime relation-column mapping for dispatch stage parameters.
///
/// Inputs:
/// - `shared_data`: Shared server state used for on-demand metastore lookups.
/// - `relation_set`: Runtime relation namespace tuples required for dispatch metadata.
/// - `relation_columns_by_relation`: Planner-time relation-column mapping keyed by relation key.
///
/// Output:
/// - Relation-key to column-name mapping for runtime dispatch payload enrichment.
///
/// Details:
/// - Reuses planner-loaded columns when available; falls back to runtime metastore lookup otherwise.
/// - Missing runtime metadata is logged and skipped to preserve existing fallback behavior.
async fn load_runtime_relation_columns(
    shared_data: &SharedData,
    relation_set: BTreeSet<(String, String, String)>,
    relation_columns_by_relation: &BTreeMap<String, Vec<String>>,
) -> BTreeMap<String, Vec<String>> {
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
                    "{} {}.{}.{}: {}",
                    RELATION_METADATA_UNAVAILABLE_FOR_RUNTIME,
                    relation_db,
                    relation_schema,
                    relation_table,
                    err
                );
            }
        }
    }

    relation_columns
}

/// What: Serialize runtime relation-column mapping into optional JSON payload.
///
/// Inputs:
/// - `relation_columns`: Relation-key to column-name mapping prepared for dispatch metadata.
///
/// Output:
/// - `Some(json)` when non-empty mapping serializes successfully, otherwise `None`.
///
/// Details:
/// - Preserves existing fallback semantics: empty mapping and serialization failure both emit no payload.
fn relation_columns_json_payload(
    relation_columns: &BTreeMap<String, Vec<String>>,
) -> Option<String> {
    if relation_columns.is_empty() {
        None
    } else {
        serde_json::to_string(relation_columns).ok()
    }
}

/// What: Resolve runtime relation-columns JSON payload for stage metadata attachment.
///
/// Inputs:
/// - `shared_data`: Shared server state used for runtime metastore lookups.
/// - `database`: Canonical base database name.
/// - `schema`: Canonical base schema name.
/// - `table`: Canonical base table name.
/// - `physical_plan`: Canonical physical plan used to derive runtime relation set.
/// - `relation_columns_by_relation`: Planner-time relation-column mapping keyed by relation key.
///
/// Output:
/// - Optional serialized relation-columns payload for stage metadata params.
///
/// Details:
/// - Preserves planner-first reuse and runtime fallback semantics from existing helpers.
async fn resolve_relation_columns_json_payload(
    shared_data: &SharedData,
    database: &str,
    schema: &str,
    table: &str,
    physical_plan: &PhysicalPlan,
    relation_columns_by_relation: &BTreeMap<String, Vec<String>>,
) -> Option<String> {
    let relation_set = collect_runtime_relation_set(database, schema, table, physical_plan);
    let relation_columns =
        load_runtime_relation_columns(shared_data, relation_set, relation_columns_by_relation)
            .await;
    relation_columns_json_payload(&relation_columns)
}

/// What: Derive canonical operator pipeline names from a physical plan.
///
/// Inputs:
/// - `physical_plan`: Canonical physical plan produced by planner translation.
///
/// Output:
/// - Ordered vector of canonical operator names for diagnostics payload emission.
fn physical_pipeline_names(physical_plan: &PhysicalPlan) -> Vec<String> {
    physical_plan
        .operators
        .iter()
        .map(|op| op.canonical_name().to_string())
        .collect::<Vec<_>>()
}

/// What: Build canonical SELECT payload JSON used for server-to-worker dispatch.
///
/// Inputs:
/// - `model`: Canonical SELECT semantic model.
/// - `physical_plan`: Canonical physical plan for downstream distributed planning.
/// - `datafusion_artifacts`: Planner diagnostics and stage extraction artifacts.
/// - `physical_plan_text`: Legacy physical-plan explain text.
/// - `physical_pipeline`: Ordered canonical operator pipeline names.
///
/// Output:
/// - Serialized canonical query payload JSON string.
///
/// Details:
/// - Preserves existing payload schema and diagnostics fields consumed by downstream decode paths.
fn build_canonical_query_payload_json(
    model: &SelectQueryModel,
    physical_plan: &PhysicalPlan,
    datafusion_artifacts: &DataFusionPlanArtifacts,
    physical_plan_text: &str,
    physical_pipeline: &[String],
) -> String {
    json!({
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
            "engine": SELECT_PLAN_ENGINE_DATAFUSION,
            "text": datafusion_artifacts.logical_plan_text,
        },
        "physical_plan": physical_plan,
        "diagnostics": {
            "logical_plan_text": datafusion_artifacts.logical_plan_text,
            "logical_plan_optimized_text": datafusion_artifacts.optimized_logical_plan_text,
            "physical_plan_text": datafusion_artifacts.physical_plan_text,
            "physical_plan_legacy_text": physical_plan_text,
            "physical_pipeline": physical_pipeline,
            "stage_extraction": {
                "stage_count": datafusion_artifacts.stage_extraction.stage_count,
                "stages": datafusion_artifacts
                    .stage_extraction
                    .stages
                    .iter()
                    .map(|stage| json!({
                        "stage_id": stage.stage_id,
                        "input_stage_ids": stage.input_stage_ids,
                        "partitions_out": stage.partitions_out,
                        "output_partitioning": stage.output_partitioning,
                        "output_partitioning_keys": stage.output_partitioning_keys,
                        "node_names": stage.node_names,
                    }))
                    .collect::<Vec<_>>(),
            },
        },
    })
    .to_string()
}

/// What: Decode the physical plan from canonical query payload JSON.
///
/// Inputs:
/// - `payload`: Canonical query payload JSON string emitted by SELECT handler.
///
/// Output:
/// - Decoded `PhysicalPlan` or normalized decode failure message.
///
/// Details:
/// - Preserves stable payload parse/decode error strings used by statement outcomes.
fn decode_physical_plan_from_payload(payload: &str) -> Result<PhysicalPlan, String> {
    let payload_value: Value = serde_json::from_str(payload)
        .map_err(|e| format!("failed to parse canonical query payload: {}", e))?;

    let physical_value = payload_value
        .get("physical_plan")
        .cloned()
        .ok_or_else(|| "canonical query payload missing physical_plan".to_string())?;

    serde_json::from_value(physical_value)
        .map_err(|e| format!("failed to decode physical_plan from payload: {}", e))
}

/// What: Build and validate distributed plan from physical plan and stage-extraction diagnostics.
///
/// Inputs:
/// - `physical_plan`: Canonical physical plan decoded from query payload.
/// - `stage_extraction`: DataFusion stage-extraction diagnostics emitted during planning.
/// - `sql`: Canonical SQL text used for compact mismatch observability logs.
///
/// Output:
/// - Validated distributed physical plan or normalized validation error string.
///
/// Details:
/// - Emits stage extraction mismatch warning using existing compact SQL preview format.
/// - Preserves distributed-plan validation error string contract.
fn build_and_validate_distributed_plan(
    physical_plan: &PhysicalPlan,
    stage_extraction: &crate::planner::DataFusionStageExtractionDiagnostics,
    sql: &str,
) -> Result<DistributedPhysicalPlan, String> {
    let distributed_plan = apply_stage_extraction_topology(
        distributed_from_physical_plan(physical_plan),
        stage_extraction,
    );

    let extracted_stage_count = stage_extraction.stage_count;
    let distributed_stage_count = distributed_plan.stages.len();
    if extracted_stage_count != distributed_stage_count {
        log::warn!(
            "query_select stage extraction mismatch: datafusion_stages={} distributed_plan_stages={} sql={} (using distributed plan shape)",
            extracted_stage_count,
            distributed_stage_count,
            sql_preview_for_logs(sql)
        );
    }

    validate_distributed_physical_plan(&distributed_plan)
        .map_err(|e| format!("distributed plan validation failed: {}", e))?;

    Ok(distributed_plan)
}

/// What: Resolve distributed plan directly from canonical payload and stage extraction diagnostics.
///
/// Inputs:
/// - `payload`: Canonical query payload JSON string containing encoded physical plan.
/// - `stage_extraction`: DataFusion stage-extraction diagnostics emitted during planning.
/// - `sql`: Canonical SQL text used for distributed-plan mismatch observability.
///
/// Output:
/// - Tuple containing decoded physical plan and validated distributed physical plan.
///
/// Details:
/// - Preserves existing payload decode errors and distributed-plan validation error contracts.
fn resolve_distributed_plan_from_payload(
    payload: &str,
    stage_extraction: &crate::planner::DataFusionStageExtractionDiagnostics,
    sql: &str,
) -> Result<(PhysicalPlan, DistributedPhysicalPlan), String> {
    let physical_plan = decode_physical_plan_from_payload(payload)?;
    let distributed_plan =
        build_and_validate_distributed_plan(&physical_plan, stage_extraction, sql)?;
    Ok((physical_plan, distributed_plan))
}

/// What: Query metadata values attached to each distributed stage-group dispatch.
///
/// Inputs:
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
/// - `query_run_id`: Stable run id for this dispatch.
/// - `scan_mode`: Effective scan mode string for dispatch metadata.
/// - `scan_delta_version_pin`: Snapshot pin value or sentinel fallback.
/// - `scan_pruning_hints_json`: Optional pruning-hints payload when available.
/// - `relation_columns_json`: Optional relation-column mapping payload when available.
///
/// Output:
/// - Immutable metadata bundle consumed by stage-param attachment.
///
/// Details:
/// - Keeps helper signatures compact while preserving emitted dispatch parameter contracts.
struct QueryStageMetadataParams<'a> {
    database: &'a str,
    schema: &'a str,
    table: &'a str,
    query_run_id: &'a str,
    scan_mode: &'a str,
    scan_delta_version_pin: u64,
    scan_pruning_hints_json: Option<&'a str>,
    relation_columns_json: Option<&'a str>,
}

/// What: Resolved scan metadata values used for stage-param attachment.
///
/// Inputs:
/// - `scan_pruning_hints_json`: Optional serialized pruning-hints payload.
/// - `scan_mode`: Effective scan mode (`metadata_pruned` or `full`).
/// - `scan_delta_version_pin`: Delta snapshot version pin or sentinel fallback.
///
/// Output:
/// - Immutable bundle consumed by query stage metadata attachment.
///
/// Details:
/// - Represents finalized scan metadata after fallback and warning checks.
struct QueryScanMetadata {
    scan_pruning_hints_json: Option<String>,
    scan_mode: &'static str,
    scan_delta_version_pin: u64,
}

/// What: Stage-extraction observability counts used for routing and telemetry attachment.
///
/// Inputs:
/// - `distributed_stage_count`: Number of distributed stages in compiled plan.
/// - `extracted_stage_count`: Number of stages reported by DataFusion extraction diagnostics.
///
/// Output:
/// - Immutable counts bundle with mismatch flag derived from both stage counts.
///
/// Details:
/// - Keeps stage-count and mismatch derivation in one place for consistent telemetry wiring.
struct StageExtractionObservabilityCounts {
    distributed_stage_count: usize,
    extracted_stage_count: usize,
    stage_extraction_mismatch: bool,
}

/// What: Resolve stage-extraction observability counts for one compiled distributed plan.
///
/// Inputs:
/// - `distributed_plan`: Distributed plan used to derive runtime stage count.
/// - `stage_extraction`: DataFusion stage-extraction diagnostics from planning.
///
/// Output:
/// - Stage-extraction observability counts and mismatch flag.
fn resolve_stage_extraction_observability_counts(
    distributed_plan: &DistributedPhysicalPlan,
    stage_extraction: &crate::planner::DataFusionStageExtractionDiagnostics,
) -> StageExtractionObservabilityCounts {
    let distributed_stage_count = distributed_plan.stages.len();
    let extracted_stage_count = stage_extraction.stage_count;
    StageExtractionObservabilityCounts {
        distributed_stage_count,
        extracted_stage_count,
        stage_extraction_mismatch: extracted_stage_count != distributed_stage_count,
    }
}

/// What: Resolve scan metadata for query dispatch stage parameters.
///
/// Inputs:
/// - `shared_data`: Shared server state used for delta snapshot pin lookup.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
/// - `physical_plan`: Canonical physical plan used for pruning-hints generation.
///
/// Output:
/// - Fully resolved scan metadata bundle for dispatch params.
///
/// Details:
/// - Preserves existing warning messages for delta-pin lookup failure and metadata-pruned sentinel fallback.
async fn resolve_query_scan_metadata(
    shared_data: &SharedData,
    database: &str,
    schema: &str,
    table: &str,
    physical_plan: &PhysicalPlan,
) -> QueryScanMetadata {
    let (scan_pruning_hints_json, scan_hints_eligible) =
        build_scan_pruning_hints_json(physical_plan);
    let scan_mode = if scan_hints_eligible {
        SCAN_MODE_METADATA_PRUNED
    } else {
        SCAN_MODE_FULL
    };
    let scan_delta_version_pin = match resolve_scan_delta_version_pin(
        shared_data,
        database,
        schema,
        table,
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            log::warn!(
                "query_select failed to resolve delta snapshot version pin for {}.{}.{}: {} ({}={})",
                database,
                schema,
                table,
                err,
                SCAN_DELTA_VERSION_PIN_SENTINEL_WARNING,
                SCAN_DELTA_VERSION_PIN_SENTINEL
            );
            SCAN_DELTA_VERSION_PIN_SENTINEL
        }
    };

    if scan_mode == SCAN_MODE_METADATA_PRUNED
        && scan_delta_version_pin == SCAN_DELTA_VERSION_PIN_SENTINEL
    {
        log::warn!(
            "query_select metadata_pruned mode enabled without concrete delta version pin; {}={}",
            SCAN_DELTA_VERSION_PIN_SENTINEL_WARNING,
            SCAN_DELTA_VERSION_PIN_SENTINEL
        );
    }

    QueryScanMetadata {
        scan_pruning_hints_json,
        scan_mode,
        scan_delta_version_pin,
    }
}

/// What: Attach query metadata parameters to each compiled stage group.
///
/// Inputs:
/// - `stage_groups`: Compiled stage groups that will be dispatched to workers.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
/// - `query_run_id`: Stable run id for this dispatch.
/// - `scan_mode`: Effective scan mode string for dispatch metadata.
/// - `scan_delta_version_pin`: Snapshot pin value or sentinel fallback.
/// - `scan_pruning_hints_json`: Optional pruning-hints payload when available.
/// - `relation_columns_json`: Optional relation-column mapping payload when available.
///
/// Output:
/// - Mutates each stage group's params map in place.
///
/// Details:
/// - Preserves FOUNDATION contract: scan metadata keys are always present.
fn attach_query_metadata_to_stage_groups(
    stage_groups: &mut [distributed_dag::StageTaskGroup],
    params: &QueryStageMetadataParams<'_>,
) {
    let database_name_key = STAGE_PARAM_DATABASE_NAME.to_string();
    let schema_name_key = STAGE_PARAM_SCHEMA_NAME.to_string();
    let table_name_key = STAGE_PARAM_TABLE_NAME.to_string();
    let query_kind_key = STAGE_PARAM_QUERY_KIND.to_string();
    let query_run_id_key = STAGE_PARAM_QUERY_RUN_ID.to_string();
    let scan_mode_key = STAGE_PARAM_SCAN_MODE.to_string();
    let scan_delta_version_pin_key = STAGE_PARAM_SCAN_DELTA_VERSION_PIN.to_string();
    let scan_pruning_hints_json_key = STAGE_PARAM_SCAN_PRUNING_HINTS_JSON.to_string();
    let relation_columns_json_key = STAGE_PARAM_RELATION_COLUMNS_JSON.to_string();

    let query_kind_value = STAGE_PARAM_QUERY_KIND_SELECT.to_string();
    let scan_mode_value = params.scan_mode.to_string();
    let scan_delta_version_pin_value = params.scan_delta_version_pin.to_string();

    for group in stage_groups {
        group
            .params
            .insert(database_name_key.clone(), params.database.to_string());
        group
            .params
            .insert(schema_name_key.clone(), params.schema.to_string());
        group
            .params
            .insert(table_name_key.clone(), params.table.to_string());
        group
            .params
            .insert(query_kind_key.clone(), query_kind_value.clone());
        group
            .params
            .insert(query_run_id_key.clone(), params.query_run_id.to_string());
        // Phase FOUNDATION contract: scan metadata is always explicit,
        // even when runtime falls back to full scan behavior.
        group
            .params
            .insert(scan_mode_key.clone(), scan_mode_value.clone());
        group.params.insert(
            scan_delta_version_pin_key.clone(),
            scan_delta_version_pin_value.clone(),
        );
        if let Some(scan_hints_json) = params.scan_pruning_hints_json {
            group.params.insert(
                scan_pruning_hints_json_key.clone(),
                scan_hints_json.to_string(),
            );
        }
        if let Some(mapping_json) = params.relation_columns_json {
            group
                .params
                .insert(relation_columns_json_key.clone(), mapping_json.to_string());
        }
    }
}

/// What: Build dispatch authorization context for SELECT stage-group execution.
///
/// Inputs:
/// - `ctx`: Request context carrying RBAC principal and query id.
/// - `database`: Canonical database name used to build auth scope.
/// - `schema`: Canonical schema name used to build auth scope.
/// - `table`: Canonical table name used to build auth scope.
///
/// Output:
/// - Fully populated dispatch auth context passed to stage-group runner.
///
/// Details:
/// - Preserves existing `select:<db>.<schema>.<table>` scope contract.
fn build_select_dispatch_auth_context(
    ctx: &RequestContext,
    database: &str,
    schema: &str,
    table: &str,
) -> helpers::DispatchAuthContext {
    helpers::DispatchAuthContext {
        rbac_user: ctx.rbac_user.clone(),
        rbac_role: ctx.role.clone(),
        scope: format!("select:{}.{}.{}", database, schema, table),
        query_id: ctx.query_id.clone(),
    }
}

/// What: Execute compiled query stage groups through the shared dispatch runner.
///
/// Inputs:
/// - `shared_data`: Shared server state required by dispatch runner.
/// - `session_id`: Current session identifier for dispatch context.
/// - `stage_groups`: Prepared stage groups to dispatch.
/// - `dispatch_auth_ctx`: Authorization context propagated to worker dispatch.
///
/// Output:
/// - Worker result location on success, or normalized dispatch failure message.
///
/// Details:
/// - Preserves existing `worker query dispatch failed: <error>` message contract.
async fn run_query_stage_groups(
    shared_data: &SharedData,
    session_id: &str,
    stage_groups: &[distributed_dag::StageTaskGroup],
    dispatch_auth_ctx: &helpers::DispatchAuthContext,
) -> Result<String, String> {
    helpers::run_stage_groups_for_input(
        shared_data,
        session_id,
        stage_groups,
        Some(dispatch_auth_ctx),
        DISPATCH_TIMEOUT_SECS,
    )
    .await
    .map_err(|e| format!("worker query dispatch failed: {}", e))
}

/// What: Emit SELECT dispatch success observability and outcome payload.
///
/// Inputs:
/// - `session_id`: Current session id used in success observability log.
/// - `database`: Canonical database name.
/// - `schema`: Canonical schema name.
/// - `table`: Canonical table name.
/// - `worker_result_location`: Worker-produced result location URI.
///
/// Output:
/// - Encoded success statement outcome with stable category/code contract.
///
/// Details:
/// - Preserves existing success log template and user-facing success message shape.
fn emit_select_dispatch_success_outcome(
    session_id: &str,
    database: &str,
    schema: &str,
    table: &str,
    worker_result_location: &str,
) -> String {
    log::info!(
        "query_select dispatched: session_id={} database={} schema={} table={} worker_result_location={}",
        session_id,
        database,
        schema,
        table,
        worker_result_location
    );

    format_outcome(
        OUTCOME_CATEGORY_SUCCESS,
        OUTCOME_CODE_QUERY_DISPATCHED,
        format!(
            "query dispatched successfully for {}.{}.{} (location: {})",
            database, schema, table, worker_result_location
        ),
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
    let (default_database, default_schema) =
        resolve_session_namespace_defaults(shared_data, session_id).await;

    let canonical_query =
        match build_select_query_model(ast.query, session_id, &default_database, &default_schema) {
            Ok(value) => value,
            Err(e) => {
                return format_validation_outcome(
                    validation_code_for_query_error(&e),
                    e.to_string(),
                );
            }
        };

    let database = canonical_query.database;
    let schema = canonical_query.schema;
    let table = canonical_query.table;
    let model = canonical_query.model;

    let planning_relation_set =
        collect_planning_relation_set(database.as_str(), schema.as_str(), table.as_str(), &model);

    let (planner_relation_inputs, relation_columns_by_relation) =
        match resolve_planner_relation_inputs(shared_data, &planning_relation_set).await {
            Ok(value) => value,
            Err(error) => {
                return format_worker_query_failed_outcome(error);
            }
        };

    let planner = DataFusionQueryPlanner::new();

    let physical_plan = match planner
        .translate_to_kionas_plan(&model, &planner_relation_inputs)
        .await
    {
        Ok(plan) => plan,
        Err(e) => {
            return format_validation_outcome(validation_code_for_planner_error(&e), e.to_string());
        }
    };
    let physical_plan_text = kionas::planner::explain::explain_physical_plan(&physical_plan);

    let datafusion_artifacts = match planner
        .build_plan_artifacts(&model.sql, &planner_relation_inputs)
        .await
    {
        Ok(artifacts) => artifacts,
        Err(err) => {
            let message = err.to_string();
            if is_relation_resolution_error(&message) {
                DataFusionPlanArtifacts {
                    logical_plan_text: format_prefixed_diagnostic_message(
                        DATAFUSION_LOGICAL_DIAGNOSTICS_UNAVAILABLE,
                        message.as_str(),
                    ),
                    optimized_logical_plan_text: format_prefixed_diagnostic_message(
                        DATAFUSION_OPTIMIZED_LOGICAL_DIAGNOSTICS_UNAVAILABLE,
                        message.as_str(),
                    ),
                    physical_plan_text: physical_plan_text.clone(),
                    stage_extraction: crate::planner::DataFusionStageExtractionDiagnostics {
                        stage_count: 0,
                        stages: Vec::<crate::planner::DataFusionExtractedStage>::new(),
                    },
                }
            } else {
                return format_validation_outcome(
                    VALIDATION_CODE_UNSUPPORTED_QUERY_SHAPE,
                    format_prefixed_diagnostic_message(
                        DATAFUSION_BUILD_LOGICAL_PLAN_FAILED,
                        message.as_str(),
                    ),
                );
            }
        }
    };

    let physical_pipeline = physical_pipeline_names(&physical_plan);
    let payload = build_canonical_query_payload_json(
        &model,
        &physical_plan,
        &datafusion_artifacts,
        physical_plan_text.as_str(),
        &physical_pipeline,
    );

    let (physical_plan, distributed_plan) = match resolve_distributed_plan_from_payload(
        &payload,
        &datafusion_artifacts.stage_extraction,
        model.sql.as_str(),
    ) {
        Ok(value) => value,
        Err(error) => {
            return format_worker_query_failed_outcome(error);
        }
    };
    let sql_log_preview = sql_preview_for_logs(model.sql.as_str());
    let stage_extraction_counts = resolve_stage_extraction_observability_counts(
        &distributed_plan,
        &datafusion_artifacts.stage_extraction,
    );

    let (effective_routing_worker_pool, routing_observability) =
        resolve_query_routing_context(shared_data, stage_extraction_counts.distributed_stage_count)
            .await;
    let mut stage_groups =
        match compile_query_stage_groups(&distributed_plan, &effective_routing_worker_pool) {
            Ok(groups) => groups,
            Err(e) => {
                return format_worker_query_failed_outcome(e);
            }
        };
    let dag_metrics_json = match resolve_routing_and_dag_metrics_json(
        &distributed_plan,
        &stage_groups,
        &routing_observability,
        sql_log_preview.as_str(),
    ) {
        Ok(value) => value,
        Err(error) => {
            return format_worker_query_failed_outcome(error);
        }
    };

    let relation_columns_json = resolve_relation_columns_json_payload(
        shared_data,
        database.as_str(),
        schema.as_str(),
        table.as_str(),
        &physical_plan,
        &relation_columns_by_relation,
    )
    .await;

    let query_run_id = Uuid::new_v4().to_string();
    let scan_metadata = resolve_query_scan_metadata(
        shared_data,
        database.as_str(),
        schema.as_str(),
        table.as_str(),
        &physical_plan,
    )
    .await;

    let stage_metadata_params = QueryStageMetadataParams {
        database: database.as_str(),
        schema: schema.as_str(),
        table: table.as_str(),
        query_run_id: query_run_id.as_str(),
        scan_mode: scan_metadata.scan_mode,
        scan_delta_version_pin: scan_metadata.scan_delta_version_pin,
        scan_pruning_hints_json: scan_metadata.scan_pruning_hints_json.as_deref(),
        relation_columns_json: relation_columns_json.as_deref(),
    };
    attach_query_metadata_to_stage_groups(&mut stage_groups, &stage_metadata_params);
    attach_observability_to_stage_groups(
        &mut stage_groups,
        dag_metrics_json.as_str(),
        stage_extraction_counts.stage_extraction_mismatch,
        stage_extraction_counts.extracted_stage_count,
        stage_extraction_counts.distributed_stage_count,
    );
    attach_distributed_routing_observability_to_stage_groups(
        &mut stage_groups,
        &routing_observability,
    );

    let dispatch_auth_ctx =
        build_select_dispatch_auth_context(ctx, database.as_str(), schema.as_str(), table.as_str());

    let worker_result_location =
        match run_query_stage_groups(shared_data, session_id, &stage_groups, &dispatch_auth_ctx)
            .await
        {
            Ok(location) => location,
            Err(e) => {
                return format_worker_query_failed_outcome(e);
            }
        };

    emit_select_dispatch_success_outcome(
        session_id,
        database.as_str(),
        schema.as_str(),
        table.as_str(),
        worker_result_location.as_str(),
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
