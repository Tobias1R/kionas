use crate::planner::stage_extractor::{StagePartitioningKind, extract_stages};
use crate::providers::{KionasRelationMetadata, build_session_context_with_kionas_providers};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use kionas::planner::PlannerError;
use kionas::planner::{
    AggregateFunction, JoinKeyPair, JoinType, LogicalRelation, PhysicalAggregateExpr,
    PhysicalAggregateSpec, PhysicalExpr, PhysicalLimitSpec, PhysicalOperator, PhysicalPlan,
    PhysicalSortExpr, PhysicalUnionOperand, parse_predicate_sql, validate_physical_plan,
};
use kionas::sql::query_model::{QueryFromSpec, SelectQueryModel, primary_relation_dependency};
use std::sync::Arc;

/// What: Thin server-side wrapper for DataFusion planning operations.
///
/// Inputs:
/// - Optional relation providers passed per planning request.
///
/// Output:
/// - Consistent entrypoints for logical/physical diagnostics and Kionas translation.
///
/// Details:
/// - This wrapper consolidates planner calls used by query handlers so integration
///   points stay explicit during Phase 2 migration.
#[derive(Debug, Default, Clone, Copy)]
pub struct DataFusionQueryPlanner;

impl DataFusionQueryPlanner {
    /// What: Construct a new DataFusion query planner wrapper.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Stateless planner wrapper instance.
    pub fn new() -> Self {
        Self
    }

    /// What: Build DataFusion planning diagnostics with relation providers.
    ///
    /// Inputs:
    /// - `sql`: Canonical SQL statement.
    /// - `relations`: Relation providers registered in DataFusion session context.
    ///
    /// Output:
    /// - Logical/optimized/physical diagnostics plus stage extraction metadata.
    pub async fn build_plan_artifacts(
        &self,
        sql: &str,
        relations: &[KionasRelationMetadata],
    ) -> Result<DataFusionPlanArtifacts, PlannerError> {
        build_datafusion_plan_artifacts_with_providers(sql, relations).await
    }

    /// What: Translate a canonical query model to Kionas physical operators.
    ///
    /// Inputs:
    /// - `model`: Canonical select query model.
    /// - `relations`: Relation providers for DataFusion planning.
    ///
    /// Output:
    /// - Kionas physical plan compatible with worker execution contract.
    pub async fn translate_to_kionas_plan(
        &self,
        model: &SelectQueryModel,
        relations: &[KionasRelationMetadata],
    ) -> Result<PhysicalPlan, PlannerError> {
        translate_datafusion_to_kionas_physical_plan_with_providers(model, relations).await
    }
}

#[derive(Debug, Clone, Copy)]
struct PlannerIntentFlags {
    has_scan: bool,
    has_filter: bool,
    has_projection: bool,
    has_sort: bool,
    has_limit: bool,
    has_hash_join: bool,
    has_sort_merge_join: bool,
    has_nested_loop_join: bool,
    has_aggregate: bool,
    has_union: bool,
    union_child_count: usize,
}

/// What: DataFusion planning artifacts generated from one SQL statement.
///
/// Inputs:
/// - Created by planning a SQL statement through DataFusion logical, optimizer, and physical phases.
///
/// Output:
/// - Text renderings for logical plan, optimized logical plan, and physical plan.
///
/// Details:
/// - Text artifacts are used for deterministic diagnostics while transport/runtime contracts are being migrated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFusionPlanArtifacts {
    pub logical_plan_text: String,
    pub optimized_logical_plan_text: String,
    pub physical_plan_text: String,
    pub stage_extraction: DataFusionStageExtractionDiagnostics,
}

/// What: Stage extraction diagnostics derived from the DataFusion physical plan.
///
/// Inputs:
/// - Output from server-side `extract_stages` traversal.
///
/// Output:
/// - Deterministic stage diagnostics attached to planning artifacts.
///
/// Details:
/// - This shape is consumed by query dispatch diagnostics and scheduling telemetry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFusionStageExtractionDiagnostics {
    pub stage_count: usize,
    pub stages: Vec<DataFusionExtractedStage>,
}

/// What: One extracted stage diagnostic row.
///
/// Inputs:
/// - Stage metadata produced by extraction traversal.
///
/// Output:
/// - Compact diagnostics record for logs and payload diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFusionExtractedStage {
    pub stage_id: u32,
    pub input_stage_ids: Vec<u32>,
    pub partitions_out: usize,
    pub output_partitioning: String,
    pub output_partitioning_keys: Vec<String>,
    pub node_names: Vec<String>,
}

/// What: Build a DataFusion logical plan and return a stable text rendering.
///
/// Inputs:
/// - `sql`: SQL statement to be parsed and planned by DataFusion.
///
/// Output:
/// - Human-readable logical plan text produced by DataFusion display APIs.
///
/// Details:
/// - This function is the server-side Objective 1 planning entrypoint.
/// - It intentionally uses DataFusion planning directly and does not involve custom logical models.
/// - Table/catalog resolution must be provided by the caller's configured DataFusion context in future objectives.
#[allow(dead_code)]
pub async fn build_datafusion_logical_plan_text(sql: &str) -> Result<String, PlannerError> {
    let context = SessionContext::new();
    let session_state = context.state();
    let logical_plan = session_state
        .create_logical_plan(sql)
        .await
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;

    Ok(logical_plan.display_indent_schema().to_string())
}

/// What: Build DataFusion logical, optimized logical, and physical plan diagnostics from SQL.
///
/// Inputs:
/// - `sql`: SQL statement to be parsed, optimized, and physically planned by DataFusion.
///
/// Output:
/// - Planning artifact bundle with text diagnostics for each stage.
///
/// Details:
/// - This function is the Objective 2 server planning entrypoint for physical planning diagnostics.
#[allow(dead_code)]
pub async fn build_datafusion_plan_artifacts(
    sql: &str,
) -> Result<DataFusionPlanArtifacts, PlannerError> {
    build_datafusion_plan_artifacts_with_providers(sql, &[]).await
}

/// What: Build DataFusion planning artifacts using pre-registered relation providers.
///
/// Inputs:
/// - `sql`: SQL statement to plan.
/// - `relations`: Relation provider metadata used to register context tables.
///
/// Output:
/// - Planning artifact bundle with logical, optimized logical, and physical text diagnostics.
///
/// Details:
/// - Providers are built server-side from metastore metadata and avoid missing-table failures.
pub async fn build_datafusion_plan_artifacts_with_providers(
    sql: &str,
    relations: &[KionasRelationMetadata],
) -> Result<DataFusionPlanArtifacts, PlannerError> {
    let context = build_session_context_with_kionas_providers(relations)
        .map_err(PlannerError::InvalidLogicalPlan)?;
    let session_state = context.state();
    let logical_plan = session_state
        .create_logical_plan(sql)
        .await
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;
    let optimized_logical_plan = session_state
        .optimize(&logical_plan)
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;
    let physical_plan = session_state
        .create_physical_plan(&optimized_logical_plan)
        .await
        .map_err(|err| PlannerError::InvalidPhysicalPipeline(err.to_string()))?;
    let extracted_stages = extract_stages(Arc::clone(&physical_plan));
    let stage_extraction = DataFusionStageExtractionDiagnostics {
        stage_count: extracted_stages.len(),
        stages: extracted_stages
            .into_iter()
            .map(|stage| {
                let output_partitioning = match stage.output_partitioning.kind {
                    StagePartitioningKind::Single => "single".to_string(),
                    StagePartitioningKind::RoundRobinBatch => "round_robin_batch".to_string(),
                    StagePartitioningKind::Hash => "hash".to_string(),
                    StagePartitioningKind::Unknown => "unknown".to_string(),
                };

                DataFusionExtractedStage {
                    stage_id: stage.stage_id,
                    input_stage_ids: stage.input_stage_ids,
                    partitions_out: stage.partitions_out,
                    output_partitioning,
                    output_partitioning_keys: stage.output_partitioning.hash_keys,
                    node_names: stage.node_names,
                }
            })
            .collect::<Vec<_>>(),
    };

    Ok(DataFusionPlanArtifacts {
        logical_plan_text: logical_plan.display_indent_schema().to_string(),
        optimized_logical_plan_text: optimized_logical_plan.display_indent_schema().to_string(),
        physical_plan_text: displayable(physical_plan.as_ref()).indent(true).to_string(),
        stage_extraction,
    })
}

/// What: Translate DataFusion-driven planning intent into Kionas physical operator plan.
///
/// Inputs:
/// - `model`: Canonical SELECT query model used by server dispatch pipeline.
///
/// Output:
/// - Kionas physical plan used for worker task operator payloads.
///
/// Details:
/// - DataFusion remains the source of planning diagnostics and optimization intent.
/// - This translation boundary preserves the worker contract: Kionas operator tasks only.
/// - Current implementation reuses existing Kionas logical->physical conversion while the
///   DataFusion physical-plan-to-operator mapping is incrementally implemented.
#[allow(dead_code)]
pub async fn translate_datafusion_to_kionas_physical_plan(
    model: &SelectQueryModel,
) -> Result<PhysicalPlan, PlannerError> {
    translate_datafusion_to_kionas_physical_plan_with_providers(model, &[]).await
}

/// What: Translate DataFusion-driven planning intent into Kionas operators with providers.
///
/// Inputs:
/// - `model`: Canonical SELECT query model used by server dispatch pipeline.
/// - `relations`: Relation provider metadata used to initialize DataFusion context.
///
/// Output:
/// - Kionas physical plan used for worker task operator payloads.
///
/// Details:
/// - Fallback intent derivation remains as a graceful safeguard when planning fails.
pub async fn translate_datafusion_to_kionas_physical_plan_with_providers(
    model: &SelectQueryModel,
    relations: &[KionasRelationMetadata],
) -> Result<PhysicalPlan, PlannerError> {
    let context = build_session_context_with_kionas_providers(relations)
        .map_err(PlannerError::InvalidLogicalPlan)?;
    let session_state = context.state();
    let logical_plan = match session_state.create_logical_plan(&model.sql).await {
        Ok(plan) => plan,
        Err(err) => {
            let message = err.to_string();
            if is_fallback_eligible_datafusion_error(&message) {
                if requires_strict_datafusion_planning(model)
                    && !is_object_store_registration_error(&message)
                {
                    return Err(PlannerError::InvalidLogicalPlan(format!(
                        "subquery/CTE query requires DataFusion planning and cannot use SQL-text fallback: {}",
                        message
                    )));
                }
                let intent = derive_intent_from_sql_text(&model.sql);
                return build_kionas_plan_from_intent(model, intent);
            }

            return Err(PlannerError::InvalidLogicalPlan(message));
        }
    };
    let optimized_logical_plan = session_state
        .optimize(&logical_plan)
        .map_err(|err| PlannerError::InvalidLogicalPlan(err.to_string()))?;
    let physical_plan = match session_state
        .create_physical_plan(&optimized_logical_plan)
        .await
    {
        Ok(plan) => plan,
        Err(err) => {
            let message = err.to_string();
            if is_fallback_eligible_datafusion_error(&message) {
                if requires_strict_datafusion_planning(model)
                    && !is_object_store_registration_error(&message)
                {
                    return Err(PlannerError::InvalidPhysicalPipeline(format!(
                        "subquery/CTE query requires DataFusion physical planning and cannot use SQL-text fallback: {}",
                        message
                    )));
                }
                let intent = derive_intent_from_sql_text(&model.sql);
                return build_kionas_plan_from_intent(model, intent);
            }

            return Err(PlannerError::InvalidPhysicalPipeline(message));
        }
    };

    let intent = derive_intent_from_exec_plan(&physical_plan);
    let extracted_stages = extract_stages(Arc::clone(&physical_plan));
    log::debug!(
        "datafusion stage extraction completed with {} stages",
        extracted_stages.len()
    );
    build_kionas_plan_from_intent(model, intent)
}

fn is_fallback_eligible_datafusion_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    (lower.contains("table") && lower.contains("not found"))
        || (lower.contains("no suitable object store found")
            || lower.contains("register_object_store"))
}

fn is_object_store_registration_error(message: &str) -> bool {
    let lower = message.to_ascii_lowercase();
    lower.contains("no suitable object store found") || lower.contains("register_object_store")
}

fn requires_strict_datafusion_planning(model: &SelectQueryModel) -> bool {
    !matches!(model.from, QueryFromSpec::Table { .. }) || !model.ctes.is_empty()
}

fn translation_source_model(model: &SelectQueryModel) -> &SelectQueryModel {
    if let QueryFromSpec::CteRef { name } = &model.from
        && let Some(cte) = model
            .ctes
            .iter()
            .find(|cte| cte.name.eq_ignore_ascii_case(name))
    {
        return cte.query.as_ref();
    }

    model
}

fn collect_exec_node_names(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
    out.push(plan.name().to_string());
    for child in plan.children() {
        collect_exec_node_names(child, out);
    }
}

fn collect_union_child_counts(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<usize>) {
    if plan.name().to_ascii_lowercase().contains("unionexec") {
        out.push(plan.children().len());
    }

    for child in plan.children() {
        collect_union_child_counts(child, out);
    }
}

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

fn parse_projection_alias(expr: &str) -> (String, Option<String>) {
    let lower = expr.to_ascii_lowercase();
    if let Some(index) = lower.rfind(" as ") {
        let lhs = expr[..index].trim().to_string();
        let rhs = expr[index + 4..].trim();
        if !rhs.is_empty() {
            return (lhs, Some(normalize_identifier(rhs)));
        }
    }

    (expr.trim().to_string(), None)
}

fn is_simple_identifier_reference(expr: &str) -> bool {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.contains('(')
        || trimmed.contains(')')
        || trimmed.contains('+')
        || trimmed.contains('-')
        || trimmed.contains('*')
        || trimmed.contains('/')
        || trimmed.contains(',')
        || trimmed.contains(':')
        || trimmed.contains(' ')
    {
        return false;
    }

    let parts = trimmed.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 2 {
        return false;
    }

    parts.into_iter().all(|segment| {
        let raw = segment
            .trim()
            .trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']');

        !raw.is_empty()
            && raw
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    })
}

fn parse_aggregate_expr_from_projection(expr: &str) -> Option<(AggregateFunction, Option<String>)> {
    let (base_expr, _) = parse_projection_alias(expr);
    let trimmed = base_expr.trim();
    let lower = trimmed.to_ascii_lowercase();

    for (name, function) in [
        ("count", AggregateFunction::Count),
        ("sum", AggregateFunction::Sum),
        ("min", AggregateFunction::Min),
        ("max", AggregateFunction::Max),
        ("avg", AggregateFunction::Avg),
    ] {
        let prefix = format!("{}(", name);
        if lower.starts_with(prefix.as_str()) && lower.ends_with(')') {
            let raw_arg = trimmed[prefix.len()..trimmed.len() - 1].trim();
            if matches!(function, AggregateFunction::Count) && raw_arg == "*" {
                return Some((function, None));
            }

            if is_simple_identifier_reference(raw_arg) {
                return Some((function, Some(raw_arg.to_string())));
            }

            return None;
        }
    }

    None
}

fn projection_output_name(expr: &str) -> String {
    let lower = expr.to_ascii_lowercase();
    if let Some(index) = lower.rfind(" as ") {
        return normalize_identifier(&expr[index + 4..]);
    }

    if let Some((prefix, function)) = [
        ("count(", "count"),
        ("sum(", "sum"),
        ("min(", "min"),
        ("max(", "max"),
        ("avg(", "avg"),
    ]
    .iter()
    .find(|(prefix, _)| lower.starts_with(*prefix) && lower.ends_with(')'))
    {
        let arg = expr[prefix.len()..expr.len() - 1].trim();
        if arg == "*" {
            return (*function).to_string();
        }
        return format!("{}_{}", function, normalize_identifier(arg));
    }

    normalize_identifier(expr)
}

fn build_aggregate_spec_from_model(
    model: &SelectQueryModel,
) -> Result<Option<PhysicalAggregateSpec>, PlannerError> {
    if model.group_by.is_empty()
        && !model
            .projection
            .iter()
            .any(|expr| parse_aggregate_expr_from_projection(expr).is_some())
    {
        return Ok(None);
    }

    let grouping_exprs = model
        .group_by
        .iter()
        .map(|expr| PhysicalExpr::Raw { sql: expr.clone() })
        .collect::<Vec<_>>();

    let mut aggregates = Vec::<PhysicalAggregateExpr>::new();
    for expr in &model.projection {
        if let Some((function, input)) = parse_aggregate_expr_from_projection(expr) {
            let (_, alias) = parse_projection_alias(expr);
            let output_name = alias.unwrap_or_else(|| projection_output_name(expr));
            aggregates.push(PhysicalAggregateExpr {
                function,
                input: input.map(|column| PhysicalExpr::Raw { sql: column }),
                output_name,
            });
        }
    }

    Ok(Some(PhysicalAggregateSpec {
        grouping_exprs,
        aggregates,
    }))
}

fn model_contains_aggregate(model: &SelectQueryModel) -> bool {
    !model.group_by.is_empty()
        || model
            .projection
            .iter()
            .any(|expr| parse_aggregate_expr_from_projection(expr).is_some())
}

/// What: Detect join execution node families present in a DataFusion physical plan tree.
///
/// Inputs:
/// - `node_names`: Flattened execution node names collected from DataFusion plan tree.
///
/// Output:
/// - Tuple flags in order: `(hash_join, sort_merge_join, nested_loop_join)`.
///
/// Details:
/// - This supports explicit contract checks so unsupported join families fail fast.
fn detect_join_families(node_names: &[String]) -> (bool, bool, bool) {
    let has_hash_join = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("hashjoin"));
    let has_sort_merge_join = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("sortmergejoin"));
    let has_nested_loop_join = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("nestedloopjoin"));

    (has_hash_join, has_sort_merge_join, has_nested_loop_join)
}

/// What: Derive planner intent flags from DataFusion physical-plan node names.
///
/// Inputs:
/// - `plan`: DataFusion execution plan root.
///
/// Output:
/// - Normalized intent flags used by Kionas operator translation.
///
/// Details:
/// - This keeps translation decisions tied to DataFusion physical planning when available.
fn derive_intent_from_exec_plan(plan: &Arc<dyn ExecutionPlan>) -> PlannerIntentFlags {
    let mut node_names = Vec::new();
    collect_exec_node_names(plan, &mut node_names);

    let has_scan = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("datasource")
            || lower.contains("parquet")
            || lower.contains("csv")
            || lower.contains("memoryexec")
    });
    let has_filter = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("filterexec"));
    let has_projection = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("projectionexec"));
    let has_sort = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("sortexec") || lower.contains("sortpreservingmergeexec")
    });
    let has_limit = node_names.iter().any(|name| {
        let lower = name.to_ascii_lowercase();
        lower.contains("globallimitexec") || lower.contains("locallimitexec")
    });
    let (has_hash_join, has_sort_merge_join, has_nested_loop_join) =
        detect_join_families(&node_names);
    let has_aggregate = node_names
        .iter()
        .any(|name| name.to_ascii_lowercase().contains("aggregate"));
    let mut union_child_counts = Vec::<usize>::new();
    collect_union_child_counts(plan, &mut union_child_counts);
    let has_union = !union_child_counts.is_empty();
    let union_child_count = union_child_counts.into_iter().max().unwrap_or(0);

    PlannerIntentFlags {
        has_scan,
        has_filter,
        has_projection,
        has_sort,
        has_limit,
        has_hash_join,
        has_sort_merge_join,
        has_nested_loop_join,
        has_aggregate,
        has_union,
        union_child_count,
    }
}

/// What: Remove SQL literal and comment content before lightweight clause detection.
///
/// Inputs:
/// - `sql`: Canonical SQL text.
///
/// Output:
/// - Lowercased SQL text where literal/comment bodies are replaced with spaces.
///
/// Details:
/// - This is used only by fallback intent detection and avoids false positives from
///   keyword-like text in `'string literals'`, `-- line comments`, and `/* block comments */`.
fn sanitize_sql_for_clause_detection(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;

        if ch == '\'' {
            out.push(' ');
            i += 1;
            while i < bytes.len() {
                let current = bytes[i] as char;
                if current == '\'' {
                    if i + 1 < bytes.len() && (bytes[i + 1] as char) == '\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        if ch == '-' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '-' {
            out.push(' ');
            i += 2;
            while i < bytes.len() {
                let current = bytes[i] as char;
                i += 1;
                if current == '\n' {
                    out.push('\n');
                    break;
                }
            }
            continue;
        }

        if ch == '/' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '*' {
            out.push(' ');
            i += 2;
            while i + 1 < bytes.len() {
                if (bytes[i] as char) == '*' && (bytes[i + 1] as char) == '/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }

        out.push(ch.to_ascii_lowercase());
        i += 1;
    }

    out
}

/// What: Collapse SQL whitespace to single spaces for stable clause keyword matching.
///
/// Inputs:
/// - `sql`: Lowercased/sanitized SQL text.
///
/// Output:
/// - SQL text with all whitespace runs normalized to single ASCII spaces.
///
/// Details:
/// - This allows fallback intent detection to match clauses even when SQL is formatted
///   across lines or tabs.
fn normalize_sql_whitespace_for_clause_detection(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// What: Derive planner intent flags from SQL text when DataFusion physical planning is unavailable.
///
/// Inputs:
/// - `sql`: Canonical SQL string from query model.
///
/// Output:
/// - Best-effort intent flags based on SQL clause presence.
///
/// Details:
/// - Used only as a fallback for unresolved relation planning errors.
/// - Join family defaults to hash-join compatibility for current worker contract.
fn derive_intent_from_sql_text(sql: &str) -> PlannerIntentFlags {
    let sanitized = sanitize_sql_for_clause_detection(sql);
    let normalized = normalize_sql_whitespace_for_clause_detection(&sanitized);
    let has_join = normalized.contains(" join ");
    let has_limit = normalized.contains(" limit ")
        || normalized.contains(" fetch first ")
        || normalized.contains(" fetch next ");
    let has_aggregate = normalized.contains(" group by ")
        || normalized.contains("count(")
        || normalized.contains("sum(")
        || normalized.contains("min(")
        || normalized.contains("max(")
        || normalized.contains("avg(");
    let has_union = normalized.contains(" union ");

    PlannerIntentFlags {
        has_scan: true,
        has_filter: normalized.contains(" where "),
        has_projection: true,
        has_sort: normalized.contains(" order by "),
        has_limit,
        has_hash_join: has_join,
        has_sort_merge_join: false,
        has_nested_loop_join: false,
        has_aggregate,
        has_union,
        union_child_count: 0,
    }
}

fn build_kionas_plan_from_intent(
    model: &SelectQueryModel,
    intent: PlannerIntentFlags,
) -> Result<PhysicalPlan, PlannerError> {
    let source_model = translation_source_model(model);
    let primary_relation = primary_relation_dependency(source_model).ok_or_else(|| {
        PlannerError::InvalidPhysicalPipeline(
            "query model has no relation dependencies for table scan translation".to_string(),
        )
    })?;

    let has_scan = intent.has_scan;
    let has_filter = intent.has_filter;
    let has_projection = intent.has_projection || !model.projection.is_empty();
    let has_sort = intent.has_sort;
    let has_limit = intent.has_limit;
    let has_hash_join = intent.has_hash_join;
    let has_sort_merge_join = intent.has_sort_merge_join;
    let has_nested_loop_join = intent.has_nested_loop_join;
    let has_join = has_hash_join || has_sort_merge_join || has_nested_loop_join;
    let has_aggregate = intent.has_aggregate;
    let has_union = intent.has_union;

    if has_union {
        let union_spec = model.union.as_ref().ok_or_else(|| {
            PlannerError::InvalidPhysicalPipeline(
                "DataFusion union node detected but query model contains no union spec".to_string(),
            )
        })?;

        if union_spec.operands.len() < 2 {
            return Err(PlannerError::InvalidPhysicalPipeline(
                "union requires at least two operands".to_string(),
            ));
        }

        if intent.union_child_count > 0 && intent.union_child_count != union_spec.operands.len() {
            return Err(PlannerError::InvalidPhysicalPipeline(format!(
                "union translation mismatch: datafusion_union_child_count={} model_union_operand_count={}",
                intent.union_child_count,
                union_spec.operands.len()
            )));
        }

        let mut operators = Vec::<PhysicalOperator>::new();
        operators.push(PhysicalOperator::TableScan {
            relation: LogicalRelation {
                database: primary_relation.database.clone(),
                schema: primary_relation.schema.clone(),
                table: primary_relation.table.clone(),
            },
        });
        operators.push(PhysicalOperator::Union {
            operands: union_spec
                .operands
                .iter()
                .map(|operand| {
                    let operand_relation =
                        primary_relation_dependency(operand).ok_or_else(|| {
                            PlannerError::InvalidPhysicalPipeline(
                                "union operand has no relation dependencies".to_string(),
                            )
                        })?;

                    let filter = operand
                        .selection
                        .as_ref()
                        .map(|selection| parse_predicate_sql(selection))
                        .transpose()
                        .map_err(PlannerError::UnsupportedPredicate)?;

                    Ok(PhysicalUnionOperand {
                        relation: LogicalRelation {
                            database: operand_relation.database.clone(),
                            schema: operand_relation.schema.clone(),
                            table: operand_relation.table.clone(),
                        },
                        filter,
                    })
                })
                .collect::<Result<Vec<_>, PlannerError>>()?,
            distinct: union_spec.distinct,
        });

        if has_projection {
            operators.push(PhysicalOperator::Projection {
                expressions: model
                    .projection
                    .iter()
                    .map(|sql| PhysicalExpr::Raw { sql: sql.clone() })
                    .collect::<Vec<_>>(),
            });
        }

        if has_sort && !model.order_by.is_empty() {
            let keys = model
                .order_by
                .iter()
                .map(|spec| PhysicalSortExpr {
                    expression: PhysicalExpr::Raw {
                        sql: spec.expression.clone(),
                    },
                    ascending: spec.ascending,
                })
                .collect::<Vec<_>>();
            operators.push(PhysicalOperator::Sort { keys });
        }

        if has_limit && let Some(count) = model.limit {
            operators.push(PhysicalOperator::Limit {
                spec: PhysicalLimitSpec {
                    count,
                    offset: model.offset.unwrap_or(0),
                },
            });
        }

        operators.push(PhysicalOperator::Materialize);

        let translated = PhysicalPlan {
            operators,
            sql: model.sql.clone(),
            schema_metadata: None,
        };

        validate_physical_plan(&translated)?;
        return Ok(translated);
    }

    let model_has_join = !source_model.joins.is_empty();
    if has_join != model_has_join {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "join translation mismatch: datafusion_has_join={} model_has_join={}",
            has_join, model_has_join
        )));
    }

    let model_has_aggregate = model_contains_aggregate(model);
    if has_aggregate != model_has_aggregate {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "aggregate translation mismatch: datafusion_has_aggregate={} model_has_aggregate={}",
            has_aggregate, model_has_aggregate
        )));
    }

    let model_has_filter = source_model.selection.is_some();
    if has_filter != model_has_filter {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "filter translation mismatch: datafusion_has_filter={} model_has_filter={}",
            has_filter, model_has_filter
        )));
    }

    let model_has_sort = !model.order_by.is_empty();
    if has_sort != model_has_sort {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "sort translation mismatch: datafusion_has_sort={} model_has_sort={}",
            has_sort, model_has_sort
        )));
    }

    let model_has_limit = model.limit.is_some();
    if has_limit != model_has_limit {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "limit translation mismatch: datafusion_has_limit={} model_has_limit={}",
            has_limit, model_has_limit
        )));
    }

    if !has_scan {
        return Err(PlannerError::InvalidPhysicalPipeline(
            "DataFusion physical plan does not contain a supported scan node".to_string(),
        ));
    }

    if has_sort_merge_join {
        return Err(PlannerError::UnsupportedPhysicalOperator(
            "SortMergeJoin".to_string(),
        ));
    }

    if has_nested_loop_join {
        return Err(PlannerError::UnsupportedPhysicalOperator(
            "NestedLoopJoin".to_string(),
        ));
    }

    let mut operators = Vec::<PhysicalOperator>::new();
    operators.push(PhysicalOperator::TableScan {
        relation: LogicalRelation {
            database: primary_relation.database.clone(),
            schema: primary_relation.schema.clone(),
            table: primary_relation.table.clone(),
        },
    });

    if has_filter && let Some(selection_sql) = source_model.selection.as_ref() {
        let predicate =
            parse_predicate_sql(selection_sql).map_err(PlannerError::UnsupportedPredicate)?;
        operators.push(PhysicalOperator::Filter {
            predicate: PhysicalExpr::Predicate { predicate },
        });
    }

    if has_hash_join {
        if source_model.joins.is_empty() {
            return Err(PlannerError::InvalidPhysicalPipeline(
                "DataFusion join node detected but query model contains no join spec".to_string(),
            ));
        }

        for join in &source_model.joins {
            operators.push(PhysicalOperator::HashJoin {
                spec: kionas::planner::PhysicalJoinSpec {
                    join_type: match join.join_type {
                        kionas::sql::query_model::QueryJoinType::Inner => JoinType::Inner,
                    },
                    right_relation: LogicalRelation {
                        database: join.right.database.clone(),
                        schema: join.right.schema.clone(),
                        table: join.right.table.clone(),
                    },
                    keys: join
                        .keys
                        .iter()
                        .map(|key| JoinKeyPair {
                            left: key.left.clone(),
                            right: key.right.clone(),
                        })
                        .collect::<Vec<_>>(),
                },
            });
        }
    }

    if has_aggregate {
        let aggregate_spec = build_aggregate_spec_from_model(model)?.ok_or_else(|| {
            PlannerError::InvalidPhysicalPipeline(
                "DataFusion aggregate node detected but query model has no aggregate spec"
                    .to_string(),
            )
        })?;

        operators.push(PhysicalOperator::AggregatePartial {
            spec: aggregate_spec.clone(),
        });
        operators.push(PhysicalOperator::AggregateFinal {
            spec: aggregate_spec,
        });
    }

    if has_projection {
        let expressions = if has_aggregate {
            model
                .projection
                .iter()
                .map(|expr| PhysicalExpr::ColumnRef {
                    name: projection_output_name(expr),
                })
                .collect::<Vec<_>>()
        } else {
            model
                .projection
                .iter()
                .map(|sql| PhysicalExpr::Raw { sql: sql.clone() })
                .collect::<Vec<_>>()
        };
        operators.push(PhysicalOperator::Projection { expressions });
    }

    if has_sort && !model.order_by.is_empty() {
        let keys = model
            .order_by
            .iter()
            .map(|spec| PhysicalSortExpr {
                expression: PhysicalExpr::Raw {
                    sql: spec.expression.clone(),
                },
                ascending: spec.ascending,
            })
            .collect::<Vec<_>>();
        operators.push(PhysicalOperator::Sort { keys });
    }

    if has_limit && let Some(count) = model.limit {
        operators.push(PhysicalOperator::Limit {
            spec: PhysicalLimitSpec {
                count,
                offset: model.offset.unwrap_or(0),
            },
        });
    }

    operators.push(PhysicalOperator::Materialize);

    let translated = PhysicalPlan {
        operators,
        sql: model.sql.clone(),
        schema_metadata: None,
    };

    validate_physical_plan(&translated)?;
    Ok(translated)
}

#[cfg(test)]
#[path = "../tests/planner_engine_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "../tests/spike_union_exec.rs"]
mod spike_union_exec;
