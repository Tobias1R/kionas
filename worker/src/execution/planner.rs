use crate::services::worker_service_server::worker_service;
use kionas::planner::{
    PhysicalAggregateSpec, PhysicalExpr, PhysicalJoinSpec, PhysicalLimitSpec, PhysicalOperator,
    PhysicalPlan, PhysicalSortExpr, PhysicalWindowSpec, PredicateComparisonOp, PredicateExpr,
    PredicateValue,
};
use kionas::sql::datatypes::ColumnDatatypeSpec;
use std::collections::HashMap;

#[cfg(test)]
#[path = "../tests/execution_planner_tests.rs"]
mod tests;

/// What: Convert protobuf scalar filter value to planner predicate value.
///
/// Inputs:
/// - `value`: Transport filter value.
/// - `value_type`: Transport type tag.
///
/// Output:
/// - Planner predicate literal value.
fn decode_proto_scalar_value(
    value: &worker_service::FilterValue,
    value_type: i32,
) -> Result<PredicateValue, String> {
    let declared_type = worker_service::FilterValueType::try_from(value_type)
        .unwrap_or(worker_service::FilterValueType::Unspecified);

    let variant = value
        .variant
        .as_ref()
        .ok_or_else(|| "filter value variant is missing".to_string())?;

    let decoded = match variant {
        worker_service::filter_value::Variant::IntValue(v) => PredicateValue::Int(*v),
        worker_service::filter_value::Variant::BoolValue(v) => PredicateValue::Bool(*v),
        worker_service::filter_value::Variant::StringValue(v) => PredicateValue::Str(v.clone()),
        worker_service::filter_value::Variant::FloatValue(_)
        | worker_service::filter_value::Variant::DoubleValue(_) => {
            return Err(
                "float predicate values are not supported by runtime predicate model".to_string(),
            );
        }
    };

    let inferred_type = match decoded {
        PredicateValue::Int(_) => worker_service::FilterValueType::Int,
        PredicateValue::Bool(_) => worker_service::FilterValueType::Bool,
        PredicateValue::Str(_) => worker_service::FilterValueType::String,
        PredicateValue::IntList(_) | PredicateValue::BoolList(_) | PredicateValue::StrList(_) => {
            return Err("invalid scalar predicate value: list type was provided".to_string());
        }
    };

    if declared_type != worker_service::FilterValueType::Unspecified
        && declared_type != inferred_type
    {
        return Err(format!(
            "filter value type mismatch: declared {:?}, inferred {:?}",
            declared_type, inferred_type
        ));
    }

    Ok(decoded)
}

/// What: Convert protobuf IN list values to planner predicate list value.
///
/// Inputs:
/// - `values`: Transport list values.
/// - `value_type`: Declared list type tag.
///
/// Output:
/// - Homogeneous planner list literal value.
fn decode_proto_list_value(
    values: &[worker_service::FilterValue],
    value_type: i32,
) -> Result<PredicateValue, String> {
    let declared_type = worker_service::FilterValueType::try_from(value_type)
        .unwrap_or(worker_service::FilterValueType::Unspecified);

    match declared_type {
        worker_service::FilterValueType::Int => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match decode_proto_scalar_value(value, value_type)? {
                    PredicateValue::Int(v) => out.push(v),
                    _ => {
                        return Err("IN list value type mismatch: expected int".to_string());
                    }
                }
            }
            Ok(PredicateValue::IntList(out))
        }
        worker_service::FilterValueType::Bool => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match decode_proto_scalar_value(value, value_type)? {
                    PredicateValue::Bool(v) => out.push(v),
                    _ => {
                        return Err("IN list value type mismatch: expected bool".to_string());
                    }
                }
            }
            Ok(PredicateValue::BoolList(out))
        }
        worker_service::FilterValueType::String => {
            let mut out = Vec::with_capacity(values.len());
            for value in values {
                match decode_proto_scalar_value(value, value_type)? {
                    PredicateValue::Str(v) => out.push(v),
                    _ => {
                        return Err("IN list value type mismatch: expected string".to_string());
                    }
                }
            }
            Ok(PredicateValue::StrList(out))
        }
        worker_service::FilterValueType::Unspecified => {
            if values.is_empty() {
                return Err("IN list is empty and has unspecified value type".to_string());
            }

            let mut inferred = Vec::with_capacity(values.len());
            for value in values {
                inferred.push(decode_proto_scalar_value(
                    value,
                    worker_service::FilterValueType::Unspecified as i32,
                )?);
            }

            match &inferred[0] {
                PredicateValue::Int(_) => {
                    let mut out = Vec::with_capacity(inferred.len());
                    for value in inferred {
                        if let PredicateValue::Int(v) = value {
                            out.push(v);
                        } else {
                            return Err(
                                "IN list value type mismatch: mixed scalar types are not supported"
                                    .to_string(),
                            );
                        }
                    }
                    Ok(PredicateValue::IntList(out))
                }
                PredicateValue::Bool(_) => {
                    let mut out = Vec::with_capacity(inferred.len());
                    for value in inferred {
                        if let PredicateValue::Bool(v) = value {
                            out.push(v);
                        } else {
                            return Err(
                                "IN list value type mismatch: mixed scalar types are not supported"
                                    .to_string(),
                            );
                        }
                    }
                    Ok(PredicateValue::BoolList(out))
                }
                PredicateValue::Str(_) => {
                    let mut out = Vec::with_capacity(inferred.len());
                    for value in inferred {
                        if let PredicateValue::Str(v) = value {
                            out.push(v);
                        } else {
                            return Err(
                                "IN list value type mismatch: mixed scalar types are not supported"
                                    .to_string(),
                            );
                        }
                    }
                    Ok(PredicateValue::StrList(out))
                }
                PredicateValue::IntList(_)
                | PredicateValue::BoolList(_)
                | PredicateValue::StrList(_) => {
                    Err("IN list contains nested list value".to_string())
                }
            }
        }
        worker_service::FilterValueType::Float | worker_service::FilterValueType::Double => {
            Err("float IN list values are not supported by runtime predicate model".to_string())
        }
    }
}

/// What: Convert protobuf filter predicate transport node into planner predicate expression.
///
/// Inputs:
/// - `predicate`: Transport predicate node from task request.
///
/// Output:
/// - Planner structured predicate expression.
fn decode_proto_predicate(
    predicate: &worker_service::FilterPredicate,
) -> Result<PredicateExpr, String> {
    let variant = predicate
        .variant
        .as_ref()
        .ok_or_else(|| "filter predicate variant is missing".to_string())?;

    match variant {
        worker_service::filter_predicate::Variant::Conjunction(node) => {
            let mut clauses = Vec::with_capacity(node.clauses.len());
            for child in &node.clauses {
                clauses.push(decode_proto_predicate(child)?);
            }
            Ok(PredicateExpr::And { clauses })
        }
        worker_service::filter_predicate::Variant::Disjunction(node) => {
            let mut clauses = Vec::with_capacity(node.clauses.len());
            for child in &node.clauses {
                clauses.push(decode_proto_predicate(child)?);
            }
            Ok(PredicateExpr::Or { clauses })
        }
        worker_service::filter_predicate::Variant::Comparison(node) => {
            let op = match worker_service::ComparisonOperator::try_from(node.operator)
                .unwrap_or(worker_service::ComparisonOperator::Unspecified)
            {
                worker_service::ComparisonOperator::Equal => PredicateComparisonOp::Eq,
                worker_service::ComparisonOperator::NotEqual => PredicateComparisonOp::Ne,
                worker_service::ComparisonOperator::GreaterThan => PredicateComparisonOp::Gt,
                worker_service::ComparisonOperator::GreaterEqual => PredicateComparisonOp::Ge,
                worker_service::ComparisonOperator::LessThan => PredicateComparisonOp::Lt,
                worker_service::ComparisonOperator::LessEqual => PredicateComparisonOp::Le,
                worker_service::ComparisonOperator::Unspecified => {
                    return Err("comparison operator is unspecified".to_string());
                }
            };

            let value = node
                .value
                .as_ref()
                .ok_or_else(|| "comparison value is missing".to_string())
                .and_then(|value| decode_proto_scalar_value(value, node.value_type))?;

            Ok(PredicateExpr::Comparison {
                column: node.column_name.clone(),
                op,
                value,
            })
        }
        worker_service::filter_predicate::Variant::Between(node) => {
            let lower = node
                .lower
                .as_ref()
                .ok_or_else(|| "between lower bound is missing".to_string())
                .and_then(|value| decode_proto_scalar_value(value, node.value_type))?;
            let upper = node
                .upper
                .as_ref()
                .ok_or_else(|| "between upper bound is missing".to_string())
                .and_then(|value| decode_proto_scalar_value(value, node.value_type))?;

            Ok(PredicateExpr::Between {
                column: node.column_name.clone(),
                lower,
                upper,
                negated: node.is_negated,
            })
        }
        worker_service::filter_predicate::Variant::InList(node) => {
            let values = decode_proto_list_value(&node.values, node.value_type)?;
            Ok(PredicateExpr::InList {
                column: node.column_name.clone(),
                values,
            })
        }
        worker_service::filter_predicate::Variant::IsNull(node) => Ok(PredicateExpr::IsNull {
            column: node.column_name.clone(),
        }),
        worker_service::filter_predicate::Variant::IsNotNull(node) => {
            Ok(PredicateExpr::IsNotNull {
                column: node.column_name.clone(),
            })
        }
        worker_service::filter_predicate::Variant::Not(node) => {
            let inner = node
                .predicate
                .as_ref()
                .ok_or_else(|| "NOT predicate payload is missing inner predicate".to_string())?;
            Ok(PredicateExpr::Not {
                expr: Box::new(decode_proto_predicate(inner)?),
            })
        }
    }
}

/// What: Executable subset extracted from validated physical operators.
///
/// Inputs:
/// - `filter_predicate`: Optional structured predicate for worker filtering.
/// - `projection_exprs`: Ordered projection expressions from payload.
/// - `schema_metadata`: Optional column type contract for Phase 9b type validation.
///
/// Output:
/// - Runtime projection/filter directives for the local worker pipeline.
#[derive(Debug, Clone)]
pub(crate) enum RuntimeJoinPlan {
    Hash(PhysicalJoinSpec),
    NestedLoop(PhysicalJoinSpec),
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimePlan {
    pub(crate) filter_predicate: Option<PredicateExpr>,
    pub(crate) schema_metadata: Option<HashMap<String, ColumnDatatypeSpec>>,
    pub(crate) join_plan: Option<RuntimeJoinPlan>,
    pub(crate) union_spec: Option<RuntimeUnionSpec>,
    pub(crate) aggregate_partial_spec: Option<PhysicalAggregateSpec>,
    pub(crate) aggregate_final_spec: Option<PhysicalAggregateSpec>,
    pub(crate) window_spec: Option<PhysicalWindowSpec>,
    pub(crate) projection_exprs: Vec<PhysicalExpr>,
    pub(crate) sort_exprs: Vec<PhysicalSortExpr>,
    pub(crate) sort_before_projection: bool,
    pub(crate) limit_spec: Option<PhysicalLimitSpec>,
    pub(crate) has_materialize: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeUnionSpec {
    pub(crate) operands: Vec<RuntimeUnionOperand>,
    pub(crate) distinct: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeUnionOperand {
    pub(crate) relation: crate::execution::query::QueryNamespace,
    pub(crate) filter: Option<PredicateExpr>,
}

#[derive(Debug, Clone)]
pub(crate) struct StageExecutionContext {
    pub(crate) stage_id: u32,
    pub(crate) upstream_stage_ids: Vec<u32>,
    pub(crate) upstream_partition_counts: HashMap<u32, u32>,
    pub(crate) upstream_stage_flight_endpoints: HashMap<u32, HashMap<u32, String>>,
    pub(crate) partition_count: u32,
    pub(crate) partition_index: u32,
    pub(crate) query_run_id: String,
    pub(crate) rbac_user: Option<String>,
    pub(crate) rbac_role: Option<String>,
    pub(crate) auth_scope: Option<String>,
    pub(crate) query_id: Option<String>,
    pub(crate) scan_hints: RuntimeScanHints,
}

/// What: Runtime scan execution mode resolved from task metadata.
///
/// Inputs:
/// - Serialized `scan_mode` value from task params.
///
/// Output:
/// - Concrete runtime mode used by scan loading flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuntimeScanMode {
    Full,
    MetadataPruned,
}

impl RuntimeScanMode {
    /// What: Parse runtime scan mode from task param value.
    ///
    /// Inputs:
    /// - `raw`: Raw mode string from task params.
    ///
    /// Output:
    /// - Parsed mode or `Full` when the value is unknown.
    fn parse_or_default(raw: Option<&str>) -> Self {
        match raw.map(str::trim) {
            Some("metadata_pruned") => Self::MetadataPruned,
            _ => Self::Full,
        }
    }
}

/// What: Scan metadata contract passed from planner/server to worker runtime.
///
/// Inputs:
/// - `mode`: Requested scan mode for the task.
/// - `pruning_hints_json`: Optional compact JSON hints for future pruning.
/// - `delta_version_pin`: Optional Delta version pin used for freshness checks.
///
/// Output:
/// - Runtime scan hint bundle consumed by scan loading functions.
#[derive(Debug, Clone)]
pub(crate) struct RuntimeScanHints {
    pub(crate) mode: RuntimeScanMode,
    pub(crate) pruning_hints_json: Option<String>,
    pub(crate) delta_version_pin: Option<u64>,
    pub(crate) pruning_eligible: bool,
    pub(crate) pruning_reason: Option<String>,
}

impl RuntimeScanHints {
    /// What: Build a default full-scan hint set.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Full-scan runtime hints with no optional metadata.
    pub(crate) fn full_scan() -> Self {
        Self {
            mode: RuntimeScanMode::Full,
            pruning_hints_json: None,
            delta_version_pin: None,
            pruning_eligible: false,
            pruning_reason: None,
        }
    }
}

fn parse_pruning_eligibility(pruning_hints_json: Option<&str>) -> (bool, Option<String>) {
    let Some(raw) = pruning_hints_json else {
        return (false, Some("missing_pruning_hints".to_string()));
    };

    let parsed: serde_json::Value = match serde_json::from_str(raw) {
        Ok(value) => value,
        Err(_) => return (false, Some("invalid_pruning_hints_json".to_string())),
    };

    let eligible = parsed
        .get("eligible")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let reason = parsed
        .get("reason")
        .and_then(serde_json::Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            if eligible {
                Some("eligible".to_string())
            } else {
                Some("ineligible_or_missing_reason".to_string())
            }
        });

    (eligible, reason)
}

/// What: Parse and extract executable runtime operators from validated physical plan payload.
///
/// Inputs:
/// - `task`: Query task containing canonical payload JSON with physical plan data.
///
/// Output:
/// - Runtime plan that includes optional filter SQL, schema metadata, and other operator specs.
///
/// Details:
/// - Schema metadata is extracted for Phase 9b type coercion validation.
/// - If schema_metadata absent, filter execution skips type checking (interim behavior).
pub(crate) fn extract_runtime_plan(
    task: &worker_service::StagePartitionExecution,
) -> Result<RuntimePlan, String> {
    let payload_opt = serde_json::from_str::<serde_json::Value>(&task.input).ok();

    // Extract schema_metadata when available in payload (Phase 9b support).
    let schema_metadata = payload_opt
        .as_ref()
        .and_then(|payload| payload.get("schema_metadata"))
        .and_then(|metadata_value| {
            serde_json::from_value::<HashMap<String, ColumnDatatypeSpec>>(metadata_value.clone())
                .ok()
        })
        .filter(|map| !map.is_empty());

    let operators = extract_runtime_operators(task)?;

    let task_filter_predicate = match &task.filter_predicate {
        Some(predicate) => Some(decode_proto_predicate(predicate)?),
        None => None,
    };

    let mut plan_filter_predicate = None;
    let mut join_plan = None;
    let mut union_spec = None;
    let mut aggregate_partial_spec = None;
    let mut aggregate_final_spec = None;
    let mut window_spec = None;
    let mut projection_exprs = Vec::new();
    let mut sort_exprs = Vec::new();
    let mut projection_index = None::<usize>;
    let mut sort_index = None::<usize>;
    let mut limit_spec = None;
    let mut has_materialize = false;

    for (index, op) in operators.into_iter().enumerate() {
        match op {
            PhysicalOperator::TableScan { .. } => {}
            PhysicalOperator::Filter { predicate } => {
                let structured = match predicate {
                    PhysicalExpr::Predicate { predicate } => Some(predicate),
                    _ => None,
                };
                if let Some(predicate) = structured {
                    plan_filter_predicate = Some(predicate);
                } else {
                    return Err(
                        "Filter operator must contain structured predicate expression".to_string(),
                    );
                }
            }
            PhysicalOperator::HashJoin { spec } => {
                join_plan = Some(RuntimeJoinPlan::Hash(spec));
            }
            PhysicalOperator::NestedLoopJoin { spec } => {
                join_plan = Some(RuntimeJoinPlan::NestedLoop(spec));
            }
            PhysicalOperator::Union { operands, distinct } => {
                let operand_filter_count = operands
                    .iter()
                    .filter(|operand| operand.filter.is_some())
                    .count();
                log::info!(
                    "Detected UNION runtime operator: operands={} filtered_operands={} distinct={}",
                    operands.len(),
                    operand_filter_count,
                    distinct
                );
                union_spec = Some(RuntimeUnionSpec {
                    operands: operands
                        .iter()
                        .map(|operand| RuntimeUnionOperand {
                            relation: crate::execution::query::QueryNamespace {
                                database: operand.relation.database.clone(),
                                schema: operand.relation.schema.clone(),
                                table: operand.relation.table.clone(),
                            },
                            filter: operand.filter.clone(),
                        })
                        .collect::<Vec<_>>(),
                    distinct,
                });
            }
            PhysicalOperator::AggregatePartial { spec } => {
                aggregate_partial_spec = Some(spec);
            }
            PhysicalOperator::AggregateFinal { spec } => {
                aggregate_final_spec = Some(spec);
            }
            PhysicalOperator::WindowAggr { spec } => {
                window_spec = Some(spec);
            }
            PhysicalOperator::Projection { expressions } => {
                projection_exprs = expressions;
                projection_index = Some(index);
            }
            PhysicalOperator::Sort { keys } => {
                sort_exprs = keys;
                sort_index = Some(index);
            }
            PhysicalOperator::Limit { spec } => {
                limit_spec = Some(spec);
            }
            PhysicalOperator::Materialize => {
                has_materialize = true;
            }
            other => {
                return Err(format!(
                    "physical operator '{}' is not executable in this phase",
                    other.canonical_name()
                ));
            }
        }
    }

    let sort_before_projection =
        matches!((sort_index, projection_index), (Some(s), Some(p)) if s < p);

    let filter_predicate = match (task_filter_predicate, plan_filter_predicate) {
        (Some(task_predicate), Some(plan_predicate)) => {
            if task_predicate != plan_predicate {
                return Err(
                    "task filter_predicate does not match physical plan filter predicate"
                        .to_string(),
                );
            }
            log::info!(
                "Using structured predicate execution from both task transport and physical plan"
            );
            Some(task_predicate)
        }
        (Some(task_predicate), None) => {
            log::info!("Using structured predicate execution from task transport only");
            Some(task_predicate)
        }
        (None, Some(plan_predicate)) => {
            log::info!("Using structured predicate execution from physical plan only");
            Some(plan_predicate)
        }
        (None, None) => {
            log::info!("No filter predicate provided for this runtime plan");
            None
        }
    };

    Ok(RuntimePlan {
        filter_predicate,
        schema_metadata,
        join_plan,
        union_spec,
        aggregate_partial_spec,
        aggregate_final_spec,
        window_spec,
        projection_exprs,
        sort_exprs,
        sort_before_projection,
        limit_spec,
        has_materialize,
    })
}

/// What: Decode executable operator pipeline from canonical or stage payload shape.
///
/// Inputs:
/// - `task`: Query task carrying serialized payload.
///
/// Output:
/// - Ordered executable physical operators.
fn extract_runtime_operators(
    task: &worker_service::StagePartitionExecution,
) -> Result<Vec<PhysicalOperator>, String> {
    if !task.execution_plan.is_empty() {
        let execution_plan_payload =
            serde_json::from_slice::<serde_json::Value>(&task.execution_plan)
                .map_err(|e| format!("invalid execution_plan payload: {}", e))?;
        return decode_runtime_operators_from_payload(&execution_plan_payload)
            .map_err(|e| format!("invalid execution_plan payload: {}", e));
    }

    let payload: serde_json::Value =
        serde_json::from_str(&task.input).map_err(|e| format!("invalid query payload: {}", e))?;

    decode_runtime_operators_from_payload(&payload)
}

/// What: Decode executable operators from one canonical/stage payload shape.
///
/// Inputs:
/// - `payload`: Serialized stage payload, operators array, or canonical query payload.
///
/// Output:
/// - Ordered executable physical operators.
fn decode_runtime_operators_from_payload(
    payload: &serde_json::Value,
) -> Result<Vec<PhysicalOperator>, String> {
    if let Some(operators) = payload.as_array() {
        return serde_json::from_value(serde_json::Value::Array(operators.clone()))
            .map_err(|e| format!("invalid stage operator payload: {}", e));
    }

    if let Some(operators) = payload
        .get("operators")
        .and_then(serde_json::Value::as_array)
    {
        return serde_json::from_value(serde_json::Value::Array(operators.clone()))
            .map_err(|e| format!("invalid stage payload operators: {}", e));
    }

    let physical_plan = payload
        .get("physical_plan")
        .cloned()
        .ok_or_else(|| "query payload missing physical_plan".to_string())?;
    let physical_plan: PhysicalPlan = serde_json::from_value(physical_plan)
        .map_err(|e| format!("invalid physical_plan payload: {}", e))?;

    Ok(physical_plan.operators)
}

/// What: Build stage execution context from task params.
///
/// Inputs:
/// - `task`: Query task metadata and params.
///
/// Output:
/// - Stage execution context with deterministic defaults or contextual validation failure.
pub(crate) fn stage_execution_context(
    task: &worker_service::StagePartitionExecution,
) -> Result<StageExecutionContext, String> {
    let is_staged_task = task.stage_id > 0
        || task.partition_id > 0
        || task.partition_count > 1
        || !task.upstream_stage_ids.is_empty();

    let stage_id = if is_staged_task { task.stage_id } else { 0 };
    let upstream_stage_ids = task.upstream_stage_ids.clone();
    let partition_count = if is_staged_task {
        if task.partition_count == 0 {
            return Err(format!(
                "EXECUTION_WORKER_EXECUTION_STAGE_CONTEXT_MISSING: partition_count missing for task_id={} stage_id={}",
                task.task_id, stage_id
            ));
        }
        task.partition_count
    } else {
        1
    };
    let upstream_partition_counts = task.upstream_partition_counts.clone();
    let upstream_stage_flight_endpoints = task
        .params
        .get("__upstream_stage_flight_endpoints_json")
        .filter(|value| !value.trim().is_empty())
        .map(|raw| {
            let parsed = serde_json::from_str::<serde_json::Value>(raw).map_err(|e| {
                format!(
                    "invalid __upstream_stage_flight_endpoints_json payload for task_id={}: {}",
                    task.task_id, e
                )
            })?;

            let object = parsed.as_object().ok_or_else(|| {
                format!(
                    "invalid __upstream_stage_flight_endpoints_json payload for task_id={}: expected JSON object",
                    task.task_id
                )
            })?;

            let mut normalized =
                HashMap::<u32, HashMap<u32, String>>::with_capacity(object.len());
            for (stage_key, endpoint_value) in object {
                let stage_id = stage_key.trim().parse::<u32>().map_err(|e| {
                    format!(
                        "invalid upstream stage id '{}' in __upstream_stage_flight_endpoints_json for task_id={}: {}",
                        stage_key, task.task_id, e
                    )
                })?;

                // Backward-compatible parse: allow either
                // 1) stage -> endpoint string
                // 2) stage -> { partition_id -> endpoint string }
                let partition_map = if let Some(endpoint) = endpoint_value.as_str() {
                    let endpoint = endpoint.trim().to_string();
                    if endpoint.is_empty() {
                        return Err(format!(
                            "empty upstream endpoint for stage_id={} in task_id={}",
                            stage_id, task.task_id
                        ));
                    }
                    let mut map = HashMap::<u32, String>::new();
                    map.insert(0, endpoint);
                    map
                } else {
                    let endpoint_object = endpoint_value.as_object().ok_or_else(|| {
                        format!(
                            "invalid upstream endpoint map for stage_id={} in task_id={}: expected string or object",
                            stage_id, task.task_id
                        )
                    })?;
                    let mut map = HashMap::<u32, String>::with_capacity(endpoint_object.len());
                    for (partition_key, endpoint_value) in endpoint_object {
                        let partition_id = partition_key.trim().parse::<u32>().map_err(|e| {
                            format!(
                                "invalid upstream partition id '{}' for stage_id={} in task_id={}: {}",
                                partition_key, stage_id, task.task_id, e
                            )
                        })?;
                        let endpoint = endpoint_value
                            .as_str()
                            .map(str::trim)
                            .filter(|value| !value.is_empty())
                            .ok_or_else(|| {
                                format!(
                                    "empty upstream endpoint for stage_id={} partition_id={} in task_id={}",
                                    stage_id, partition_id, task.task_id
                                )
                            })?
                            .to_string();
                        map.insert(partition_id, endpoint);
                    }
                    map
                };

                if partition_map.is_empty() {
                    return Err(format!(
                        "upstream endpoint map is empty for stage_id={} in task_id={}",
                        stage_id, task.task_id
                    ));
                }

                normalized.insert(stage_id, partition_map);
            }

            Ok::<HashMap<u32, HashMap<u32, String>>, String>(normalized)
        })
        .transpose()?
        .unwrap_or_default();
    let partition_index = if is_staged_task {
        if task.partition_id >= partition_count {
            return Err(format!(
                "EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING: partition_index invalid for task_id={} stage_id={}",
                task.task_id, stage_id
            ));
        }
        task.partition_id
    } else {
        0
    };
    let query_run_id = if task.query_run_id.trim().is_empty() {
        format!("legacy-task-{}", task.task_id)
    } else {
        task.query_run_id.clone()
    };
    let rbac_user = task
        .params
        .get("__rbac_user")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let rbac_role = task
        .params
        .get("__rbac_role")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let auth_scope = task
        .params
        .get("__auth_scope")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let query_id = task
        .params
        .get("__query_id")
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    let scan_hints = RuntimeScanHints {
        mode: RuntimeScanMode::parse_or_default(task.params.get("scan_mode").map(String::as_str)),
        pruning_hints_json: task
            .params
            .get("scan_pruning_hints_json")
            .filter(|value| !value.trim().is_empty())
            .cloned(),
        delta_version_pin: task
            .params
            .get("scan_delta_version_pin")
            .and_then(|value| value.parse::<u64>().ok()),
        pruning_eligible: false,
        pruning_reason: None,
    };
    let (pruning_eligible, pruning_reason) =
        parse_pruning_eligibility(scan_hints.pruning_hints_json.as_deref());
    let scan_hints = RuntimeScanHints {
        pruning_eligible,
        pruning_reason,
        ..scan_hints
    };

    Ok(StageExecutionContext {
        stage_id,
        upstream_stage_ids,
        upstream_partition_counts,
        upstream_stage_flight_endpoints,
        partition_count,
        partition_index,
        query_run_id,
        rbac_user,
        rbac_role,
        auth_scope,
        query_id,
        scan_hints,
    })
}
