use crate::services::worker_service_server::worker_service;
use kionas::planner::{
    PhysicalAggregateSpec, PhysicalExpr, PhysicalJoinSpec, PhysicalLimitSpec, PhysicalOperator,
    PhysicalPlan, PhysicalSortExpr,
};
use std::collections::HashMap;

/// What: Executable subset extracted from validated physical operators.
///
/// Inputs:
/// - `filter_sql`: Optional raw SQL predicate for simple conjunction filtering.
/// - `projection_exprs`: Ordered projection expressions from payload.
///
/// Output:
/// - Runtime projection/filter directives for the local worker pipeline.
#[derive(Debug, Clone)]
pub(crate) struct RuntimePlan {
    pub(crate) filter_sql: Option<String>,
    pub(crate) join_spec: Option<PhysicalJoinSpec>,
    pub(crate) aggregate_partial_spec: Option<PhysicalAggregateSpec>,
    pub(crate) aggregate_final_spec: Option<PhysicalAggregateSpec>,
    pub(crate) projection_exprs: Vec<PhysicalExpr>,
    pub(crate) sort_exprs: Vec<PhysicalSortExpr>,
    pub(crate) limit_spec: Option<PhysicalLimitSpec>,
    pub(crate) has_materialize: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct StageExecutionContext {
    pub(crate) stage_id: u32,
    pub(crate) upstream_stage_ids: Vec<u32>,
    pub(crate) upstream_partition_counts: HashMap<u32, u32>,
    pub(crate) partition_count: u32,
    pub(crate) partition_index: u32,
    pub(crate) query_run_id: String,
}

/// What: Parse and extract executable runtime operators from validated physical plan payload.
///
/// Inputs:
/// - `task`: Query task containing canonical payload JSON.
///
/// Output:
/// - Runtime plan that includes optional filter SQL and projection expressions.
pub(crate) fn extract_runtime_plan(task: &worker_service::Task) -> Result<RuntimePlan, String> {
    let operators = extract_runtime_operators(task)?;

    let mut filter_sql = None;
    let mut join_spec = None;
    let mut aggregate_partial_spec = None;
    let mut aggregate_final_spec = None;
    let mut projection_exprs = Vec::new();
    let mut sort_exprs = Vec::new();
    let mut limit_spec = None;
    let mut has_materialize = false;

    for op in operators {
        match op {
            PhysicalOperator::TableScan { .. } => {}
            PhysicalOperator::Filter { predicate } => {
                let raw = match predicate {
                    PhysicalExpr::Raw { sql } => sql,
                    PhysicalExpr::ColumnRef { name } => name,
                };
                filter_sql = Some(raw);
            }
            PhysicalOperator::HashJoin { spec } => {
                join_spec = Some(spec);
            }
            PhysicalOperator::AggregatePartial { spec } => {
                aggregate_partial_spec = Some(spec);
            }
            PhysicalOperator::AggregateFinal { spec } => {
                aggregate_final_spec = Some(spec);
            }
            PhysicalOperator::Projection { expressions } => {
                projection_exprs = expressions;
            }
            PhysicalOperator::Sort { keys } => {
                sort_exprs = keys;
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

    Ok(RuntimePlan {
        filter_sql,
        join_spec,
        aggregate_partial_spec,
        aggregate_final_spec,
        projection_exprs,
        sort_exprs,
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
fn extract_runtime_operators(task: &worker_service::Task) -> Result<Vec<PhysicalOperator>, String> {
    let payload: serde_json::Value =
        serde_json::from_str(&task.input).map_err(|e| format!("invalid query payload: {}", e))?;

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
/// - Stage execution context with deterministic defaults.
pub(crate) fn stage_execution_context(task: &worker_service::Task) -> StageExecutionContext {
    let stage_id = task
        .params
        .get("stage_id")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let upstream_stage_ids = task
        .params
        .get("upstream_stage_ids")
        .and_then(|value| serde_json::from_str::<Vec<u32>>(value).ok())
        .unwrap_or_default();
    let partition_count = task
        .params
        .get("partition_count")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);
    let upstream_partition_counts = task
        .params
        .get("upstream_partition_counts")
        .and_then(|value| serde_json::from_str::<HashMap<u32, u32>>(value).ok())
        .unwrap_or_default();
    let partition_index = task
        .params
        .get("partition_index")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let query_run_id = task
        .params
        .get("query_run_id")
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| format!("legacy-task-{}", task.task_id));

    StageExecutionContext {
        stage_id,
        upstream_stage_ids,
        upstream_partition_counts,
        partition_count,
        partition_index,
        query_run_id,
    }
}
