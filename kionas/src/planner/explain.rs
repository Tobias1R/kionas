use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_plan::{PhysicalExpr, PhysicalOperator, PhysicalPlan};
use crate::planner::predicate_expr::render_predicate_expr;

/// What: Render a physical expression for concise diagnostics.
///
/// Inputs:
/// - `expr`: Physical expression to render.
///
/// Output:
/// - Stable expression string for explain text.
fn render_physical_expr(expr: &PhysicalExpr) -> String {
    match expr {
        PhysicalExpr::ColumnRef { name } => name.clone(),
        PhysicalExpr::Raw { sql } => sql.clone(),
        PhysicalExpr::Predicate { predicate } => render_predicate_expr(predicate),
    }
}

/// What: Render an operator label with optional detail for diagnostics.
///
/// Inputs:
/// - `operator`: Physical operator node.
///
/// Output:
/// - Operator label suitable for explain pipeline output.
fn render_operator_diagnostic(operator: &PhysicalOperator) -> String {
    match operator {
        PhysicalOperator::Sort { keys } => {
            let key_text = keys
                .iter()
                .map(|key| {
                    let direction = if key.ascending { "ASC" } else { "DESC" };
                    format!("{} {}", render_physical_expr(&key.expression), direction)
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("Sort({})", key_text)
        }
        PhysicalOperator::Limit { spec } => {
            format!("Limit(count={}, offset={})", spec.count, spec.offset)
        }
        PhysicalOperator::HashJoin { spec } => {
            let keys = spec
                .keys
                .iter()
                .map(|key| format!("{}={}", key.left, key.right))
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "HashJoin(type={:?}, right={}.{}.{}, keys=[{}])",
                spec.join_type,
                spec.right_relation.database,
                spec.right_relation.schema,
                spec.right_relation.table,
                keys
            )
        }
        PhysicalOperator::NestedLoopJoin { spec } => {
            format!(
                "NestedLoopJoin(type={:?}, right={}.{}.{}, predicates={}, keys={})",
                spec.join_type,
                spec.right_relation.database,
                spec.right_relation.schema,
                spec.right_relation.table,
                spec.predicates.len(),
                spec.keys.len()
            )
        }
        PhysicalOperator::AggregatePartial { spec } => {
            let keys = spec
                .grouping_exprs
                .iter()
                .map(render_physical_expr)
                .collect::<Vec<_>>()
                .join(", ");
            let aggregates = spec
                .aggregates
                .iter()
                .map(|aggregate| format!("{:?}:{}", aggregate.function, aggregate.output_name))
                .collect::<Vec<_>>()
                .join(", ");
            format!("AggregatePartial(keys=[{}], aggs=[{}])", keys, aggregates)
        }
        PhysicalOperator::AggregateFinal { spec } => {
            let keys = spec
                .grouping_exprs
                .iter()
                .map(render_physical_expr)
                .collect::<Vec<_>>()
                .join(", ");
            let aggregates = spec
                .aggregates
                .iter()
                .map(|aggregate| format!("{:?}:{}", aggregate.function, aggregate.output_name))
                .collect::<Vec<_>>()
                .join(", ");
            format!("AggregateFinal(keys=[{}], aggs=[{}])", keys, aggregates)
        }
        PhysicalOperator::WindowAggr { spec } => {
            let partition_by = spec
                .partition_by
                .iter()
                .map(render_physical_expr)
                .collect::<Vec<_>>()
                .join(", ");
            let order_by = spec
                .order_by
                .iter()
                .map(|key| {
                    let direction = if key.ascending { "ASC" } else { "DESC" };
                    format!("{} {}", render_physical_expr(&key.expression), direction)
                })
                .collect::<Vec<_>>()
                .join(", ");
            let functions = spec
                .functions
                .iter()
                .map(|function| function.output_name.clone())
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "WindowAggr(partition_by=[{}], order_by=[{}], outputs=[{}])",
                partition_by, order_by, functions
            )
        }
        _ => operator.canonical_name().to_string(),
    }
}

/// What: Build a concise human-readable explanation string for a logical plan.
///
/// Inputs:
/// - `plan`: Logical plan to explain.
///
/// Output:
/// - Deterministic explanation text for debugging and contributor onboarding.
pub fn explain_logical_plan(plan: &LogicalPlan) -> String {
    let selection = if plan.selection.is_some() {
        "present"
    } else {
        "absent"
    };

    format!(
        "LogicalPlan relation={}.{}.{} projection_count={} selection={} join_count={} group_count={} aggregate_count={}",
        plan.relation.database,
        plan.relation.schema,
        plan.relation.table,
        plan.projection.expressions.len(),
        selection,
        plan.joins.len(),
        plan.grouping_keys.len(),
        plan.aggregates.len()
    )
}

/// What: Serialize a logical plan to pretty JSON for deterministic inspection.
///
/// Inputs:
/// - `plan`: Logical plan to serialize.
///
/// Output:
/// - Pretty-printed JSON string representation.
///
/// Details:
/// - Returns compact fallback JSON string if pretty serialization fails unexpectedly.
pub fn explain_logical_plan_json(plan: &LogicalPlan) -> String {
    match serde_json::to_string_pretty(plan) {
        Ok(serialized) => serialized,
        Err(_) => "{}".to_string(),
    }
}

/// What: Build a concise human-readable explanation string for a physical plan.
///
/// Inputs:
/// - `plan`: Physical plan to explain.
///
/// Output:
/// - Deterministic explanation text with operator ordering metadata.
pub fn explain_physical_plan(plan: &PhysicalPlan) -> String {
    let pipeline = plan
        .operators
        .iter()
        .map(render_operator_diagnostic)
        .collect::<Vec<_>>()
        .join("->");

    format!(
        "PhysicalPlan operators={} pipeline={} sql=\"{}\"",
        plan.operators.len(),
        pipeline,
        plan.sql
    )
}

/// What: Serialize a physical plan to pretty JSON for deterministic inspection.
///
/// Inputs:
/// - `plan`: Physical plan to serialize.
///
/// Output:
/// - Pretty-printed JSON string representation.
pub fn explain_physical_plan_json(plan: &PhysicalPlan) -> String {
    match serde_json::to_string_pretty(plan) {
        Ok(serialized) => serialized,
        Err(_) => "{}".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        explain_logical_plan, explain_logical_plan_json, explain_physical_plan,
        explain_physical_plan_json,
    };
    use crate::planner::logical_plan::{
        LogicalExpr, LogicalPlan, LogicalProjection, LogicalRelation, LogicalSelection,
    };
    use crate::planner::physical_plan::{
        PhysicalExpr, PhysicalLimitSpec, PhysicalOperator, PhysicalPlan, PhysicalSortExpr,
    };

    #[test]
    fn explains_plan_text_and_json() {
        let plan = LogicalPlan {
            relation: LogicalRelation {
                database: "sales".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
            },
            projection: LogicalProjection {
                expressions: vec![LogicalExpr::Raw {
                    sql: "id".to_string(),
                }],
            },
            selection: Some(LogicalSelection {
                predicate: LogicalExpr::Raw {
                    sql: "active = true".to_string(),
                },
            }),
            joins: Vec::new(),
            grouping_keys: Vec::new(),
            aggregates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            sql: "SELECT id FROM sales.public.users WHERE active = true".to_string(),
        };

        let text = explain_logical_plan(&plan);
        let json = explain_logical_plan_json(&plan);

        assert!(text.contains("projection_count=1"));
        assert!(json.contains("\"relation\""));
    }

    #[test]
    fn explains_physical_plan_text_and_json() {
        let plan = PhysicalPlan {
            operators: vec![
                PhysicalOperator::TableScan {
                    relation: LogicalRelation {
                        database: "sales".to_string(),
                        schema: "public".to_string(),
                        table: "users".to_string(),
                    },
                },
                PhysicalOperator::Projection {
                    expressions: vec![PhysicalExpr::Raw {
                        sql: "id".to_string(),
                    }],
                },
                PhysicalOperator::AggregatePartial {
                    spec: crate::planner::PhysicalAggregateSpec {
                        grouping_exprs: vec![PhysicalExpr::Raw {
                            sql: "country".to_string(),
                        }],
                        aggregates: vec![crate::planner::PhysicalAggregateExpr {
                            function: crate::planner::AggregateFunction::Count,
                            input: None,
                            output_name: "count".to_string(),
                        }],
                    },
                },
                PhysicalOperator::Sort {
                    keys: vec![PhysicalSortExpr {
                        expression: PhysicalExpr::Raw {
                            sql: "id".to_string(),
                        },
                        ascending: false,
                    }],
                },
                PhysicalOperator::Limit {
                    spec: PhysicalLimitSpec {
                        count: 5,
                        offset: 2,
                    },
                },
                PhysicalOperator::Materialize,
            ],
            sql: "SELECT id FROM sales.public.users".to_string(),
            schema_metadata: None,
        };

        let text = explain_physical_plan(&plan);
        let json = explain_physical_plan_json(&plan);

        assert_eq!(
            text,
            "PhysicalPlan operators=6 pipeline=TableScan->Projection->AggregatePartial(keys=[country], aggs=[Count:count])->Sort(id DESC)->Limit(count=5, offset=2)->Materialize sql=\"SELECT id FROM sales.public.users\""
        );
        assert!(json.contains("\"operators\""));
        assert!(json.contains("\"Sort\""));
    }
}
