use crate::planner::distributed_plan::{DistributedPhysicalPlan, distributed_from_physical_plan};
use crate::planner::distributed_validate::validate_distributed_physical_plan;
use crate::planner::error::PlannerError;
use crate::planner::join_spec::PhysicalJoinSpec;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_plan::PhysicalExpr;
use crate::planner::physical_plan::{
    PhysicalLimitSpec, PhysicalOperator, PhysicalPlan, PhysicalSortExpr,
};
use crate::planner::predicate_expr::parse_predicate_sql;

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
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

/// What: Build a Phase 2 physical plan from a validated logical plan.
///
/// Inputs:
/// - `plan`: Logical plan produced by query model translation.
///
/// Output:
/// - Physical plan with deterministic operator ordering.
///
/// Details:
/// - The pipeline is always `TableScan -> [Filter] -> Projection -> [Sort] -> [Limit] -> Materialize`.
/// - Capability checks are delegated to physical validation.
pub fn build_physical_plan_from_logical_plan(
    plan: &LogicalPlan,
) -> Result<PhysicalPlan, PlannerError> {
    let mut operators = Vec::new();
    operators.push(PhysicalOperator::TableScan {
        relation: plan.relation.clone(),
    });

    if let Some(selection) = &plan.selection {
        let predicate = match &selection.predicate {
            crate::planner::LogicalExpr::Raw { sql } => {
                let parsed = parse_predicate_sql(sql).map_err(PlannerError::InvalidLogicalPlan)?;
                PhysicalExpr::Predicate { predicate: parsed }
            }
            other => PhysicalExpr::from(other),
        };
        operators.push(PhysicalOperator::Filter { predicate });
    }

    for join in &plan.joins {
        operators.push(PhysicalOperator::HashJoin {
            spec: PhysicalJoinSpec {
                join_type: join.join_type.clone(),
                right_relation: join.right_relation.clone(),
                keys: join.keys.clone(),
            },
        });
    }

    if !plan.aggregates.is_empty() {
        let grouping_exprs = plan
            .grouping_keys
            .iter()
            .map(PhysicalExpr::from)
            .collect::<Vec<_>>();
        let aggregates = plan
            .aggregates
            .iter()
            .map(|aggregate| crate::planner::PhysicalAggregateExpr {
                function: aggregate.function.clone(),
                input: aggregate.input.as_ref().map(PhysicalExpr::from),
                output_name: aggregate.output_name.clone(),
            })
            .collect::<Vec<_>>();

        let aggregate_spec = crate::planner::PhysicalAggregateSpec {
            grouping_exprs,
            aggregates,
        };
        operators.push(PhysicalOperator::AggregatePartial {
            spec: aggregate_spec.clone(),
        });
        operators.push(PhysicalOperator::AggregateFinal {
            spec: aggregate_spec,
        });
    }

    let projection_exprs = if plan.aggregates.is_empty() {
        plan.projection
            .expressions
            .iter()
            .map(PhysicalExpr::from)
            .collect::<Vec<_>>()
    } else {
        plan.projection
            .expressions
            .iter()
            .map(|expr| match expr {
                crate::planner::LogicalExpr::Raw { sql } => PhysicalExpr::ColumnRef {
                    name: projection_output_name(sql),
                },
                crate::planner::LogicalExpr::Column { name } => PhysicalExpr::ColumnRef {
                    name: projection_output_name(name),
                },
            })
            .collect::<Vec<_>>()
    };
    operators.push(PhysicalOperator::Projection {
        expressions: projection_exprs,
    });

    if !plan.order_by.is_empty() {
        operators.push(PhysicalOperator::Sort {
            keys: plan
                .order_by
                .iter()
                .map(|sort| PhysicalSortExpr {
                    expression: PhysicalExpr::from(&sort.expression),
                    ascending: sort.ascending,
                })
                .collect::<Vec<_>>(),
        });
    }

    if let Some(count) = plan.limit {
        operators.push(PhysicalOperator::Limit {
            spec: PhysicalLimitSpec {
                count,
                offset: plan.offset.unwrap_or(0),
            },
        });
    }

    operators.push(PhysicalOperator::Materialize);

    let physical = PhysicalPlan {
        operators,
        sql: plan.sql.clone(),
        schema_metadata: None,
    };

    super::physical_validate::validate_physical_plan(&physical)?;
    Ok(physical)
}

/// What: Build a distributed physical plan from a validated logical plan.
///
/// Inputs:
/// - `plan`: Logical plan produced by query model translation.
///
/// Output:
/// - Distributed physical plan with stage nodes and dependency graph.
///
/// Details:
/// - Current implementation lifts the existing single-stage physical plan into distributed shape.
/// - This preserves current execution behavior while enabling stage DAG compilation in later steps.
pub fn build_distributed_plan_from_logical_plan(
    plan: &LogicalPlan,
) -> Result<DistributedPhysicalPlan, PlannerError> {
    let physical = build_physical_plan_from_logical_plan(plan)?;
    let distributed = distributed_from_physical_plan(&physical);
    validate_distributed_physical_plan(&distributed)?;
    Ok(distributed)
}

#[cfg(test)]
mod tests {
    use super::{build_distributed_plan_from_logical_plan, build_physical_plan_from_logical_plan};
    use crate::planner::logical_plan::{
        LogicalExpr, LogicalPlan, LogicalProjection, LogicalRelation, LogicalSelection,
        LogicalSortExpr,
    };
    use crate::planner::physical_plan::PhysicalOperator;
    use crate::planner::{JoinKeyPair, JoinType, LogicalJoinSpec};

    #[test]
    fn builds_linear_pipeline_with_filter() {
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
            order_by: vec![LogicalSortExpr {
                expression: LogicalExpr::Raw {
                    sql: "id".to_string(),
                },
                ascending: false,
            }],
            limit: Some(3),
            offset: Some(1),
            sql: "SELECT id FROM sales.public.users WHERE active = true".to_string(),
        };

        let physical = build_physical_plan_from_logical_plan(&plan).expect("physical plan");
        assert_eq!(physical.operators.len(), 6);
        assert!(matches!(
            physical.operators[0],
            PhysicalOperator::TableScan { .. }
        ));
        assert!(matches!(
            physical.operators[1],
            PhysicalOperator::Filter { .. }
        ));
        assert!(matches!(
            physical.operators[2],
            PhysicalOperator::Projection { .. }
        ));
        assert!(matches!(
            physical.operators[3],
            PhysicalOperator::Sort { .. }
        ));
        assert!(matches!(
            physical.operators[4],
            PhysicalOperator::Limit { .. }
        ));
        assert!(matches!(
            physical.operators[5],
            PhysicalOperator::Materialize
        ));
    }

    #[test]
    fn builds_distributed_single_stage_plan() {
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
            selection: None,
            joins: Vec::new(),
            grouping_keys: Vec::new(),
            aggregates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            sql: "SELECT id FROM sales.public.users".to_string(),
        };

        let distributed =
            build_distributed_plan_from_logical_plan(&plan).expect("distributed plan");
        assert_eq!(distributed.stages.len(), 1);
        assert!(distributed.dependencies.is_empty());
        assert_eq!(distributed.sql, plan.sql);
    }

    #[test]
    fn emits_hash_join_operator_for_join_spec() {
        let plan = LogicalPlan {
            relation: LogicalRelation {
                database: "sales".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
            },
            projection: LogicalProjection {
                expressions: vec![LogicalExpr::Raw {
                    sql: "users.id".to_string(),
                }],
            },
            selection: None,
            joins: vec![LogicalJoinSpec {
                join_type: JoinType::Inner,
                right_relation: LogicalRelation {
                    database: "sales".to_string(),
                    schema: "public".to_string(),
                    table: "orders".to_string(),
                },
                keys: vec![JoinKeyPair {
                    left: "users.id".to_string(),
                    right: "orders.user_id".to_string(),
                }],
            }],
            grouping_keys: Vec::new(),
            aggregates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None,
            sql: "SELECT users.id FROM sales.public.users INNER JOIN sales.public.orders ON users.id = orders.user_id".to_string(),
        };

        let physical = build_physical_plan_from_logical_plan(&plan).expect("physical plan");
        assert!(
            physical
                .operators
                .iter()
                .any(|op| matches!(op, PhysicalOperator::HashJoin { .. }))
        );
    }

    #[test]
    fn emits_aggregate_partial_and_final_operators() {
        let plan = LogicalPlan {
            relation: LogicalRelation {
                database: "sales".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
            },
            projection: LogicalProjection {
                expressions: vec![
                    LogicalExpr::Raw {
                        sql: "country".to_string(),
                    },
                    LogicalExpr::Raw {
                        sql: "COUNT(*)".to_string(),
                    },
                ],
            },
            selection: None,
            joins: Vec::new(),
            grouping_keys: vec![LogicalExpr::Raw {
                sql: "country".to_string(),
            }],
            aggregates: vec![crate::planner::LogicalAggregateExpr {
                function: crate::planner::AggregateFunction::Count,
                input: None,
                output_name: "count".to_string(),
            }],
            order_by: Vec::new(),
            limit: None,
            offset: None,
            sql: "SELECT country, COUNT(*) FROM sales.public.users GROUP BY country".to_string(),
        };

        let physical = build_physical_plan_from_logical_plan(&plan).expect("physical plan");
        assert!(
            physical
                .operators
                .iter()
                .any(|op| matches!(op, PhysicalOperator::AggregatePartial { .. }))
        );
        assert!(
            physical
                .operators
                .iter()
                .any(|op| matches!(op, PhysicalOperator::AggregateFinal { .. }))
        );
    }
}
