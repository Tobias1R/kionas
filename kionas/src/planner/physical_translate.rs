use crate::planner::error::PlannerError;
use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_plan::{PhysicalExpr, PhysicalOperator, PhysicalPlan};

/// What: Build a Phase 2 physical plan from a validated logical plan.
///
/// Inputs:
/// - `plan`: Logical plan produced by query model translation.
///
/// Output:
/// - Physical plan with deterministic operator ordering.
///
/// Details:
/// - The pipeline is always `TableScan -> [Filter] -> Projection -> Materialize`.
/// - Capability checks are delegated to physical validation.
pub fn build_physical_plan_from_logical_plan(
    plan: &LogicalPlan,
) -> Result<PhysicalPlan, PlannerError> {
    let mut operators = Vec::new();
    operators.push(PhysicalOperator::TableScan {
        relation: plan.relation.clone(),
    });

    if let Some(selection) = &plan.selection {
        operators.push(PhysicalOperator::Filter {
            predicate: PhysicalExpr::from(&selection.predicate),
        });
    }

    let projection_exprs = plan
        .projection
        .expressions
        .iter()
        .map(PhysicalExpr::from)
        .collect::<Vec<_>>();
    operators.push(PhysicalOperator::Projection {
        expressions: projection_exprs,
    });
    operators.push(PhysicalOperator::Materialize);

    let physical = PhysicalPlan {
        operators,
        sql: plan.sql.clone(),
    };

    super::physical_validate::validate_physical_plan(&physical)?;
    Ok(physical)
}

#[cfg(test)]
mod tests {
    use super::build_physical_plan_from_logical_plan;
    use crate::planner::logical_plan::{
        LogicalExpr, LogicalPlan, LogicalProjection, LogicalRelation, LogicalSelection,
    };
    use crate::planner::physical_plan::PhysicalOperator;

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
            sql: "SELECT id FROM sales.public.users WHERE active = true".to_string(),
        };

        let physical = build_physical_plan_from_logical_plan(&plan).expect("physical plan");
        assert_eq!(physical.operators.len(), 4);
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
            PhysicalOperator::Materialize
        ));
    }
}
