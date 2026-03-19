use crate::planner::logical_plan::LogicalPlan;
use crate::planner::physical_plan::PhysicalPlan;

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
        "LogicalPlan relation={}.{}.{} projection_count={} selection={}",
        plan.relation.database,
        plan.relation.schema,
        plan.relation.table,
        plan.projection.expressions.len(),
        selection
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
        .map(|op| op.canonical_name())
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
    use crate::planner::physical_plan::{PhysicalExpr, PhysicalOperator, PhysicalPlan};

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
                PhysicalOperator::Materialize,
            ],
            sql: "SELECT id FROM sales.public.users".to_string(),
        };

        let text = explain_physical_plan(&plan);
        let json = explain_physical_plan_json(&plan);

        assert_eq!(
            text,
            "PhysicalPlan operators=3 pipeline=TableScan->Projection->Materialize sql=\"SELECT id FROM sales.public.users\""
        );
        assert!(json.contains("\"operators\""));
    }
}
