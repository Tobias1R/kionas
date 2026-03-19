use crate::planner::logical_plan::LogicalPlan;

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

#[cfg(test)]
mod tests {
    use super::{explain_logical_plan, explain_logical_plan_json};
    use crate::planner::logical_plan::{
        LogicalExpr, LogicalPlan, LogicalProjection, LogicalRelation, LogicalSelection,
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
            sql: "SELECT id FROM sales.public.users WHERE active = true".to_string(),
        };

        let text = explain_logical_plan(&plan);
        let json = explain_logical_plan_json(&plan);

        assert!(text.contains("projection_count=1"));
        assert!(json.contains("\"relation\""));
    }
}
