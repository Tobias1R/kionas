use crate::planner::error::PlannerError;
use crate::planner::logical_plan::{
    LogicalExpr, LogicalPlan, LogicalProjection, LogicalRelation, LogicalSelection, LogicalSortExpr,
};
use crate::sql::query_model::SelectQueryModel;

/// What: Build a Phase 1 logical plan from the shared select query model.
///
/// Inputs:
/// - `model`: Shared semantic query model produced before planning.
///
/// Output:
/// - Typed logical plan used by downstream validation and explain surfaces.
///
/// Details:
/// - This phase keeps expression translation simple and deterministic by preserving
///   projection and filter SQL text in typed expression wrappers.
pub fn build_logical_plan_from_select_model(
    model: &SelectQueryModel,
) -> Result<LogicalPlan, PlannerError> {
    let projection_exprs = model
        .projection
        .iter()
        .map(|expr| LogicalExpr::Raw { sql: expr.clone() })
        .collect::<Vec<_>>();

    let plan = LogicalPlan {
        relation: LogicalRelation {
            database: model.namespace.database.clone(),
            schema: model.namespace.schema.clone(),
            table: model.namespace.table.clone(),
        },
        projection: LogicalProjection {
            expressions: projection_exprs,
        },
        selection: model.selection.as_ref().map(|predicate| LogicalSelection {
            predicate: LogicalExpr::Raw {
                sql: predicate.clone(),
            },
        }),
        order_by: model
            .order_by
            .iter()
            .map(|sort| LogicalSortExpr {
                expression: LogicalExpr::Raw {
                    sql: sort.expression.clone(),
                },
                ascending: sort.ascending,
            })
            .collect::<Vec<_>>(),
        limit: model.limit,
        offset: model.offset,
        sql: model.sql.clone(),
    };

    super::validate::validate_logical_plan(&plan)?;
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::build_logical_plan_from_select_model;
    use crate::sql::query_model::{QueryNamespace, SelectQueryModel, SortSpec};

    #[test]
    fn builds_plan_from_select_model() {
        let model = SelectQueryModel {
            version: 1,
            statement: "Select".to_string(),
            session_id: "s1".to_string(),
            namespace: QueryNamespace {
                database: "sales".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                raw: "sales.public.users".to_string(),
            },
            projection: vec!["id".to_string(), "name".to_string()],
            selection: Some("active = true".to_string()),
            order_by: vec![SortSpec {
                expression: "id".to_string(),
                ascending: true,
            }],
            limit: Some(5),
            offset: Some(2),
            sql: "SELECT id, name FROM sales.public.users WHERE active = true".to_string(),
        };

        let plan = build_logical_plan_from_select_model(&model).expect("plan should build");
        assert_eq!(plan.relation.database, "sales");
        assert_eq!(plan.relation.schema, "public");
        assert_eq!(plan.relation.table, "users");
        assert_eq!(plan.projection.expressions.len(), 2);
        assert!(plan.selection.is_some());
        assert_eq!(plan.order_by.len(), 1);
        assert_eq!(plan.limit, Some(5));
        assert_eq!(plan.offset, Some(2));
    }
}
