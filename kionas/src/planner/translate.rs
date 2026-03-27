use crate::planner::error::PlannerError;
use crate::planner::join_spec::{JoinKeyPair, JoinType, LogicalJoinSpec};
use crate::planner::logical_plan::{
    LogicalExpr, LogicalPlan, LogicalProjection, LogicalRelation, LogicalSelection, LogicalSortExpr,
};
use crate::planner::{AggregateFunction, LogicalAggregateExpr};
use crate::sql::query_model::{SelectQueryModel, primary_relation_dependency};

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

fn parse_projection_aggregates(
    projection: &[String],
) -> Result<Vec<LogicalAggregateExpr>, PlannerError> {
    let mut aggregates = Vec::new();

    for expr in projection {
        if let Some((function, input)) = parse_aggregate_expr_from_projection(expr) {
            let (_, alias) = parse_projection_alias(expr);
            let output_name = if let Some(alias) = alias {
                alias
            } else {
                match (&function, input.as_deref()) {
                    (AggregateFunction::Count, None) => "count".to_string(),
                    (AggregateFunction::Count, Some(column)) => {
                        format!("count_{}", normalize_identifier(column))
                    }
                    (AggregateFunction::Sum, Some(column)) => {
                        format!("sum_{}", normalize_identifier(column))
                    }
                    (AggregateFunction::Min, Some(column)) => {
                        format!("min_{}", normalize_identifier(column))
                    }
                    (AggregateFunction::Max, Some(column)) => {
                        format!("max_{}", normalize_identifier(column))
                    }
                    (AggregateFunction::Avg, Some(column)) => {
                        format!("avg_{}", normalize_identifier(column))
                    }
                    _ => {
                        return Err(PlannerError::InvalidLogicalPlan(format!(
                            "unsupported aggregate expression '{}'",
                            expr
                        )));
                    }
                }
            };

            aggregates.push(LogicalAggregateExpr {
                function,
                input: input.map(|column| LogicalExpr::Raw { sql: column }),
                output_name,
            });
        }
    }

    Ok(aggregates)
}

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
    let relation = primary_relation_dependency(model).ok_or_else(|| {
        PlannerError::InvalidLogicalPlan("query model has no relation dependencies".to_string())
    })?;

    let projection_exprs = model
        .projection
        .iter()
        .map(|expr| LogicalExpr::Raw { sql: expr.clone() })
        .collect::<Vec<_>>();
    let grouping_keys = model
        .group_by
        .iter()
        .map(|expr| LogicalExpr::Raw { sql: expr.clone() })
        .collect::<Vec<_>>();
    let aggregates = parse_projection_aggregates(&model.projection)?;

    let plan = LogicalPlan {
        relation: LogicalRelation {
            database: relation.database.clone(),
            schema: relation.schema.clone(),
            table: relation.table.clone(),
        },
        projection: LogicalProjection {
            expressions: projection_exprs,
        },
        selection: model.selection.as_ref().map(|predicate| LogicalSelection {
            predicate: LogicalExpr::Raw {
                sql: predicate.clone(),
            },
        }),
        joins: model
            .joins
            .iter()
            .map(|join| LogicalJoinSpec {
                join_type: match join.join_type {
                    crate::sql::query_model::QueryJoinType::Inner => JoinType::Inner,
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
            })
            .collect::<Vec<_>>(),
        grouping_keys,
        aggregates,
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
    use crate::sql::query_model::{QueryFromSpec, QueryNamespace, SelectQueryModel, SortSpec};

    #[test]
    fn builds_plan_from_select_model() {
        let model = SelectQueryModel {
            version: 1,
            statement: "Select".to_string(),
            session_id: "s1".to_string(),
            from: QueryFromSpec::Table {
                namespace: QueryNamespace {
                    database: "sales".to_string(),
                    schema: "public".to_string(),
                    table: "users".to_string(),
                    raw: "sales.public.users".to_string(),
                },
            },
            projection: vec!["id".to_string(), "name".to_string()],
            selection: Some("active = true".to_string()),
            joins: Vec::new(),
            group_by: Vec::new(),
            order_by: vec![SortSpec {
                expression: "id".to_string(),
                ascending: true,
            }],
            limit: Some(5),
            offset: Some(2),
            ctes: Vec::new(),
            relation_dependencies: vec![QueryNamespace {
                database: "sales".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
                raw: "sales.public.users".to_string(),
            }],
            union: None,
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
