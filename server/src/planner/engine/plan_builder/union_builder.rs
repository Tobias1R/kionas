use crate::planner::engine::types::PlannerIntentFlags;
use kionas::planner::{
    LogicalRelation, PhysicalExpr, PhysicalLimitSpec, PhysicalOperator, PhysicalPlan,
    PhysicalSortExpr, PhysicalUnionOperand, PlannerError, parse_predicate_sql,
    validate_physical_plan,
};
use kionas::sql::query_model::{SelectQueryModel, primary_relation_dependency};

/// What: Build the Kionas physical plan for UNION query intent.
///
/// Inputs:
/// - `model`: Canonical query model including union specification.
/// - `intent`: Derived planner intent flags.
/// - `scan_relation`: Primary relation used for the initial table scan operator.
///
/// Output:
/// - Validated physical plan with union, optional projection/sort/limit, and materialization.
///
/// Details:
/// - This path enforces union shape checks against DataFusion intent metadata.
pub(super) fn build_union_plan(
    model: &SelectQueryModel,
    intent: PlannerIntentFlags,
    scan_relation: &LogicalRelation,
) -> Result<PhysicalPlan, PlannerError> {
    let has_projection = intent.has_projection || !model.projection.is_empty();
    let has_sort = intent.has_sort;
    let has_limit = intent.has_limit;

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
        relation: scan_relation.clone(),
    });
    operators.push(PhysicalOperator::Union {
        operands: union_spec
            .operands
            .iter()
            .map(|operand| {
                let operand_relation = primary_relation_dependency(operand).ok_or_else(|| {
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
    Ok(translated)
}
