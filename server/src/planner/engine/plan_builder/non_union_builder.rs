use crate::planner::engine::join_mapping::{
    map_query_join_predicate_to_physical, remap_join_key_pair_with_cte_source,
};
use crate::planner::engine::projection_parsing::{
    build_aggregate_spec_from_model, build_window_spec_from_model, model_contains_aggregate,
    model_contains_window, projection_output_name, should_project_cte_source_columns,
};
use crate::planner::engine::translation_sources::{
    is_subquery_predicate_sql, should_skip_strict_intent_mismatch,
};
use crate::planner::engine::types::PlannerIntentFlags;
use crate::planner::join_planning::{JoinAlgorithm, detect_join_algorithm};
use kionas::planner::{
    JoinType, LogicalRelation, PhysicalExpr, PhysicalLimitSpec, PhysicalOperator, PhysicalPlan,
    PhysicalSortExpr, PlannerError, PredicateExpr, parse_predicate_sql, validate_physical_plan,
};
use kionas::sql::query_model::{QueryFromSpec, QueryJoinSpec, SelectQueryModel};
use std::collections::HashMap;

/// What: Build the Kionas physical plan for non-union query intent.
///
/// Inputs:
/// - `model`: Canonical query model to translate.
/// - `intent`: Derived planner intent flags.
/// - `source_model`: CTE-resolved source model for translation.
/// - `source_chain`: Source chain used for aggregate lookup.
/// - `translation_joins`: Deduplicated join specifications collected for translation.
/// - `cte_sources`: Alias-to-source map for CTE projection remapping.
/// - `scan_relation`: Primary scan relation for the first operator.
///
/// Output:
/// - Validated physical plan for non-union execution.
///
/// Details:
/// - This preserves strict mismatch checks and join-family behavior from the previous monolith.
pub(super) fn build_non_union_plan<'a>(
    model: &'a SelectQueryModel,
    intent: PlannerIntentFlags,
    source_model: &'a SelectQueryModel,
    source_chain: &[&'a SelectQueryModel],
    translation_joins: &[&'a QueryJoinSpec],
    cte_sources: &HashMap<String, String>,
    scan_relation: &LogicalRelation,
) -> Result<PhysicalPlan, PlannerError> {
    let has_scan = intent.has_scan;
    let has_filter = intent.has_filter;
    let has_projection = intent.has_projection || !model.projection.is_empty();
    let has_sort = intent.has_sort;
    let has_limit = intent.has_limit;
    let has_hash_join = intent.has_hash_join;
    let has_sort_merge_join = intent.has_sort_merge_join;
    let has_nested_loop_join = intent.has_nested_loop_join;
    let has_join = has_hash_join || has_sort_merge_join || has_nested_loop_join;
    let has_aggregate = intent.has_aggregate;
    let has_window = intent.has_window;
    let has_chain_aggregate = source_chain
        .iter()
        .any(|candidate| model_contains_aggregate(candidate));

    let allow_mismatch = should_skip_strict_intent_mismatch(model);

    let model_has_join = !translation_joins.is_empty();
    if has_join != model_has_join && !allow_mismatch {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "join translation mismatch: datafusion_has_join={} model_has_join={}",
            has_join, model_has_join
        )));
    }

    let model_has_aggregate = model_contains_aggregate(model);
    if has_aggregate != model_has_aggregate && !allow_mismatch {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "aggregate translation mismatch: datafusion_has_aggregate={} model_has_aggregate={}",
            has_aggregate, model_has_aggregate
        )));
    }

    let model_has_window = model_contains_window(source_model) || model_contains_window(model);
    if has_window != model_has_window && !allow_mismatch {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "window translation mismatch: datafusion_has_window={} model_has_window={}",
            has_window, model_has_window
        )));
    }

    let model_has_filter = source_model.selection.is_some() || model.selection.is_some();
    if has_filter != model_has_filter && !allow_mismatch {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "filter translation mismatch: datafusion_has_filter={} model_has_filter={}",
            has_filter, model_has_filter
        )));
    }

    let model_has_sort = !model.order_by.is_empty()
        || (has_window
            && build_window_spec_from_model(source_model)
                .map(|spec| !spec.order_by.is_empty())
                .unwrap_or(false));
    if has_sort != model_has_sort && !allow_mismatch {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "sort translation mismatch: datafusion_has_sort={} model_has_sort={}",
            has_sort, model_has_sort
        )));
    }

    let model_has_limit = model.limit.is_some();
    if has_limit != model_has_limit && !allow_mismatch {
        return Err(PlannerError::InvalidPhysicalPipeline(format!(
            "limit translation mismatch: datafusion_has_limit={} model_has_limit={}",
            has_limit, model_has_limit
        )));
    }

    if !has_scan {
        return Err(PlannerError::InvalidPhysicalPipeline(
            "DataFusion physical plan does not contain a supported scan node".to_string(),
        ));
    }

    if has_sort_merge_join {
        return Err(PlannerError::UnsupportedPhysicalOperator(
            "SortMergeJoin".to_string(),
        ));
    }

    if has_nested_loop_join {
        log::warn!(
            "DataFusion selected nested-loop join family; planner will route via join algorithm detection"
        );
    }

    let mut operators = Vec::<PhysicalOperator>::new();
    operators.push(PhysicalOperator::TableScan {
        relation: scan_relation.clone(),
    });

    let is_cte_source_translation = matches!(model.from, QueryFromSpec::CteRef { .. });
    let mut deferred_join_filter_predicate: Option<PredicateExpr> = None;

    if has_filter {
        let selection_sql = source_model.selection.as_ref().or(model.selection.as_ref());

        if let Some(selection_sql) = selection_sql {
            if is_cte_source_translation
                && source_model.selection.is_none()
                && model.selection.is_some()
            {
                log::warn!(
                    "Skipping outer CTE filter pushdown during source-table translation: {}",
                    selection_sql
                );
            } else {
                match parse_predicate_sql(selection_sql) {
                    Ok(predicate) => {
                        if has_join {
                            deferred_join_filter_predicate = Some(predicate);
                        } else {
                            operators.push(PhysicalOperator::Filter {
                                predicate: PhysicalExpr::Predicate { predicate },
                            });
                        }
                    }
                    Err(err) if is_subquery_predicate_sql(selection_sql) && has_filter => {
                        log::warn!(
                            "Skipping filter translation for subquery predicate; runtime filter parser cannot deserialize this predicate shape yet: {}",
                            err
                        );
                    }
                    Err(err) => return Err(PlannerError::UnsupportedPredicate(err)),
                }
            }
        }
    }

    if has_join {
        if translation_joins.is_empty() {
            return Err(PlannerError::InvalidPhysicalPipeline(
                "DataFusion join node detected but query model contains no join spec".to_string(),
            ));
        }

        for join in translation_joins {
            let join_predicates = join
                .predicates
                .iter()
                .map(|predicate| map_query_join_predicate_to_physical(predicate, cte_sources))
                .collect::<Vec<_>>();

            let join_algorithm = detect_join_algorithm(join, 0, 0);
            match join_algorithm {
                JoinAlgorithm::Hash => {
                    operators.push(PhysicalOperator::HashJoin {
                        spec: kionas::planner::PhysicalJoinSpec {
                            join_type: match join.join_type {
                                kionas::sql::query_model::QueryJoinType::Inner
                                | kionas::sql::query_model::QueryJoinType::InnerTheta
                                | kionas::sql::query_model::QueryJoinType::InnerCross => {
                                    JoinType::Inner
                                }
                                kionas::sql::query_model::QueryJoinType::Left => JoinType::Left,
                            },
                            right_relation: LogicalRelation {
                                database: join.right.database.clone(),
                                schema: join.right.schema.clone(),
                                table: join.right.table.clone(),
                            },
                            predicates: join_predicates,
                            keys: join
                                .keys
                                .iter()
                                .map(|key| remap_join_key_pair_with_cte_source(key, cte_sources))
                                .collect::<Vec<_>>(),
                        },
                    });
                }
                JoinAlgorithm::NestedLoop
                | JoinAlgorithm::BroadcastCross
                | JoinAlgorithm::PartitionedCross => {
                    operators.push(PhysicalOperator::NestedLoopJoin {
                        spec: kionas::planner::PhysicalJoinSpec {
                            join_type: match join.join_type {
                                kionas::sql::query_model::QueryJoinType::Left => JoinType::Left,
                                _ => JoinType::Inner,
                            },
                            right_relation: LogicalRelation {
                                database: join.right.database.clone(),
                                schema: join.right.schema.clone(),
                                table: join.right.table.clone(),
                            },
                            predicates: join_predicates,
                            keys: join
                                .keys
                                .iter()
                                .map(|key| remap_join_key_pair_with_cte_source(key, cte_sources))
                                .collect::<Vec<_>>(),
                        },
                    });
                }
                JoinAlgorithm::SortMerge => {
                    return Err(PlannerError::UnsupportedPhysicalOperator(
                        "SortMergeJoin".to_string(),
                    ));
                }
            }
        }
    }

    if let Some(predicate) = deferred_join_filter_predicate.take() {
        operators.push(PhysicalOperator::Filter {
            predicate: PhysicalExpr::Predicate { predicate },
        });
    }

    if has_aggregate || has_chain_aggregate {
        let mut aggregate_spec = None;
        for candidate in source_chain {
            aggregate_spec = build_aggregate_spec_from_model(candidate)?;
            if aggregate_spec.is_some() {
                break;
            }
        }

        if aggregate_spec.is_none() {
            aggregate_spec = build_aggregate_spec_from_model(model)?;
        }

        if let Some(aggregate_spec) = aggregate_spec {
            operators.push(PhysicalOperator::AggregatePartial {
                spec: aggregate_spec.clone(),
            });
            operators.push(PhysicalOperator::AggregateFinal {
                spec: aggregate_spec,
            });
        } else if allow_mismatch {
            log::warn!("Skipping aggregate translation for strict-intent-mismatch query shape");
        } else {
            return Err(PlannerError::InvalidPhysicalPipeline(
                "DataFusion aggregate node detected but query model has no aggregate spec"
                    .to_string(),
            ));
        }
    }

    if has_window {
        let window_spec = build_window_spec_from_model(source_model)
            .or_else(|| build_window_spec_from_model(model))
            .ok_or_else(|| {
                PlannerError::InvalidPhysicalPipeline(
                    "DataFusion window node detected but query model has no window spec"
                        .to_string(),
                )
            })?;

        operators.push(PhysicalOperator::WindowAggr { spec: window_spec });
    }

    let should_sort_before_projection = has_sort && !has_aggregate && !has_window;

    if should_sort_before_projection && !model.order_by.is_empty() {
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

    if has_projection {
        let expressions = if has_aggregate || has_window {
            model
                .projection
                .iter()
                .map(|expr| {
                    let trimmed = expr.trim();
                    if trimmed == "*" {
                        PhysicalExpr::Raw {
                            sql: "*".to_string(),
                        }
                    } else {
                        PhysicalExpr::ColumnRef {
                            name: projection_output_name(expr),
                        }
                    }
                })
                .collect::<Vec<_>>()
        } else if should_project_cte_source_columns(model, source_model, has_aggregate, has_window)
        {
            source_model
                .projection
                .iter()
                .map(|sql| PhysicalExpr::Raw { sql: sql.clone() })
                .collect::<Vec<_>>()
        } else {
            model
                .projection
                .iter()
                .map(|sql| PhysicalExpr::Raw { sql: sql.clone() })
                .collect::<Vec<_>>()
        };
        operators.push(PhysicalOperator::Projection { expressions });
    }

    if has_sort && !should_sort_before_projection && !model.order_by.is_empty() {
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
