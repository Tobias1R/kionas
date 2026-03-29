use crate::planner::engine::fallback::requires_strict_datafusion_planning;
use kionas::sql::query_model::{
    QueryFromSpec, QueryJoinPredicate, QueryJoinSpec, SelectQueryModel,
};

pub(crate) fn translation_source_model(model: &SelectQueryModel) -> &SelectQueryModel {
    if let QueryFromSpec::CteRef { name } = &model.from
        && let Some(cte) = model
            .ctes
            .iter()
            .find(|cte| cte.name.eq_ignore_ascii_case(name))
    {
        return cte.query.as_ref();
    }

    model
}

fn resolve_cte_model_from_root<'a>(
    root: &'a SelectQueryModel,
    cte_name: &str,
) -> Option<&'a SelectQueryModel> {
    root.ctes
        .iter()
        .find(|cte| cte.name.eq_ignore_ascii_case(cte_name))
        .map(|cte| cte.query.as_ref())
}

fn cte_reference_name_for_model<'a>(
    root: &'a SelectQueryModel,
    current: &'a SelectQueryModel,
) -> Option<&'a str> {
    match &current.from {
        QueryFromSpec::CteRef { name } => Some(name.as_str()),
        QueryFromSpec::Table { namespace } => root
            .ctes
            .iter()
            .find(|cte| cte.name.eq_ignore_ascii_case(namespace.table.as_str()))
            .map(|cte| cte.name.as_str()),
        QueryFromSpec::Derived { .. } => None,
    }
}

pub(crate) fn collect_translation_source_chain(model: &SelectQueryModel) -> Vec<&SelectQueryModel> {
    let mut chain = Vec::<&SelectQueryModel>::new();
    let mut current = translation_source_model(model);
    chain.push(current);

    while let Some(cte_name) = cte_reference_name_for_model(model, current) {
        let Some(next) = resolve_cte_model_from_root(model, cte_name) else {
            break;
        };

        // Guard against malformed/self-referential CTE graphs.
        if chain.iter().any(|existing| std::ptr::eq(*existing, next)) {
            break;
        }

        chain.push(next);
        current = next;
    }

    chain
}

pub(crate) fn deepest_translation_source_model(model: &SelectQueryModel) -> &SelectQueryModel {
    let chain = collect_translation_source_chain(model);
    chain.last().copied().unwrap_or(model)
}

fn joins_equivalent(left: &QueryJoinSpec, right: &QueryJoinSpec) -> bool {
    fn predicates_equivalent(left: &[QueryJoinPredicate], right: &[QueryJoinPredicate]) -> bool {
        left.len() == right.len() && left.iter().zip(right.iter()).all(|(lhs, rhs)| lhs == rhs)
    }

    left.join_type == right.join_type
        && left
            .right
            .database
            .eq_ignore_ascii_case(right.right.database.as_str())
        && left
            .right
            .schema
            .eq_ignore_ascii_case(right.right.schema.as_str())
        && left
            .right
            .table
            .eq_ignore_ascii_case(right.right.table.as_str())
        && left.keys.len() == right.keys.len()
        && left.keys.iter().zip(right.keys.iter()).all(|(lhs, rhs)| {
            lhs.left.eq_ignore_ascii_case(rhs.left.as_str())
                && lhs.right.eq_ignore_ascii_case(rhs.right.as_str())
        })
        && predicates_equivalent(&left.predicates, &right.predicates)
}

pub(crate) fn collect_translation_joins<'a>(
    model: &'a SelectQueryModel,
    source_chain: &[&'a SelectQueryModel],
) -> Vec<&'a QueryJoinSpec> {
    let mut joins = Vec::<&QueryJoinSpec>::new();

    for candidate in source_chain.iter().copied().chain(std::iter::once(model)) {
        for join in &candidate.joins {
            if joins
                .iter()
                .any(|existing| joins_equivalent(existing, join))
            {
                continue;
            }
            joins.push(join);
        }
    }

    joins
}

pub(crate) fn should_skip_strict_intent_mismatch(model: &SelectQueryModel) -> bool {
    requires_strict_datafusion_planning(model)
}

pub(crate) fn is_subquery_predicate_sql(selection_sql: &str) -> bool {
    let normalized = selection_sql.to_ascii_lowercase();
    normalized.contains(" in (select")
        || normalized.contains(" exists (")
        || normalized.contains(" not exists (")
}
