use crate::planner::engine::projection_parsing::{
    is_simple_identifier_reference, parse_projection_alias,
};
use kionas::planner::{JoinKeyPair, PhysicalJoinPredicate};
use kionas::sql::query_model::{QueryJoinKey, QueryJoinPredicate, SelectQueryModel};
use std::collections::HashMap;

fn normalize_join_key_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    let no_prefix = if let Some((_, rhs)) = trimmed.rsplit_once('.') {
        rhs
    } else {
        trimmed
    };

    no_prefix
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

pub(crate) fn cte_projection_source_map(model: &SelectQueryModel) -> HashMap<String, String> {
    let mut sources = HashMap::<String, String>::new();

    if !model.alias_column_map.is_empty() {
        for (alias, source) in &model.alias_column_map {
            sources.insert(
                normalize_join_key_identifier(alias.as_str()),
                normalize_join_key_identifier(source.column.as_str()),
            );
        }
        return sources;
    }

    // Backward-compatible fallback for legacy payloads that do not include
    // query-model alias metadata.
    for projection in &model.projection {
        let (base_expr, alias) = parse_projection_alias(projection);
        let Some(alias) = alias else {
            continue;
        };

        if !is_simple_identifier_reference(base_expr.as_str()) {
            continue;
        }

        sources.insert(
            normalize_join_key_identifier(alias.as_str()),
            normalize_join_key_identifier(base_expr.as_str()),
        );
    }

    sources
}

fn remap_join_key_with_cte_source(raw: &str, sources: &HashMap<String, String>) -> String {
    let normalized = normalize_join_key_identifier(raw);

    // If key references a CTE output alias, resolve it to the source column that
    // exists before projection materialization.
    if let Some(source_column) = sources.get(normalized.as_str()) {
        return source_column.clone();
    }

    normalized
}

fn remap_join_operand_with_cte_source(raw: &str, sources: &HashMap<String, String>) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return trimmed.to_string();
    }

    let is_quoted_string =
        trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2;
    let is_integer_literal = trimmed.parse::<i64>().is_ok();
    let is_boolean_literal =
        trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false");
    if is_quoted_string || is_integer_literal || is_boolean_literal {
        return trimmed.to_string();
    }

    remap_join_key_with_cte_source(trimmed, sources)
}

fn key_references_cte_alias(raw: &str, sources: &HashMap<String, String>) -> bool {
    let normalized = normalize_join_key_identifier(raw);
    sources.contains_key(normalized.as_str())
}

pub(crate) fn remap_join_key_pair_with_cte_source(
    key: &QueryJoinKey,
    sources: &HashMap<String, String>,
) -> JoinKeyPair {
    let left_is_cte_alias = key_references_cte_alias(key.left.as_str(), sources);
    let right_is_cte_alias = key_references_cte_alias(key.right.as_str(), sources);

    // Join runtime expects key.left to reference the accumulated left-side batch and
    // key.right to reference the current right-side join relation.
    if left_is_cte_alias && !right_is_cte_alias {
        return JoinKeyPair {
            left: remap_join_key_with_cte_source(key.left.as_str(), sources),
            right: remap_join_key_with_cte_source(key.right.as_str(), sources),
        };
    }

    if right_is_cte_alias && !left_is_cte_alias {
        return JoinKeyPair {
            left: remap_join_key_with_cte_source(key.right.as_str(), sources),
            right: remap_join_key_with_cte_source(key.left.as_str(), sources),
        };
    }

    JoinKeyPair {
        left: remap_join_key_with_cte_source(key.left.as_str(), sources),
        right: remap_join_key_with_cte_source(key.right.as_str(), sources),
    }
}

pub(crate) fn map_query_join_predicate_to_physical(
    predicate: &QueryJoinPredicate,
    cte_sources: &HashMap<String, String>,
) -> PhysicalJoinPredicate {
    match predicate {
        QueryJoinPredicate::Equality { left, right } => PhysicalJoinPredicate::Equality {
            left: remap_join_operand_with_cte_source(left.as_str(), cte_sources),
            right: remap_join_operand_with_cte_source(right.as_str(), cte_sources),
        },
        QueryJoinPredicate::Theta { left, op, right } => PhysicalJoinPredicate::Theta {
            left: remap_join_operand_with_cte_source(left.as_str(), cte_sources),
            op: op.clone(),
            right: remap_join_operand_with_cte_source(right.as_str(), cte_sources),
        },
        QueryJoinPredicate::Composite { predicates } => PhysicalJoinPredicate::Composite {
            predicates: predicates
                .iter()
                .map(|inner| map_query_join_predicate_to_physical(inner, cte_sources))
                .collect::<Vec<_>>(),
        },
    }
}
