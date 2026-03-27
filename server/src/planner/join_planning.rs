use kionas::sql::query_model::{QueryJoinPredicate, QueryJoinSpec, QueryJoinType};

/// What: Join algorithm families considered during planner routing.
///
/// Inputs:
/// - Variant selected by join feature shape and coarse cardinality hints.
///
/// Output:
/// - Deterministic algorithm tag used to route physical operator construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinAlgorithm {
    Hash,
    NestedLoop,
    BroadcastCross,
    PartitionedCross,
    SortMerge,
}

const NESTED_LOOP_RIGHT_ROW_THRESHOLD: u64 = 1_000_000;

/// What: Detect preferred join algorithm for one query-model join specification.
///
/// Inputs:
/// - `spec`: Canonical join specification extracted from SQL.
/// - `left_cardinality`: Estimated left row count (0 when unknown).
/// - `right_cardinality`: Estimated right row count (0 when unknown).
///
/// Output:
/// - Selected join algorithm tag.
///
/// Details:
/// - Equality-only inner joins route to `Hash`.
/// - Theta joins route to `NestedLoop` for small right side, otherwise `SortMerge` (future path).
/// - Cross joins route to broadcast or partitioned strategy based on right-cardinality threshold.
pub fn detect_join_algorithm(
    spec: &QueryJoinSpec,
    left_cardinality: u64,
    right_cardinality: u64,
) -> JoinAlgorithm {
    let has_theta = contains_theta_predicate(&spec.predicates);
    let has_only_equalities = !spec.predicates.is_empty() && !has_theta;

    match spec.join_type {
        QueryJoinType::Inner => {
            if has_only_equalities || !spec.keys.is_empty() {
                JoinAlgorithm::Hash
            } else {
                detect_theta_path(right_cardinality)
            }
        }
        QueryJoinType::InnerTheta => detect_theta_path(right_cardinality),
        QueryJoinType::InnerCross => detect_cross_path(left_cardinality, right_cardinality),
        QueryJoinType::Left => JoinAlgorithm::NestedLoop,
    }
}

fn detect_theta_path(right_cardinality: u64) -> JoinAlgorithm {
    if right_cardinality == 0 || right_cardinality < NESTED_LOOP_RIGHT_ROW_THRESHOLD {
        JoinAlgorithm::NestedLoop
    } else {
        JoinAlgorithm::SortMerge
    }
}

fn detect_cross_path(left_cardinality: u64, right_cardinality: u64) -> JoinAlgorithm {
    let effective_right = if right_cardinality == 0 {
        left_cardinality
    } else {
        right_cardinality
    };

    if effective_right == 0 || effective_right < NESTED_LOOP_RIGHT_ROW_THRESHOLD {
        JoinAlgorithm::BroadcastCross
    } else {
        JoinAlgorithm::PartitionedCross
    }
}

fn contains_theta_predicate(predicates: &[QueryJoinPredicate]) -> bool {
    predicates.iter().any(|predicate| match predicate {
        QueryJoinPredicate::Theta { .. } => true,
        QueryJoinPredicate::Composite { predicates } => contains_theta_predicate(predicates),
        QueryJoinPredicate::Equality { .. } => false,
    })
}

#[cfg(test)]
mod tests {
    use super::{JoinAlgorithm, detect_join_algorithm};
    use kionas::sql::query_model::{
        QueryJoinKey, QueryJoinPredicate, QueryJoinSpec, QueryJoinType, QueryNamespace,
    };

    fn sample_spec(join_type: QueryJoinType) -> QueryJoinSpec {
        QueryJoinSpec {
            join_type,
            right: QueryNamespace {
                database: "db".to_string(),
                schema: "s".to_string(),
                table: "t".to_string(),
                raw: "db.s.t".to_string(),
            },
            predicates: Vec::new(),
            keys: Vec::new(),
        }
    }

    #[test]
    fn detects_hash_for_equality_joins() {
        let mut spec = sample_spec(QueryJoinType::Inner);
        spec.keys.push(QueryJoinKey {
            left: "l.id".to_string(),
            right: "r.id".to_string(),
        });
        spec.predicates.push(QueryJoinPredicate::Equality {
            left: "l.id".to_string(),
            right: "r.id".to_string(),
        });

        assert_eq!(detect_join_algorithm(&spec, 10, 10), JoinAlgorithm::Hash);
    }

    #[test]
    fn detects_nested_loop_for_small_theta_join() {
        let mut spec = sample_spec(QueryJoinType::InnerTheta);
        spec.predicates.push(QueryJoinPredicate::Theta {
            left: "l.id".to_string(),
            op: "<".to_string(),
            right: "r.id".to_string(),
        });

        assert_eq!(
            detect_join_algorithm(&spec, 100_000, 100),
            JoinAlgorithm::NestedLoop
        );
    }

    #[test]
    fn detects_sort_merge_for_large_theta_join() {
        let mut spec = sample_spec(QueryJoinType::InnerTheta);
        spec.predicates.push(QueryJoinPredicate::Theta {
            left: "l.id".to_string(),
            op: "<".to_string(),
            right: "r.id".to_string(),
        });

        assert_eq!(
            detect_join_algorithm(&spec, 5_000_000, 2_000_000),
            JoinAlgorithm::SortMerge
        );
    }
}
