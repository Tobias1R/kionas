use crate::planner::physical_plan::{PhysicalOperator, PhysicalPlan};
use serde::{Deserialize, Serialize};

/// What: Partitioning directives for stage outputs and exchanges.
///
/// Inputs:
/// - Variant fields encode partitioning strategy details.
///
/// Output:
/// - Serializable partition specification attached to distributed stages.
///
/// Details:
/// - This phase supports basic partition directives needed for parallel scan plus merge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionSpec {
    Single,
    Hash { keys: Vec<String> },
    Broadcast,
}

/// What: A stage node in a distributed physical plan.
///
/// Inputs:
/// - `stage_id`: Stable numeric stage identifier.
/// - `operators`: Ordered operators executed inside this stage.
/// - `partition_spec`: Partitioning directive for stage output.
///
/// Output:
/// - One executable stage with deterministic identity and operator pipeline.
///
/// Details:
/// - Stage-local operators preserve physical operator ordering from prior phases.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedStage {
    pub stage_id: u32,
    pub operators: Vec<PhysicalOperator>,
    pub partition_spec: PartitionSpec,
    #[serde(default)]
    pub output_partition_count: Option<u32>,
}

/// What: Directed dependency edge between distributed stages.
///
/// Inputs:
/// - `from_stage_id`: Upstream stage that must complete first.
/// - `to_stage_id`: Downstream stage unlocked by upstream completion.
///
/// Output:
/// - Serializable dependency relation used by stage schedulers.
///
/// Details:
/// - Edges are interpreted as precedence constraints and must remain acyclic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StageDependency {
    pub from_stage_id: u32,
    pub to_stage_id: u32,
}

/// What: Distributed physical plan model for stage-based orchestration.
///
/// Inputs:
/// - `stages`: Collection of stage nodes.
/// - `dependencies`: Directed precedence constraints.
/// - `sql`: Canonical SQL string for diagnostics.
///
/// Output:
/// - Serializable distributed plan ready for DAG compilation.
///
/// Details:
/// - This structure is intentionally additive and does not replace the existing single-stage physical plan.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DistributedPhysicalPlan {
    pub stages: Vec<DistributedStage>,
    pub dependencies: Vec<StageDependency>,
    pub sql: String,
}

/// What: Determine whether a physical operator starts a new distributed stage.
///
/// Inputs:
/// - `operator`: Physical operator in pipeline order.
///
/// Output:
/// - `true` when the operator should begin a downstream stage.
///
/// Details:
/// - Boundaries include explicit exchange/repartition operators and existing
///   aggregate/join split points for compatibility.
fn is_stage_boundary_operator(operator: &PhysicalOperator) -> bool {
    matches!(
        operator,
        PhysicalOperator::Repartition
            | PhysicalOperator::ExchangeShuffle { .. }
            | PhysicalOperator::ExchangeBroadcast
            | PhysicalOperator::AggregateFinal { .. }
            | PhysicalOperator::HashJoin { .. }
    )
}

/// What: Convert one physical expression into a stable partition key string.
///
/// Inputs:
/// - `expr`: Physical expression from grouping or partitioning metadata.
///
/// Output:
/// - Rendered key string used by hash partition specs.
fn partition_key_for_expr(expr: &crate::planner::physical_plan::PhysicalExpr) -> String {
    match expr {
        crate::planner::physical_plan::PhysicalExpr::ColumnRef { name } => name.clone(),
        crate::planner::physical_plan::PhysicalExpr::Raw { sql } => sql.clone(),
        crate::planner::physical_plan::PhysicalExpr::Predicate { predicate } => {
            crate::planner::predicate_expr::render_predicate_expr(predicate)
        }
    }
}

/// What: Infer upstream partitioning from one stage boundary operator.
///
/// Inputs:
/// - `boundary_operator`: Operator that begins the downstream stage.
/// - `upstream_operators`: Operators that belong to the upstream stage.
///
/// Output:
/// - Partitioning directive for the upstream stage output.
///
/// Details:
/// - This preserves compatibility with previous aggregate/join inference and
///   extends it for explicit exchange operators.
fn infer_partition_spec_for_boundary(
    boundary_operator: &PhysicalOperator,
    upstream_operators: &[PhysicalOperator],
) -> PartitionSpec {
    match boundary_operator {
        PhysicalOperator::ExchangeShuffle { keys } if !keys.is_empty() => {
            PartitionSpec::Hash { keys: keys.clone() }
        }
        PhysicalOperator::ExchangeBroadcast => PartitionSpec::Broadcast,
        PhysicalOperator::HashJoin { spec } if !spec.keys.is_empty() => PartitionSpec::Hash {
            keys: spec.keys.iter().map(|k| k.left.clone()).collect::<Vec<_>>(),
        },
        PhysicalOperator::AggregateFinal { .. } => upstream_operators
            .iter()
            .rev()
            .find_map(|operator| match operator {
                PhysicalOperator::AggregatePartial { spec } => Some(PartitionSpec::Hash {
                    keys: spec
                        .grouping_exprs
                        .iter()
                        .map(partition_key_for_expr)
                        .collect::<Vec<_>>(),
                }),
                _ => None,
            })
            .unwrap_or(PartitionSpec::Single),
        _ => PartitionSpec::Single,
    }
}

/// What: Finalize one stage operator slice as a valid stage operator vector.
///
/// Inputs:
/// - `operators`: Ordered stage operators.
///
/// Output:
/// - Non-empty stage operators ending in `Materialize`.
fn finalize_stage_operators(operators: Vec<PhysicalOperator>) -> Vec<PhysicalOperator> {
    let mut stage_operators = operators;
    if stage_operators.is_empty() {
        return stage_operators;
    }
    if !matches!(stage_operators.last(), Some(PhysicalOperator::Materialize)) {
        stage_operators.push(PhysicalOperator::Materialize);
    }
    stage_operators
}

/// What: Lift a single-stage physical plan into distributed-plan shape.
///
/// Inputs:
/// - `plan`: Existing physical plan from logical translation.
///
/// Output:
/// - Distributed plan containing one stage and no dependencies.
///
/// Details:
/// - This compatibility bridge allows phased rollout without changing current execution behavior.
pub fn distributed_from_physical_plan(plan: &PhysicalPlan) -> DistributedPhysicalPlan {
    let boundary_indices = plan
        .operators
        .iter()
        .enumerate()
        .filter_map(|(index, operator)| {
            if is_stage_boundary_operator(operator) {
                Some(index)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if boundary_indices.is_empty() {
        return DistributedPhysicalPlan {
            stages: vec![DistributedStage {
                stage_id: 0,
                operators: plan.operators.clone(),
                partition_spec: PartitionSpec::Single,
                output_partition_count: None,
            }],
            dependencies: Vec::new(),
            sql: plan.sql.clone(),
        };
    }

    let mut stages = Vec::<DistributedStage>::new();
    let mut dependencies = Vec::<StageDependency>::new();
    let mut stage_start = 0usize;
    let mut next_stage_id = 0u32;

    for boundary_index in boundary_indices {
        if boundary_index <= stage_start {
            continue;
        }

        let upstream_ops =
            finalize_stage_operators(plan.operators[stage_start..boundary_index].to_vec());
        if upstream_ops.is_empty() {
            stage_start = boundary_index;
            continue;
        }

        let partition_spec =
            infer_partition_spec_for_boundary(&plan.operators[boundary_index], &upstream_ops);

        stages.push(DistributedStage {
            stage_id: next_stage_id,
            operators: upstream_ops,
            partition_spec,
            output_partition_count: None,
        });

        if next_stage_id > 0 {
            dependencies.push(StageDependency {
                from_stage_id: next_stage_id - 1,
                to_stage_id: next_stage_id,
            });
        }

        next_stage_id += 1;
        stage_start = boundary_index;
    }

    let final_stage_ops = finalize_stage_operators(plan.operators[stage_start..].to_vec());
    if final_stage_ops.is_empty() {
        return DistributedPhysicalPlan {
            stages,
            dependencies,
            sql: plan.sql.clone(),
        };
    }

    stages.push(DistributedStage {
        stage_id: next_stage_id,
        operators: final_stage_ops,
        partition_spec: PartitionSpec::Single,
        output_partition_count: None,
    });
    if next_stage_id > 0 {
        dependencies.push(StageDependency {
            from_stage_id: next_stage_id - 1,
            to_stage_id: next_stage_id,
        });
    }

    DistributedPhysicalPlan {
        stages,
        dependencies,
        sql: plan.sql.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::{PartitionSpec, distributed_from_physical_plan};
    use crate::planner::aggregate_spec::{
        AggregateFunction, PhysicalAggregateExpr, PhysicalAggregateSpec,
    };
    use crate::planner::join_spec::PhysicalJoinSpec;
    use crate::planner::logical_plan::LogicalRelation;
    use crate::planner::physical_plan::{PhysicalExpr, PhysicalOperator, PhysicalPlan};

    #[test]
    fn lifts_single_stage_physical_plan() {
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
            schema_metadata: None,
        };

        let distributed = distributed_from_physical_plan(&plan);
        assert_eq!(distributed.stages.len(), 1);
        assert!(distributed.dependencies.is_empty());
        assert_eq!(distributed.stages[0].stage_id, 0);
        assert_eq!(distributed.stages[0].operators, plan.operators);
    }

    #[test]
    fn splits_hash_join_into_two_stages() {
        let plan = PhysicalPlan {
            operators: vec![
                PhysicalOperator::TableScan {
                    relation: LogicalRelation {
                        database: "sales".to_string(),
                        schema: "public".to_string(),
                        table: "users".to_string(),
                    },
                },
                PhysicalOperator::Filter {
                    predicate: PhysicalExpr::Raw {
                        sql: "active = true".to_string(),
                    },
                },
                PhysicalOperator::HashJoin {
                    spec: PhysicalJoinSpec {
                        join_type: crate::planner::JoinType::Inner,
                        right_relation: LogicalRelation {
                            database: "sales".to_string(),
                            schema: "public".to_string(),
                            table: "orders".to_string(),
                        },
                        keys: vec![crate::planner::JoinKeyPair {
                            left: "users.id".to_string(),
                            right: "orders.user_id".to_string(),
                        }],
                    },
                },
                PhysicalOperator::Projection {
                    expressions: vec![PhysicalExpr::Raw {
                        sql: "users.id".to_string(),
                    }],
                },
                PhysicalOperator::Materialize,
            ],
            sql: "SELECT users.id FROM sales.public.users INNER JOIN sales.public.orders ON users.id = orders.user_id".to_string(),
            schema_metadata: None,
        };

        let distributed = distributed_from_physical_plan(&plan);
        assert_eq!(distributed.stages.len(), 2);
        assert_eq!(distributed.dependencies.len(), 1);
        assert_eq!(distributed.dependencies[0].from_stage_id, 0);
        assert_eq!(distributed.dependencies[0].to_stage_id, 1);
    }

    #[test]
    fn splits_aggregate_pipeline_into_two_stages() {
        let plan = PhysicalPlan {
            operators: vec![
                PhysicalOperator::TableScan {
                    relation: LogicalRelation {
                        database: "sales".to_string(),
                        schema: "public".to_string(),
                        table: "users".to_string(),
                    },
                },
                PhysicalOperator::AggregatePartial {
                    spec: PhysicalAggregateSpec {
                        grouping_exprs: vec![PhysicalExpr::Raw {
                            sql: "country".to_string(),
                        }],
                        aggregates: vec![PhysicalAggregateExpr {
                            function: AggregateFunction::Count,
                            input: None,
                            output_name: "count".to_string(),
                        }],
                    },
                },
                PhysicalOperator::AggregateFinal {
                    spec: PhysicalAggregateSpec {
                        grouping_exprs: vec![PhysicalExpr::Raw {
                            sql: "country".to_string(),
                        }],
                        aggregates: vec![PhysicalAggregateExpr {
                            function: AggregateFunction::Count,
                            input: None,
                            output_name: "count".to_string(),
                        }],
                    },
                },
                PhysicalOperator::Projection {
                    expressions: vec![PhysicalExpr::Raw {
                        sql: "count".to_string(),
                    }],
                },
                PhysicalOperator::Materialize,
            ],
            sql: "SELECT country, COUNT(*) FROM sales.public.users GROUP BY country".to_string(),
            schema_metadata: None,
        };

        let distributed = distributed_from_physical_plan(&plan);
        assert_eq!(distributed.stages.len(), 2);
        assert_eq!(distributed.dependencies.len(), 1);
        assert_eq!(distributed.dependencies[0].from_stage_id, 0);
        assert_eq!(distributed.dependencies[0].to_stage_id, 1);
    }

    #[test]
    fn splits_multiple_boundaries_into_linear_stage_chain() {
        let plan = PhysicalPlan {
            operators: vec![
                PhysicalOperator::TableScan {
                    relation: LogicalRelation {
                        database: "sales".to_string(),
                        schema: "public".to_string(),
                        table: "users".to_string(),
                    },
                },
                PhysicalOperator::HashJoin {
                    spec: PhysicalJoinSpec {
                        join_type: crate::planner::JoinType::Inner,
                        right_relation: LogicalRelation {
                            database: "sales".to_string(),
                            schema: "public".to_string(),
                            table: "orders".to_string(),
                        },
                        keys: vec![crate::planner::JoinKeyPair {
                            left: "users.id".to_string(),
                            right: "orders.user_id".to_string(),
                        }],
                    },
                },
                PhysicalOperator::AggregatePartial {
                    spec: PhysicalAggregateSpec {
                        grouping_exprs: vec![PhysicalExpr::Raw {
                            sql: "country".to_string(),
                        }],
                        aggregates: vec![PhysicalAggregateExpr {
                            function: AggregateFunction::Count,
                            input: None,
                            output_name: "count".to_string(),
                        }],
                    },
                },
                PhysicalOperator::AggregateFinal {
                    spec: PhysicalAggregateSpec {
                        grouping_exprs: vec![PhysicalExpr::Raw {
                            sql: "country".to_string(),
                        }],
                        aggregates: vec![PhysicalAggregateExpr {
                            function: AggregateFunction::Count,
                            input: None,
                            output_name: "count".to_string(),
                        }],
                    },
                },
                PhysicalOperator::Projection {
                    expressions: vec![PhysicalExpr::Raw {
                        sql: "count".to_string(),
                    }],
                },
                PhysicalOperator::Materialize,
            ],
            sql: "SELECT country, COUNT(*) FROM sales.public.users INNER JOIN sales.public.orders ON users.id = orders.user_id GROUP BY country".to_string(),
            schema_metadata: None,
        };

        let distributed = distributed_from_physical_plan(&plan);
        assert_eq!(distributed.stages.len(), 3);
        assert_eq!(distributed.dependencies.len(), 2);
        assert_eq!(distributed.dependencies[0].from_stage_id, 0);
        assert_eq!(distributed.dependencies[0].to_stage_id, 1);
        assert_eq!(distributed.dependencies[1].from_stage_id, 1);
        assert_eq!(distributed.dependencies[1].to_stage_id, 2);
        assert_eq!(
            distributed.stages[0].partition_spec,
            PartitionSpec::Hash {
                keys: vec!["users.id".to_string()],
            }
        );
        assert_eq!(
            distributed.stages[1].partition_spec,
            PartitionSpec::Hash {
                keys: vec!["country".to_string()],
            }
        );
    }

    #[test]
    fn infers_hash_partition_from_exchange_shuffle_boundary() {
        let plan = PhysicalPlan {
            operators: vec![
                PhysicalOperator::TableScan {
                    relation: LogicalRelation {
                        database: "sales".to_string(),
                        schema: "public".to_string(),
                        table: "users".to_string(),
                    },
                },
                PhysicalOperator::Filter {
                    predicate: PhysicalExpr::Raw {
                        sql: "active = true".to_string(),
                    },
                },
                PhysicalOperator::ExchangeShuffle {
                    keys: vec!["country".to_string(), "city".to_string()],
                },
                PhysicalOperator::Projection {
                    expressions: vec![PhysicalExpr::Raw {
                        sql: "id".to_string(),
                    }],
                },
                PhysicalOperator::Materialize,
            ],
            sql: "SELECT id FROM sales.public.users WHERE active = true".to_string(),
            schema_metadata: None,
        };

        let distributed = distributed_from_physical_plan(&plan);
        assert_eq!(distributed.stages.len(), 2);
        assert_eq!(
            distributed.stages[0].partition_spec,
            PartitionSpec::Hash {
                keys: vec!["country".to_string(), "city".to_string()],
            }
        );
    }
}
