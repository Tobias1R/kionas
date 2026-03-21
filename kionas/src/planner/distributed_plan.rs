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
    if let Some(final_index) = plan
        .operators
        .iter()
        .position(|op| matches!(op, PhysicalOperator::AggregateFinal { .. }))
    {
        let mut stage0_ops = plan.operators[..final_index].to_vec();
        if !matches!(stage0_ops.last(), Some(PhysicalOperator::Materialize)) {
            stage0_ops.push(PhysicalOperator::Materialize);
        }

        let mut stage1_ops = plan.operators[final_index..].to_vec();
        if !matches!(stage1_ops.last(), Some(PhysicalOperator::Materialize)) {
            stage1_ops.push(PhysicalOperator::Materialize);
        }

        let stage0_partition = stage0_ops
            .iter()
            .find_map(|op| match op {
                PhysicalOperator::AggregatePartial { spec } => Some(PartitionSpec::Hash {
                    keys: spec
                        .grouping_exprs
                        .iter()
                        .map(|expr| match expr {
                            crate::planner::physical_plan::PhysicalExpr::ColumnRef { name } => {
                                name.clone()
                            }
                            crate::planner::physical_plan::PhysicalExpr::Raw { sql } => sql.clone(),
                        })
                        .collect::<Vec<_>>(),
                }),
                _ => None,
            })
            .unwrap_or(PartitionSpec::Single);

        return DistributedPhysicalPlan {
            stages: vec![
                DistributedStage {
                    stage_id: 0,
                    operators: stage0_ops,
                    partition_spec: stage0_partition,
                },
                DistributedStage {
                    stage_id: 1,
                    operators: stage1_ops,
                    partition_spec: PartitionSpec::Single,
                },
            ],
            dependencies: vec![StageDependency {
                from_stage_id: 0,
                to_stage_id: 1,
            }],
            sql: plan.sql.clone(),
        };
    }

    if let Some(join_index) = plan
        .operators
        .iter()
        .position(|op| matches!(op, PhysicalOperator::HashJoin { .. }))
    {
        let mut stage0_ops = plan.operators[..join_index].to_vec();
        if !matches!(stage0_ops.last(), Some(PhysicalOperator::Materialize)) {
            stage0_ops.push(PhysicalOperator::Materialize);
        }

        let mut stage1_ops = plan.operators[join_index..].to_vec();
        if !matches!(stage1_ops.last(), Some(PhysicalOperator::Materialize)) {
            stage1_ops.push(PhysicalOperator::Materialize);
        }

        let stage0_partition = match &plan.operators[join_index] {
            PhysicalOperator::HashJoin { spec } if !spec.keys.is_empty() => PartitionSpec::Hash {
                keys: spec.keys.iter().map(|k| k.left.clone()).collect::<Vec<_>>(),
            },
            _ => PartitionSpec::Single,
        };

        return DistributedPhysicalPlan {
            stages: vec![
                DistributedStage {
                    stage_id: 0,
                    operators: stage0_ops,
                    partition_spec: stage0_partition,
                },
                DistributedStage {
                    stage_id: 1,
                    operators: stage1_ops,
                    partition_spec: PartitionSpec::Single,
                },
            ],
            dependencies: vec![StageDependency {
                from_stage_id: 0,
                to_stage_id: 1,
            }],
            sql: plan.sql.clone(),
        };
    }

    DistributedPhysicalPlan {
        stages: vec![DistributedStage {
            stage_id: 0,
            operators: plan.operators.clone(),
            partition_spec: PartitionSpec::Single,
        }],
        dependencies: Vec::new(),
        sql: plan.sql.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::distributed_from_physical_plan;
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
}
