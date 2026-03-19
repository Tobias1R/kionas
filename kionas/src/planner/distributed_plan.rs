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
        };

        let distributed = distributed_from_physical_plan(&plan);
        assert_eq!(distributed.stages.len(), 1);
        assert!(distributed.dependencies.is_empty());
        assert_eq!(distributed.stages[0].stage_id, 0);
        assert_eq!(distributed.stages[0].operators, plan.operators);
    }
}
