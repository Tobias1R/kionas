use crate::planner::distributed_plan::DistributedPhysicalPlan;
use crate::planner::error::PlannerError;
use std::collections::{HashMap, HashSet, VecDeque};

/// What: Validate distributed physical plan invariants for stage orchestration.
///
/// Inputs:
/// - `plan`: Distributed physical plan produced by translation layer.
///
/// Output:
/// - `Ok(())` when the distributed plan is structurally valid.
/// - `Err(PlannerError::InvalidDistributedPlan)` when stage graph is malformed.
///
/// Details:
/// - Validates stage uniqueness, non-empty stage operators, dependency references, and cycle absence.
pub fn validate_distributed_physical_plan(
    plan: &DistributedPhysicalPlan,
) -> Result<(), PlannerError> {
    if plan.stages.is_empty() {
        return Err(PlannerError::InvalidDistributedPlan(
            "distributed plan must include at least one stage".to_string(),
        ));
    }

    let mut ids = HashSet::new();
    for stage in &plan.stages {
        if !ids.insert(stage.stage_id) {
            return Err(PlannerError::InvalidDistributedPlan(format!(
                "duplicate stage_id {}",
                stage.stage_id
            )));
        }
        if stage.operators.is_empty() {
            return Err(PlannerError::InvalidDistributedPlan(format!(
                "stage {} has no operators",
                stage.stage_id
            )));
        }
    }

    let mut indegree = HashMap::<u32, usize>::new();
    let mut adjacency = HashMap::<u32, Vec<u32>>::new();
    for id in &ids {
        indegree.insert(*id, 0);
        adjacency.insert(*id, Vec::new());
    }

    for dep in &plan.dependencies {
        if !ids.contains(&dep.from_stage_id) {
            return Err(PlannerError::InvalidDistributedPlan(format!(
                "dependency references unknown upstream stage {}",
                dep.from_stage_id
            )));
        }
        if !ids.contains(&dep.to_stage_id) {
            return Err(PlannerError::InvalidDistributedPlan(format!(
                "dependency references unknown downstream stage {}",
                dep.to_stage_id
            )));
        }
        if dep.from_stage_id == dep.to_stage_id {
            return Err(PlannerError::InvalidDistributedPlan(format!(
                "self dependency detected on stage {}",
                dep.from_stage_id
            )));
        }

        adjacency
            .get_mut(&dep.from_stage_id)
            .expect("validated stage id must exist")
            .push(dep.to_stage_id);
        *indegree
            .get_mut(&dep.to_stage_id)
            .expect("validated stage id must exist") += 1;
    }

    let mut queue = indegree
        .iter()
        .filter_map(|(id, degree)| if *degree == 0 { Some(*id) } else { None })
        .collect::<VecDeque<_>>();

    let mut visited = 0usize;
    while let Some(id) = queue.pop_front() {
        visited += 1;
        if let Some(next) = adjacency.get(&id) {
            for neighbor in next {
                if let Some(entry) = indegree.get_mut(neighbor) {
                    *entry -= 1;
                    if *entry == 0 {
                        queue.push_back(*neighbor);
                    }
                }
            }
        }
    }

    if visited != plan.stages.len() {
        return Err(PlannerError::InvalidDistributedPlan(
            "cycle detected in stage dependencies".to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate_distributed_physical_plan;
    use crate::planner::distributed_plan::{
        DistributedPhysicalPlan, DistributedStage, PartitionSpec, StageDependency,
    };
    use crate::planner::logical_plan::LogicalRelation;
    use crate::planner::physical_plan::PhysicalOperator;

    fn stage(id: u32) -> DistributedStage {
        DistributedStage {
            stage_id: id,
            operators: vec![PhysicalOperator::TableScan {
                relation: LogicalRelation {
                    database: "db".to_string(),
                    schema: "public".to_string(),
                    table: "t".to_string(),
                },
            }],
            partition_spec: PartitionSpec::Single,
        }
    }

    #[test]
    fn accepts_acyclic_dependencies() {
        let plan = DistributedPhysicalPlan {
            stages: vec![stage(0), stage(1), stage(2)],
            dependencies: vec![
                StageDependency {
                    from_stage_id: 0,
                    to_stage_id: 1,
                },
                StageDependency {
                    from_stage_id: 1,
                    to_stage_id: 2,
                },
            ],
            sql: "SELECT * FROM db.public.t".to_string(),
        };

        validate_distributed_physical_plan(&plan).expect("plan should be valid");
    }

    #[test]
    fn rejects_dependency_cycles() {
        let plan = DistributedPhysicalPlan {
            stages: vec![stage(0), stage(1)],
            dependencies: vec![
                StageDependency {
                    from_stage_id: 0,
                    to_stage_id: 1,
                },
                StageDependency {
                    from_stage_id: 1,
                    to_stage_id: 0,
                },
            ],
            sql: "SELECT * FROM db.public.t".to_string(),
        };

        let err = validate_distributed_physical_plan(&plan).expect_err("cycle must fail");
        assert_eq!(
            err.to_string(),
            "invalid distributed plan: cycle detected in stage dependencies"
        );
    }
}
