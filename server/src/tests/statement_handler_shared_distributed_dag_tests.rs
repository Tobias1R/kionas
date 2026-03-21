use super::{compile_stage_task_groups, resolve_partition_count_for_stage};
use kionas::planner::{
    DistributedPhysicalPlan, DistributedStage, LogicalRelation, PartitionSpec, PhysicalOperator,
    StageDependency,
};

#[test]
fn compiles_groups_with_dependency_params() {
    let plan = DistributedPhysicalPlan {
        stages: vec![
            DistributedStage {
                stage_id: 1,
                operators: vec![PhysicalOperator::TableScan {
                    relation: LogicalRelation {
                        database: "sales".to_string(),
                        schema: "public".to_string(),
                        table: "users".to_string(),
                    },
                }],
                partition_spec: PartitionSpec::Hash {
                    keys: vec!["id".to_string()],
                },
            },
            DistributedStage {
                stage_id: 2,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
            },
        ],
        dependencies: vec![StageDependency {
            from_stage_id: 1,
            to_stage_id: 2,
        }],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");
    let expected_stage1_partitions = resolve_partition_count_for_stage(&PartitionSpec::Hash {
        keys: vec!["id".to_string()],
    });
    assert_eq!(groups.len(), expected_stage1_partitions as usize + 1);
    assert_eq!(groups[0].stage_id, 1);
    assert_eq!(groups[0].partition_index, 0);
    assert_eq!(groups[0].partition_count, expected_stage1_partitions);
    assert!(groups[0].upstream_stage_ids.is_empty());
    assert_eq!(
        groups[(expected_stage1_partitions - 1) as usize].partition_index,
        expected_stage1_partitions - 1
    );
    assert_eq!(
        groups[(expected_stage1_partitions - 1) as usize].stage_id,
        1
    );
    assert_eq!(groups[expected_stage1_partitions as usize].stage_id, 2);
    assert_eq!(
        groups[expected_stage1_partitions as usize].partition_count,
        1
    );
    assert_eq!(
        groups[expected_stage1_partitions as usize].upstream_stage_ids,
        vec![1]
    );
    assert_eq!(
        groups[expected_stage1_partitions as usize].operation,
        "query"
    );
    assert!(
        groups[expected_stage1_partitions as usize]
            .params
            .contains_key("partition_spec")
    );
    assert_eq!(
        groups[expected_stage1_partitions as usize]
            .params
            .get("partition_index"),
        Some(&"0".to_string())
    );
    assert_eq!(
        groups[expected_stage1_partitions as usize]
            .params
            .get("upstream_partition_counts")
            .expect("upstream_partition_counts param must exist"),
        &format!("{{\"1\":{}}}", expected_stage1_partitions)
    );
}
