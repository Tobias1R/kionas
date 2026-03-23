use super::{
    ExecutionModeHint, build_stage_execution_waves, compile_stage_task_groups,
    resolve_partition_count_for_stage,
};
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
    assert_eq!(
        groups[expected_stage1_partitions as usize]
            .upstream_partition_counts
            .get(&1)
            .copied(),
        Some(expected_stage1_partitions)
    );
    assert_eq!(
        groups[expected_stage1_partitions as usize].partition_spec,
        PartitionSpec::Single
    );
    assert_eq!(
        groups[0].execution_mode_hint,
        ExecutionModeHint::Distributed
    );

    let stage1_destinations = &groups[0].output_destinations;
    assert_eq!(stage1_destinations.len(), 1);
    assert_eq!(stage1_destinations[0].downstream_stage_id, 2);
    assert_eq!(
        stage1_destinations[0].downstream_partition_count,
        groups[expected_stage1_partitions as usize].partition_count
    );
    assert_eq!(stage1_destinations[0].partitioning, PartitionSpec::Single);
    assert_eq!(stage1_destinations[0].worker_addresses, vec!["localhost"]);

    let stage2_destinations = &groups[expected_stage1_partitions as usize].output_destinations;
    assert!(stage2_destinations.is_empty());
    assert!(groups[0].params.is_empty());
}

#[test]
fn builds_execution_waves_for_branching_stages() {
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
                partition_spec: PartitionSpec::Single,
            },
            DistributedStage {
                stage_id: 2,
                operators: vec![PhysicalOperator::TableScan {
                    relation: LogicalRelation {
                        database: "sales".to_string(),
                        schema: "public".to_string(),
                        table: "orders".to_string(),
                    },
                }],
                partition_spec: PartitionSpec::Single,
            },
            DistributedStage {
                stage_id: 3,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
            },
        ],
        dependencies: vec![
            StageDependency {
                from_stage_id: 1,
                to_stage_id: 3,
            },
            StageDependency {
                from_stage_id: 2,
                to_stage_id: 3,
            },
        ],
        sql: "SELECT * FROM sales.public.users u JOIN sales.public.orders o ON u.id = o.user_id"
            .to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");
    let waves = build_stage_execution_waves(&groups).expect("waves");
    assert_eq!(waves, vec![vec![1, 2], vec![3]]);
}

#[test]
fn rejects_execution_waves_when_dependencies_are_cyclic() {
    let plan = DistributedPhysicalPlan {
        stages: vec![
            DistributedStage {
                stage_id: 1,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
            },
            DistributedStage {
                stage_id: 2,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
            },
        ],
        dependencies: vec![
            StageDependency {
                from_stage_id: 1,
                to_stage_id: 2,
            },
            StageDependency {
                from_stage_id: 2,
                to_stage_id: 1,
            },
        ],
        sql: "SELECT 1".to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");
    let err = build_stage_execution_waves(&groups).expect_err("must fail on cycles");
    assert!(err.contains("no schedulable stage found"));
}
