use super::{
    ExecutionModeHint, build_stage_execution_waves, compile_stage_task_groups,
    compute_distributed_dag_metrics, resolve_effective_routing_worker_pool_with_source,
    resolve_partition_count_for_stage, route_downstream_partitions_to_workers,
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
                output_partition_count: None,
            },
            DistributedStage {
                stage_id: 2,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
                output_partition_count: None,
            },
        ],
        dependencies: vec![StageDependency {
            from_stage_id: 1,
            to_stage_id: 2,
        }],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");
    let expected_stage1_partitions = resolve_partition_count_for_stage(&plan.stages[0]);
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
                output_partition_count: None,
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
                output_partition_count: None,
            },
            DistributedStage {
                stage_id: 3,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
                output_partition_count: None,
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
                output_partition_count: None,
            },
            DistributedStage {
                stage_id: 2,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
                output_partition_count: None,
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

#[test]
fn explicit_output_partition_count_overrides_hash_partition_default_fanout() {
    let plan = DistributedPhysicalPlan {
        stages: vec![
            DistributedStage {
                stage_id: 10,
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
                output_partition_count: Some(2),
            },
            DistributedStage {
                stage_id: 11,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
                output_partition_count: None,
            },
        ],
        dependencies: vec![StageDependency {
            from_stage_id: 10,
            to_stage_id: 11,
        }],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");
    let stage10_groups = groups
        .iter()
        .filter(|group| group.stage_id == 10)
        .collect::<Vec<_>>();
    let stage11_groups = groups
        .iter()
        .filter(|group| group.stage_id == 11)
        .collect::<Vec<_>>();

    assert_eq!(stage10_groups.len(), 2);
    assert_eq!(stage11_groups.len(), 1);
    assert_eq!(stage10_groups[0].partition_count, 2);
    assert_eq!(
        stage11_groups[0].upstream_partition_counts.get(&10),
        Some(&2)
    );
    assert_eq!(
        stage10_groups[0].execution_mode_hint,
        ExecutionModeHint::Distributed
    );
}

#[test]
fn zero_output_partition_count_falls_back_to_partition_spec() {
    let stage = DistributedStage {
        stage_id: 20,
        operators: vec![PhysicalOperator::Materialize],
        partition_spec: PartitionSpec::Single,
        output_partition_count: Some(0),
    };

    assert_eq!(resolve_partition_count_for_stage(&stage), 1);
}

#[test]
fn output_destinations_reflect_downstream_partition_counts_for_multiple_consumers() {
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
                output_partition_count: Some(3),
            },
            DistributedStage {
                stage_id: 2,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Hash {
                    keys: vec!["region".to_string()],
                },
                output_partition_count: Some(4),
            },
            DistributedStage {
                stage_id: 3,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
                output_partition_count: None,
            },
        ],
        dependencies: vec![
            StageDependency {
                from_stage_id: 1,
                to_stage_id: 2,
            },
            StageDependency {
                from_stage_id: 1,
                to_stage_id: 3,
            },
        ],
        sql: "SELECT id, region FROM sales.public.users".to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");
    let stage1_group = groups
        .iter()
        .find(|group| group.stage_id == 1 && group.partition_index == 0)
        .expect("stage 1 group should exist");

    assert_eq!(stage1_group.output_destinations.len(), 2);
    assert_eq!(stage1_group.output_destinations[0].downstream_stage_id, 2);
    assert_eq!(
        stage1_group.output_destinations[0].downstream_partition_count,
        4
    );
    assert_eq!(
        stage1_group.output_destinations[0].partitioning,
        PartitionSpec::Hash {
            keys: vec!["region".to_string()],
        }
    );
    assert_eq!(
        stage1_group.output_destinations[0].worker_addresses.len(),
        4
    );
    assert_eq!(
        stage1_group.output_destinations[0].worker_addresses,
        vec![
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
        ]
    );

    assert_eq!(stage1_group.output_destinations[1].downstream_stage_id, 3);
    assert_eq!(
        stage1_group.output_destinations[1].downstream_partition_count,
        1
    );
    assert_eq!(
        stage1_group.output_destinations[1].partitioning,
        PartitionSpec::Single
    );
    assert_eq!(
        stage1_group.output_destinations[1].worker_addresses.len(),
        1
    );
    assert_eq!(
        stage1_group.output_destinations[1].worker_addresses,
        vec!["localhost".to_string()]
    );
}

#[test]
fn branching_explicit_fanout_keeps_destination_and_upstream_partition_counts_consistent() {
    let plan = DistributedPhysicalPlan {
        stages: vec![
            DistributedStage {
                stage_id: 100,
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
                output_partition_count: Some(3),
            },
            DistributedStage {
                stage_id: 200,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Hash {
                    keys: vec!["region".to_string()],
                },
                output_partition_count: Some(4),
            },
            DistributedStage {
                stage_id: 300,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Hash {
                    keys: vec!["country".to_string()],
                },
                output_partition_count: Some(5),
            },
        ],
        dependencies: vec![
            StageDependency {
                from_stage_id: 100,
                to_stage_id: 200,
            },
            StageDependency {
                from_stage_id: 100,
                to_stage_id: 300,
            },
        ],
        sql: "SELECT id, region, country FROM sales.public.users".to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");

    let stage100_groups = groups
        .iter()
        .filter(|group| group.stage_id == 100)
        .collect::<Vec<_>>();
    let stage200_groups = groups
        .iter()
        .filter(|group| group.stage_id == 200)
        .collect::<Vec<_>>();
    let stage300_groups = groups
        .iter()
        .filter(|group| group.stage_id == 300)
        .collect::<Vec<_>>();

    assert_eq!(stage100_groups.len(), 3);
    assert_eq!(stage200_groups.len(), 4);
    assert_eq!(stage300_groups.len(), 5);

    let stage100_group = stage100_groups[0];
    assert_eq!(stage100_group.output_destinations.len(), 2);
    assert_eq!(
        stage100_group.output_destinations[0].downstream_stage_id,
        200
    );
    assert_eq!(
        stage100_group.output_destinations[0].downstream_partition_count,
        4
    );
    assert_eq!(
        stage100_group.output_destinations[0].worker_addresses.len(),
        4
    );
    assert_eq!(
        stage100_group.output_destinations[0].worker_addresses,
        vec![
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
        ]
    );
    assert_eq!(
        stage100_group.output_destinations[1].downstream_stage_id,
        300
    );
    assert_eq!(
        stage100_group.output_destinations[1].downstream_partition_count,
        5
    );
    assert_eq!(
        stage100_group.output_destinations[1].worker_addresses.len(),
        5
    );
    assert_eq!(
        stage100_group.output_destinations[1].worker_addresses,
        vec![
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
        ]
    );

    assert_eq!(stage200_groups[0].upstream_stage_ids, vec![100]);
    assert_eq!(stage300_groups[0].upstream_stage_ids, vec![100]);
    assert_eq!(
        stage200_groups[0].upstream_partition_counts.get(&100),
        Some(&3)
    );
    assert_eq!(
        stage300_groups[0].upstream_partition_counts.get(&100),
        Some(&3)
    );
}

#[test]
fn computes_distributed_dag_metrics_for_branching_plan() {
    let plan = DistributedPhysicalPlan {
        stages: vec![
            DistributedStage {
                stage_id: 100,
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
                output_partition_count: Some(3),
            },
            DistributedStage {
                stage_id: 200,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Hash {
                    keys: vec!["region".to_string()],
                },
                output_partition_count: Some(4),
            },
            DistributedStage {
                stage_id: 300,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Hash {
                    keys: vec!["country".to_string()],
                },
                output_partition_count: Some(5),
            },
        ],
        dependencies: vec![
            StageDependency {
                from_stage_id: 100,
                to_stage_id: 200,
            },
            StageDependency {
                from_stage_id: 100,
                to_stage_id: 300,
            },
        ],
        sql: "SELECT id, region, country FROM sales.public.users".to_string(),
    };

    let groups = compile_stage_task_groups(&plan, "query").expect("groups");
    let metrics = compute_distributed_dag_metrics(&plan, &groups).expect("metrics");

    assert_eq!(metrics.stage_count, 3);
    assert_eq!(metrics.dependency_count, 2);
    assert_eq!(metrics.wave_count, 2);
    assert_eq!(metrics.max_wave_width, 2);
    assert_eq!(metrics.total_task_groups, 12);
    assert_eq!(metrics.total_partitions, 12);
    assert_eq!(metrics.max_partitions_per_stage, 5);
}

#[test]
fn routes_downstream_partitions_round_robin_across_worker_pool() {
    let worker_pool = vec![
        "worker-a:32001".to_string(),
        "worker-b:32001".to_string(),
        "worker-c:32001".to_string(),
    ];

    let routed = route_downstream_partitions_to_workers(7, &worker_pool);
    assert_eq!(
        routed,
        vec![
            "worker-a:32001".to_string(),
            "worker-b:32001".to_string(),
            "worker-c:32001".to_string(),
            "worker-a:32001".to_string(),
            "worker-b:32001".to_string(),
            "worker-c:32001".to_string(),
            "worker-a:32001".to_string(),
        ]
    );
}

#[test]
fn routes_downstream_partitions_falls_back_to_localhost_for_empty_worker_pool() {
    let routed = route_downstream_partitions_to_workers(3, &[]);
    assert_eq!(
        routed,
        vec![
            "localhost".to_string(),
            "localhost".to_string(),
            "localhost".to_string(),
        ]
    );
}

#[test]
fn resolve_effective_worker_pool_with_source_marks_runtime_pool() {
    let runtime_pool = vec![
        "worker-a:32001".to_string(),
        "worker-b:32001".to_string(),
        "worker-a:32001".to_string(),
        "  ".to_string(),
    ];

    let (resolved, source) = resolve_effective_routing_worker_pool_with_source(&runtime_pool);

    assert_eq!(source, super::ROUTING_POOL_SOURCE_RUNTIME);
    assert_eq!(
        resolved,
        vec!["worker-a:32001".to_string(), "worker-b:32001".to_string()]
    );
}

#[test]
fn resolve_effective_worker_pool_with_source_fallback_is_env_or_default() {
    let (resolved, source) = resolve_effective_routing_worker_pool_with_source(&[]);

    assert!(!resolved.is_empty());
    assert!(
        source == super::ROUTING_POOL_SOURCE_ENV || source == super::ROUTING_POOL_SOURCE_DEFAULT
    );

    if source == super::ROUTING_POOL_SOURCE_DEFAULT {
        assert_eq!(resolved, vec!["localhost".to_string()]);
    }
}
