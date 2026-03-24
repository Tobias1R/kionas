use crate::parser::sql::parse_query;
use crate::planner::{DataFusionExtractedStage, DataFusionStageExtractionDiagnostics};
use kionas::planner::{
    DistributedPhysicalPlan, DistributedStage, LogicalRelation, PartitionSpec, PhysicalOperator,
};
use kionas::sql::query_model::{
    QueryModelError, VALIDATION_CODE_UNSUPPORTED_OPERATOR, VALIDATION_CODE_UNSUPPORTED_PIPELINE,
    VALIDATION_CODE_UNSUPPORTED_PREDICATE, build_select_query_dispatch_envelope,
    validation_code_for_query_error,
};
use std::collections::HashMap;

#[tokio::test]
async fn minimal_select_payload_builds() {
    let statements = parse_query("SELECT id, name FROM sales.public.users WHERE active = true")
        .expect("statement should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let canonical = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
        .await
        .expect("payload should build");
    let payload = canonical.payload;
    assert!(payload.contains("\"statement\":\"Select\""));
    assert!(payload.contains("\"database\":\"sales\""));
    assert!(payload.contains("\"table\":\"users\""));
}

#[tokio::test]
async fn rejects_multi_table_shape() {
    let statements = parse_query("SELECT * FROM a, b").expect("statement should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let err = build_select_query_dispatch_envelope(query, "s1", "default", "public")
        .await
        .expect_err("should reject multi-table select");
    assert!(err.to_string().contains("exactly one table"));
}

#[tokio::test]
async fn extracts_namespace_from_payload() {
    let statements =
        parse_query("SELECT id FROM sales.public.users").expect("statement should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let canonical = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
        .await
        .expect("payload should build");
    let database = canonical.database;
    let schema = canonical.schema;
    let table = canonical.table;

    assert_eq!(database, "sales");
    assert_eq!(schema, "public");
    assert_eq!(table, "users");
}

#[test]
fn maps_capability_error_codes() {
    assert_eq!(
        validation_code_for_query_error(&QueryModelError::InvalidPhysicalPipeline(
            "pipeline must end with materialize".to_string(),
        )),
        VALIDATION_CODE_UNSUPPORTED_PIPELINE
    );
    assert_eq!(
        validation_code_for_query_error(&QueryModelError::UnsupportedPhysicalOperator(
            "HashJoin".to_string(),
        )),
        VALIDATION_CODE_UNSUPPORTED_OPERATOR
    );
    assert_eq!(
        validation_code_for_query_error(
            &QueryModelError::UnsupportedPredicate("LIKE".to_string(),)
        ),
        VALIDATION_CODE_UNSUPPORTED_PREDICATE
    );
}

#[test]
fn parse_foundation_predicate_ast_accepts_case_insensitive_and() {
    let parsed = super::parse_foundation_predicate_ast("id >= 10 aNd score < 99");
    assert!(parsed.is_some());
}

#[test]
fn parse_foundation_predicate_ast_rejects_case_insensitive_or() {
    let parsed = super::parse_foundation_predicate_ast("id >= 10 oR score < 99");
    assert!(parsed.is_none());
}

fn stage(stage_id: u32) -> DistributedStage {
    DistributedStage {
        stage_id,
        operators: vec![PhysicalOperator::TableScan {
            relation: LogicalRelation {
                database: "sales".to_string(),
                schema: "public".to_string(),
                table: "users".to_string(),
            },
        }],
        partition_spec: PartitionSpec::Single,
        output_partition_count: None,
    }
}

#[test]
fn apply_stage_extraction_topology_replaces_linear_dependencies_with_branching() {
    let plan = DistributedPhysicalPlan {
        stages: vec![stage(0), stage(1), stage(2)],
        dependencies: vec![
            kionas::planner::StageDependency {
                from_stage_id: 0,
                to_stage_id: 1,
            },
            kionas::planner::StageDependency {
                from_stage_id: 1,
                to_stage_id: 2,
            },
        ],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let extraction = DataFusionStageExtractionDiagnostics {
        stage_count: 3,
        stages: vec![
            DataFusionExtractedStage {
                stage_id: 0,
                input_stage_ids: vec![1, 2],
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["ProjectionExec".to_string()],
            },
            DataFusionExtractedStage {
                stage_id: 1,
                input_stage_ids: Vec::new(),
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["RepartitionExec".to_string()],
            },
            DataFusionExtractedStage {
                stage_id: 2,
                input_stage_ids: Vec::new(),
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["RepartitionExec".to_string()],
            },
        ],
    };

    let rewritten = super::apply_stage_extraction_topology(plan, &extraction);
    assert_eq!(rewritten.dependencies.len(), 2);
    assert_eq!(rewritten.dependencies[0].from_stage_id, 1);
    assert_eq!(rewritten.dependencies[0].to_stage_id, 0);
    assert_eq!(rewritten.dependencies[1].from_stage_id, 2);
    assert_eq!(rewritten.dependencies[1].to_stage_id, 0);
}

#[test]
fn apply_stage_extraction_topology_keeps_original_dependencies_on_count_mismatch() {
    let plan = DistributedPhysicalPlan {
        stages: vec![stage(0), stage(1)],
        dependencies: vec![kionas::planner::StageDependency {
            from_stage_id: 0,
            to_stage_id: 1,
        }],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let extraction = DataFusionStageExtractionDiagnostics {
        stage_count: 1,
        stages: vec![DataFusionExtractedStage {
            stage_id: 0,
            input_stage_ids: Vec::new(),
            partitions_out: 1,
            output_partitioning: "single".to_string(),
            output_partitioning_keys: Vec::new(),
            node_names: vec!["ProjectionExec".to_string()],
        }],
    };

    let rewritten = super::apply_stage_extraction_topology(plan.clone(), &extraction);
    assert_eq!(rewritten.dependencies, plan.dependencies);
}

#[test]
fn apply_stage_extraction_topology_keeps_original_dependencies_on_stage_id_mismatch() {
    let plan = DistributedPhysicalPlan {
        stages: vec![stage(0), stage(1)],
        dependencies: vec![kionas::planner::StageDependency {
            from_stage_id: 0,
            to_stage_id: 1,
        }],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let extraction = DataFusionStageExtractionDiagnostics {
        stage_count: 2,
        stages: vec![
            DataFusionExtractedStage {
                stage_id: 0,
                input_stage_ids: vec![],
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["ProjectionExec".to_string()],
            },
            DataFusionExtractedStage {
                stage_id: 999,
                input_stage_ids: vec![0],
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["RepartitionExec".to_string()],
            },
        ],
    };

    let rewritten = super::apply_stage_extraction_topology(plan.clone(), &extraction);
    assert_eq!(rewritten.dependencies, plan.dependencies);
}

#[test]
fn apply_stage_extraction_topology_keeps_original_dependencies_on_self_dependency() {
    let plan = DistributedPhysicalPlan {
        stages: vec![stage(0), stage(1)],
        dependencies: vec![kionas::planner::StageDependency {
            from_stage_id: 0,
            to_stage_id: 1,
        }],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let extraction = DataFusionStageExtractionDiagnostics {
        stage_count: 2,
        stages: vec![
            DataFusionExtractedStage {
                stage_id: 0,
                input_stage_ids: vec![0],
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["ProjectionExec".to_string()],
            },
            DataFusionExtractedStage {
                stage_id: 1,
                input_stage_ids: vec![0],
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["RepartitionExec".to_string()],
            },
        ],
    };

    let rewritten = super::apply_stage_extraction_topology(plan.clone(), &extraction);
    assert_eq!(rewritten.dependencies, plan.dependencies);
}

#[test]
fn apply_stage_extraction_topology_projects_partitioning_hints() {
    let plan = DistributedPhysicalPlan {
        stages: vec![stage(0), stage(1), stage(2)],
        dependencies: vec![
            kionas::planner::StageDependency {
                from_stage_id: 0,
                to_stage_id: 1,
            },
            kionas::planner::StageDependency {
                from_stage_id: 1,
                to_stage_id: 2,
            },
        ],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let extraction = DataFusionStageExtractionDiagnostics {
        stage_count: 3,
        stages: vec![
            DataFusionExtractedStage {
                stage_id: 0,
                input_stage_ids: vec![1, 2],
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["ProjectionExec".to_string()],
            },
            DataFusionExtractedStage {
                stage_id: 1,
                input_stage_ids: Vec::new(),
                partitions_out: 8,
                output_partitioning: "round_robin_batch".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["RepartitionExec".to_string()],
            },
            DataFusionExtractedStage {
                stage_id: 2,
                input_stage_ids: Vec::new(),
                partitions_out: 8,
                output_partitioning: "hash".to_string(),
                output_partitioning_keys: vec!["country".to_string(), "city".to_string()],
                node_names: vec!["RepartitionExec".to_string()],
            },
        ],
    };

    let rewritten = super::apply_stage_extraction_topology(plan, &extraction);
    assert_eq!(rewritten.stages[0].partition_spec, PartitionSpec::Single);
    assert_eq!(rewritten.stages[0].output_partition_count, Some(1));
    assert_eq!(rewritten.stages[1].partition_spec, PartitionSpec::Single);
    assert_eq!(rewritten.stages[1].output_partition_count, Some(8));
    assert_eq!(
        rewritten.stages[2].partition_spec,
        PartitionSpec::Hash {
            keys: vec!["country".to_string(), "city".to_string()],
        }
    );
    assert_eq!(rewritten.stages[2].output_partition_count, Some(8));
}

#[test]
fn apply_stage_extraction_topology_keeps_partition_spec_for_unknown_partitioning() {
    let plan = DistributedPhysicalPlan {
        stages: vec![
            DistributedStage {
                stage_id: 0,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Single,
                output_partition_count: None,
            },
            DistributedStage {
                stage_id: 1,
                operators: vec![PhysicalOperator::Materialize],
                partition_spec: PartitionSpec::Hash {
                    keys: vec!["id".to_string()],
                },
                output_partition_count: None,
            },
        ],
        dependencies: vec![kionas::planner::StageDependency {
            from_stage_id: 1,
            to_stage_id: 0,
        }],
        sql: "SELECT id FROM sales.public.users".to_string(),
    };

    let extraction = DataFusionStageExtractionDiagnostics {
        stage_count: 2,
        stages: vec![
            DataFusionExtractedStage {
                stage_id: 0,
                input_stage_ids: vec![1],
                partitions_out: 1,
                output_partitioning: "single".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["ProjectionExec".to_string()],
            },
            DataFusionExtractedStage {
                stage_id: 1,
                input_stage_ids: Vec::new(),
                partitions_out: 4,
                output_partitioning: "unknown".to_string(),
                output_partitioning_keys: Vec::new(),
                node_names: vec!["RepartitionExec".to_string()],
            },
        ],
    };

    let rewritten = super::apply_stage_extraction_topology(plan.clone(), &extraction);
    assert_eq!(
        rewritten.stages[1].partition_spec,
        plan.stages[1].partition_spec
    );
    assert_eq!(rewritten.stages[1].output_partition_count, Some(4));
}

#[test]
fn attach_distributed_observability_params_sets_expected_keys() {
    let mut params = HashMap::<String, String>::new();

    super::attach_distributed_observability_params(&mut params, "{\"stage_count\":3}", true, 3, 2);

    assert_eq!(
        params.get("distributed_dag_metrics_json"),
        Some(&"{\"stage_count\":3}".to_string())
    );
    assert_eq!(
        params.get("distributed_plan_validation_status"),
        Some(&"passed".to_string())
    );
    assert_eq!(
        params.get("stage_extraction_mismatch"),
        Some(&"true".to_string())
    );
    assert_eq!(params.get("datafusion_stage_count"), Some(&"3".to_string()));
    assert_eq!(
        params.get("distributed_stage_count"),
        Some(&"2".to_string())
    );
}

#[test]
fn attach_observability_to_stage_groups_sets_expected_keys_on_all_groups() {
    let mut stage_groups = vec![
        crate::statement_handler::shared::distributed_dag::StageTaskGroup {
            stage_id: 1,
            partition_index: 0,
            partition_count: 1,
            upstream_stage_ids: Vec::new(),
            upstream_partition_counts: HashMap::new(),
            partition_spec: PartitionSpec::Single,
            execution_mode_hint:
                crate::statement_handler::shared::distributed_dag::ExecutionModeHint::LocalOnly,
            output_destinations: Vec::new(),
            operation: "query".to_string(),
            payload: "{}".to_string(),
            params: HashMap::new(),
        },
        crate::statement_handler::shared::distributed_dag::StageTaskGroup {
            stage_id: 2,
            partition_index: 0,
            partition_count: 1,
            upstream_stage_ids: vec![1],
            upstream_partition_counts: HashMap::new(),
            partition_spec: PartitionSpec::Single,
            execution_mode_hint:
                crate::statement_handler::shared::distributed_dag::ExecutionModeHint::Distributed,
            output_destinations: Vec::new(),
            operation: "query".to_string(),
            payload: "{}".to_string(),
            params: HashMap::new(),
        },
    ];

    super::attach_observability_to_stage_groups(
        &mut stage_groups,
        "{\"stage_count\":2}",
        false,
        2,
        2,
    );

    for group in stage_groups {
        assert_eq!(
            group.params.get("distributed_dag_metrics_json"),
            Some(&"{\"stage_count\":2}".to_string())
        );
        assert_eq!(
            group.params.get("distributed_plan_validation_status"),
            Some(&"passed".to_string())
        );
        assert_eq!(
            group.params.get("stage_extraction_mismatch"),
            Some(&"false".to_string())
        );
        assert_eq!(
            group.params.get("datafusion_stage_count"),
            Some(&"2".to_string())
        );
        assert_eq!(
            group.params.get("distributed_stage_count"),
            Some(&"2".to_string())
        );
    }
}
