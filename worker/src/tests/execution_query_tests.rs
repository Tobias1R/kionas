use super::{
    is_stage_query_task, resolve_query_namespace, resolve_query_namespace_from_params,
    resolve_query_result_endpoint_with_overrides,
};

#[test]
fn accepts_unit_variant_string_operator_shape() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let namespace = resolve_query_namespace(&task).expect("must accept unit variant shape");
    assert_eq!(namespace.database, "db1");
}

#[test]
fn endpoint_resolution_uses_worker_defaults_when_proxy_absent() {
    let (host, port) = resolve_query_result_endpoint_with_overrides(None, None, "worker1", 32000);
    assert_eq!(host, "worker1");
    assert_eq!(port, 32001);
}

#[test]
fn endpoint_resolution_prefers_proxy_overrides_when_present() {
    let (host, port) = resolve_query_result_endpoint_with_overrides(
        Some("flight-proxy"),
        Some(33000),
        "worker1",
        32000,
    );
    assert_eq!(host, "flight-proxy");
    assert_eq!(port, 33000);
}

#[test]
fn rejects_non_select_statement() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Insert",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task).expect_err("must reject non-select statement");
    assert!(err.contains("only Select"));
}

#[test]
fn resolves_namespace_from_stage_params() {
    let mut params = std::collections::HashMap::new();
    params.insert("database_name".to_string(), "Sales".to_string());
    params.insert("schema_name".to_string(), "Public".to_string());
    params.insert("table_name".to_string(), "Users".to_string());

    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 2,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: "[]".to_string(),
        output: String::new(),
        params,
        filter_predicate: None,
    };

    assert!(is_stage_query_task(&task));
    let namespace = resolve_query_namespace_from_params(&task)
        .expect("stage task must resolve namespace from params");
    assert_eq!(namespace.database, "sales");
    assert_eq!(namespace.schema, "public");
    assert_eq!(namespace.table, "users");
}

#[test]
fn accepts_valid_select_namespace() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
            task_id: "t1".to_string(),
            operation: "query".to_string(),
            input: serde_json::json!({
                "version": 2,
                "statement": "Select",
                "namespace": {
                    "database": "DB1",
                    "schema": "Public",
                    "table": "Users"
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "public", "table": "users"}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let namespace = resolve_query_namespace(&task).expect("must parse namespace");
    assert_eq!(namespace.database, "db1");
    assert_eq!(namespace.schema, "public");
    assert_eq!(namespace.table, "users");
}

#[test]
fn rejects_missing_logical_plan_for_v2() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task).expect_err("must reject missing logical_plan object");
    assert!(err.contains("missing logical_plan"));
}

#[test]
fn rejects_missing_physical_plan_for_v2() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {}
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task).expect_err("must reject missing physical_plan object");
    assert!(err.contains("missing physical_plan"));
}

#[test]
fn rejects_empty_physical_plan_operators_for_v2() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": []
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task)
        .expect_err("must reject empty physical_plan operators for phase 2 payloads");
    assert!(err.contains("empty physical_plan.operators"));
}

#[test]
fn rejects_physical_plan_without_tablescan_first() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task)
        .expect_err("must reject non-conforming phase 2 pipeline shape");
    assert!(err.contains("start with TableScan"));
}

#[test]
fn rejects_physical_plan_without_materialize_last() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}}
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err =
        resolve_query_namespace(&task).expect_err("must reject pipeline missing final materialize");
    assert!(err.contains("end with Materialize"));
}

#[test]
fn rejects_physical_plan_without_single_projection() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task).expect_err("must reject pipeline without projection");
    assert!(err.contains("exactly one Projection"));
}

#[test]
fn accepts_hash_join_operator_variant_in_worker_payload() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
            task_id: "t1".to_string(),
            operation: "query".to_string(),
            input: serde_json::json!({
                "version": 2,
                "statement": "Select",
                "namespace": {
                    "database": "db1",
                    "schema": "s1",
                    "table": "t1"
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        {"HashJoin": {"spec": {"join_type": "Inner", "right_relation": {"database": "db1", "schema": "s1", "table": "t2"}, "keys": [{"left": "t1.id", "right": "t2.id"}]}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let namespace = resolve_query_namespace(&task)
        .expect("must accept constrained hash join operator in worker payload");
    assert_eq!(namespace.table, "t1");
}

#[test]
fn rejects_deferred_predicate_variant_in_worker_payload() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    {"Filter": {"predicate": {"Raw": {"sql": "name LIKE 'a%'"}}}},
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task)
        .expect_err("must reject deferred predicate in worker payload");
    assert!(err.contains("predicate is not supported"));
    assert!(err.contains("LIKE"));
}

#[test]
fn accepts_limit_after_sort_pipeline() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                    {"Sort": {"keys": [{"expression": {"Raw": {"sql": "id"}}, "ascending": true}]}},
                    {"Limit": {"spec": {"count": 5, "offset": 2}}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let namespace = resolve_query_namespace(&task).expect("must accept limit pipeline");
    assert_eq!(namespace.table, "t1");
}

#[test]
fn rejects_limit_before_sort_pipeline() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "version": 2,
            "statement": "Select",
            "namespace": {
                "database": "db1",
                "schema": "s1",
                "table": "t1"
            },
            "logical_plan": {},
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                    {"Limit": {"spec": {"count": 5, "offset": 0}}},
                    {"Sort": {"keys": [{"expression": {"Raw": {"sql": "id"}}, "ascending": true}]}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    let err = resolve_query_namespace(&task).expect_err("must reject invalid limit placement");
    assert!(err.contains("Limit must appear after Sort"));
}

#[test]
fn detects_staged_query_task_with_zero_based_stage_id_and_partition_fanout() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 2,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: "[]".to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    assert!(is_stage_query_task(&task));
}

#[test]
fn detects_staged_query_task_with_zero_based_stage_id_and_upstream_dependencies() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 1,
        upstream_stage_ids: vec![0],
        upstream_partition_counts: std::collections::HashMap::from([(0_u32, 1_u32)]),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: "[]".to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    assert!(is_stage_query_task(&task));
}

#[test]
fn treats_zero_based_task_as_legacy_when_no_stage_signals_are_present() {
    let task = crate::services::worker_service_server::worker_service::StagePartitionExecution {
        execution_mode_hint: 0,
        execution_plan: Vec::new(),
        output_destinations: Vec::new(),
        partition_count: 0,
        upstream_stage_ids: Vec::new(),
        upstream_partition_counts: std::collections::HashMap::new(),
        partition_spec: String::new(),
        query_run_id: String::new(),
        query_id: String::new(),
        stage_id: 0,
        partition_id: 0,
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: "[]".to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    };

    assert!(!is_stage_query_task(&task));
}
