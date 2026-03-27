use crate::execution::planner::{extract_runtime_plan, stage_execution_context};
use crate::services::worker_service_server::worker_service;

fn build_query_task() -> worker_service::StagePartitionExecution {
    worker_service::StagePartitionExecution {
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
        input: "{}".to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    }
}

#[test]
fn stage_execution_context_accepts_zero_based_stage_id_for_staged_task() {
    let mut task = build_query_task();
    task.stage_id = 0;
    task.partition_count = 2;
    task.partition_id = 0;

    let context = stage_execution_context(&task)
        .expect("zero-based stage_id with valid partition context should be accepted");

    assert_eq!(context.stage_id, 0);
    assert_eq!(context.partition_count, 2);
    assert_eq!(context.partition_index, 0);
}

#[test]
fn stage_execution_context_defaults_legacy_query_task_context() {
    let mut task = build_query_task();
    task.task_id = "legacy-task".to_string();
    task.stage_id = 0;
    task.partition_count = 0;
    task.partition_id = 0;
    task.upstream_stage_ids = Vec::new();
    task.query_run_id = String::new();

    let context = stage_execution_context(&task)
        .expect("legacy query task without staged metadata should use deterministic defaults");

    assert_eq!(context.stage_id, 0);
    assert_eq!(context.partition_count, 1);
    assert_eq!(context.partition_index, 0);
    assert_eq!(context.query_run_id, "legacy-task-legacy-task");
}

#[test]
fn stage_execution_context_rejects_zero_based_stage_with_invalid_partition_bounds() {
    let mut task = build_query_task();
    task.stage_id = 0;
    task.partition_count = 2;
    task.partition_id = 2;

    let err = stage_execution_context(&task)
        .expect_err("staged zero-based task should fail when partition_id is out of bounds");
    assert!(err.starts_with("EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING:"));
}

#[test]
fn extract_runtime_plan_prefers_execution_plan_bytes_for_stage_task() {
    let mut task = build_query_task();
    task.input = "not-json".to_string();
    task.execution_plan = serde_json::to_vec(&serde_json::json!([
        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
        "Materialize"
    ]))
    .expect("execution plan payload should serialize");

    let plan = extract_runtime_plan(&task)
        .expect("runtime planner should decode operators from execution_plan bytes");
    assert!(plan.has_materialize);
    assert_eq!(plan.projection_exprs.len(), 1);
}

#[test]
fn extract_runtime_plan_rejects_invalid_execution_plan_bytes() {
    let mut task = build_query_task();
    task.execution_plan = b"not-json".to_vec();

    let err = extract_runtime_plan(&task)
        .expect_err("invalid execution_plan bytes must be rejected with actionable error");
    assert!(err.contains("invalid execution_plan payload"));
}

#[test]
fn extract_runtime_plan_decodes_union_operator() {
    let mut task = build_query_task();
    task.execution_plan = serde_json::to_vec(&serde_json::json!([
        {"TableScan": {"relation": {"database": "sales", "schema": "public", "table": "users"}}},
        {"Union": {
            "operands": [
                {"relation": {"database": "sales", "schema": "public", "table": "users"}},
                {"relation": {"database": "sales", "schema": "public", "table": "users_archive"}}
            ],
            "distinct": true
        }},
        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
        "Materialize"
    ]))
    .expect("execution plan payload should serialize");

    let plan = extract_runtime_plan(&task)
        .expect("runtime planner should decode union operator from execution_plan bytes");
    let union = plan.union_spec.expect("union metadata should be present");
    assert_eq!(union.operands.len(), 2);
    assert!(union.distinct);
    assert_eq!(union.operands[1].relation.table, "users_archive");
}

#[test]
fn extract_runtime_plan_decodes_union_operand_filters() {
    let mut task = build_query_task();
    task.execution_plan = serde_json::to_vec(&serde_json::json!([
        {"TableScan": {"relation": {"database": "bench", "schema": "seed1", "table": "customers"}}},
        {"Union": {
            "operands": [
                {
                    "relation": {"database": "bench", "schema": "seed1", "table": "customers"},
                    "filter": {"Comparison": {"column": "id", "op": "Le", "value": {"Int": 5}}}
                },
                {
                    "relation": {"database": "bench", "schema": "seed1", "table": "customers"},
                    "filter": {
                        "Conjunction": {
                            "clauses": [
                                {"Comparison": {"column": "id", "op": "Gt", "value": {"Int": 5}}},
                                {"Comparison": {"column": "id", "op": "Le", "value": {"Int": 10}}}
                            ]
                        }
                    }
                }
            ],
            "distinct": false
        }},
        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
        "Materialize"
    ]))
    .expect("execution plan payload should serialize");

    let plan =
        extract_runtime_plan(&task).expect("runtime planner should decode union operand filters");
    let union = plan.union_spec.expect("union metadata should be present");

    assert_eq!(union.operands.len(), 2);
    assert!(union.operands[0].filter.is_some());
    assert!(union.operands[1].filter.is_some());
}

#[test]
fn extract_runtime_plan_decodes_window_operator() {
    let mut task = build_query_task();
    task.execution_plan = serde_json::to_vec(&serde_json::json!([
        {"TableScan": {"relation": {"database": "sales", "schema": "public", "table": "employees"}}},
        {"WindowAggr": {
            "spec": {
                "partition_by": [{"ColumnRef": {"name": "dept"}}],
                "order_by": [{"expression": {"ColumnRef": {"name": "salary"}}, "ascending": false}],
                "functions": [{
                    "function_name": "ROW_NUMBER",
                    "args": [],
                    "output_name": "rn",
                    "frame": null
                }]
            }
        }},
        {"Projection": {"expressions": [{"Raw": {"sql": "dept"}}, {"Raw": {"sql": "rn"}}]}},
        "Materialize"
    ]))
    .expect("execution plan payload should serialize");

    let plan = extract_runtime_plan(&task)
        .expect("runtime planner should decode window operator from execution_plan bytes");

    let window_spec = plan.window_spec.expect("window spec should be extracted");
    assert_eq!(window_spec.partition_by.len(), 1);
    assert_eq!(window_spec.order_by.len(), 1);
    assert_eq!(window_spec.functions.len(), 1);
    assert_eq!(window_spec.functions[0].output_name, "rn");
}

#[test]
fn extract_runtime_plan_marks_sort_before_projection_when_ordered_that_way() {
    let mut task = build_query_task();
    task.execution_plan = serde_json::to_vec(&serde_json::json!([
        {"TableScan": {"relation": {"database": "sales", "schema": "public", "table": "orders"}}},
        {"Sort": {"keys": [{"expression": {"Raw": {"sql": "o.quantity"}}, "ascending": false}]}},
        {"Projection": {"expressions": [{"Raw": {"sql": "c.name"}}, {"Raw": {"sql": "p.name AS product_name"}}]}},
        "Materialize"
    ]))
    .expect("execution plan payload should serialize");

    let plan =
        extract_runtime_plan(&task).expect("runtime planner should decode sort/projection order");

    assert!(plan.sort_before_projection);
}

#[test]
fn extract_runtime_plan_marks_projection_before_sort_when_ordered_that_way() {
    let mut task = build_query_task();
    task.execution_plan = serde_json::to_vec(&serde_json::json!([
        {"TableScan": {"relation": {"database": "sales", "schema": "public", "table": "orders"}}},
        {"Projection": {"expressions": [{"Raw": {"sql": "name"}}]}},
        {"Sort": {"keys": [{"expression": {"Raw": {"sql": "name"}}, "ascending": true}]}},
        "Materialize"
    ]))
    .expect("execution plan payload should serialize");

    let plan =
        extract_runtime_plan(&task).expect("runtime planner should decode projection/sort order");

    assert!(!plan.sort_before_projection);
}
