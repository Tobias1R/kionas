use crate::execution::planner::stage_execution_context;
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
