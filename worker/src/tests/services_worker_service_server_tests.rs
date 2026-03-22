use super::{normalize_worker_error_for_response, validate_query_task_context, worker_service};

fn build_query_task() -> worker_service::Task {
    worker_service::Task {
        task_id: "t1".to_string(),
        operation: "query".to_string(),
        input: "{}".to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
        filter_predicate: None,
    }
}

#[test]
fn keeps_structured_outcome_errors_stable() {
    let raw = "RESULT|INFRA|INFRA_WORKER_STORAGE_IO_FAILED|failed to read parquet object";
    assert_eq!(normalize_worker_error_for_response(raw), raw);
}

#[test]
fn maps_known_validation_and_constraint_patterns() {
    let validation =
        normalize_worker_error_for_response("DECIMAL_COERCION_FAILED: invalid decimal literal");
    assert!(validation.starts_with("RESULT|VALIDATION|VALIDATION_DECIMAL_COERCION_FAILED|"));

    let constraint =
        normalize_worker_error_for_response("not null constraint violated for column id");
    assert!(constraint.starts_with("RESULT|CONSTRAINT|CONSTRAINT_NOT_NULL_VIOLATION|"));
}

#[test]
fn maps_unknown_errors_to_infra_fallback() {
    let normalized =
        normalize_worker_error_for_response("unexpected runtime panic at operator stage");
    assert!(normalized.starts_with("RESULT|INFRA|INFRA_WORKER_TASK_FAILED|"));
}

#[test]
fn rejects_query_task_without_query_id_context() {
    let task = build_query_task();
    let err = validate_query_task_context(&task).expect_err("missing query_id must fail");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message()
            .starts_with("RESULT|INFRA|INFRA_WORKER_EXECUTION_CONTEXT_MISSING|")
    );
}

#[test]
fn rejects_staged_query_task_without_partition_context() {
    let mut task = build_query_task();
    task.params
        .insert("__query_id".to_string(), "q1".to_string());
    task.params.insert("stage_id".to_string(), "2".to_string());

    let err = validate_query_task_context(&task)
        .expect_err("missing partition_index for staged task must fail");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message()
            .starts_with("RESULT|EXECUTION|EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING|")
    );
}

#[test]
fn accepts_staged_query_task_with_required_context() {
    let mut task = build_query_task();
    task.params
        .insert("__query_id".to_string(), "q1".to_string());
    task.params.insert("stage_id".to_string(), "2".to_string());
    task.params
        .insert("partition_index".to_string(), "0".to_string());

    assert!(validate_query_task_context(&task).is_ok());
}
