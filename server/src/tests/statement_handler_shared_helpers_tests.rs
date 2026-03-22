use super::{
    DispatchAuthContext, format_stage_dispatch_boundary_event, validate_query_dispatch_context,
};
use std::collections::HashMap;

fn auth_ctx_with_query_id(query_id: &str) -> DispatchAuthContext {
    DispatchAuthContext {
        rbac_user: "u1".to_string(),
        rbac_role: "r1".to_string(),
        scope: "select:*.*.*".to_string(),
        query_id: query_id.to_string(),
    }
}

#[test]
fn query_dispatch_requires_query_id() {
    let params = HashMap::new();
    let err = validate_query_dispatch_context("query", &params, None)
        .expect_err("query dispatch must fail without query_id context");
    let msg = err.to_string();
    assert!(msg.starts_with("RESULT|INFRA|INFRA_WORKER_EXECUTION_CONTEXT_MISSING|"));
}

#[test]
fn staged_query_dispatch_requires_partition_context() {
    let mut params = HashMap::new();
    params.insert("stage_id".to_string(), "1".to_string());

    let err =
        validate_query_dispatch_context("query", &params, Some(&auth_ctx_with_query_id("q1")))
            .expect_err("staged query dispatch must fail without partition_index");
    let msg = err.to_string();
    assert!(msg.starts_with("RESULT|EXECUTION|EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING|"));
}

#[test]
fn staged_query_dispatch_accepts_valid_context() {
    let mut params = HashMap::new();
    params.insert("stage_id".to_string(), "1".to_string());
    params.insert("partition_index".to_string(), "0".to_string());

    assert!(
        validate_query_dispatch_context("query", &params, Some(&auth_ctx_with_query_id("q1")))
            .is_ok()
    );
}

#[test]
fn non_query_dispatch_does_not_require_query_context() {
    let params = HashMap::new();
    assert!(validate_query_dispatch_context("insert", &params, None).is_ok());
}

#[test]
fn formats_stage_dispatch_boundary_event_with_required_dimensions() {
    let event = format_stage_dispatch_boundary_event("q1", 3, "task-7", 8);

    assert!(event.starts_with("event=execution.stage_dispatch_boundary "));
    assert!(event.contains("query_id=q1"));
    assert!(event.contains("stage_id=3"));
    assert!(event.contains("task_id=task-7"));
    assert!(event.contains("category=execution"));
    assert!(event.contains("origin=server_dispatch"));
    assert!(event.contains("partition_count=8"));
}
