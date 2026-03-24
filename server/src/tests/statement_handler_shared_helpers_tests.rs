use super::{
    DispatchAuthContext, format_stage_dispatch_boundary_event, validate_query_dispatch_context,
};
use crate::statement_handler::shared::distributed_dag::{ExecutionModeHint, StageTaskGroup};
use kionas::planner::PartitionSpec;
use std::collections::HashMap;
use std::sync::Arc;

fn stage_group(
    stage_id: u32,
    partition_index: u32,
    partition_count: u32,
    upstream_stage_ids: Vec<u32>,
) -> StageTaskGroup {
    StageTaskGroup {
        stage_id,
        partition_index,
        partition_count,
        upstream_stage_ids,
        upstream_partition_counts: HashMap::new(),
        partition_spec: PartitionSpec::Single,
        execution_mode_hint: ExecutionModeHint::Distributed,
        output_destinations: Vec::new(),
        operation: "query".to_string(),
        payload: "{}".to_string(),
        params: HashMap::new(),
    }
}

fn stage_result_bytes(group: &StageTaskGroup) -> Vec<u8> {
    format!(
        "{{\"stage_id\":{},\"partition_index\":{},\"partition_count\":{},\"payload\":{}}}",
        group.stage_id, group.partition_index, group.partition_count, group.payload
    )
    .into_bytes()
}

fn stage_result_location(group: &StageTaskGroup) -> String {
    String::from_utf8(stage_result_bytes(group)).expect("stage result bytes must be utf8")
}

async fn run_in_memory_baseline(stage_groups: &[StageTaskGroup]) -> String {
    let mut groups_by_stage = HashMap::<u32, Vec<StageTaskGroup>>::new();
    for group in stage_groups {
        groups_by_stage
            .entry(group.stage_id)
            .or_default()
            .push(group.clone());
    }

    for groups in groups_by_stage.values_mut() {
        groups.sort_by_key(|group| group.partition_index);
    }

    let mut stage_results = HashMap::<u32, String>::new();
    let stage_waves =
        crate::statement_handler::shared::distributed_dag::build_stage_execution_waves(
            stage_groups,
        )
        .expect("baseline stage waves should build");

    for wave in stage_waves {
        for stage_id in wave {
            let groups = groups_by_stage
                .get(&stage_id)
                .cloned()
                .expect("stage groups must exist");
            let stage_result = groups
                .first()
                .map(stage_result_location)
                .expect("stage must have at least one partition");
            stage_results.insert(stage_id, stage_result);
        }
    }

    let final_stage_id = stage_groups
        .iter()
        .map(|group| group.stage_id)
        .max()
        .expect("final stage must exist");

    stage_results
        .remove(&final_stage_id)
        .expect("final stage result must exist")
}

async fn run_scheduler_with_deterministic_in_memory_executor(
    stage_groups: &[StageTaskGroup],
) -> String {
    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        });

    super::run_stage_groups_with_partition_executor(
        stage_groups,
        Some(&auth_ctx_with_query_id("q-byte-identical")),
        1,
        execute_partition,
    )
    .await
    .expect("scheduler should succeed with deterministic in-memory executor")
}

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
    let err = validate_query_dispatch_context("query", &params, None, None)
        .expect_err("query dispatch must fail without query_id context");
    let msg = err.to_string();
    assert!(msg.starts_with("RESULT|INFRA|INFRA_WORKER_EXECUTION_CONTEXT_MISSING|"));
}

#[test]
fn staged_query_dispatch_requires_partition_context() {
    let params = HashMap::new();
    let stage_metadata = crate::tasks::StageTaskMetadata {
        stage_id: 1,
        partition_id: 0,
        partition_count: 0,
        upstream_stage_ids: vec![],
        upstream_partition_counts: HashMap::new(),
        partition_spec: "\"Single\"".to_string(),
        query_run_id: None,
        execution_mode_hint: 0,
        output_destinations: vec![],
    };

    let err = validate_query_dispatch_context(
        "query",
        &params,
        Some(&stage_metadata),
        Some(&auth_ctx_with_query_id("q1")),
    )
    .expect_err("staged query dispatch must fail without partition_index");
    let msg = err.to_string();
    assert!(msg.starts_with("RESULT|EXECUTION|EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING|"));
}

#[test]
fn staged_query_dispatch_accepts_valid_context() {
    let params = HashMap::new();
    let stage_metadata = crate::tasks::StageTaskMetadata {
        stage_id: 1,
        partition_id: 0,
        partition_count: 1,
        upstream_stage_ids: vec![],
        upstream_partition_counts: HashMap::new(),
        partition_spec: "\"Single\"".to_string(),
        query_run_id: None,
        execution_mode_hint: 0,
        output_destinations: vec![],
    };

    assert!(
        validate_query_dispatch_context(
            "query",
            &params,
            Some(&stage_metadata),
            Some(&auth_ctx_with_query_id("q1"))
        )
        .is_ok()
    );
}

#[test]
fn staged_query_dispatch_accepts_zero_based_stage_id_context() {
    let params = HashMap::new();
    let stage_metadata = crate::tasks::StageTaskMetadata {
        stage_id: 0,
        partition_id: 0,
        partition_count: 1,
        upstream_stage_ids: vec![],
        upstream_partition_counts: HashMap::new(),
        partition_spec: "\"Single\"".to_string(),
        query_run_id: None,
        execution_mode_hint: 0,
        output_destinations: vec![],
    };

    assert!(
        validate_query_dispatch_context(
            "query",
            &params,
            Some(&stage_metadata),
            Some(&auth_ctx_with_query_id("q0"))
        )
        .is_ok()
    );
}

#[test]
fn query_dispatch_accepts_legacy_context_without_stage_metadata() {
    let params = HashMap::new();

    assert!(
        validate_query_dispatch_context(
            "query",
            &params,
            None,
            Some(&auth_ctx_with_query_id("q-legacy"))
        )
        .is_ok()
    );
}

#[test]
fn query_dispatch_rejects_param_stage_without_partition_index() {
    let mut params = HashMap::new();
    params.insert("stage_id".to_string(), "0".to_string());

    let err = validate_query_dispatch_context(
        "query",
        &params,
        None,
        Some(&auth_ctx_with_query_id("q-stage-param")),
    )
    .expect_err("query dispatch must fail when stage_id param is set without partition_index");

    assert!(
        err.to_string()
            .starts_with("RESULT|EXECUTION|EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING|")
    );
}

#[test]
fn query_dispatch_accepts_param_stage_with_partition_index() {
    let mut params = HashMap::new();
    params.insert("stage_id".to_string(), "0".to_string());
    params.insert("partition_index".to_string(), "0".to_string());

    assert!(
        validate_query_dispatch_context(
            "query",
            &params,
            None,
            Some(&auth_ctx_with_query_id("q-stage-partition-param"))
        )
        .is_ok()
    );
}

#[test]
fn non_query_dispatch_does_not_require_query_context() {
    let params = HashMap::new();
    assert!(validate_query_dispatch_context("insert", &params, None, None).is_ok());
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

#[tokio::test]
async fn in_memory_harness_executes_single_stage_and_returns_location() {
    let stage_groups = vec![stage_group(1, 0, 1, Vec::new())];
    let observed = Arc::new(tokio::sync::Mutex::new(Vec::<u32>::new()));
    let observed_for_executor = observed.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let observed_for_executor = observed_for_executor.clone();
            Box::pin(async move {
                observed_for_executor.lock().await.push(group.stage_id);
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(format!(
                    "mem://stage-{}/partition-{}",
                    group.stage_id, group.partition_index
                ))
            }) as super::StagePartitionFuture
        });

    let result = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-single")),
        1,
        execute_partition,
    )
    .await
    .expect("single-stage in-memory harness should succeed");

    assert_eq!(result, "mem://stage-1/partition-0");
    assert_eq!(*observed.lock().await, vec![1]);
}

#[tokio::test]
async fn in_memory_harness_executes_two_stage_chain_in_topological_order() {
    let stage_groups = vec![
        stage_group(1, 0, 1, Vec::new()),
        stage_group(2, 0, 1, vec![1]),
    ];
    let observed = Arc::new(tokio::sync::Mutex::new(Vec::<u32>::new()));
    let observed_for_executor = observed.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let observed_for_executor = observed_for_executor.clone();
            Box::pin(async move {
                observed_for_executor.lock().await.push(group.stage_id);
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(format!(
                    "mem://stage-{}/partition-{}",
                    group.stage_id, group.partition_index
                ))
            }) as super::StagePartitionFuture
        });

    let result = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-two")),
        1,
        execute_partition,
    )
    .await
    .expect("two-stage in-memory harness should succeed");

    assert_eq!(result, "mem://stage-2/partition-0");
    assert_eq!(*observed.lock().await, vec![1, 2]);
}

#[tokio::test]
async fn in_memory_harness_executes_three_stage_chain_in_topological_order() {
    let stage_groups = vec![
        stage_group(1, 0, 1, Vec::new()),
        stage_group(2, 0, 1, vec![1]),
        stage_group(3, 0, 1, vec![2]),
    ];
    let observed = Arc::new(tokio::sync::Mutex::new(Vec::<u32>::new()));
    let observed_for_executor = observed.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let observed_for_executor = observed_for_executor.clone();
            Box::pin(async move {
                observed_for_executor.lock().await.push(group.stage_id);
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(format!(
                    "mem://stage-{}/partition-{}",
                    group.stage_id, group.partition_index
                ))
            }) as super::StagePartitionFuture
        });

    let result = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-three")),
        1,
        execute_partition,
    )
    .await
    .expect("three-stage in-memory harness should succeed");

    assert_eq!(result, "mem://stage-3/partition-0");
    assert_eq!(*observed.lock().await, vec![1, 2, 3]);
}

#[tokio::test]
async fn byte_identical_matrix_single_stage_matches_baseline() {
    let stage_groups = vec![stage_group(1, 0, 1, Vec::new())];

    let baseline = run_in_memory_baseline(&stage_groups).await;
    let scheduler = run_scheduler_with_deterministic_in_memory_executor(&stage_groups).await;

    assert_eq!(scheduler.as_bytes(), baseline.as_bytes());
}

#[tokio::test]
async fn byte_identical_matrix_two_stage_chain_matches_baseline() {
    let stage_groups = vec![
        stage_group(1, 0, 1, Vec::new()),
        stage_group(2, 0, 1, vec![1]),
    ];

    let baseline = run_in_memory_baseline(&stage_groups).await;
    let scheduler = run_scheduler_with_deterministic_in_memory_executor(&stage_groups).await;

    assert_eq!(scheduler.as_bytes(), baseline.as_bytes());
}

#[tokio::test]
async fn byte_identical_matrix_three_stage_chain_matches_baseline() {
    let stage_groups = vec![
        stage_group(1, 0, 1, Vec::new()),
        stage_group(2, 0, 1, vec![1]),
        stage_group(3, 0, 1, vec![2]),
    ];

    let baseline = run_in_memory_baseline(&stage_groups).await;
    let scheduler = run_scheduler_with_deterministic_in_memory_executor(&stage_groups).await;

    assert_eq!(scheduler.as_bytes(), baseline.as_bytes());
}

#[tokio::test]
async fn in_memory_harness_surfaces_partition_dispatch_failure_with_stage_context() {
    let stage_groups = vec![
        stage_group(1, 0, 1, Vec::new()),
        stage_group(2, 0, 1, vec![1]),
    ];

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                if group.stage_id == 2 {
                    return Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                        std::io::Error::other("simulated worker failure"),
                    ));
                }
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        });

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-error")),
        1,
        execute_partition,
    )
    .await
    .expect_err("scheduler should propagate stage partition dispatch failure");

    let message = err.to_string();
    assert!(message.contains("stage 2 partition 0 dispatch failed"));
    assert!(message.contains("simulated worker failure"));
}

#[tokio::test]
async fn scheduler_error_matrix_rejects_missing_query_id_context() {
    let stage_groups = vec![stage_group(1, 0, 1, Vec::new())];

    let execute_partition =
        std::sync::Arc::new(move |_group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>("mem://ok".to_string())
            }) as super::StagePartitionFuture
        });

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("   ")),
        1,
        execute_partition,
    )
    .await
    .expect_err("scheduler should fail when query id context is blank");

    assert!(
        err.to_string()
            .contains("dispatch checkpoint failed: query_id is missing for staged query dispatch")
    );
}

#[tokio::test]
async fn scheduler_error_matrix_detects_partition_fanout_mismatch() {
    let stage_groups = vec![stage_group(1, 0, 2, Vec::new())];

    let execute_partition =
        std::sync::Arc::new(move |_group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>("mem://ok".to_string())
            }) as super::StagePartitionFuture
        });

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-fanout")),
        1,
        execute_partition,
    )
    .await
    .expect_err("scheduler should fail when stage fanout does not match group count");

    assert!(
        err.to_string()
            .contains("stage 1 partition fan-out mismatch: expected 2 tasks, found 1")
    );
}

#[tokio::test]
async fn scheduler_error_matrix_detects_stage_cycle() {
    let stage_groups = vec![stage_group(1, 0, 1, vec![2]), stage_group(2, 0, 1, vec![1])];

    let execute_partition =
        std::sync::Arc::new(move |_group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>("mem://ok".to_string())
            }) as super::StagePartitionFuture
        });

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-cycle")),
        1,
        execute_partition,
    )
    .await
    .expect_err("scheduler should fail when dependency cycle exists");

    assert!(err.to_string().contains("no schedulable stage found"));
}

#[tokio::test]
async fn scheduler_error_matrix_propagates_timeout_style_partition_failure() {
    let stage_groups = vec![stage_group(1, 0, 1, Vec::new())];

    let execute_partition = std::sync::Arc::new(
        move |_group: StageTaskGroup, _auth_ctx, timeout_secs: u64| {
            Box::pin(async move {
                let wait_secs = timeout_secs.saturating_add(1);
                tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "simulated partition timeout",
                    ),
                ))
            }) as super::StagePartitionFuture
        },
    );

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-timeout")),
        0,
        execute_partition,
    )
    .await
    .expect_err("scheduler should surface timeout-style partition failures");

    let message = err.to_string();
    assert!(message.contains("stage 1 partition 0 dispatch failed"));
    assert!(message.contains("simulated partition timeout"));
}

#[tokio::test]
async fn scheduler_error_matrix_rejects_empty_stage_group_set() {
    let stage_groups = Vec::<StageTaskGroup>::new();

    let execute_partition =
        std::sync::Arc::new(move |_group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>("mem://ok".to_string())
            }) as super::StagePartitionFuture
        });

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-empty")),
        1,
        execute_partition,
    )
    .await
    .expect_err("scheduler should fail when stage groups are empty");

    assert!(
        err.to_string()
            .contains("stage scheduler received no stage groups")
    );
}

#[tokio::test]
async fn scheduler_error_matrix_surfaces_partition_task_panic_with_stage_context() {
    let stage_groups = vec![stage_group(1, 0, 1, Vec::new())];

    let execute_partition =
        std::sync::Arc::new(move |_group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                panic!("simulated partition panic");
                #[allow(unreachable_code)]
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>("mem://ok".to_string())
            }) as super::StagePartitionFuture
        });

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-panic")),
        1,
        execute_partition,
    )
    .await
    .expect_err("scheduler should surface partition panic with stage context");

    let message = err.to_string();
    assert!(message.contains("stage 1 partition task panicked"));
    assert!(message.contains("simulated partition panic"));
}

#[tokio::test]
async fn scheduler_preserves_observability_params_on_dispatched_partitions() {
    let mut stage0 = stage_group(1, 0, 2, Vec::new());
    let mut stage1 = stage_group(1, 1, 2, Vec::new());
    let mut stage2 = stage_group(2, 0, 1, vec![1]);

    for group in [&mut stage0, &mut stage1, &mut stage2] {
        group.params.insert(
            "distributed_dag_metrics_json".to_string(),
            "{\"stage_count\":2}".to_string(),
        );
        group.params.insert(
            "distributed_plan_validation_status".to_string(),
            "passed".to_string(),
        );
        group
            .params
            .insert("stage_extraction_mismatch".to_string(), "false".to_string());
        group
            .params
            .insert("datafusion_stage_count".to_string(), "2".to_string());
        group
            .params
            .insert("distributed_stage_count".to_string(), "2".to_string());
    }

    let stage_groups = vec![stage0, stage1, stage2];
    let seen_params = Arc::new(tokio::sync::Mutex::new(
        Vec::<HashMap<String, String>>::new(),
    ));
    let seen_params_for_executor = seen_params.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let seen_params_for_executor = seen_params_for_executor.clone();
            Box::pin(async move {
                seen_params_for_executor
                    .lock()
                    .await
                    .push(group.params.clone());
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        });

    let _result = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-observability")),
        1,
        execute_partition,
    )
    .await
    .expect("scheduler should succeed with observability params attached");

    let captured = seen_params.lock().await;
    assert_eq!(captured.len(), 3);
    for params in captured.iter() {
        assert_eq!(
            params.get("distributed_dag_metrics_json"),
            Some(&"{\"stage_count\":2}".to_string())
        );
        assert_eq!(
            params.get("distributed_plan_validation_status"),
            Some(&"passed".to_string())
        );
        assert_eq!(
            params.get("stage_extraction_mismatch"),
            Some(&"false".to_string())
        );
        assert_eq!(params.get("datafusion_stage_count"), Some(&"2".to_string()));
        assert_eq!(
            params.get("distributed_stage_count"),
            Some(&"2".to_string())
        );
    }
}

#[tokio::test]
async fn mixed_version_upgrade_fails_fast_when_legacy_worker_rejects_stage_contract() {
    let stage_groups = vec![
        stage_group(0, 0, 1, Vec::new()),
        stage_group(1, 0, 1, vec![0]),
    ];

    // Simulate legacy worker behavior: any staged task (dependency-aware) is rejected.
    let execute_partition = std::sync::Arc::new(
        move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                if !group.upstream_stage_ids.is_empty() || group.stage_id > 0 {
                    return Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                        std::io::Error::other(
                            "RESULT|EXECUTION|EXECUTION_WORKER_EXECUTION_STAGE_CONTEXT_MISSING|legacy worker rejected stage-aware contract",
                        ),
                    ));
                }

                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        },
    );

    let err = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-mixed-version-legacy")),
        1,
        execute_partition,
    )
    .await
    .expect_err("mixed-version run should fail when legacy worker rejects stage contract");

    let message = err.to_string();
    assert!(message.contains("stage 1 partition 0 dispatch failed"));
    assert!(message.contains("EXECUTION_WORKER_EXECUTION_STAGE_CONTEXT_MISSING"));
}

#[tokio::test]
async fn mixed_version_upgrade_succeeds_after_worker_contract_upgrade() {
    let stage_groups = vec![
        stage_group(0, 0, 1, Vec::new()),
        stage_group(1, 0, 1, vec![0]),
    ];

    // Simulate upgraded worker behavior: stage-aware contract including zero-based stage ids is accepted.
    let execute_partition = std::sync::Arc::new(
        move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            Box::pin(async move {
                if group.partition_count == 0 || group.partition_index >= group.partition_count {
                    return Err::<String, Box<dyn std::error::Error + Send + Sync>>(Box::new(
                        std::io::Error::other(
                            "RESULT|EXECUTION|EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING|invalid partition bounds",
                        ),
                    ));
                }

                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        },
    );

    let final_location = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-mixed-version-upgraded")),
        1,
        execute_partition,
    )
    .await
    .expect("run should succeed after worker contract upgrade");

    assert_eq!(
        final_location,
        "{\"stage_id\":1,\"partition_index\":0,\"partition_count\":1,\"payload\":{}}"
    );
}
