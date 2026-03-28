use super::{
    DispatchAuthContext, format_stage_dispatch_boundary_event,
    format_stage_dispatch_observability_event, validate_query_dispatch_context,
};
use crate::statement_handler::shared::distributed_dag::{
    self, ExecutionModeHint, OutputDestination, StageTaskGroup,
};
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

fn insert_distributed_observability_params(
    params: &mut HashMap<String, String>,
    stage_count: usize,
) {
    params.insert(
        distributed_dag::OBS_DAG_METRICS_JSON_PARAM.to_string(),
        format!("{{\"stage_count\":{}}}", stage_count),
    );
    params.insert(
        distributed_dag::OBS_PLAN_VALIDATION_STATUS_PARAM.to_string(),
        distributed_dag::OBS_PLAN_VALIDATION_STATUS_PASSED.to_string(),
    );
    params.insert(
        distributed_dag::OBS_STAGE_EXTRACTION_MISMATCH_PARAM.to_string(),
        "false".to_string(),
    );
    params.insert(
        distributed_dag::OBS_DATAFUSION_STAGE_COUNT_PARAM.to_string(),
        stage_count.to_string(),
    );
    params.insert(
        distributed_dag::OBS_DISTRIBUTED_STAGE_COUNT_PARAM.to_string(),
        stage_count.to_string(),
    );
}

fn assert_distributed_observability_params(params: &HashMap<String, String>, stage_count: usize) {
    let expected_metrics = format!("{{\"stage_count\":{}}}", stage_count);
    let expected_stage_count = stage_count.to_string();

    assert_eq!(
        params.get(distributed_dag::OBS_DAG_METRICS_JSON_PARAM),
        Some(&expected_metrics)
    );
    assert_eq!(
        params.get(distributed_dag::OBS_PLAN_VALIDATION_STATUS_PARAM),
        Some(&distributed_dag::OBS_PLAN_VALIDATION_STATUS_PASSED.to_string())
    );
    assert_eq!(
        params.get(distributed_dag::OBS_STAGE_EXTRACTION_MISMATCH_PARAM),
        Some(&"false".to_string())
    );
    assert_eq!(
        params.get(distributed_dag::OBS_DATAFUSION_STAGE_COUNT_PARAM),
        Some(&expected_stage_count)
    );
    assert_eq!(
        params.get(distributed_dag::OBS_DISTRIBUTED_STAGE_COUNT_PARAM),
        Some(&expected_stage_count)
    );
}

fn assert_contains_all_five_telemetry_keys(params: &HashMap<String, String>) {
    let required_keys = [
        distributed_dag::OBS_DAG_METRICS_JSON_PARAM,
        distributed_dag::OBS_PLAN_VALIDATION_STATUS_PARAM,
        distributed_dag::OBS_STAGE_EXTRACTION_MISMATCH_PARAM,
        distributed_dag::OBS_DATAFUSION_STAGE_COUNT_PARAM,
        distributed_dag::OBS_DISTRIBUTED_STAGE_COUNT_PARAM,
    ];

    for key in required_keys {
        assert!(
            params.contains_key(key),
            "expected telemetry key '{}' to be present in stage params",
            key
        );
    }
}

fn insert_distributed_routing_observability_params(
    params: &mut HashMap<String, String>,
    routing_source: &str,
    runtime_worker_count: usize,
    effective_worker_count: usize,
    env_fallback_applied: bool,
    fallback_kind: &str,
    fallback_active: bool,
) {
    params.insert(
        distributed_dag::ROUTING_WORKER_SOURCE_PARAM.to_string(),
        routing_source.to_string(),
    );
    params.insert(
        distributed_dag::ROUTING_WORKER_COUNT_PARAM.to_string(),
        effective_worker_count.to_string(),
    );
    params.insert(
        distributed_dag::ROUTING_RUNTIME_WORKER_COUNT_PARAM.to_string(),
        runtime_worker_count.to_string(),
    );
    params.insert(
        distributed_dag::ROUTING_EFFECTIVE_WORKER_COUNT_PARAM.to_string(),
        effective_worker_count.to_string(),
    );
    params.insert(
        distributed_dag::ROUTING_ENV_FALLBACK_APPLIED_PARAM.to_string(),
        env_fallback_applied.to_string(),
    );
    params.insert(
        distributed_dag::ROUTING_FALLBACK_KIND_PARAM.to_string(),
        fallback_kind.to_string(),
    );
    params.insert(
        distributed_dag::ROUTING_FALLBACK_ACTIVE_PARAM.to_string(),
        fallback_active.to_string(),
    );
}

fn assert_distributed_routing_observability_params(
    params: &HashMap<String, String>,
    routing_source: &str,
    runtime_worker_count: usize,
    effective_worker_count: usize,
    env_fallback_applied: bool,
    fallback_kind: &str,
    fallback_active: bool,
) {
    assert_eq!(
        params.get(distributed_dag::ROUTING_WORKER_SOURCE_PARAM),
        Some(&routing_source.to_string())
    );
    assert_eq!(
        params.get(distributed_dag::ROUTING_WORKER_COUNT_PARAM),
        Some(&effective_worker_count.to_string())
    );
    assert_eq!(
        params.get(distributed_dag::ROUTING_RUNTIME_WORKER_COUNT_PARAM),
        Some(&runtime_worker_count.to_string())
    );
    assert_eq!(
        params.get(distributed_dag::ROUTING_EFFECTIVE_WORKER_COUNT_PARAM),
        Some(&effective_worker_count.to_string())
    );
    assert_eq!(
        params.get(distributed_dag::ROUTING_ENV_FALLBACK_APPLIED_PARAM),
        Some(&env_fallback_applied.to_string())
    );
    assert_eq!(
        params.get(distributed_dag::ROUTING_FALLBACK_KIND_PARAM),
        Some(&fallback_kind.to_string())
    );
    assert_eq!(
        params.get(distributed_dag::ROUTING_FALLBACK_ACTIVE_PARAM),
        Some(&fallback_active.to_string())
    );
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

#[test]
fn formats_stage_dispatch_observability_event_with_required_dimensions() {
    let event = format_stage_dispatch_observability_event(
        "q-ob1", 5, "consul", "worker-1", "default", true, "passed", "false", "2", "2",
    );

    assert!(event.starts_with("event=execution.stage_dispatch_observability "));
    assert!(event.contains("query_id=q-ob1"));
    assert!(event.contains("stage_id=5"));
    assert!(event.contains("route_source=consul"));
    assert!(event.contains("worker_destination=worker-1"));
    assert!(event.contains("warehouse=default"));
    assert!(event.contains("dag_metrics_present=true"));
    assert!(event.contains("plan_validation_status=passed"));
    assert!(event.contains("stage_extraction_mismatch=false"));
    assert!(event.contains("datafusion_stage_count=2"));
    assert!(event.contains("distributed_stage_count=2"));
}

#[test]
fn parses_stage_latency_from_result_location_query_params() {
    let location = "flight://worker1:50052/query/sales/public/users/worker-1?session_id=s1&task_id=t1&stage_latency_queue_ms=21&stage_latency_exec_ms=55&stage_latency_network_ms=8";

    let parsed = super::parse_stage_latency_from_result_location(location)
        .expect("stage latency should parse from result location query params");

    assert_eq!(parsed.queue_ms, 21);
    assert_eq!(parsed.exec_ms, 55);
    assert_eq!(parsed.network_ms, 8);
    assert_eq!(parsed.network_write_ms, 0);
    assert_eq!(parsed.network_bytes, 0);
    assert_eq!(parsed.network_batches, 0);
}

#[test]
fn appends_stage_latency_breakdown_to_final_result_location() {
    let base = "flight://worker1:50052/query/sales/public/users/worker-1?session_id=s1&task_id=t1";
    let mut per_stage = std::collections::BTreeMap::new();

    per_stage.insert(
        0,
        super::StageLatencyBreakdown {
            queue_ms: 10,
            exec_ms: 30,
            network_ms: 5,
            network_write_ms: 4,
            network_bytes: 1200,
            network_batches: 12,
        },
    );
    per_stage.insert(
        1,
        super::StageLatencyBreakdown {
            queue_ms: 4,
            exec_ms: 16,
            network_ms: 2,
            network_write_ms: 1,
            network_bytes: 300,
            network_batches: 3,
        },
    );

    let enriched = super::append_stage_latency_breakdown_to_result_location(base, &per_stage);
    assert!(enriched.contains("latency_stage_count=2"));
    assert!(enriched.contains("latency_stage_0_queue_ms=10"));
    assert!(enriched.contains("latency_stage_0_exec_ms=30"));
    assert!(enriched.contains("latency_stage_0_network_ms=5"));
    assert!(enriched.contains("latency_stage_1_queue_ms=4"));
    assert!(enriched.contains("latency_stage_1_exec_ms=16"));
    assert!(enriched.contains("latency_stage_1_network_ms=2"));
    assert!(enriched.contains("latency_total_queue_ms=14"));
    assert!(enriched.contains("latency_total_exec_ms=46"));
    assert!(enriched.contains("latency_total_network_ms=7"));
    assert!(enriched.contains("latency_total_network_write_ms=5"));
    assert!(enriched.contains("latency_total_network_bytes=1500"));
    assert!(enriched.contains("latency_total_network_batches=15"));
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
        insert_distributed_observability_params(&mut group.params, 2);
        insert_distributed_routing_observability_params(
            &mut group.params,
            "fallback",
            0,
            1,
            true,
            distributed_dag::ROUTING_POOL_SOURCE_DEFAULT,
            true,
        );
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
        assert_distributed_observability_params(params, 2);
        assert_distributed_routing_observability_params(
            params,
            "fallback",
            0,
            1,
            true,
            distributed_dag::ROUTING_POOL_SOURCE_DEFAULT,
            true,
        );
    }
}

#[tokio::test]
async fn query_dispatch_to_worker_includes_all_five_telemetry_keys() {
    let mut scan_stage = stage_group(0, 0, 1, Vec::new());
    scan_stage.payload = "{\"sql\":\"SELECT id FROM sales.public.users\"}".to_string();
    insert_distributed_observability_params(&mut scan_stage.params, 1);

    let mut join_stage = stage_group(1, 0, 1, vec![0]);
    join_stage.payload = "{\"sql\":\"SELECT a.id, b.value FROM sales.public.a a JOIN sales.public.b b ON a.id = b.id\"}".to_string();
    insert_distributed_observability_params(&mut join_stage.params, 2);

    let stage_groups = vec![scan_stage, join_stage];
    let captured_params = Arc::new(tokio::sync::Mutex::new(
        Vec::<HashMap<String, String>>::new(),
    ));
    let captured_params_for_executor = captured_params.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let captured_params_for_executor = captured_params_for_executor.clone();
            Box::pin(async move {
                captured_params_for_executor
                    .lock()
                    .await
                    .push(group.params.clone());
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        });

    let _ = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-five-keys")),
        1,
        execute_partition,
    )
    .await
    .expect("query dispatch should preserve telemetry keys to worker stage metadata");

    let captured = captured_params.lock().await;
    assert_eq!(captured.len(), 2);
    for params in captured.iter() {
        assert_contains_all_five_telemetry_keys(params);
    }
}

#[tokio::test]
async fn query_dispatch_to_worker_includes_all_five_telemetry_keys_across_query_types() {
    let query_matrix = [
        "SELECT id FROM sales.public.users",
        "SELECT id FROM sales.public.users WHERE id > 5",
        "SELECT a.id, b.value FROM sales.public.a a JOIN sales.public.b b ON a.id = b.id",
        "SELECT region, SUM(amount) FROM sales.public.orders GROUP BY region",
    ];

    let mut stage_groups = Vec::new();
    for (index, query_sql) in query_matrix.iter().enumerate() {
        let stage_id = u32::try_from(index).expect("query matrix index should fit u32");
        let upstream = if stage_id == 0 {
            Vec::new()
        } else {
            vec![stage_id - 1]
        };

        let mut group = stage_group(stage_id, 0, 1, upstream);
        group.payload = format!("{{\"sql\":\"{}\"}}", query_sql);
        insert_distributed_observability_params(&mut group.params, query_matrix.len());
        stage_groups.push(group);
    }

    let captured_params = Arc::new(tokio::sync::Mutex::new(
        Vec::<HashMap<String, String>>::new(),
    ));
    let captured_params_for_executor = captured_params.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let captured_params_for_executor = captured_params_for_executor.clone();
            Box::pin(async move {
                captured_params_for_executor
                    .lock()
                    .await
                    .push(group.params.clone());
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        });

    let _ = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-query-type-matrix")),
        1,
        execute_partition,
    )
    .await
    .expect("query-type matrix dispatch should preserve telemetry keys");

    let captured = captured_params.lock().await;
    assert_eq!(captured.len(), query_matrix.len());
    for params in captured.iter() {
        assert_contains_all_five_telemetry_keys(params);
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

#[tokio::test]
async fn query_dispatch_includes_auth_params_for_worker_pull_context() {
    let stage_groups = vec![stage_group(0, 0, 1, Vec::new())];
    let captured = Arc::new(tokio::sync::Mutex::new(
        Vec::<HashMap<String, String>>::new(),
    ));
    let captured_for_executor = captured.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let captured_for_executor = captured_for_executor.clone();
            Box::pin(async move {
                captured_for_executor
                    .lock()
                    .await
                    .push(group.params.clone());
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        });

    let _ = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-auth-params")),
        1,
        execute_partition,
    )
    .await
    .expect("query dispatch should include auth metadata in stage params");

    let values = captured.lock().await;
    assert_eq!(values.len(), 1);
    let params = &values[0];
    assert_eq!(params.get("__query_id"), Some(&"q-auth-params".to_string()));
    assert_eq!(params.get("__rbac_user"), Some(&"u1".to_string()));
    assert_eq!(params.get("__rbac_role"), Some(&"r1".to_string()));
    assert_eq!(
        params.get("__auth_scope"),
        Some(&"select:*.*.*".to_string())
    );
}

#[tokio::test]
async fn dependent_stage_receives_upstream_flight_endpoint_map() {
    let mut stage0 = stage_group(0, 0, 1, Vec::new());
    stage0.output_destinations = vec![OutputDestination {
        downstream_stage_id: 1,
        worker_addresses: vec!["http://10.0.0.9:4100".to_string()],
        partitioning: PartitionSpec::Single,
        downstream_partition_count: 1,
    }];

    let stage1 = stage_group(1, 0, 1, vec![0]);
    let stage_groups = vec![stage0, stage1];

    let captured = Arc::new(tokio::sync::Mutex::new(Vec::<StageTaskGroup>::new()));
    let captured_for_executor = captured.clone();

    let execute_partition =
        std::sync::Arc::new(move |group: StageTaskGroup, _auth_ctx, _timeout_secs| {
            let captured_for_executor = captured_for_executor.clone();
            Box::pin(async move {
                captured_for_executor.lock().await.push(group.clone());
                Ok::<String, Box<dyn std::error::Error + Send + Sync>>(stage_result_location(
                    &group,
                ))
            }) as super::StagePartitionFuture
        });

    let _ = super::run_stage_groups_with_partition_executor(
        &stage_groups,
        Some(&auth_ctx_with_query_id("q-endpoint-map")),
        1,
        execute_partition,
    )
    .await
    .expect("dependent stage should receive upstream endpoint map");

    let values = captured.lock().await;
    let stage1_params = values
        .iter()
        .find(|entry| entry.stage_id == 1)
        .map(|entry| &entry.params)
        .expect("stage 1 params should be captured");

    let payload = stage1_params
        .get("__upstream_stage_flight_endpoints_json")
        .expect("stage 1 should receive upstream endpoint map payload");
    let parsed: serde_json::Value =
        serde_json::from_str(payload).expect("endpoint payload should be valid JSON");

    let configured_flight_port = std::env::var("WORKER_FLIGHT_PORT")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(4101);
    let expected_endpoint = format!("http://10.0.0.9:{}", configured_flight_port);

    assert_eq!(
        parsed
            .get("0")
            .and_then(|value| value.get("0"))
            .and_then(serde_json::Value::as_str),
        Some(expected_endpoint.as_str())
    );
}
