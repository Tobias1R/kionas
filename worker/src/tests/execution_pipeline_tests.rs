use super::{
    ScanDecisionContext, ScanPruningDecisionFields, StageRuntimeTelemetryFields,
    build_empty_batch_from_relation_columns, format_exchange_io_decision_event,
    format_materialization_decision_event, format_scan_fallback_reason_event,
    format_scan_mode_chosen_event, format_scan_pruning_decision_event, format_stage_runtime_event,
    load_scan_batches, maybe_prune_scan_keys, parse_partition_index_from_exchange_key,
    validate_upstream_exchange_partition_set,
};
use crate::execution::planner::{RuntimeScanHints, RuntimeScanMode};
use crate::state::SharedData;
use crate::storage::StorageProvider;
use crate::storage::mock::MockProvider;
use std::sync::Arc;

fn metadata_pruned_hints(predicate_ast: serde_json::Value, pin: u64) -> RuntimeScanHints {
    RuntimeScanHints {
        mode: RuntimeScanMode::MetadataPruned,
        pruning_hints_json: Some(
            serde_json::json!({
                "hint_version": 2,
                "eligible": true,
                "reason": "eligible_foundation_and_comparisons",
                "predicate_ast": predicate_ast,
            })
            .to_string(),
        ),
        delta_version_pin: Some(pin),
        pruning_eligible: true,
        pruning_reason: Some("eligible_foundation_and_comparisons".to_string()),
    }
}

fn as_storage(provider: Arc<MockProvider>) -> Arc<dyn StorageProvider + Send + Sync> {
    provider
}

fn metadata_pruned_hints_raw(raw: &str, pin: u64) -> RuntimeScanHints {
    RuntimeScanHints {
        mode: RuntimeScanMode::MetadataPruned,
        pruning_hints_json: Some(raw.to_string()),
        delta_version_pin: Some(pin),
        pruning_eligible: true,
        pruning_reason: Some("eligible_foundation_and_comparisons".to_string()),
    }
}

fn decision_context() -> ScanDecisionContext<'static> {
    ScanDecisionContext {
        query_id: "q-edge",
        stage_id: 1,
        task_id: "t-edge",
    }
}

#[test]
fn builds_empty_batch_from_relation_columns() {
    let batch = build_empty_batch_from_relation_columns(&["Id".to_string(), "Name".to_string()])
        .expect("empty batch should build");

    assert_eq!(batch.num_rows(), 0);
    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.schema().field(0).name(), "id");
    assert_eq!(batch.schema().field(1).name(), "name");
}

#[test]
fn formats_scan_mode_chosen_event_with_required_dimensions() {
    let event = format_scan_mode_chosen_event("q1", 3, "t1", "metadata_pruned");

    assert!(event.starts_with("event=execution.scan_mode_chosen "));
    assert!(event.contains("query_id=q1"));
    assert!(event.contains("stage_id=3"));
    assert!(event.contains("task_id=t1"));
    assert!(event.contains("operator_family=scan"));
    assert!(event.contains("category=execution"));
    assert!(event.contains("origin=worker_runtime_planner"));
    assert!(event.contains("scan_mode=metadata_pruned"));
}

#[test]
fn formats_scan_fallback_reason_event_with_required_dimensions() {
    let event = format_scan_fallback_reason_event("q2", 4, "t2", "missing_pruning_hints");

    assert!(event.starts_with("event=execution.scan_fallback_reason "));
    assert!(event.contains("query_id=q2"));
    assert!(event.contains("stage_id=4"));
    assert!(event.contains("task_id=t2"));
    assert!(event.contains("reason=missing_pruning_hints"));
}

#[test]
fn formats_exchange_io_decision_event_with_required_dimensions() {
    let event = format_exchange_io_decision_event("q3", 5, "t3", 2, "write");

    assert!(event.starts_with("event=execution.exchange_io_decision "));
    assert!(event.contains("query_id=q3"));
    assert!(event.contains("stage_id=5"));
    assert!(event.contains("task_id=t3"));
    assert!(event.contains("partition_index=2"));
    assert!(event.contains("operator_family=exchange"));
    assert!(event.contains("direction=write"));
}

#[test]
fn formats_materialization_decision_event_with_required_dimensions() {
    let event = format_materialization_decision_event("q4", 6, "t4", "parquet");

    assert!(event.starts_with("event=execution.materialization_decision "));
    assert!(event.contains("query_id=q4"));
    assert!(event.contains("stage_id=6"));
    assert!(event.contains("task_id=t4"));
    assert!(event.contains("operator_family=materialization"));
    assert!(event.contains("output_format=parquet"));
}

#[test]
fn formats_scan_pruning_decision_event_with_required_dimensions() {
    let ctx = ScanDecisionContext {
        query_id: "q5",
        stage_id: 7,
        task_id: "t5",
    };
    let event = format_scan_pruning_decision_event(
        &ctx,
        &ScanPruningDecisionFields {
            requested_mode: "metadata_pruned",
            effective_mode: "full",
            reason: "delta_pin_mismatch",
            delta_version_pin: Some(12),
            cache_hit: Some(true),
            total_files: 10,
            retained_files: 10,
        },
    );

    assert!(event.starts_with("event=execution.scan_pruning_decision "));
    assert!(event.contains("query_id=q5"));
    assert!(event.contains("stage_id=7"));
    assert!(event.contains("task_id=t5"));
    assert!(event.contains("requested_mode=metadata_pruned"));
    assert!(event.contains("effective_mode=full"));
    assert!(event.contains("reason=delta_pin_mismatch"));
    assert!(event.contains("delta_version_pin=12"));
    assert!(event.contains("cache_hit=hit"));
    assert!(event.contains("total_files=10"));
    assert!(event.contains("retained_files=10"));
}

#[test]
fn formats_stage_runtime_event_with_required_dimensions_and_metrics() {
    let event = format_stage_runtime_event(
        "q6",
        8,
        "t6",
        &StageRuntimeTelemetryFields {
            operator_family: "stage",
            origin: "worker_pipeline",
            stage_runtime_ms: 123,
            operator_rows_in: 100,
            operator_rows_out: 80,
            batch_count: 4,
            artifact_bytes: 4096,
        },
    );

    assert!(event.starts_with("event=execution.stage_runtime "));
    assert!(event.contains("query_id=q6"));
    assert!(event.contains("stage_id=8"));
    assert!(event.contains("task_id=t6"));
    assert!(event.contains("operator_family=stage"));
    assert!(event.contains("category=execution"));
    assert!(event.contains("origin=worker_pipeline"));
    assert!(event.contains("stage_runtime_ms=123"));
    assert!(event.contains("operator_rows_in=100"));
    assert!(event.contains("operator_rows_out=80"));
    assert!(event.contains("batch_count=4"));
    assert!(event.contains("artifact_bytes=4096"));
}

#[test]
fn parses_exchange_partition_index_from_key() {
    let idx = parse_partition_index_from_exchange_key("exchange/stage_5/part-00007.parquet");
    assert_eq!(idx, Some(7));
}

#[test]
fn rejects_exchange_partition_key_with_invalid_shape() {
    let idx = parse_partition_index_from_exchange_key("exchange/stage_5/not-a-partition.txt");
    assert_eq!(idx, None);
}

#[test]
fn validates_upstream_exchange_partition_set_success() {
    let keys = vec![
        "exchange/stage_3/part-00000.parquet".to_string(),
        "exchange/stage_3/part-00001.parquet".to_string(),
    ];

    validate_upstream_exchange_partition_set(3, &keys, 2)
        .expect("partition coverage should be complete");
}

#[test]
fn rejects_upstream_exchange_partition_set_with_missing_partition() {
    let keys = vec!["exchange/stage_3/part-00000.parquet".to_string()];

    let err = validate_upstream_exchange_partition_set(3, &keys, 2)
        .expect_err("missing partition must be rejected");
    assert!(err.contains("exchange artifact count mismatch"));
}

#[tokio::test]
async fn pruning_falls_back_on_pin_mismatch() {
    let provider = MockProvider::new().into_arc();
    provider
        .put_object(
            "databases/sales/schemas/public/tables/users/_delta_log/00000000000000000001.json",
            b"{}".to_vec(),
        )
        .await
        .expect("delta log should be seeded");

    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = metadata_pruned_hints(
        serde_json::json!({
            "kind": "comparison",
            "column": "id",
            "op": ">",
            "value": "5",
        }),
        99,
    );

    let outcome = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(outcome.keys, keys);
    assert_eq!(outcome.reason, "delta_pin_mismatch");
    assert_eq!(outcome.effective_mode, RuntimeScanMode::Full);
}

#[tokio::test]
async fn pruning_falls_back_when_stats_missing() {
    let provider = MockProvider::new().into_arc();
    provider
        .put_object(
            "databases/sales/schemas/public/tables/users/_delta_log/00000000000000000001.json",
            br#"{"add":{"path":"staging/a.parquet"}}"#.to_vec(),
        )
        .await
        .expect("delta log should be seeded");

    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = metadata_pruned_hints(
        serde_json::json!({
            "kind": "comparison",
            "column": "id",
            "op": ">",
            "value": "5",
        }),
        1,
    );

    let outcome = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(outcome.keys, keys);
    assert_eq!(outcome.reason, "delta_stats_unavailable");
    assert_eq!(outcome.effective_mode, RuntimeScanMode::Full);
}

#[tokio::test]
async fn pruning_removes_definitely_false_file() {
    let provider = MockProvider::new().into_arc();
    provider
        .put_object(
            "databases/sales/schemas/public/tables/users/_delta_log/00000000000000000001.json",
            br#"
{"add":{"path":"staging/a.parquet","stats":"{\"minValues\":{\"id\":1},\"maxValues\":{\"id\":3}}"}}
{"add":{"path":"staging/b.parquet","stats":"{\"minValues\":{\"id\":9},\"maxValues\":{\"id\":10}}"}}
"#
            .to_vec(),
        )
        .await
        .expect("delta log should be seeded");

    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = metadata_pruned_hints(
        serde_json::json!({
            "kind": "comparison",
            "column": "id",
            "op": ">",
            "value": "5",
        }),
        1,
    );

    let outcome = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(outcome.reason, "pruning_applied");
    assert_eq!(outcome.effective_mode, RuntimeScanMode::MetadataPruned);
    assert_eq!(outcome.keys.len(), 1);
    assert_eq!(
        outcome.keys[0],
        "databases/sales/schemas/public/tables/users/staging/b.parquet"
    );
}

#[tokio::test]
async fn pruning_falls_back_when_hints_are_invalid_json() {
    let provider = MockProvider::new().into_arc();
    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = metadata_pruned_hints_raw("{invalid-json", 1);

    let outcome = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(outcome.keys, keys);
    assert_eq!(outcome.reason, "hints_invalid");
    assert_eq!(outcome.effective_mode, RuntimeScanMode::Full);
}

#[tokio::test]
async fn pruning_returns_mode_full_requested_when_mode_is_full() {
    let provider = MockProvider::new().into_arc();
    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = RuntimeScanHints {
        mode: RuntimeScanMode::Full,
        pruning_hints_json: Some("{}".to_string()),
        delta_version_pin: Some(1),
        pruning_eligible: true,
        pruning_reason: Some("eligible_foundation_and_comparisons".to_string()),
    };

    let outcome = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(outcome.keys, keys);
    assert_eq!(outcome.reason, "mode_full_requested");
    assert_eq!(outcome.effective_mode, RuntimeScanMode::Full);
}

#[tokio::test]
async fn load_scan_batches_rejects_empty_source_for_full_mode() {
    let provider = MockProvider::new().into_arc();
    let hints = RuntimeScanHints::full_scan();

    let err = load_scan_batches(
        &SharedData {
            storage_provider: Some(as_storage(provider)),
            ..SharedData::default()
        },
        "databases/sales/schemas/public/tables/users/staging/",
        &hints,
        &decision_context(),
    )
    .await
    .expect_err("empty source should fail with explicit message");

    assert!(
        err.contains("no source parquet files found for query prefix"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn load_scan_batches_rejects_empty_source_for_metadata_pruned_mode() {
    let provider = MockProvider::new().into_arc();
    let hints = metadata_pruned_hints(
        serde_json::json!({
            "kind": "comparison",
            "column": "id",
            "op": ">",
            "value": "5",
        }),
        1,
    );

    let err = load_scan_batches(
        &SharedData {
            storage_provider: Some(as_storage(provider)),
            ..SharedData::default()
        },
        "databases/sales/schemas/public/tables/users/staging/",
        &hints,
        &decision_context(),
    )
    .await
    .expect_err("empty source should fail with explicit message");

    assert!(
        err.contains("no source parquet files found for query prefix"),
        "unexpected error: {err}"
    );
}
