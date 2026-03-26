use super::{
    ScanDecisionContext, ScanPruningDecisionFields, StageRuntimeTelemetryFields, StorageErrorClass,
    build_empty_batch_from_relation_columns, classify_storage_error, deduplicate_union_batches,
    format_exchange_io_decision_event, format_exchange_retry_decision_event,
    format_materialization_decision_event, format_scan_fallback_reason_event,
    format_scan_mode_chosen_event, format_scan_pruning_decision_event, format_stage_runtime_event,
    load_scan_batches, load_upstream_exchange_batches, maybe_prune_scan_keys,
    parse_partition_index_from_exchange_key, select_exchange_keys_for_downstream_partition,
    validate_upstream_exchange_partition_set,
};
use crate::execution::artifacts::persist_stage_exchange_artifacts;
use crate::execution::planner::StageExecutionContext;
use crate::execution::planner::{RuntimeScanHints, RuntimeScanMode};
use crate::state::SharedData;
use crate::state::WorkerInformation;
use crate::storage::StorageProvider;
use crate::storage::mock::MockProvider;
use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

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

fn sample_shared_data_with_storage(provider: Arc<MockProvider>) -> SharedData {
    let worker_info = WorkerInformation {
        worker_id: "worker-1".to_string(),
        host: "127.0.0.1".to_string(),
        port: 32001,
        server_url: "http://127.0.0.1:32001".to_string(),
        tls_cert_path: String::new(),
        tls_key_path: String::new(),
        ca_cert_path: String::new(),
    };

    let mut shared = SharedData::new(worker_info, kionas::consul::ClusterInfo::default());
    shared.set_storage_provider(provider);
    shared
}

fn single_value_batch(value: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![value])) as ArrayRef],
    )
    .expect("single-row batch should build")
}

#[test]
fn deduplicate_union_batches_keeps_first_occurrence() {
    let input = vec![
        single_value_batch(1),
        single_value_batch(1),
        single_value_batch(2),
    ];

    let deduplicated =
        deduplicate_union_batches(&input).expect("union distinct deduplication should succeed");
    let row_count = deduplicated
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();

    assert_eq!(row_count, 2);
}

#[test]
fn deduplicate_union_batches_preserves_unique_rows() {
    let input = vec![
        single_value_batch(10),
        single_value_batch(11),
        single_value_batch(12),
    ];

    let deduplicated =
        deduplicate_union_batches(&input).expect("union deduplication should succeed");
    let row_count = deduplicated
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();

    assert_eq!(row_count, 3);
}

#[derive(Debug, Clone)]
struct TestStorageError(String);

impl fmt::Display for TestStorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Error for TestStorageError {}

#[derive(Default)]
struct FlakyProvider {
    list_failures_remaining: AtomicUsize,
    get_failures_remaining: AtomicUsize,
    map: tokio::sync::Mutex<HashMap<String, Vec<u8>>>,
}

impl FlakyProvider {
    fn new(list_failures: usize, get_failures: usize) -> Self {
        Self {
            list_failures_remaining: AtomicUsize::new(list_failures),
            get_failures_remaining: AtomicUsize::new(get_failures),
            map: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    async fn seed(&self, key: &str, value: Vec<u8>) {
        let mut guard = self.map.lock().await;
        guard.insert(key.to_string(), value);
    }
}

#[async_trait]
impl StorageProvider for FlakyProvider {
    async fn put_object(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut guard = self.map.lock().await;
        guard.insert(key.to_string(), data);
        Ok(())
    }

    async fn get_object(
        &self,
        key: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        if self
            .get_failures_remaining
            .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |count| {
                if count == 0 { None } else { Some(count - 1) }
            })
            .is_ok()
        {
            return Err(Box::new(TestStorageError(
                "timeout while reading object".to_string(),
            )));
        }

        let guard = self.map.lock().await;
        Ok(guard.get(key).cloned())
    }

    async fn list_objects(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        if self
            .list_failures_remaining
            .fetch_update(AtomicOrdering::SeqCst, AtomicOrdering::SeqCst, |count| {
                if count == 0 { None } else { Some(count - 1) }
            })
            .is_ok()
        {
            return Err(Box::new(TestStorageError(
                "temporarily unavailable during list".to_string(),
            )));
        }

        let guard = self.map.lock().await;
        let mut keys = Vec::new();
        for key in guard.keys() {
            if key.starts_with(prefix) {
                keys.push(key.clone());
            }
        }
        Ok(keys)
    }

    async fn delete_object(
        &self,
        key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut guard = self.map.lock().await;
        guard.remove(key);
        Ok(())
    }
}

#[derive(Default)]
struct NonRetriableProvider {
    list_calls: AtomicUsize,
    get_calls: AtomicUsize,
}

impl NonRetriableProvider {
    fn new() -> Self {
        Self {
            list_calls: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
        }
    }
}

#[async_trait]
impl StorageProvider for NonRetriableProvider {
    async fn put_object(
        &self,
        _key: &str,
        _data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
    }

    async fn get_object(
        &self,
        _key: &str,
    ) -> Result<Option<Vec<u8>>, Box<dyn std::error::Error + Send + Sync>> {
        self.get_calls.fetch_add(1, AtomicOrdering::SeqCst);
        Err(Box::new(TestStorageError(
            "invalid object key format".to_string(),
        )))
    }

    async fn list_objects(
        &self,
        _prefix: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
        self.list_calls.fetch_add(1, AtomicOrdering::SeqCst);
        Err(Box::new(TestStorageError(
            "invalid object key format".to_string(),
        )))
    }

    async fn delete_object(
        &self,
        _key: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        Ok(())
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
fn formats_exchange_retry_decision_event_with_required_dimensions() {
    let event = format_exchange_retry_decision_event(
        "q-retry",
        11,
        "list",
        2,
        "retrying",
        "retriable",
        "upstream_stage=9",
    );

    assert!(event.starts_with("event=execution.exchange_retry_decision "));
    assert!(event.contains("query_id=q-retry"));
    assert!(event.contains("stage_id=11"));
    assert!(event.contains("operator_family=exchange"));
    assert!(event.contains("category=execution"));
    assert!(event.contains("origin=worker_pipeline"));
    assert!(event.contains("operation=list"));
    assert!(event.contains("attempt=2"));
    assert!(event.contains("outcome=retrying"));
    assert!(event.contains("failure_class=retriable"));
    assert!(event.contains("target=upstream_stage=9"));
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

#[test]
fn selects_exchange_keys_for_one_to_one_partition_mapping() {
    let keys = vec![
        "exchange/q1/s1/stage_7/part-00000.parquet".to_string(),
        "exchange/q1/s1/stage_7/part-00001.parquet".to_string(),
        "exchange/q1/s1/stage_7/part-00002.parquet".to_string(),
    ];

    let selected = select_exchange_keys_for_downstream_partition(7, &keys, 3, 3, 1)
        .expect("one-to-one partition mapping should select exact partition index");

    assert_eq!(
        selected,
        vec!["exchange/q1/s1/stage_7/part-00001.parquet".to_string()]
    );
}

#[test]
fn selects_exchange_keys_for_many_to_one_partition_mapping() {
    let keys = vec![
        "exchange/q2/s1/stage_9/part-00000.parquet".to_string(),
        "exchange/q2/s1/stage_9/part-00001.parquet".to_string(),
        "exchange/q2/s1/stage_9/part-00002.parquet".to_string(),
        "exchange/q2/s1/stage_9/part-00003.parquet".to_string(),
        "exchange/q2/s1/stage_9/part-00004.parquet".to_string(),
    ];

    let selected = select_exchange_keys_for_downstream_partition(9, &keys, 5, 2, 1)
        .expect("many-to-one partition mapping should select modulo-matched upstream partitions");

    assert_eq!(
        selected,
        vec![
            "exchange/q2/s1/stage_9/part-00001.parquet".to_string(),
            "exchange/q2/s1/stage_9/part-00003.parquet".to_string(),
        ]
    );
}

#[test]
fn many_to_one_partition_mapping_covers_all_upstream_partitions_without_overlap() {
    let keys = vec![
        "exchange/q3/s1/stage_11/part-00000.parquet".to_string(),
        "exchange/q3/s1/stage_11/part-00001.parquet".to_string(),
        "exchange/q3/s1/stage_11/part-00002.parquet".to_string(),
        "exchange/q3/s1/stage_11/part-00003.parquet".to_string(),
        "exchange/q3/s1/stage_11/part-00004.parquet".to_string(),
    ];

    let selected_part0 = select_exchange_keys_for_downstream_partition(11, &keys, 5, 2, 0)
        .expect("partition 0 mapping should succeed");
    let selected_part1 = select_exchange_keys_for_downstream_partition(11, &keys, 5, 2, 1)
        .expect("partition 1 mapping should succeed");

    let set0 = selected_part0
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let set1 = selected_part1
        .iter()
        .cloned()
        .collect::<std::collections::BTreeSet<_>>();
    let overlap = set0.intersection(&set1).cloned().collect::<Vec<_>>();
    assert!(overlap.is_empty());

    let union = set0.union(&set1).cloned().collect::<Vec<_>>();
    assert_eq!(union.len(), keys.len());
    for key in keys {
        assert!(union.iter().any(|candidate| candidate == &key));
    }
}

#[tokio::test]
async fn load_upstream_exchange_batches_applies_many_to_one_distribution_mapping() {
    let provider = MockProvider::new().into_arc();
    let shared = sample_shared_data_with_storage(provider.clone());
    let session_id = "sess-many-to-one";
    let query_run_id = "run-many-to-one";
    let upstream_stage_id = 9_u32;

    for partition_index in 0..5_u32 {
        let batch = single_value_batch(i64::from(partition_index));
        persist_stage_exchange_artifacts(
            &shared,
            session_id,
            query_run_id,
            upstream_stage_id,
            partition_index,
            5,
            &[],
            &[batch],
        )
        .await
        .expect("upstream exchange artifact should persist");
    }

    let mut upstream_partition_counts = HashMap::new();
    upstream_partition_counts.insert(upstream_stage_id, 5_u32);
    let stage_context = StageExecutionContext {
        stage_id: 20,
        upstream_stage_ids: vec![upstream_stage_id],
        upstream_partition_counts,
        partition_count: 2,
        partition_index: 1,
        query_run_id: query_run_id.to_string(),
        scan_hints: RuntimeScanHints::full_scan(),
    };

    let batches = load_upstream_exchange_batches(&shared, session_id, &stage_context)
        .await
        .expect("many-to-one exchange routing should load mapped upstream partitions");

    let mut values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("column should be Int64")
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    values.sort_unstable();

    assert_eq!(values, vec![1_i64, 3_i64]);
}

#[tokio::test]
async fn load_upstream_exchange_batches_many_to_one_distribution_covers_all_rows() {
    let provider = MockProvider::new().into_arc();
    let shared = sample_shared_data_with_storage(provider.clone());
    let session_id = "sess-many-to-one-cover";
    let query_run_id = "run-many-to-one-cover";
    let upstream_stage_id = 17_u32;

    for partition_index in 0..5_u32 {
        let batch = single_value_batch(i64::from(partition_index));
        persist_stage_exchange_artifacts(
            &shared,
            session_id,
            query_run_id,
            upstream_stage_id,
            partition_index,
            5,
            &[],
            &[batch],
        )
        .await
        .expect("upstream exchange artifact should persist");
    }

    let mut upstream_partition_counts = HashMap::new();
    upstream_partition_counts.insert(upstream_stage_id, 5_u32);

    let mut loaded_values = Vec::<i64>::new();
    for partition_index in 0..2_u32 {
        let stage_context = StageExecutionContext {
            stage_id: 21,
            upstream_stage_ids: vec![upstream_stage_id],
            upstream_partition_counts: upstream_partition_counts.clone(),
            partition_count: 2,
            partition_index,
            query_run_id: query_run_id.to_string(),
            scan_hints: RuntimeScanHints::full_scan(),
        };

        let batches = load_upstream_exchange_batches(&shared, session_id, &stage_context)
            .await
            .expect("many-to-one exchange routing should load mapped upstream partitions");

        loaded_values.extend(batches.iter().flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("column should be Int64")
                .iter()
                .flatten()
                .collect::<Vec<_>>()
        }));
    }

    loaded_values.sort_unstable();
    assert_eq!(loaded_values, vec![0_i64, 1_i64, 2_i64, 3_i64, 4_i64]);
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

#[test]
fn classifies_transient_storage_errors_as_retriable() {
    assert_eq!(
        classify_storage_error("timeout while reading object"),
        StorageErrorClass::Retriable
    );
    assert_eq!(
        classify_storage_error("connection reset by peer"),
        StorageErrorClass::Retriable
    );
    assert_eq!(
        classify_storage_error("invalid object key format"),
        StorageErrorClass::NonRetriable
    );
}

#[tokio::test]
async fn retries_exchange_list_then_succeeds() {
    use super::list_stage_exchange_data_keys_with_retry;

    let provider = Arc::new(FlakyProvider::new(2, 0));
    provider
        .seed(
            "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet",
            b"p0".to_vec(),
        )
        .await;
    provider
        .seed(
            "distributed_exchange/run-1/sess-1/stage-2/part-00000.metadata.json",
            b"{}".to_vec(),
        )
        .await;

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();

    let keys = list_stage_exchange_data_keys_with_retry(&provider_storage, "run-1", "sess-1", 2)
        .await
        .expect("retriable list should eventually succeed");

    assert_eq!(keys.len(), 1);
    assert!(keys[0].ends_with("part-00000.parquet"));
}

#[tokio::test]
async fn retries_exchange_get_until_exhaustion() {
    use super::get_exchange_artifact_with_retry;

    let provider = Arc::new(FlakyProvider::new(0, 5));
    provider
        .seed(
            "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet",
            b"p0".to_vec(),
        )
        .await;

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();

    let err = get_exchange_artifact_with_retry(
        &provider_storage,
        "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet",
    )
    .await
    .expect_err("persistent retriable failures must exhaust retries");

    assert!(err.contains("after 3 attempt(s)"));
}

#[tokio::test]
async fn retries_exchange_get_then_succeeds() {
    use super::get_exchange_artifact_with_retry;

    let provider = Arc::new(FlakyProvider::new(0, 2));
    provider
        .seed(
            "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet",
            b"ok".to_vec(),
        )
        .await;

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();

    let bytes = get_exchange_artifact_with_retry(
        &provider_storage,
        "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet",
    )
    .await
    .expect("transient get failures should recover")
    .expect("artifact should exist");

    assert_eq!(bytes, b"ok".to_vec());
}

#[tokio::test]
async fn non_retriable_exchange_list_fails_without_retry() {
    use super::list_stage_exchange_data_keys_with_retry;

    let provider = Arc::new(NonRetriableProvider::new());
    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();

    let err = list_stage_exchange_data_keys_with_retry(&provider_storage, "run-1", "sess-1", 2)
        .await
        .expect_err("non-retriable list failure must fail immediately");

    assert!(err.contains("after 1 attempt(s)"));
    assert_eq!(provider.list_calls.load(AtomicOrdering::SeqCst), 1);
}

#[tokio::test]
async fn non_retriable_exchange_get_fails_without_retry() {
    use super::get_exchange_artifact_with_retry;

    let provider = Arc::new(NonRetriableProvider::new());
    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();

    let err = get_exchange_artifact_with_retry(
        &provider_storage,
        "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet",
    )
    .await
    .expect_err("non-retriable get failure must fail immediately");

    assert!(err.contains("after 1 attempt(s)"));
    assert_eq!(provider.get_calls.load(AtomicOrdering::SeqCst), 1);
}

#[tokio::test]
async fn validates_exchange_artifact_with_matching_metadata() {
    use super::load_validated_exchange_artifact_with_retry;

    let provider = MockProvider::new().into_arc();
    let data_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet";
    let metadata_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.metadata.json";

    provider
        .put_object(data_key, b"abc".to_vec())
        .await
        .expect("data must seed");
    provider
        .put_object(
            metadata_key,
            serde_json::json!({
                "artifacts": [{
                    "key": data_key,
                    "size_bytes": 3,
                    "checksum_fnv64": "e71fa2190541574b"
                }]
            })
            .to_string()
            .into_bytes(),
        )
        .await
        .expect("metadata must seed");

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();
    let bytes = load_validated_exchange_artifact_with_retry(
        &provider_storage,
        "run-1",
        "sess-1",
        2,
        data_key,
    )
    .await
    .expect("matching metadata should validate");

    assert_eq!(bytes, b"abc".to_vec());
}

#[tokio::test]
async fn rejects_exchange_artifact_with_checksum_mismatch() {
    use super::load_validated_exchange_artifact_with_retry;

    let provider = MockProvider::new().into_arc();
    let data_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet";
    let metadata_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.metadata.json";

    provider
        .put_object(data_key, b"abc".to_vec())
        .await
        .expect("data must seed");
    provider
        .put_object(
            metadata_key,
            serde_json::json!({
                "artifacts": [{
                    "key": data_key,
                    "size_bytes": 3,
                    "checksum_fnv64": "0000000000000000"
                }]
            })
            .to_string()
            .into_bytes(),
        )
        .await
        .expect("metadata must seed");

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();
    let err = load_validated_exchange_artifact_with_retry(
        &provider_storage,
        "run-1",
        "sess-1",
        2,
        data_key,
    )
    .await
    .expect_err("checksum mismatch must fail validation");

    assert!(err.contains("exchange artifact checksum mismatch"));
}

#[tokio::test]
async fn rejects_exchange_artifact_when_metadata_is_missing() {
    use super::load_validated_exchange_artifact_with_retry;

    let provider = MockProvider::new().into_arc();
    let data_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet";

    provider
        .put_object(data_key, b"abc".to_vec())
        .await
        .expect("data must seed");

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();
    let err = load_validated_exchange_artifact_with_retry(
        &provider_storage,
        "run-1",
        "sess-1",
        2,
        data_key,
    )
    .await
    .expect_err("missing metadata must fail validation");

    assert!(err.contains("exchange metadata not found"));
}

#[tokio::test]
async fn rejects_exchange_artifact_with_size_mismatch() {
    use super::load_validated_exchange_artifact_with_retry;

    let provider = MockProvider::new().into_arc();
    let data_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet";
    let metadata_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.metadata.json";

    provider
        .put_object(data_key, b"abc".to_vec())
        .await
        .expect("data must seed");
    provider
        .put_object(
            metadata_key,
            serde_json::json!({
                "artifacts": [{
                    "key": data_key,
                    "size_bytes": 99,
                    "checksum_fnv64": "e71fa2190541574b"
                }]
            })
            .to_string()
            .into_bytes(),
        )
        .await
        .expect("metadata must seed");

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();
    let err = load_validated_exchange_artifact_with_retry(
        &provider_storage,
        "run-1",
        "sess-1",
        2,
        data_key,
    )
    .await
    .expect_err("size mismatch must fail validation");

    assert!(err.contains("exchange artifact size mismatch"));
}

#[tokio::test]
async fn recovers_exchange_artifact_validation_after_transient_metadata_read_failure() {
    use super::load_validated_exchange_artifact_with_retry;

    let provider = Arc::new(FlakyProvider::new(0, 1));
    let data_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet";
    let metadata_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.metadata.json";

    provider.seed(data_key, b"abc".to_vec()).await;
    provider
        .seed(
            metadata_key,
            serde_json::json!({
                "artifacts": [{
                    "key": data_key,
                    "size_bytes": 3,
                    "checksum_fnv64": "e71fa2190541574b"
                }]
            })
            .to_string()
            .into_bytes(),
        )
        .await;

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();
    let bytes = load_validated_exchange_artifact_with_retry(
        &provider_storage,
        "run-1",
        "sess-1",
        2,
        data_key,
    )
    .await
    .expect("transient metadata read failure should recover");

    assert_eq!(bytes, b"abc".to_vec());
}

#[tokio::test]
async fn fails_exchange_artifact_validation_after_persistent_metadata_read_failure() {
    use super::load_validated_exchange_artifact_with_retry;

    let provider = Arc::new(FlakyProvider::new(0, 5));
    let data_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.parquet";
    let metadata_key = "distributed_exchange/run-1/sess-1/stage-2/part-00000.metadata.json";

    provider.seed(data_key, b"abc".to_vec()).await;
    provider.seed(metadata_key, b"{}".to_vec()).await;

    let provider_storage: Arc<dyn StorageProvider + Send + Sync> = provider.clone();
    let err = load_validated_exchange_artifact_with_retry(
        &provider_storage,
        "run-1",
        "sess-1",
        2,
        data_key,
    )
    .await
    .expect_err("persistent metadata read failure must exhaust retries");

    assert!(err.contains("after 3 attempt(s)"));
    assert!(err.contains("part-00000.metadata.json"));
}
