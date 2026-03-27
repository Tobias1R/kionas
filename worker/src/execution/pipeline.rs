use crate::execution::aggregate::{
    apply_aggregate_final_pipeline, apply_aggregate_partial_pipeline,
};
use crate::execution::artifacts::{persist_query_artifacts, persist_stage_exchange_artifacts};
use crate::execution::join::{apply_hash_join_pipeline, apply_nested_loop_join_pipeline};
use crate::execution::planner::StageExecutionContext;
use crate::execution::planner::{
    RuntimeJoinPlan, RuntimeUnionSpec, extract_runtime_plan, stage_execution_context,
};
use crate::execution::planner::{RuntimeScanHints, RuntimeScanMode};
use crate::execution::query::QueryNamespace;
use crate::execution::router::{route_batch_from_output_partitioning, route_batch_roundrobin};
use crate::execution::window::apply_window_pipeline;
use crate::services::query_execution::{
    apply_filter_predicate_pipeline, apply_limit_pipeline, apply_projection_pipeline,
    apply_sort_pipeline, normalize_batches_for_sort, resolve_schema_column_index,
};
use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;
use crate::storage::StorageProvider;
use crate::storage::exchange::{list_stage_exchange_data_keys, stage_exchange_metadata_key};
use crate::telemetry::{
    StageExecutionTelemetry, StageLatencyMetrics, StageMemoryMetrics, StageNetworkMetrics,
};
use arrow::array::StringArray;
use arrow::array::{BooleanArray, new_empty_array};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use bytes::Bytes;
use kionas::planner::render_predicate_expr;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[derive(Debug, Clone)]
struct PredicateComparisonNode {
    column: String,
    op: String,
    value: String,
}

#[derive(Debug, Clone)]
enum PredicateAstNode {
    Comparison(PredicateComparisonNode),
    And(Vec<PredicateAstNode>),
}

#[derive(Debug, Clone)]
enum PredicateLiteralValue {
    Number(f64),
    String(String),
    Bool(bool),
}

#[derive(Debug, Clone)]
struct DeltaFileStats {
    min_values: serde_json::Map<String, Value>,
    max_values: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone)]
struct DeltaPruningCacheEntry {
    cached_at: Instant,
    latest_version: u64,
    file_stats: HashMap<String, DeltaFileStats>,
}

const DELTA_PRUNING_CACHE_TTL: Duration = Duration::from_secs(30);
const EXCHANGE_IO_MAX_ATTEMPTS: usize = 3;
const EXCHANGE_IO_RETRY_DELAY_MS: u64 = 25;

static DELTA_PRUNING_CACHE: LazyLock<Mutex<HashMap<String, DeltaPruningCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// What: Storage error class used by EP-5 exchange retry logic.
///
/// Inputs:
/// - Error message text from storage operations.
///
/// Output:
/// - A bounded class indicating whether retries are allowed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageErrorClass {
    Retriable,
    NonRetriable,
}

/// What: Classify storage operation failures for bounded retry decisions.
///
/// Inputs:
/// - `error_message`: Storage-layer error text.
///
/// Output:
/// - `StorageErrorClass::Retriable` for transient transport/system faults.
/// - `StorageErrorClass::NonRetriable` for permanent/semantic failures.
///
/// Details:
/// - Classification is intentionally conservative and string-based to stay backend-agnostic.
pub(crate) fn classify_storage_error(error_message: &str) -> StorageErrorClass {
    let normalized = error_message.to_ascii_lowercase();

    let retriable_signals = [
        "timeout",
        "timed out",
        "connection reset",
        "connection refused",
        "temporary",
        "temporarily unavailable",
        "unavailable",
        "too many requests",
        "throttl",
        "deadline exceeded",
        "broken pipe",
        "eof",
        "io error",
    ];

    if retriable_signals
        .iter()
        .any(|signal| normalized.contains(signal))
    {
        StorageErrorClass::Retriable
    } else {
        StorageErrorClass::NonRetriable
    }
}

/// What: List upstream stage exchange artifact keys with bounded retry semantics.
///
/// Inputs:
/// - `provider`: Configured storage provider.
/// - `query_run_id`: Query run identifier for exchange scope.
/// - `session_id`: Session identifier.
/// - `upstream_stage_id`: Upstream stage identifier.
///
/// Output:
/// - Sorted parquet exchange keys for the upstream stage, or explicit terminal error.
pub(crate) async fn list_stage_exchange_data_keys_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    query_run_id: &str,
    session_id: &str,
    upstream_stage_id: u32,
) -> Result<Vec<String>, String> {
    let mut attempt = 1usize;
    loop {
        match list_stage_exchange_data_keys(provider, query_run_id, session_id, upstream_stage_id)
            .await
        {
            Ok(keys) => return Ok(keys),
            Err(error) => {
                let msg = error.to_string();
                let class = classify_storage_error(&msg);
                let should_retry =
                    class == StorageErrorClass::Retriable && attempt < EXCHANGE_IO_MAX_ATTEMPTS;
                let failure_class = match class {
                    StorageErrorClass::Retriable => "retriable",
                    StorageErrorClass::NonRetriable => "non_retriable",
                };

                if should_retry {
                    log::warn!(
                        "{} reason={}",
                        format_exchange_retry_decision_event(
                            query_run_id,
                            upstream_stage_id,
                            "list",
                            attempt,
                            "retrying",
                            failure_class,
                            format!("upstream_stage={}", upstream_stage_id).as_str(),
                        ),
                        msg
                    );
                    sleep(Duration::from_millis(EXCHANGE_IO_RETRY_DELAY_MS)).await;
                    attempt += 1;
                    continue;
                }

                log::warn!(
                    "{} reason={}",
                    format_exchange_retry_decision_event(
                        query_run_id,
                        upstream_stage_id,
                        "list",
                        attempt,
                        "terminal",
                        failure_class,
                        format!("upstream_stage={}", upstream_stage_id).as_str(),
                    ),
                    msg
                );

                return Err(format!(
                    "failed to list exchange artifacts for upstream stage {} after {} attempt(s): {}",
                    upstream_stage_id, attempt, msg
                ));
            }
        }
    }
}

/// What: Read one exchange artifact with bounded retry semantics.
///
/// Inputs:
/// - `provider`: Configured storage provider.
/// - `key`: Artifact object key.
///
/// Output:
/// - Artifact bytes when found, `None` when missing, or explicit terminal error.
pub(crate) async fn get_exchange_artifact_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    key: &str,
) -> Result<Option<Vec<u8>>, String> {
    let mut attempt = 1usize;
    loop {
        match provider.get_object(key).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                let msg = error.to_string();
                let class = classify_storage_error(&msg);
                let should_retry =
                    class == StorageErrorClass::Retriable && attempt < EXCHANGE_IO_MAX_ATTEMPTS;
                let failure_class = match class {
                    StorageErrorClass::Retriable => "retriable",
                    StorageErrorClass::NonRetriable => "non_retriable",
                };

                if should_retry {
                    log::warn!(
                        "{} reason={}",
                        format_exchange_retry_decision_event(
                            "unknown",
                            0,
                            "get",
                            attempt,
                            "retrying",
                            failure_class,
                            key,
                        ),
                        msg
                    );
                    sleep(Duration::from_millis(EXCHANGE_IO_RETRY_DELAY_MS)).await;
                    attempt += 1;
                    continue;
                }

                log::warn!(
                    "{} reason={}",
                    format_exchange_retry_decision_event(
                        "unknown",
                        0,
                        "get",
                        attempt,
                        "terminal",
                        failure_class,
                        key,
                    ),
                    msg
                );

                return Err(format!(
                    "failed to read exchange artifact {} after {} attempt(s): {}",
                    key, attempt, msg
                ));
            }
        }
    }
}

/// What: Parse expected artifact size/checksum from exchange metadata sidecar.
///
/// Inputs:
/// - `metadata_bytes`: Metadata JSON bytes for one exchange partition.
/// - `data_key`: Expected parquet artifact key.
///
/// Output:
/// - `(size_bytes, checksum_fnv64)` expected for the parquet artifact.
pub(crate) fn expected_exchange_artifact_from_metadata(
    metadata_bytes: &[u8],
    data_key: &str,
) -> Result<(u64, String), String> {
    let metadata: serde_json::Value = serde_json::from_slice(metadata_bytes)
        .map_err(|e| format!("failed to parse exchange metadata JSON: {}", e))?;

    let artifacts = metadata
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "exchange metadata missing artifacts array".to_string())?;

    let artifact = artifacts
        .iter()
        .find(|entry| {
            entry
                .get("key")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == data_key)
        })
        .ok_or_else(|| {
            format!(
                "exchange metadata missing artifact entry for object {}",
                data_key
            )
        })?;

    let size_bytes = artifact
        .get("size_bytes")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            format!(
                "exchange metadata artifact missing size_bytes for object {}",
                data_key
            )
        })?;

    let checksum_fnv64 = artifact
        .get("checksum_fnv64")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            format!(
                "exchange metadata artifact missing checksum_fnv64 for object {}",
                data_key
            )
        })?
        .to_string();

    Ok((size_bytes, checksum_fnv64))
}

/// What: Compute deterministic FNV-1a 64-bit checksum as lowercase hex.
///
/// Inputs:
/// - `bytes`: Raw artifact bytes.
///
/// Output:
/// - 16-char lowercase hex checksum.
fn checksum_fnv64_hex(bytes: &[u8]) -> String {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }

    format!("{hash:016x}")
}

/// What: Load one exchange parquet artifact and validate it against metadata sidecar.
///
/// Inputs:
/// - `provider`: Configured storage provider.
/// - `query_run_id`: Query run identifier for exchange scope.
/// - `session_id`: Session identifier for exchange scope.
/// - `upstream_stage_id`: Upstream stage identifier.
/// - `data_key`: Parquet artifact key.
///
/// Output:
/// - Raw parquet bytes when size/checksum validation succeeds.
pub(crate) async fn load_validated_exchange_artifact_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    query_run_id: &str,
    session_id: &str,
    upstream_stage_id: u32,
    data_key: &str,
) -> Result<Vec<u8>, String> {
    let partition_index = parse_partition_index_from_exchange_key(data_key).ok_or_else(|| {
        format!(
            "exchange artifact has invalid partition key format: {}",
            data_key
        )
    })?;

    let metadata_key =
        stage_exchange_metadata_key(query_run_id, session_id, upstream_stage_id, partition_index);

    let metadata_bytes = get_exchange_artifact_with_retry(provider, &metadata_key)
        .await?
        .ok_or_else(|| format!("exchange metadata not found: {}", metadata_key))?;

    let (expected_size_bytes, expected_checksum) =
        expected_exchange_artifact_from_metadata(&metadata_bytes, data_key)?;

    let bytes = get_exchange_artifact_with_retry(provider, data_key)
        .await?
        .ok_or_else(|| format!("exchange artifact not found: {}", data_key))?;

    if expected_size_bytes != bytes.len() as u64 {
        return Err(format!(
            "exchange artifact size mismatch for {}: expected={} actual={}",
            data_key,
            expected_size_bytes,
            bytes.len()
        ));
    }

    let checksum = checksum_fnv64_hex(&bytes);
    if expected_checksum != checksum {
        return Err(format!(
            "exchange artifact checksum mismatch for {}",
            data_key
        ));
    }

    Ok(bytes)
}

/// What: Convert runtime scan mode enum into a stable diagnostics string value.
///
/// Inputs:
/// - `mode`: Runtime scan mode selected for the task.
///
/// Output:
/// - Stable snake_case label used in diagnostics events.
fn runtime_scan_mode_label(mode: &RuntimeScanMode) -> &'static str {
    match mode {
        RuntimeScanMode::Full => "full",
        RuntimeScanMode::MetadataPruned => "metadata_pruned",
    }
}

/// What: Context fields required to emit scan decision diagnostics.
///
/// Inputs:
/// - `query_id`: Query correlation identifier.
/// - `stage_id`: Stage identifier for the current task.
/// - `task_id`: Task identifier.
///
/// Output:
/// - Immutable view used by scan decision telemetry emission.
pub(crate) struct ScanDecisionContext<'a> {
    pub(crate) query_id: &'a str,
    pub(crate) stage_id: u32,
    pub(crate) task_id: &'a str,
}

/// What: Structured result from scan pruning evaluation.
///
/// Inputs:
/// - Internal pruning decision data.
///
/// Output:
/// - Effective mode, bounded reason, and resulting key set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanPruningOutcome {
    pub(crate) keys: Vec<String>,
    pub(crate) effective_mode: RuntimeScanMode,
    pub(crate) reason: &'static str,
    pub(crate) cache_hit: Option<bool>,
}

/// What: Inputs for rendering one scan pruning decision event.
///
/// Inputs:
/// - `requested_mode`: Requested scan mode from task params.
/// - `effective_mode`: Runtime mode actually used for file reads.
/// - `reason`: Bounded reason label for applied mode/fallback.
/// - `delta_version_pin`: Optional delta version pin provided by planner.
/// - `cache_hit`: Whether pruning metadata came from cache.
/// - `total_files`: Candidate file count before pruning.
/// - `retained_files`: File count after pruning decision.
///
/// Output:
/// - Struct consumed by `format_scan_pruning_decision_event`.
pub(crate) struct ScanPruningDecisionFields<'a> {
    pub(crate) requested_mode: &'a str,
    pub(crate) effective_mode: &'a str,
    pub(crate) reason: &'a str,
    pub(crate) delta_version_pin: Option<u64>,
    pub(crate) cache_hit: Option<bool>,
    pub(crate) total_files: usize,
    pub(crate) retained_files: usize,
}

/// What: Format `execution.scan_pruning_decision` diagnostics event payload.
///
/// Inputs:
/// - `ctx`: Query/stage/task context for this decision.
/// - `requested_mode`: Requested scan mode from task params.
/// - `effective_mode`: Runtime mode actually used for read path.
/// - `reason`: Bounded reason label.
/// - `delta_version_pin`: Optional delta pin from task params.
/// - `cache_hit`: Cache-hit state for pruning metadata.
/// - `total_files`: File count before pruning.
/// - `retained_files`: File count after pruning decision.
///
/// Output:
/// - Stable event line suitable for structured log parsing.
pub(crate) fn format_scan_pruning_decision_event(
    ctx: &ScanDecisionContext<'_>,
    fields: &ScanPruningDecisionFields<'_>,
) -> String {
    let cache_label = match fields.cache_hit {
        Some(true) => "hit",
        Some(false) => "miss",
        None => "n/a",
    };
    format!(
        "event=execution.scan_pruning_decision query_id={} stage_id={} task_id={} operator_family=scan category=execution origin=worker_scan requested_mode={} effective_mode={} reason={} delta_version_pin={} cache_hit={} total_files={} retained_files={} pruned_files={}",
        ctx.query_id,
        ctx.stage_id,
        ctx.task_id,
        fields.requested_mode,
        fields.effective_mode,
        fields.reason,
        fields.delta_version_pin.unwrap_or(0),
        cache_label,
        fields.total_files,
        fields.retained_files,
        fields.total_files.saturating_sub(fields.retained_files)
    )
}

/// What: Format `execution.scan_mode_chosen` diagnostics event payload.
///
/// Inputs:
/// - `query_id`: Query correlation id.
/// - `stage_id`: Stage identifier.
/// - `task_id`: Task identifier.
/// - `scan_mode`: Chosen scan mode label.
///
/// Output:
/// - Stable event line suitable for structured log parsing.
pub(crate) fn format_scan_mode_chosen_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    scan_mode: &str,
) -> String {
    format!(
        "event=execution.scan_mode_chosen query_id={} stage_id={} task_id={} operator_family=scan category=execution origin=worker_runtime_planner scan_mode={}",
        query_id, stage_id, task_id, scan_mode
    )
}

/// What: Format `execution.scan_fallback_reason` diagnostics event payload.
///
/// Inputs:
/// - `query_id`: Query correlation id.
/// - `stage_id`: Stage identifier.
/// - `task_id`: Task identifier.
/// - `reason`: Bounded fallback reason identifier.
///
/// Output:
/// - Stable event line suitable for structured log parsing.
pub(crate) fn format_scan_fallback_reason_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    reason: &str,
) -> String {
    format!(
        "event=execution.scan_fallback_reason query_id={} stage_id={} task_id={} operator_family=scan category=execution origin=worker_runtime_planner reason={}",
        query_id, stage_id, task_id, reason
    )
}

/// What: Format `execution.exchange_io_decision` diagnostics event payload.
///
/// Inputs:
/// - `query_id`: Query correlation id.
/// - `stage_id`: Stage identifier.
/// - `task_id`: Task identifier.
/// - `partition_index`: Partition index involved in exchange I/O.
/// - `direction`: Exchange direction (`read` or `write`).
///
/// Output:
/// - Stable event line suitable for structured log parsing.
pub(crate) fn format_exchange_io_decision_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    partition_index: u32,
    direction: &str,
) -> String {
    format!(
        "event=execution.exchange_io_decision query_id={} stage_id={} task_id={} partition_index={} operator_family=exchange category=execution origin=worker_pipeline direction={}",
        query_id, stage_id, task_id, partition_index, direction
    )
}

/// What: Format `execution.exchange_retry_decision` diagnostics event payload.
///
/// Inputs:
/// - `query_id`: Query correlation id.
/// - `stage_id`: Stage identifier.
/// - `operation`: Exchange operation name (`list`/`get`).
/// - `attempt`: 1-based attempt index.
/// - `outcome`: Retry outcome (`retrying`/`terminal`).
/// - `failure_class`: Classified storage failure class.
/// - `target`: Target identifier (stage or object key).
///
/// Output:
/// - Stable event line suitable for structured log parsing.
pub(crate) fn format_exchange_retry_decision_event(
    query_id: &str,
    stage_id: u32,
    operation: &str,
    attempt: usize,
    outcome: &str,
    failure_class: &str,
    target: &str,
) -> String {
    format!(
        "event=execution.exchange_retry_decision query_id={} stage_id={} operator_family=exchange category=execution origin=worker_pipeline operation={} attempt={} outcome={} failure_class={} target={}",
        query_id, stage_id, operation, attempt, outcome, failure_class, target,
    )
}

/// What: Format `execution.materialization_decision` diagnostics event payload.
///
/// Inputs:
/// - `query_id`: Query correlation id.
/// - `stage_id`: Stage identifier.
/// - `task_id`: Task identifier.
/// - `output_format`: Selected output representation.
///
/// Output:
/// - Stable event line suitable for structured log parsing.
pub(crate) fn format_materialization_decision_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    output_format: &str,
) -> String {
    format!(
        "event=execution.materialization_decision query_id={} stage_id={} task_id={} operator_family=materialization category=execution origin=worker_pipeline output_format={}",
        query_id, stage_id, task_id, output_format
    )
}

/// What: Fields required to emit EP-3 stage runtime telemetry.
///
/// Inputs:
/// - `operator_family`: Stable operator-family label.
/// - `origin`: Stable event origin label.
/// - `stage_runtime_ms`: End-to-end stage runtime in milliseconds.
/// - `operator_rows_in`: Input rows observed before operator pipeline transforms.
/// - `operator_rows_out`: Output rows emitted after normalization.
/// - `batch_count`: Output batch count after normalization.
/// - `artifact_bytes`: Persisted artifact bytes for exchange and optional materialization.
///
/// Output:
/// - Struct consumed by `format_stage_runtime_event`.
pub(crate) struct StageRuntimeTelemetryFields<'a> {
    pub(crate) operator_family: &'a str,
    pub(crate) origin: &'a str,
    pub(crate) stage_runtime_ms: u128,
    pub(crate) operator_rows_in: usize,
    pub(crate) operator_rows_out: usize,
    pub(crate) batch_count: usize,
    pub(crate) artifact_bytes: u64,
}

/// What: Format `execution.stage_runtime` telemetry event payload for EP-3.
///
/// Inputs:
/// - `query_id`: Query correlation id.
/// - `stage_id`: Stage identifier.
/// - `task_id`: Task identifier.
/// - `fields`: Structured telemetry fields emitted for operations.
///
/// Output:
/// - Stable event line suitable for structured log parsing.
pub(crate) fn format_stage_runtime_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    fields: &StageRuntimeTelemetryFields<'_>,
) -> String {
    format!(
        "event=execution.stage_runtime query_id={} stage_id={} task_id={} operator_family={} category=execution origin={} stage_runtime_ms={} operator_rows_in={} operator_rows_out={} batch_count={} artifact_bytes={}",
        query_id,
        stage_id,
        task_id,
        fields.operator_family,
        fields.origin,
        fields.stage_runtime_ms,
        fields.operator_rows_in,
        fields.operator_rows_out,
        fields.batch_count,
        fields.artifact_bytes,
    )
}

/// What: Load cached Delta version and file stats for a table when entry is fresh.
///
/// Inputs:
/// - `cache_key`: Stable table key derived from source prefix.
///
/// Output:
/// - Cached `(latest_version, file_stats)` when available and not expired.
fn get_cached_delta_pruning_metadata(
    cache_key: &str,
) -> Option<(u64, HashMap<String, DeltaFileStats>)> {
    let mut cache = DELTA_PRUNING_CACHE.lock().ok()?;
    let entry = cache.get(cache_key)?;
    if entry.cached_at.elapsed() > DELTA_PRUNING_CACHE_TTL {
        cache.remove(cache_key);
        return None;
    }

    Some((entry.latest_version, entry.file_stats.clone()))
}

/// What: Store Delta version and file stats in the pruning cache.
///
/// Inputs:
/// - `cache_key`: Stable table key derived from source prefix.
/// - `latest_version`: Latest delta log version used for pruning metadata.
/// - `file_stats`: Parsed file-level min/max stats.
///
/// Output:
/// - None. Cache is updated in place.
fn put_cached_delta_pruning_metadata(
    cache_key: String,
    latest_version: u64,
    file_stats: HashMap<String, DeltaFileStats>,
) {
    if let Ok(mut cache) = DELTA_PRUNING_CACHE.lock() {
        cache.insert(
            cache_key,
            DeltaPruningCacheEntry {
                cached_at: Instant::now(),
                latest_version,
                file_stats,
            },
        );
    }
}

/// What: Parse predicate AST from scan pruning hints JSON.
///
/// Inputs:
/// - `scan_hints_json`: Serialized pruning hints payload.
///
/// Output:
/// - Parsed predicate AST when a supported `predicate_ast` is present.
fn parse_predicate_ast_from_hints(scan_hints_json: &str) -> Result<PredicateAstNode, &'static str> {
    let hints: Value = serde_json::from_str(scan_hints_json).map_err(|_| "hints_invalid")?;
    let ast = hints.get("predicate_ast").ok_or("predicate_ast_missing")?;
    parse_predicate_ast_node(ast).ok_or("predicate_ast_missing")
}

fn parse_predicate_ast_node(node: &Value) -> Option<PredicateAstNode> {
    let kind = node.get("kind")?.as_str()?;
    match kind {
        "comparison" => Some(PredicateAstNode::Comparison(PredicateComparisonNode {
            column: node.get("column")?.as_str()?.to_string(),
            op: node.get("op")?.as_str()?.to_string(),
            value: node.get("value")?.as_str()?.to_string(),
        })),
        "and" => {
            let terms = node
                .get("terms")?
                .as_array()?
                .iter()
                .filter_map(parse_predicate_ast_node)
                .collect::<Vec<_>>();
            if terms.is_empty() {
                None
            } else {
                Some(PredicateAstNode::And(terms))
            }
        }
        _ => None,
    }
}

/// What: Resolve table object prefix from source staging prefix.
///
/// Inputs:
/// - `source_prefix`: Source prefix ending with `staging/`.
///
/// Output:
/// - Table root prefix ending with `/`.
fn table_prefix_from_source_prefix(source_prefix: &str) -> Option<String> {
    source_prefix
        .strip_suffix("staging/")
        .map(ToString::to_string)
}

fn delta_log_prefix_from_source_prefix(source_prefix: &str) -> Option<String> {
    table_prefix_from_source_prefix(source_prefix).map(|prefix| format!("{}_delta_log/", prefix))
}

/// What: Read latest Delta log version from `_delta_log` JSON files.
///
/// Inputs:
/// - `provider`: Storage provider used for object listing.
/// - `delta_log_prefix`: Delta log object prefix.
///
/// Output:
/// - Latest JSON log version when present.
async fn latest_delta_log_version(
    provider: &std::sync::Arc<dyn crate::storage::StorageProvider + Send + Sync>,
    delta_log_prefix: &str,
) -> Result<Option<u64>, String> {
    let keys = provider.list_objects(delta_log_prefix).await.map_err(|e| {
        format!(
            "failed to list delta log objects for {}: {}",
            delta_log_prefix, e
        )
    })?;

    let mut latest = None::<u64>;
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        let file_name = key.rsplit('/').next().unwrap_or_default();
        let stem = file_name.strip_suffix(".json").unwrap_or_default();
        if stem.len() != 20 || !stem.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        if let Ok(version) = stem.parse::<u64>() {
            latest = Some(latest.map_or(version, |current| current.max(version)));
        }
    }

    Ok(latest)
}

/// What: Load per-file Delta stats from one JSON log version.
///
/// Inputs:
/// - `provider`: Storage provider used for object reads.
/// - `delta_log_prefix`: Delta log object prefix.
/// - `table_prefix`: Table root object prefix.
/// - `version`: Delta log version to load.
///
/// Output:
/// - Map from full object key to parsed min/max stats.
async fn load_delta_file_stats_for_version(
    provider: &std::sync::Arc<dyn crate::storage::StorageProvider + Send + Sync>,
    delta_log_prefix: &str,
    table_prefix: &str,
    version: u64,
) -> Result<HashMap<String, DeltaFileStats>, String> {
    let delta_log_key = format!("{}{:020}.json", delta_log_prefix, version);
    let bytes = provider
        .get_object(&delta_log_key)
        .await
        .map_err(|e| format!("failed to read delta log {}: {}", delta_log_key, e))?
        .ok_or_else(|| format!("delta log object not found: {}", delta_log_key))?;
    let content = String::from_utf8(bytes)
        .map_err(|e| format!("delta log {} is not valid UTF-8: {}", delta_log_key, e))?;

    let mut file_stats = HashMap::<String, DeltaFileStats>::new();

    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let row: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let add = match row.get("add") {
            Some(value) => value,
            None => continue,
        };

        let relative_path = match add.get("path").and_then(Value::as_str) {
            Some(value) if !value.trim().is_empty() => value,
            _ => continue,
        };

        let stats_raw = match add.get("stats").and_then(Value::as_str) {
            Some(value) if !value.trim().is_empty() => value,
            _ => continue,
        };

        let stats_value: Value = match serde_json::from_str(stats_raw) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let min_values = match stats_value.get("minValues").and_then(Value::as_object) {
            Some(map) => map.clone(),
            None => continue,
        };
        let max_values = match stats_value.get("maxValues").and_then(Value::as_object) {
            Some(map) => map.clone(),
            None => continue,
        };

        let relative_key = relative_path.trim_start_matches('/');
        let full_key = format!("{}{}", table_prefix, relative_key);
        file_stats.insert(
            full_key,
            DeltaFileStats {
                min_values,
                max_values,
            },
        );
    }

    Ok(file_stats)
}

fn normalize_column_name(column: &str) -> String {
    column
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .rsplit('.')
        .next()
        .unwrap_or(column)
        .to_string()
}

fn parse_predicate_literal(raw: &str) -> Option<PredicateLiteralValue> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(stripped) = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            trimmed
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
    {
        return Some(PredicateLiteralValue::String(stripped.to_string()));
    }

    if trimmed.eq_ignore_ascii_case("true") {
        return Some(PredicateLiteralValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(PredicateLiteralValue::Bool(false));
    }

    trimmed
        .parse::<f64>()
        .ok()
        .map(PredicateLiteralValue::Number)
}

fn compare_value_and_literal(value: &Value, literal: &PredicateLiteralValue) -> Option<Ordering> {
    match literal {
        PredicateLiteralValue::Number(right) => value.as_f64()?.partial_cmp(right),
        PredicateLiteralValue::String(right) => Some(value.as_str()?.cmp(right)),
        PredicateLiteralValue::Bool(right) => Some(value.as_bool()?.cmp(right)),
    }
}

/// What: Determine if one comparison predicate is definitely false for a file.
///
/// Inputs:
/// - `comparison`: Parsed comparison node.
/// - `stats`: Delta file min/max stats.
///
/// Output:
/// - `true` when the file can be safely skipped.
fn is_comparison_definitely_false(
    comparison: &PredicateComparisonNode,
    stats: &DeltaFileStats,
) -> bool {
    let column = normalize_column_name(&comparison.column);
    let min_value = match stats.min_values.get(&column) {
        Some(value) => value,
        None => return false,
    };
    let max_value = match stats.max_values.get(&column) {
        Some(value) => value,
        None => return false,
    };
    let literal = match parse_predicate_literal(&comparison.value) {
        Some(value) => value,
        None => return false,
    };

    let min_cmp = compare_value_and_literal(min_value, &literal);
    let max_cmp = compare_value_and_literal(max_value, &literal);

    match comparison.op.as_str() {
        "=" => {
            matches!(max_cmp, Some(Ordering::Less)) || matches!(min_cmp, Some(Ordering::Greater))
        }
        ">" => matches!(max_cmp, Some(Ordering::Less | Ordering::Equal)),
        ">=" => matches!(max_cmp, Some(Ordering::Less)),
        "<" => matches!(min_cmp, Some(Ordering::Greater | Ordering::Equal)),
        "<=" => matches!(min_cmp, Some(Ordering::Greater)),
        "!=" => {
            matches!(min_cmp, Some(Ordering::Equal)) && matches!(max_cmp, Some(Ordering::Equal))
        }
        _ => false,
    }
}

fn predicate_definitely_false_for_file(
    predicate: &PredicateAstNode,
    stats: &DeltaFileStats,
) -> bool {
    match predicate {
        PredicateAstNode::Comparison(node) => is_comparison_definitely_false(node, stats),
        PredicateAstNode::And(terms) => terms
            .iter()
            .any(|term| predicate_definitely_false_for_file(term, stats)),
    }
}

/// What: Compute pruned file key set from Delta metadata and predicate AST.
///
/// Inputs:
/// - `keys`: Candidate parquet object keys.
/// - `stats`: Per-file min/max stats map.
/// - `predicate`: Parsed predicate AST.
///
/// Output:
/// - Filtered key list preserving deterministic order.
fn prune_keys_by_delta_stats(
    keys: &[String],
    stats: &HashMap<String, DeltaFileStats>,
    predicate: &PredicateAstNode,
) -> Vec<String> {
    keys.iter()
        .filter(|key| match stats.get(key.as_str()) {
            Some(entry) => !predicate_definitely_false_for_file(predicate, entry),
            None => true,
        })
        .cloned()
        .collect()
}

/// What: Attempt metadata-only pruning using Delta log stats and scan hints.
///
/// Inputs:
/// - `provider`: Storage provider used for metadata access.
/// - `source_prefix`: Source staging prefix.
/// - `keys`: Candidate parquet object keys.
/// - `scan_hints`: Runtime scan hint contract.
///
/// Output:
/// - Pruned key list when metadata checks pass; original key list otherwise.
async fn maybe_prune_scan_keys(
    provider: &std::sync::Arc<dyn crate::storage::StorageProvider + Send + Sync>,
    source_prefix: &str,
    keys: &[String],
    scan_hints: &RuntimeScanHints,
) -> ScanPruningOutcome {
    if scan_hints.mode != crate::execution::planner::RuntimeScanMode::MetadataPruned {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "mode_full_requested",
            cache_hit: None,
        };
    }

    if !scan_hints.pruning_eligible {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "ineligible_or_missing_reason",
            cache_hit: None,
        };
    }

    let Some(scan_hints_json) = scan_hints.pruning_hints_json.as_deref() else {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "hints_missing",
            cache_hit: None,
        };
    };

    let predicate_ast = match parse_predicate_ast_from_hints(scan_hints_json) {
        Ok(value) => value,
        Err(reason) => {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason,
                cache_hit: None,
            };
        }
    };

    let Some(delta_log_prefix) = delta_log_prefix_from_source_prefix(source_prefix) else {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "source_prefix_invalid",
            cache_hit: None,
        };
    };
    let Some(table_prefix) = table_prefix_from_source_prefix(source_prefix) else {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "table_prefix_invalid",
            cache_hit: None,
        };
    };

    let cache_key = table_prefix.clone();
    if let Some((cached_version, cached_stats)) = get_cached_delta_pruning_metadata(&cache_key) {
        if let Some(pin) = scan_hints.delta_version_pin
            && pin > 0
            && pin != cached_version
        {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_pin_mismatch",
                cache_hit: Some(true),
            };
        }

        if cached_stats.is_empty() {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_stats_unavailable",
                cache_hit: Some(true),
            };
        }

        return ScanPruningOutcome {
            keys: prune_keys_by_delta_stats(keys, &cached_stats, &predicate_ast),
            effective_mode: RuntimeScanMode::MetadataPruned,
            reason: "pruning_applied",
            cache_hit: Some(true),
        };
    }

    let latest_version = match latest_delta_log_version(provider, &delta_log_prefix).await {
        Ok(Some(version)) => version,
        Ok(None) | Err(_) => {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_version_unavailable",
                cache_hit: Some(false),
            };
        }
    };

    if let Some(pin) = scan_hints.delta_version_pin
        && pin > 0
        && pin != latest_version
    {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "delta_pin_mismatch",
            cache_hit: Some(false),
        };
    }

    let stats = match load_delta_file_stats_for_version(
        provider,
        &delta_log_prefix,
        &table_prefix,
        latest_version,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_stats_unavailable",
                cache_hit: Some(false),
            };
        }
    };

    if stats.is_empty() {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "delta_stats_unavailable",
            cache_hit: Some(false),
        };
    }

    put_cached_delta_pruning_metadata(cache_key, latest_version, stats.clone());

    ScanPruningOutcome {
        keys: prune_keys_by_delta_stats(keys, &stats, &predicate_ast),
        effective_mode: RuntimeScanMode::MetadataPruned,
        reason: "pruning_applied",
        cache_hit: Some(false),
    }
}

/// What: Execute validated query payload on the worker and materialize deterministic artifacts.
///
/// Inputs:
/// - `shared`: Worker state with storage provider and cluster storage config.
/// - `task`: Worker task carrying canonical payload with physical plan.
/// - `session_id`: Session id used for deterministic artifact partitioning.
/// - `namespace`: Canonical namespace resolved by payload validation.
/// - `result_location`: Flight URI returned to server/client.
///
/// Output:
/// - `Ok(())` when execution succeeds and parquet artifacts are persisted.
/// - `Err(message)` when runtime execution or storage operations fail.
pub(crate) async fn execute_query_task(
    shared: &SharedData,
    task: &worker_service::StagePartitionExecution,
    session_id: &str,
    namespace: &QueryNamespace,
    result_location: &str,
) -> Result<StageExecutionTelemetry, String> {
    let auth_scope = task
        .params
        .get("__auth_scope")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "query authorization scope metadata is missing".to_string())?;
    let rbac_user = task
        .params
        .get("__rbac_user")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "query rbac_user metadata is missing".to_string())?;
    let rbac_role = task
        .params
        .get("__rbac_role")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .ok_or_else(|| "query rbac_role metadata is missing".to_string())?;

    let expected_scope = format!(
        "select:{}.{}.{}",
        namespace.database, namespace.schema, namespace.table
    );
    if auth_scope != expected_scope && auth_scope != "select:*" && auth_scope != "select:*.*.*" {
        return Err(format!(
            "query authorization denied for principal {} with role {} on {}",
            rbac_user, rbac_role, expected_scope
        ));
    }

    let stage_started_at = Instant::now();
    let now_epoch_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| format!("system clock error while computing queue latency: {}", e))?
        .as_millis();
    let dispatch_enqueued_at_ms = task
        .params
        .get("__dispatch_enqueued_at_ms")
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(now_epoch_ms);
    let queue_ms = now_epoch_ms.saturating_sub(dispatch_enqueued_at_ms);

    let runtime_plan = extract_runtime_plan(task)?;
    let stage_context = stage_execution_context(task)?;
    let memory_tracking_enabled = task
        .params
        .get("memory_tracking_enabled")
        .and_then(|v| v.parse::<bool>().ok())
        .or_else(|| {
            std::env::var("WORKER_MEMORY_TRACKING_ENABLED")
                .ok()
                .and_then(|v| v.parse::<bool>().ok())
        })
        .unwrap_or(false);
    let spill_threshold_mb = task
        .params
        .get("memory_tracking_spill_threshold_mb")
        .and_then(|v| v.parse::<usize>().ok())
        .or_else(|| {
            std::env::var("WORKER_MEMORY_SPILL_THRESHOLD_MB")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
        })
        .unwrap_or(512);
    let mut stage_memory_metrics = StageMemoryMetrics::new(
        stage_context.stage_id,
        memory_tracking_enabled,
        spill_threshold_mb,
    );
    let execution_started_at = Instant::now();
    let query_id = task
        .params
        .get("__query_id")
        .cloned()
        .unwrap_or_else(|| "unknown".to_string());
    log::info!(
        "{}",
        format_scan_mode_chosen_event(
            &query_id,
            stage_context.stage_id,
            &task.task_id,
            runtime_scan_mode_label(&stage_context.scan_hints.mode),
        )
    );
    if let Some(reason) = stage_context
        .scan_hints
        .pruning_reason
        .as_deref()
        .filter(|v| !v.is_empty())
    {
        log::info!(
            "{}",
            format_scan_fallback_reason_event(
                &query_id,
                stage_context.stage_id,
                &task.task_id,
                reason,
            )
        );
    }
    let context_tag = format!(
        "query_id={} stage_id={} task_id={} partition_index={}",
        query_id, stage_context.stage_id, task.task_id, stage_context.partition_index
    );
    let decision_ctx = ScanDecisionContext {
        query_id: &query_id,
        stage_id: stage_context.stage_id,
        task_id: &task.task_id,
    };
    let relation_columns_by_key = parse_relation_columns_map(task);
    let source_relation_key = relation_key(namespace);
    let mut batches =
        load_input_batches(shared, session_id, namespace, &stage_context, &decision_ctx)
            .await
            .map_err(|e| format!("{} [{}]", e, context_tag))?;

    if stage_context.upstream_stage_ids.is_empty()
        && batches.is_empty()
        && let Some(columns) = relation_columns_by_key.get(source_relation_key.as_str())
    {
        log::info!(
            "query source scan has no parquet files for {}; materializing empty table result",
            source_relation_key
        );
        batches.push(build_empty_batch_from_relation_columns(columns)?);
    }

    if stage_context.upstream_stage_ids.is_empty()
        && let Some(columns) = relation_columns_by_key.get(source_relation_key.as_str())
    {
        batches = apply_relation_column_names(&batches, columns)?;
    } else {
        log::info!(
            "{}",
            format_exchange_io_decision_event(
                &query_id,
                stage_context.stage_id,
                &task.task_id,
                stage_context.partition_index,
                "read",
            )
        );
    }

    let operator_rows_in = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

    if let Some(predicate) = runtime_plan.filter_predicate.as_ref() {
        batches = apply_filter_predicate_pipeline(
            &batches,
            predicate,
            runtime_plan.schema_metadata.as_ref(),
        )?;
    }

    if let Some(join_plan) = runtime_plan.join_plan.as_ref() {
        let join_spec = match join_plan {
            RuntimeJoinPlan::Hash(spec) | RuntimeJoinPlan::NestedLoop(spec) => spec,
        };

        let right_namespace = QueryNamespace {
            database: join_spec.right_relation.database.clone(),
            schema: join_spec.right_relation.schema.clone(),
            table: join_spec.right_relation.table.clone(),
        };
        let right_prefix = source_table_staging_prefix(&right_namespace);
        let mut right_batches = load_scan_batches(
            shared,
            &right_prefix,
            &RuntimeScanHints::full_scan(),
            &decision_ctx,
        )
        .await
        .map_err(|e| format!("{} [{}]", e, context_tag))?;
        let right_relation_key = relation_key(&right_namespace);
        if let Some(columns) = relation_columns_by_key.get(right_relation_key.as_str()) {
            right_batches = apply_relation_column_names(&right_batches, columns)?;
        }
        batches = match join_plan {
            RuntimeJoinPlan::Hash(spec) => apply_hash_join_pipeline(&batches, &right_batches, spec),
            RuntimeJoinPlan::NestedLoop(spec) => {
                apply_nested_loop_join_pipeline(&batches, &right_batches, spec)
            }
        }?;
    }

    if let Some(union_spec) = runtime_plan.union_spec.as_ref() {
        batches = apply_union_pipeline(
            shared,
            &batches,
            union_spec,
            &decision_ctx,
            &relation_columns_by_key,
        )
        .await
        .map_err(|e| format!("{} [{}]", e, context_tag))?;
    }

    if let Some(aggregate_partial_spec) = runtime_plan.aggregate_partial_spec.as_ref() {
        if stage_memory_metrics.enabled {
            log::info!(
                "event=worker.memory.spill_threshold_check stage_id={} threshold_bytes={} phase=aggregate_partial",
                stage_context.stage_id,
                stage_memory_metrics.spill_threshold_bytes
            );
        }
        batches = apply_aggregate_partial_pipeline(&batches, aggregate_partial_spec)?;
    }

    if let Some(aggregate_final_spec) = runtime_plan.aggregate_final_spec.as_ref() {
        if stage_memory_metrics.enabled {
            log::info!(
                "event=worker.memory.spill_threshold_check stage_id={} threshold_bytes={} phase=aggregate_final",
                stage_context.stage_id,
                stage_memory_metrics.spill_threshold_bytes
            );
        }
        batches = apply_aggregate_final_pipeline(&batches, aggregate_final_spec)?;
    }

    if let Some(window_spec) = runtime_plan.window_spec.as_ref() {
        if stage_memory_metrics.enabled {
            log::info!(
                "event=worker.memory.spill_threshold_check stage_id={} threshold_bytes={} phase=window",
                stage_context.stage_id,
                stage_memory_metrics.spill_threshold_bytes
            );
        }
        batches = apply_window_pipeline(&batches, window_spec)?;
    }

    if runtime_plan.sort_before_projection {
        if !runtime_plan.sort_exprs.is_empty() {
            batches = apply_sort_pipeline(&batches, &runtime_plan.sort_exprs)?;
        }

        if !runtime_plan.projection_exprs.is_empty() {
            batches = apply_projection_pipeline(&batches, &runtime_plan.projection_exprs)?;
        }
    } else {
        if !runtime_plan.projection_exprs.is_empty() {
            batches = apply_projection_pipeline(&batches, &runtime_plan.projection_exprs)?;
        }

        if !runtime_plan.sort_exprs.is_empty() {
            batches = apply_sort_pipeline(&batches, &runtime_plan.sort_exprs)?;
        }
    }

    if let Some(limit_spec) = runtime_plan.limit_spec.as_ref() {
        batches = apply_limit_pipeline(&batches, limit_spec)?;
    }

    if batches.is_empty() {
        return Err(format!(
            "query execution produced no record batches [{}]",
            context_tag
        ));
    }

    let normalized_batches = normalize_batches_for_sort(&batches)?;
    for batch in &normalized_batches {
        stage_memory_metrics.track_batch_allocation(batch);
    }

    let exec_ms = execution_started_at.elapsed().as_millis();

    let routed_batches_by_partition = if task.output_destinations.is_empty() {
        None
    } else {
        let rr_counter = Arc::new(AtomicUsize::new(0));
        let mut routed = Vec::<Vec<RecordBatch>>::with_capacity(task.output_destinations.len());

        for destination in &task.output_destinations {
            let destination_partition_count = if destination.downstream_partition_count == 0 {
                1
            } else {
                destination.downstream_partition_count
            };
            let partitioning_lower = destination.partitioning.to_ascii_lowercase();
            let is_hash_partitioning = partitioning_lower.trim() == "hash"
                || partitioning_lower.trim_start().starts_with("hash:")
                || partitioning_lower.contains("\"hash\"")
                || partitioning_lower.contains("{hash");

            let mut destination_batches = Vec::<RecordBatch>::new();
            for batch in &normalized_batches {
                let mut split = match route_batch_from_output_partitioning(
                    batch,
                    &destination.partitioning,
                    destination_partition_count,
                    Some(&rr_counter),
                ) {
                    Ok(split) => split,
                    Err(err)
                        if is_hash_partitioning && err.contains("hash partition key column") =>
                    {
                        log::warn!(
                            "Falling back to round-robin routing because hash keys are missing in output schema (partitioning={}): {}",
                            destination.partitioning,
                            err,
                        );
                        route_batch_roundrobin(batch, destination_partition_count, &rr_counter)?
                    }
                    Err(err) => return Err(err),
                };
                destination_batches.append(&mut split);
            }

            routed.push(destination_batches);
        }

        Some(routed)
    };

    let exchange_artifact_bytes = persist_stage_exchange_artifacts(
        shared,
        session_id,
        &stage_context.query_run_id,
        stage_context.stage_id,
        stage_context.partition_index,
        stage_context.partition_count,
        &stage_context.upstream_stage_ids,
        &normalized_batches,
    )
    .await
    .map_err(|e| format!("{} [{}]", e, context_tag))?;

    let network_started_at = Instant::now();
    let downstream_network_summary =
        crate::flight::server::stream_stage_partition_to_output_destinations(
            shared,
            task,
            session_id,
            &normalized_batches,
            routed_batches_by_partition.as_deref(),
        )
        .await
        .map_err(|e| format!("{} [{}]", e, context_tag))?;
    let network_ms = network_started_at.elapsed().as_millis();

    log::info!(
        "{}",
        format_exchange_io_decision_event(
            &query_id,
            stage_context.stage_id,
            &task.task_id,
            stage_context.partition_index,
            "write",
        )
    );

    let materialization_artifact_bytes = if runtime_plan.has_materialize {
        log::info!(
            "{}",
            format_materialization_decision_event(
                &query_id,
                stage_context.stage_id,
                &task.task_id,
                "parquet",
            )
        );
        persist_query_artifacts(
            shared,
            result_location,
            session_id,
            &task.task_id,
            &normalized_batches,
        )
        .await
        .map_err(|e| format!("{} [{}]", e, context_tag))?
    } else {
        log::info!(
            "{}",
            format_materialization_decision_event(
                &query_id,
                stage_context.stage_id,
                &task.task_id,
                "exchange_only",
            )
        );
        0
    };

    let operator_rows_out = normalized_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    let stage_runtime_ms = stage_started_at.elapsed().as_millis();
    log::info!(
        "{}",
        format_stage_runtime_event(
            &query_id,
            stage_context.stage_id,
            &task.task_id,
            &StageRuntimeTelemetryFields {
                operator_family: "stage",
                origin: "worker_pipeline",
                stage_runtime_ms,
                operator_rows_in,
                operator_rows_out,
                batch_count: normalized_batches.len(),
                artifact_bytes: exchange_artifact_bytes + materialization_artifact_bytes,
            },
        )
    );

    if stage_memory_metrics.enabled {
        log::info!(
            "event=worker.stage_memory_metrics stage_id={} sampled_batches={} sampled_rows={} peak_batch_bytes={} spill_threshold_bytes={} spill_threshold_exceeded={}",
            stage_memory_metrics.stage_id,
            stage_memory_metrics.sampled_batches,
            stage_memory_metrics.sampled_rows,
            stage_memory_metrics.peak_batch_bytes,
            stage_memory_metrics.spill_threshold_bytes,
            stage_memory_metrics.spill_threshold_exceeded
        );
    }

    log::info!(
        "event=worker.stage_latency_metrics stage_id={} queue_ms={} exec_ms={} network_ms={} total_ms={}",
        stage_context.stage_id,
        queue_ms,
        exec_ms,
        network_ms,
        queue_ms.saturating_add(exec_ms).saturating_add(network_ms)
    );
    log::info!(
        "event=worker.stage_network_metrics stage_id={} downstream_endpoint_count={} downstream_write_ms={} downstream_queued_frames={} downstream_queued_bytes={}",
        stage_context.stage_id,
        downstream_network_summary.endpoint_count,
        downstream_network_summary.stream_time_ms,
        downstream_network_summary.queued_frames,
        downstream_network_summary.queued_bytes
    );

    Ok(StageExecutionTelemetry {
        latency: StageLatencyMetrics {
            stage_id: stage_context.stage_id,
            queue_ms,
            exec_ms,
            network_ms,
        },
        memory: if stage_memory_metrics.enabled {
            Some(stage_memory_metrics)
        } else {
            None
        },
        network: Some(StageNetworkMetrics {
            downstream_endpoint_count: downstream_network_summary.endpoint_count,
            downstream_write_ms: downstream_network_summary.stream_time_ms,
            downstream_queued_frames: downstream_network_summary.queued_frames,
            downstream_queued_bytes: downstream_network_summary.queued_bytes,
        }),
    })
}

fn relation_key(namespace: &QueryNamespace) -> String {
    format!(
        "{}.{}.{}",
        namespace.database.to_ascii_lowercase(),
        namespace.schema.to_ascii_lowercase(),
        namespace.table.to_ascii_lowercase()
    )
}

fn parse_relation_columns_map(
    task: &worker_service::StagePartitionExecution,
) -> HashMap<String, Vec<String>> {
    task.params
        .get("relation_columns_json")
        .and_then(|value| serde_json::from_str::<HashMap<String, Vec<String>>>(value).ok())
        .unwrap_or_default()
}

fn apply_relation_column_names(
    batches: &[RecordBatch],
    relation_columns: &[String],
) -> Result<Vec<RecordBatch>, String> {
    if relation_columns.is_empty() {
        return Ok(batches.to_vec());
    }

    let mut renamed = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_columns() != relation_columns.len() {
            return Err(format!(
                "relation column mapping mismatch: expected {} columns from metastore, batch has {}",
                relation_columns.len(),
                batch.num_columns()
            ));
        }

        let fields = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                field
                    .as_ref()
                    .clone()
                    .with_name(relation_columns[index].clone())
            })
            .collect::<Vec<_>>();

        let renamed_batch = RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
            batch.columns().to_vec(),
        )
        .map_err(|e| format!("failed to apply relation column mapping: {}", e))?;

        renamed.push(renamed_batch);
    }

    Ok(renamed)
}

fn build_empty_batch_from_relation_columns(
    relation_columns: &[String],
) -> Result<RecordBatch, String> {
    let mut fields = Vec::<Field>::with_capacity(relation_columns.len());
    let mut arrays =
        Vec::<std::sync::Arc<dyn arrow::array::Array>>::with_capacity(relation_columns.len());

    for column in relation_columns {
        let normalized = column.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }

        fields.push(Field::new(normalized, DataType::Utf8, true));
        arrays.push(std::sync::Arc::new(StringArray::from(
            Vec::<Option<String>>::new(),
        )));
    }

    let schema = std::sync::Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("failed to build empty fallback record batch: {}", e))
}

async fn apply_union_pipeline(
    shared: &SharedData,
    primary_batches: &[RecordBatch],
    union_spec: &RuntimeUnionSpec,
    decision_ctx: &ScanDecisionContext<'_>,
    relation_columns_by_key: &HashMap<String, Vec<String>>,
) -> Result<Vec<RecordBatch>, String> {
    if union_spec.operands.len() < 2 {
        return Err("union requires at least two relations".to_string());
    }

    let mut all_batches = Vec::<RecordBatch>::new();
    for (index, operand) in union_spec.operands.iter().enumerate() {
        let mut batches = if index == 0 {
            primary_batches.to_vec()
        } else {
            let prefix = source_table_staging_prefix(&operand.relation);
            let mut loaded = load_scan_batches(
                shared,
                &prefix,
                &RuntimeScanHints::full_scan(),
                decision_ctx,
            )
            .await?;

            let relation_key = relation_key(&operand.relation);
            if let Some(columns) = relation_columns_by_key.get(relation_key.as_str()) {
                loaded = apply_relation_column_names(&loaded, columns)?;
            }

            loaded
        };

        let rows_before = batches.iter().map(RecordBatch::num_rows).sum::<usize>();

        if let Some(filter) = operand.filter.as_ref() {
            log::info!(
                "UNION operand {} applying filter: {}",
                index,
                render_predicate_expr(filter)
            );
            batches = apply_filter_predicate_pipeline(&batches, filter, None)?;
            let rows_after = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
            log::info!(
                "UNION operand {} rows_before={} rows_after={}",
                index,
                rows_before,
                rows_after
            );
            if let Some(summary) = summarize_union_operand_values(&batches) {
                log::info!("UNION operand {} value summary: {}", index, summary);
            }
        } else {
            log::info!(
                "UNION operand {} has no filter rows_before={}",
                index,
                rows_before
            );
        }

        all_batches.extend(batches);
    }

    if union_spec.distinct {
        return deduplicate_union_batches(&all_batches);
    }

    Ok(all_batches)
}

fn summarize_union_operand_values(batches: &[RecordBatch]) -> Option<String> {
    let mut unique_values = HashSet::<String>::new();
    let mut sample_values = Vec::<String>::new();
    let mut total_rows = 0usize;
    let mut selected_column_name: Option<String> = None;

    for batch in batches {
        let Some(column_idx) = resolve_schema_column_index(batch.schema().as_ref(), "id")
            .or_else(|| resolve_schema_column_index(batch.schema().as_ref(), "c1"))
        else {
            continue;
        };

        if selected_column_name.is_none() {
            selected_column_name = Some(batch.schema().field(column_idx).name().to_string());
        }

        let column = batch.column(column_idx);
        for row_idx in 0..batch.num_rows() {
            total_rows += 1;
            let rendered = array_value_to_string(column.as_ref(), row_idx)
                .unwrap_or_else(|_| "<render-error>".to_string());
            if unique_values.insert(rendered.clone()) && sample_values.len() < 8 {
                sample_values.push(rendered);
            }
        }
    }

    selected_column_name.map(|name| {
        format!(
            "column={} total_rows={} unique_values={} sample={:?}",
            name,
            total_rows,
            unique_values.len(),
            sample_values
        )
    })
}

fn deduplicate_union_batches(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, String> {
    let mut seen = HashSet::<String>::new();
    let mut output = Vec::<RecordBatch>::new();

    for batch in batches {
        let mut keep_mask = Vec::<bool>::with_capacity(batch.num_rows());
        for row_index in 0..batch.num_rows() {
            let mut key = String::new();
            for (col_index, column) in batch.columns().iter().enumerate() {
                if col_index > 0 {
                    key.push('|');
                }
                let rendered = array_value_to_string(column.as_ref(), row_index)
                    .map_err(|e| format!("failed to stringify union row value: {}", e))?;
                key.push_str(rendered.as_str());
            }
            let is_new = seen.insert(key);
            keep_mask.push(is_new);
        }

        if keep_mask.iter().any(|keep| *keep) {
            let mask = BooleanArray::from(keep_mask);
            let filtered = filter_record_batch(batch, &mask)
                .map_err(|e| format!("failed to filter deduplicated union rows: {}", e))?;
            output.push(filtered);
        }
    }

    Ok(output)
}

/// What: Parse exchange partition index from a stage artifact key.
///
/// Inputs:
/// - `key`: Object-store key ending with `part-xxxxx.parquet`.
///
/// Output:
/// - Partition index when the key matches expected naming.
pub(crate) fn parse_partition_index_from_exchange_key(key: &str) -> Option<u32> {
    let file_name = key.rsplit('/').next()?;
    if !file_name.ends_with(".parquet") {
        return None;
    }
    let stem = file_name.strip_suffix(".parquet")?;
    let index_raw = stem.strip_prefix("part-")?;
    index_raw.parse::<u32>().ok()
}

/// What: Validate that upstream exchange artifacts cover the expected partition set.
///
/// Inputs:
/// - `upstream_stage_id`: Upstream stage identifier.
/// - `keys`: Exchange parquet artifact keys found for the upstream stage.
/// - `expected_partition_count`: Declared upstream partition count.
///
/// Output:
/// - `Ok(())` when all partitions are present exactly once.
/// - Error when partitions are missing, duplicated, or malformed.
pub(crate) fn validate_upstream_exchange_partition_set(
    upstream_stage_id: u32,
    keys: &[String],
    expected_partition_count: u32,
) -> Result<(), String> {
    if expected_partition_count == 0 {
        return Err(format!(
            "upstream stage {} has invalid expected partition count 0",
            upstream_stage_id
        ));
    }

    if keys.len() != expected_partition_count as usize {
        return Err(format!(
            "upstream stage {} exchange artifact count mismatch: expected {}, found {}",
            upstream_stage_id,
            expected_partition_count,
            keys.len()
        ));
    }

    let mut seen = BTreeSet::<u32>::new();
    for key in keys {
        let partition_index = parse_partition_index_from_exchange_key(key).ok_or_else(|| {
            format!(
                "upstream stage {} exchange artifact has invalid key format: {}",
                upstream_stage_id, key
            )
        })?;

        if partition_index >= expected_partition_count {
            return Err(format!(
                "upstream stage {} exchange artifact partition {} out of range [0, {})",
                upstream_stage_id, partition_index, expected_partition_count
            ));
        }

        if !seen.insert(partition_index) {
            return Err(format!(
                "upstream stage {} has duplicate exchange artifact for partition {}",
                upstream_stage_id, partition_index
            ));
        }
    }

    if seen.len() != expected_partition_count as usize {
        return Err(format!(
            "upstream stage {} exchange partition coverage mismatch: expected {} unique partitions, found {}",
            upstream_stage_id,
            expected_partition_count,
            seen.len()
        ));
    }

    Ok(())
}

/// What: Select exchange artifacts for one downstream partition from an upstream stage.
///
/// Inputs:
/// - `upstream_stage_id`: Upstream stage identifier.
/// - `keys`: Candidate upstream exchange artifact keys.
/// - `upstream_partition_count`: Declared upstream partition count.
/// - `downstream_partition_count`: Declared downstream partition count.
/// - `downstream_partition_index`: Current downstream partition index.
///
/// Output:
/// - Ordered subset of upstream keys assigned to the downstream partition.
///
/// Details:
/// - Uses one-to-one mapping when upstream fanout is less than or equal to downstream fanout.
/// - Uses modulo mapping when upstream fanout is greater than downstream fanout.
pub(crate) fn select_exchange_keys_for_downstream_partition(
    upstream_stage_id: u32,
    keys: &[String],
    upstream_partition_count: u32,
    downstream_partition_count: u32,
    downstream_partition_index: u32,
) -> Result<Vec<String>, String> {
    if downstream_partition_count == 0 {
        return Err(format!(
            "downstream stage for upstream stage {} has invalid partition count 0",
            upstream_stage_id
        ));
    }
    if downstream_partition_index >= downstream_partition_count {
        return Err(format!(
            "downstream partition {} out of range [0, {}) for upstream stage {}",
            downstream_partition_index, downstream_partition_count, upstream_stage_id
        ));
    }

    if upstream_partition_count <= downstream_partition_count {
        let suffix = format!("part-{:05}.parquet", downstream_partition_index);
        return Ok(keys
            .iter()
            .filter(|key| key.ends_with(&suffix))
            .cloned()
            .collect::<Vec<_>>());
    }

    let mut selected = Vec::<String>::new();
    for key in keys {
        let upstream_partition_index =
            parse_partition_index_from_exchange_key(key).ok_or_else(|| {
                format!(
                    "upstream stage {} exchange artifact has invalid key format: {}",
                    upstream_stage_id, key
                )
            })?;

        if upstream_partition_index >= upstream_partition_count {
            return Err(format!(
                "upstream stage {} exchange artifact partition {} out of range [0, {})",
                upstream_stage_id, upstream_partition_index, upstream_partition_count
            ));
        }

        if upstream_partition_index % downstream_partition_count == downstream_partition_index {
            selected.push(key.clone());
        }
    }

    selected.sort();
    Ok(selected)
}

/// What: Load input batches from base table scan or upstream stage exchange artifacts.
///
/// Inputs:
/// - `shared`: Worker state containing configured storage provider.
/// - `session_id`: Session identifier used for exchange scoping.
/// - `namespace`: Canonical query namespace for source scan fallback.
/// - `stage_context`: Stage metadata resolved from task params.
///
/// Output:
/// - Ordered input record batches for current stage execution.
pub(crate) async fn load_input_batches(
    shared: &SharedData,
    session_id: &str,
    namespace: &QueryNamespace,
    stage_context: &StageExecutionContext,
    decision_ctx: &ScanDecisionContext<'_>,
) -> Result<Vec<RecordBatch>, String> {
    if stage_context.upstream_stage_ids.is_empty() {
        let source_prefix = source_table_staging_prefix(namespace);
        let source_batches = match load_scan_batches(
            shared,
            &source_prefix,
            &stage_context.scan_hints,
            decision_ctx,
        )
        .await
        {
            Ok(batches) => batches,
            Err(error)
                if error
                    .to_ascii_lowercase()
                    .contains("no source parquet files found for query prefix") =>
            {
                log::info!(
                    "query source prefix {} has no parquet files; continuing with empty input",
                    source_prefix
                );
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        return partition_input_batches(
            &source_batches,
            stage_context.partition_count,
            stage_context.partition_index,
        );
    }

    load_upstream_exchange_batches(shared, session_id, stage_context).await
}

/// What: Load batches from upstream stage exchange artifacts.
///
/// Inputs:
/// - `shared`: Worker state containing configured storage provider.
/// - `session_id`: Session identifier used for exchange scoping.
/// - `stage_context`: Stage metadata resolved from task params.
///
/// Output:
/// - Decoded upstream stage batches in deterministic key order.
async fn load_upstream_exchange_batches(
    shared: &SharedData,
    session_id: &str,
    stage_context: &StageExecutionContext,
) -> Result<Vec<RecordBatch>, String> {
    let provider = shared
        .storage_provider
        .as_ref()
        .ok_or_else(|| "storage provider is not configured for exchange reads".to_string())?;

    let mut all_batches = Vec::new();
    for upstream_stage_id in &stage_context.upstream_stage_ids {
        let mut keys = list_stage_exchange_data_keys_with_retry(
            provider,
            &stage_context.query_run_id,
            session_id,
            *upstream_stage_id,
        )
        .await?;

        if keys.is_empty() {
            return Err(format!(
                "no exchange artifacts available for upstream stage {}",
                upstream_stage_id
            ));
        }

        if stage_context.partition_count == 1
            && let Some(expected_partition_count) = stage_context
                .upstream_partition_counts
                .get(upstream_stage_id)
                .copied()
        {
            validate_upstream_exchange_partition_set(
                *upstream_stage_id,
                &keys,
                expected_partition_count,
            )?;
        }

        if stage_context.partition_count > 1 {
            if let Some(expected_partition_count) = stage_context
                .upstream_partition_counts
                .get(upstream_stage_id)
                .copied()
            {
                validate_upstream_exchange_partition_set(
                    *upstream_stage_id,
                    &keys,
                    expected_partition_count,
                )?;
                keys = select_exchange_keys_for_downstream_partition(
                    *upstream_stage_id,
                    &keys,
                    expected_partition_count,
                    stage_context.partition_count,
                    stage_context.partition_index,
                )?;
            } else {
                let suffix = format!("part-{:05}.parquet", stage_context.partition_index);
                keys.retain(|key| key.ends_with(&suffix));
            }

            if keys.is_empty() {
                return Err(format!(
                    "missing exchange artifact for upstream stage {} partition {}",
                    upstream_stage_id, stage_context.partition_index
                ));
            }
        }

        all_batches.reserve(keys.len());

        for key in keys {
            let bytes = load_validated_exchange_artifact_with_retry(
                provider,
                &stage_context.query_run_id,
                session_id,
                *upstream_stage_id,
                &key,
            )
            .await?;
            let decoded = decode_parquet_batches(bytes)?;
            all_batches.extend(decoded);
        }
    }

    if all_batches.is_empty() {
        log::info!(
            "upstream exchange artifacts for stage {} contain no batches; continuing with empty input",
            stage_context.stage_id
        );
        return Ok(Vec::new());
    }

    log::info!(
        "loaded exchange input for stage_id={} upstream={:?} batches={}",
        stage_context.stage_id,
        stage_context.upstream_stage_ids,
        all_batches.len()
    );

    Ok(all_batches)
}

/// What: Slice input batches into deterministic partitions for fan-out execution.
///
/// Inputs:
/// - `input`: Source record batches.
/// - `partition_count`: Total number of partitions.
/// - `partition_index`: Current partition index.
///
/// Output:
/// - Record batches filtered to rows owned by the requested partition.
pub(crate) fn partition_input_batches(
    input: &[RecordBatch],
    partition_count: u32,
    partition_index: u32,
) -> Result<Vec<RecordBatch>, String> {
    if partition_count <= 1 {
        return Ok(input.to_vec());
    }
    if partition_index >= partition_count {
        return Err(format!(
            "invalid partition index {} for partition count {}",
            partition_index, partition_count
        ));
    }

    let mut out = Vec::with_capacity(input.len());
    let mut global_row_index: u64 = 0;
    let partition_count_u64 = u64::from(partition_count);
    let partition_index_u64 = u64::from(partition_index);

    for batch in input {
        let mut mask = Vec::with_capacity(batch.num_rows());
        for _ in 0..batch.num_rows() {
            mask.push(global_row_index % partition_count_u64 == partition_index_u64);
            global_row_index += 1;
        }

        let filtered = filter_record_batch(batch, &BooleanArray::from(mask))
            .map_err(|e| format!("failed to slice batch for partitioning: {}", e))?;
        out.push(filtered);
    }

    Ok(out)
}

/// What: Build the source Delta table staging object prefix from canonical namespace.
///
/// Inputs:
/// - `namespace`: Canonical namespace for relation lookup.
///
/// Output:
/// - Object-store prefix under which committed parquet files are listed.
pub(crate) fn source_table_staging_prefix(namespace: &QueryNamespace) -> String {
    format!(
        "databases/{}/schemas/{}/tables/{}/staging/",
        namespace.database, namespace.schema, namespace.table
    )
}

/// What: Load scan batches by decoding all parquet files under a source staging prefix.
///
/// Inputs:
/// - `shared`: Worker state containing configured storage provider.
/// - `source_prefix`: Source table staging prefix.
///
/// Output:
/// - Ordered Arrow record batches read from all parquet objects under the prefix.
pub(crate) async fn load_scan_batches(
    shared: &SharedData,
    source_prefix: &str,
    scan_hints: &RuntimeScanHints,
    decision_ctx: &ScanDecisionContext<'_>,
) -> Result<Vec<RecordBatch>, String> {
    let provider = shared
        .storage_provider
        .as_ref()
        .ok_or_else(|| "storage provider is not configured for query execution".to_string())?;

    if scan_hints.mode == crate::execution::planner::RuntimeScanMode::Full
        && (scan_hints.pruning_hints_json.is_some() || scan_hints.delta_version_pin.is_some())
    {
        log::debug!(
            "scan hints supplied while mode=full for prefix={} delta_version_pin={:?} hints_present={}",
            source_prefix,
            scan_hints.delta_version_pin,
            scan_hints.pruning_hints_json.is_some()
        );
    }

    let mut keys = provider
        .list_objects(source_prefix)
        .await
        .map_err(|e| format!("failed to list source objects for {}: {}", source_prefix, e))?;
    keys.retain(|k| k.ends_with(".parquet"));
    keys.sort();

    if keys.is_empty() {
        return Err(format!(
            "no source parquet files found for query prefix {}",
            source_prefix
        ));
    }

    let total_files = keys.len();
    let pruning_outcome = maybe_prune_scan_keys(provider, source_prefix, &keys, scan_hints).await;
    let retained_files = pruning_outcome.keys.len();
    log::info!(
        "{}",
        format_scan_pruning_decision_event(
            decision_ctx,
            &ScanPruningDecisionFields {
                requested_mode: runtime_scan_mode_label(&scan_hints.mode),
                effective_mode: runtime_scan_mode_label(&pruning_outcome.effective_mode),
                reason: pruning_outcome.reason,
                delta_version_pin: scan_hints.delta_version_pin,
                cache_hit: pruning_outcome.cache_hit,
                total_files,
                retained_files,
            },
        )
    );

    if pruning_outcome.keys.is_empty() {
        return Err(format!(
            "scan pruning eliminated all source parquet files for query prefix {}",
            source_prefix
        ));
    }

    let mut all_batches = Vec::with_capacity(retained_files);
    for key in pruning_outcome.keys {
        let bytes = provider
            .get_object(&key)
            .await
            .map_err(|e| format!("failed to read source object {}: {}", key, e))?
            .ok_or_else(|| format!("source object not found: {}", key))?;

        let decoded = decode_parquet_batches(bytes)?;
        all_batches.reserve(decoded.len());
        all_batches.extend(decoded);
    }

    if all_batches.is_empty() {
        return Err("source parquet files contain no record batches".to_string());
    }

    Ok(all_batches)
}

/// What: Decode parquet bytes into Arrow record batches.
///
/// Inputs:
/// - `parquet_bytes`: Full parquet payload bytes.
///
/// Output:
/// - Arrow record batches decoded from the input payload.
pub(crate) fn decode_parquet_batches(parquet_bytes: Vec<u8>) -> Result<Vec<RecordBatch>, String> {
    let reader_source = Bytes::from(parquet_bytes);
    let builder = ParquetRecordBatchReaderBuilder::try_new(reader_source)
        .map_err(|e| format!("failed to open parquet reader: {}", e))?
        .with_batch_size(1024);
    let schema = builder.schema().clone();

    let mut reader = builder
        .build()
        .map_err(|e| format!("failed to build parquet reader: {}", e))?;

    let mut batches = Vec::new();
    for maybe_batch in &mut reader {
        let batch = maybe_batch.map_err(|e| format!("failed reading parquet batch: {}", e))?;
        batches.push(batch);
    }

    if batches.is_empty() {
        let empty_columns = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect::<Vec<_>>();
        let empty_batch = RecordBatch::try_new(schema, empty_columns)
            .map_err(|e| format!("failed to build empty parquet batch: {}", e))?;
        batches.push(empty_batch);
    }

    Ok(batches)
}

#[cfg(test)]
#[path = "../tests/execution_pipeline_tests.rs"]
mod tests;
