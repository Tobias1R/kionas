use crate::execution::aggregate::{
    apply_aggregate_final_pipeline, apply_aggregate_partial_pipeline,
};
use crate::execution::artifacts::{persist_query_artifacts, persist_stage_exchange_artifacts};
use crate::execution::join::apply_hash_join_pipeline;
use crate::execution::planner::RuntimeScanHints;
use crate::execution::planner::StageExecutionContext;
use crate::execution::planner::{extract_runtime_plan, stage_execution_context};
use crate::execution::query::QueryNamespace;
use crate::services::query_execution::{
    apply_filter_pipeline, apply_limit_pipeline, apply_projection_pipeline, apply_sort_pipeline,
    normalize_batches_for_sort,
};
use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;
use crate::storage::exchange::list_stage_exchange_data_keys;
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

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

static DELTA_PRUNING_CACHE: LazyLock<Mutex<HashMap<String, DeltaPruningCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
fn parse_predicate_ast_from_hints(scan_hints_json: &str) -> Option<PredicateAstNode> {
    let hints: Value = serde_json::from_str(scan_hints_json).ok()?;
    let ast = hints.get("predicate_ast")?;
    parse_predicate_ast_node(ast)
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
) -> Vec<String> {
    if scan_hints.mode != crate::execution::planner::RuntimeScanMode::MetadataPruned
        || !scan_hints.pruning_eligible
    {
        return keys.to_vec();
    }

    let Some(scan_hints_json) = scan_hints.pruning_hints_json.as_deref() else {
        log::warn!(
            "scan pruning fallback: metadata_pruned mode without scan_pruning_hints_json for prefix={}",
            source_prefix
        );
        return keys.to_vec();
    };

    let Some(predicate_ast) = parse_predicate_ast_from_hints(scan_hints_json) else {
        log::warn!(
            "scan pruning fallback: unable to parse predicate_ast from hints for prefix={}",
            source_prefix
        );
        return keys.to_vec();
    };

    let Some(delta_log_prefix) = delta_log_prefix_from_source_prefix(source_prefix) else {
        log::warn!(
            "scan pruning fallback: invalid source prefix (missing staging suffix) for prefix={}",
            source_prefix
        );
        return keys.to_vec();
    };
    let Some(table_prefix) = table_prefix_from_source_prefix(source_prefix) else {
        log::warn!(
            "scan pruning fallback: invalid table prefix for source prefix={}",
            source_prefix
        );
        return keys.to_vec();
    };

    let cache_key = table_prefix.clone();
    if let Some((cached_version, cached_stats)) = get_cached_delta_pruning_metadata(&cache_key) {
        if let Some(pin) = scan_hints.delta_version_pin
            && pin > 0
            && pin != cached_version
        {
            log::warn!(
                "scan pruning fallback: delta version pin mismatch for prefix={} pin={} latest={} (cache hit)",
                source_prefix,
                pin,
                cached_version
            );
            return keys.to_vec();
        }

        if cached_stats.is_empty() {
            log::warn!(
                "scan pruning fallback: no usable delta file stats for prefix={} version={} (cache hit)",
                source_prefix,
                cached_version
            );
            return keys.to_vec();
        }

        let pruned = prune_keys_by_delta_stats(keys, &cached_stats, &predicate_ast);
        log::info!(
            "scan pruning applied for prefix={} version={} total_files={} retained_files={} pruned_files={} cache=hit",
            source_prefix,
            cached_version,
            keys.len(),
            pruned.len(),
            keys.len().saturating_sub(pruned.len())
        );
        return pruned;
    }

    let latest_version = match latest_delta_log_version(provider, &delta_log_prefix).await {
        Ok(Some(version)) => version,
        Ok(None) => {
            log::warn!(
                "scan pruning fallback: no delta log json version found under {}",
                delta_log_prefix
            );
            return keys.to_vec();
        }
        Err(err) => {
            log::warn!(
                "scan pruning fallback: failed reading latest delta log version under {}: {}",
                delta_log_prefix,
                err
            );
            return keys.to_vec();
        }
    };

    if let Some(pin) = scan_hints.delta_version_pin
        && pin > 0
        && pin != latest_version
    {
        log::warn!(
            "scan pruning fallback: delta version pin mismatch for prefix={} pin={} latest={}",
            source_prefix,
            pin,
            latest_version
        );
        return keys.to_vec();
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
        Err(err) => {
            log::warn!(
                "scan pruning fallback: failed to load delta file stats for prefix={} version={}: {}",
                source_prefix,
                latest_version,
                err
            );
            return keys.to_vec();
        }
    };

    if stats.is_empty() {
        log::warn!(
            "scan pruning fallback: no usable delta file stats for prefix={} version={}",
            source_prefix,
            latest_version
        );
        return keys.to_vec();
    }

    put_cached_delta_pruning_metadata(cache_key, latest_version, stats.clone());

    let pruned = prune_keys_by_delta_stats(keys, &stats, &predicate_ast);
    log::info!(
        "scan pruning applied for prefix={} version={} total_files={} retained_files={} pruned_files={} cache=miss",
        source_prefix,
        latest_version,
        keys.len(),
        pruned.len(),
        keys.len().saturating_sub(pruned.len())
    );
    pruned
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
    task: &worker_service::Task,
    session_id: &str,
    namespace: &QueryNamespace,
    result_location: &str,
) -> Result<(), String> {
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

    let runtime_plan = extract_runtime_plan(task)?;
    let stage_context = stage_execution_context(task);
    let relation_columns_by_key = parse_relation_columns_map(task);
    let source_relation_key = relation_key(namespace);
    let mut batches = load_input_batches(shared, session_id, namespace, &stage_context).await?;

    if stage_context.upstream_stage_ids.is_empty()
        && let Some(columns) = relation_columns_by_key.get(source_relation_key.as_str())
    {
        batches = apply_relation_column_names(&batches, columns)?;
    }

    if let Some(sql) = runtime_plan.filter_sql.as_deref() {
        batches = apply_filter_pipeline(&batches, sql, runtime_plan.schema_metadata.as_ref())?;
    }

    if let Some(join_spec) = runtime_plan.join_spec.as_ref() {
        let right_namespace = QueryNamespace {
            database: join_spec.right_relation.database.clone(),
            schema: join_spec.right_relation.schema.clone(),
            table: join_spec.right_relation.table.clone(),
        };
        let right_prefix = source_table_staging_prefix(&right_namespace);
        let mut right_batches =
            load_scan_batches(shared, &right_prefix, &RuntimeScanHints::full_scan()).await?;
        let right_relation_key = relation_key(&right_namespace);
        if let Some(columns) = relation_columns_by_key.get(right_relation_key.as_str()) {
            right_batches = apply_relation_column_names(&right_batches, columns)?;
        }
        batches = apply_hash_join_pipeline(&batches, &right_batches, join_spec)?;
    }

    if let Some(aggregate_partial_spec) = runtime_plan.aggregate_partial_spec.as_ref() {
        batches = apply_aggregate_partial_pipeline(&batches, aggregate_partial_spec)?;
    }

    if let Some(aggregate_final_spec) = runtime_plan.aggregate_final_spec.as_ref() {
        batches = apply_aggregate_final_pipeline(&batches, aggregate_final_spec)?;
    }

    if !runtime_plan.projection_exprs.is_empty() {
        batches = apply_projection_pipeline(&batches, &runtime_plan.projection_exprs)?;
    }

    if !runtime_plan.sort_exprs.is_empty() {
        batches = apply_sort_pipeline(&batches, &runtime_plan.sort_exprs)?;
    }

    if let Some(limit_spec) = runtime_plan.limit_spec.as_ref() {
        batches = apply_limit_pipeline(&batches, limit_spec)?;
    }

    if batches.is_empty() {
        return Err("query execution produced no record batches".to_string());
    }

    let normalized_batches = normalize_batches_for_sort(&batches)?;

    persist_stage_exchange_artifacts(
        shared,
        session_id,
        &stage_context.query_run_id,
        stage_context.stage_id,
        stage_context.partition_index,
        stage_context.partition_count,
        &stage_context.upstream_stage_ids,
        &normalized_batches,
    )
    .await?;

    if runtime_plan.has_materialize {
        persist_query_artifacts(
            shared,
            result_location,
            session_id,
            &task.task_id,
            &normalized_batches,
        )
        .await
    } else {
        Ok(())
    }
}

fn relation_key(namespace: &QueryNamespace) -> String {
    format!(
        "{}.{}.{}",
        namespace.database.to_ascii_lowercase(),
        namespace.schema.to_ascii_lowercase(),
        namespace.table.to_ascii_lowercase()
    )
}

fn parse_relation_columns_map(task: &worker_service::Task) -> HashMap<String, Vec<String>> {
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
) -> Result<Vec<RecordBatch>, String> {
    if stage_context.upstream_stage_ids.is_empty() {
        let source_prefix = source_table_staging_prefix(namespace);
        let source_batches =
            load_scan_batches(shared, &source_prefix, &stage_context.scan_hints).await?;
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
        let mut keys = list_stage_exchange_data_keys(
            provider,
            &stage_context.query_run_id,
            session_id,
            *upstream_stage_id,
        )
        .await
        .map_err(|e| {
            format!(
                "failed to list exchange artifacts for upstream stage {}: {}",
                upstream_stage_id, e
            )
        })?;

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
            let suffix = format!("part-{:05}.parquet", stage_context.partition_index);
            keys.retain(|key| key.ends_with(&suffix));
            if keys.is_empty() {
                return Err(format!(
                    "missing exchange artifact for upstream stage {} partition {}",
                    upstream_stage_id, stage_context.partition_index
                ));
            }
        }

        for key in keys {
            let bytes = provider
                .get_object(&key)
                .await
                .map_err(|e| format!("failed to read exchange artifact {}: {}", key, e))?
                .ok_or_else(|| format!("exchange artifact not found: {}", key))?;
            let decoded = decode_parquet_batches(bytes)?;
            all_batches.extend(decoded);
        }
    }

    if all_batches.is_empty() {
        return Err(format!(
            "upstream exchange artifacts for stage {} contain no batches",
            stage_context.stage_id
        ));
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
) -> Result<Vec<RecordBatch>, String> {
    let provider = shared
        .storage_provider
        .as_ref()
        .ok_or_else(|| "storage provider is not configured for query execution".to_string())?;

    if scan_hints.mode == crate::execution::planner::RuntimeScanMode::MetadataPruned {
        if scan_hints.pruning_eligible {
            log::info!(
                "scan_mode=metadata_pruned requested for prefix={} delta_version_pin={:?} hints_present={} reason={} (fallback to full scan until pruning phase is implemented)",
                source_prefix,
                scan_hints.delta_version_pin,
                scan_hints.pruning_hints_json.is_some(),
                scan_hints.pruning_reason.as_deref().unwrap_or("eligible")
            );
        } else {
            log::warn!(
                "scan_mode=metadata_pruned requested but hints are not eligible for prefix={} delta_version_pin={:?} reason={} (falling back to full scan)",
                source_prefix,
                scan_hints.delta_version_pin,
                scan_hints.pruning_reason.as_deref().unwrap_or("unknown")
            );
        }
    } else if scan_hints.pruning_hints_json.is_some() || scan_hints.delta_version_pin.is_some() {
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

    let keys = maybe_prune_scan_keys(provider, source_prefix, &keys, scan_hints).await;
    if keys.is_empty() {
        return Err(format!(
            "scan pruning eliminated all source parquet files for query prefix {}",
            source_prefix
        ));
    }

    let mut all_batches = Vec::new();
    for key in keys {
        let bytes = provider
            .get_object(&key)
            .await
            .map_err(|e| format!("failed to read source object {}: {}", key, e))?
            .ok_or_else(|| format!("source object not found: {}", key))?;

        let decoded = decode_parquet_batches(bytes)?;
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
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(reader_source)
        .map_err(|e| format!("failed to open parquet reader: {}", e))?
        .with_batch_size(1024)
        .build()
        .map_err(|e| format!("failed to build parquet reader: {}", e))?;

    let mut batches = Vec::new();
    for maybe_batch in &mut reader {
        let batch = maybe_batch.map_err(|e| format!("failed reading parquet batch: {}", e))?;
        batches.push(batch);
    }

    Ok(batches)
}

#[cfg(test)]
#[path = "../tests/execution_pipeline_tests.rs"]
mod tests;
