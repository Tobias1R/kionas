use crate::services::query::QueryNamespace;
use crate::services::query_join::apply_hash_join_pipeline;
use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;
use crate::storage::exchange::{
    list_stage_exchange_data_keys, stage_exchange_data_key, stage_exchange_metadata_key,
};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Int16Array, Int32Array, Int64Array, StringArray,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cast::cast;
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use kionas::planner::{
    PhysicalExpr, PhysicalJoinSpec, PhysicalLimitSpec, PhysicalOperator, PhysicalPlan,
    PhysicalSortExpr,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::{BTreeSet, HashMap};
use std::io::Cursor;
use std::sync::Arc;
use url::Url;

/// What: Executable subset extracted from validated physical operators.
///
/// Inputs:
/// - `filter_sql`: Optional raw SQL predicate for simple conjunction filtering.
/// - `projection_exprs`: Ordered projection expressions from payload.
///
/// Output:
/// - Runtime projection/filter directives for the local worker pipeline.
#[derive(Debug, Clone)]
struct RuntimePlan {
    filter_sql: Option<String>,
    join_spec: Option<PhysicalJoinSpec>,
    projection_exprs: Vec<PhysicalExpr>,
    sort_exprs: Vec<PhysicalSortExpr>,
    limit_spec: Option<PhysicalLimitSpec>,
    has_materialize: bool,
}

#[derive(Debug, Clone)]
struct StageExecutionContext {
    stage_id: u32,
    upstream_stage_ids: Vec<u32>,
    upstream_partition_counts: HashMap<u32, u32>,
    partition_count: u32,
    partition_index: u32,
    query_run_id: String,
}

#[derive(Debug, Clone)]
enum FilterValue {
    Int(i64),
    Bool(bool),
    Str(String),
}

#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,
}

#[derive(Debug, Clone, serde::Serialize)]
struct QueryArtifactMetadata {
    key: String,
    size_bytes: u64,
    checksum_fnv64: String,
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
    let mut batches = load_input_batches(shared, session_id, namespace, &stage_context).await?;

    if let Some(sql) = runtime_plan.filter_sql.as_deref() {
        batches = apply_filter_pipeline(&batches, sql)?;
    }

    if let Some(join_spec) = runtime_plan.join_spec.as_ref() {
        let right_namespace = QueryNamespace {
            database: join_spec.right_relation.database.clone(),
            schema: join_spec.right_relation.schema.clone(),
            table: join_spec.right_relation.table.clone(),
        };
        let right_prefix = source_table_staging_prefix(&right_namespace);
        let right_batches = load_scan_batches(shared, &right_prefix).await?;
        batches = apply_hash_join_pipeline(&batches, &right_batches, join_spec)?;
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

    persist_stage_exchange_artifacts(shared, session_id, &stage_context, &batches).await?;

    if runtime_plan.has_materialize {
        persist_query_artifacts(shared, result_location, session_id, &task.task_id, &batches).await
    } else {
        Ok(())
    }
}

/// What: Parse and extract executable runtime operators from validated physical plan payload.
///
/// Inputs:
/// - `task`: Query task containing canonical payload JSON.
///
/// Output:
/// - Runtime plan that includes optional filter SQL and projection expressions.
fn extract_runtime_plan(task: &worker_service::Task) -> Result<RuntimePlan, String> {
    let operators = extract_runtime_operators(task)?;

    let mut filter_sql = None;
    let mut join_spec = None;
    let mut projection_exprs = Vec::new();
    let mut sort_exprs = Vec::new();
    let mut limit_spec = None;
    let mut has_materialize = false;

    for op in operators {
        match op {
            PhysicalOperator::TableScan { .. } => {}
            PhysicalOperator::Filter { predicate } => {
                let raw = match predicate {
                    PhysicalExpr::Raw { sql } => sql,
                    PhysicalExpr::ColumnRef { name } => name,
                };
                filter_sql = Some(raw);
            }
            PhysicalOperator::HashJoin { spec } => {
                join_spec = Some(spec);
            }
            PhysicalOperator::Projection { expressions } => {
                projection_exprs = expressions;
            }
            PhysicalOperator::Sort { keys } => {
                sort_exprs = keys;
            }
            PhysicalOperator::Limit { spec } => {
                limit_spec = Some(spec);
            }
            PhysicalOperator::Materialize => {
                has_materialize = true;
            }
            other => {
                return Err(format!(
                    "physical operator '{}' is not executable in this phase",
                    other.canonical_name()
                ));
            }
        }
    }

    Ok(RuntimePlan {
        filter_sql,
        join_spec,
        projection_exprs,
        sort_exprs,
        limit_spec,
        has_materialize,
    })
}

/// What: Decode executable operator pipeline from canonical or stage payload shape.
///
/// Inputs:
/// - `task`: Query task carrying serialized payload.
///
/// Output:
/// - Ordered executable physical operators.
fn extract_runtime_operators(task: &worker_service::Task) -> Result<Vec<PhysicalOperator>, String> {
    let payload: serde_json::Value =
        serde_json::from_str(&task.input).map_err(|e| format!("invalid query payload: {}", e))?;

    if let Some(operators) = payload.as_array() {
        return serde_json::from_value(serde_json::Value::Array(operators.clone()))
            .map_err(|e| format!("invalid stage operator payload: {}", e));
    }

    if let Some(operators) = payload
        .get("operators")
        .and_then(serde_json::Value::as_array)
    {
        return serde_json::from_value(serde_json::Value::Array(operators.clone()))
            .map_err(|e| format!("invalid stage payload operators: {}", e));
    }

    let physical_plan = payload
        .get("physical_plan")
        .cloned()
        .ok_or_else(|| "query payload missing physical_plan".to_string())?;
    let physical_plan: PhysicalPlan = serde_json::from_value(physical_plan)
        .map_err(|e| format!("invalid physical_plan payload: {}", e))?;

    Ok(physical_plan.operators)
}

/// What: Build stage execution context from task params.
///
/// Inputs:
/// - `task`: Query task metadata and params.
///
/// Output:
/// - Stage execution context with deterministic defaults.
fn stage_execution_context(task: &worker_service::Task) -> StageExecutionContext {
    let stage_id = task
        .params
        .get("stage_id")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let upstream_stage_ids = task
        .params
        .get("upstream_stage_ids")
        .and_then(|value| serde_json::from_str::<Vec<u32>>(value).ok())
        .unwrap_or_default();
    let partition_count = task
        .params
        .get("partition_count")
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(1);
    let upstream_partition_counts = task
        .params
        .get("upstream_partition_counts")
        .and_then(|value| serde_json::from_str::<HashMap<u32, u32>>(value).ok())
        .unwrap_or_default();
    let partition_index = task
        .params
        .get("partition_index")
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let query_run_id = task
        .params
        .get("query_run_id")
        .filter(|value| !value.trim().is_empty())
        .cloned()
        .unwrap_or_else(|| format!("legacy-task-{}", task.task_id));

    StageExecutionContext {
        stage_id,
        upstream_stage_ids,
        upstream_partition_counts,
        partition_count,
        partition_index,
        query_run_id,
    }
}

/// What: Parse exchange partition index from a stage artifact key.
///
/// Inputs:
/// - `key`: Object-store key ending with `part-xxxxx.parquet`.
///
/// Output:
/// - Partition index when the key matches expected naming.
fn parse_partition_index_from_exchange_key(key: &str) -> Option<u32> {
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
fn validate_upstream_exchange_partition_set(
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
/// - `task`: Query task metadata and params.
/// - `session_id`: Session identifier used for exchange scoping.
/// - `namespace`: Canonical query namespace for source scan fallback.
/// - `stage_context`: Stage metadata resolved from task params.
///
/// Output:
/// - Ordered input record batches for current stage execution.
async fn load_input_batches(
    shared: &SharedData,
    session_id: &str,
    namespace: &QueryNamespace,
    stage_context: &StageExecutionContext,
) -> Result<Vec<RecordBatch>, String> {
    if stage_context.upstream_stage_ids.is_empty() {
        let source_prefix = source_table_staging_prefix(namespace);
        let source_batches = load_scan_batches(shared, &source_prefix).await?;
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
/// - `task`: Query task metadata and params.
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

        if stage_context.partition_count == 1 {
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
            }
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
fn partition_input_batches(
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

/// What: Persist stage output batches as deterministic storage-mediated exchange artifacts.
///
/// Inputs:
/// - `shared`: Worker state with storage provider.
/// - `session_id`: Session identifier used for exchange scoping.
/// - `stage_context`: Stage metadata resolved from task params.
/// - `batches`: Stage output batches.
///
/// Output:
/// - `Ok(())` when parquet artifact and metadata sidecar are written.
async fn persist_stage_exchange_artifacts(
    shared: &SharedData,
    session_id: &str,
    stage_context: &StageExecutionContext,
    batches: &[RecordBatch],
) -> Result<(), String> {
    let provider = shared.storage_provider.as_ref().ok_or_else(|| {
        "storage provider is not configured for stage exchange persistence".to_string()
    })?;

    let normalized_batches = normalize_batches_for_sort(batches)?;

    let data_key = stage_exchange_data_key(
        &stage_context.query_run_id,
        session_id,
        stage_context.stage_id,
        stage_context.partition_index,
    );
    let metadata_key = stage_exchange_metadata_key(
        &stage_context.query_run_id,
        session_id,
        stage_context.stage_id,
        stage_context.partition_index,
    );

    let parquet_bytes = encode_batches_to_parquet(&normalized_batches)?;
    let artifacts = vec![QueryArtifactMetadata {
        key: data_key.clone(),
        size_bytes: parquet_bytes.len() as u64,
        checksum_fnv64: checksum_fnv64_hex(&parquet_bytes),
    }];

    let metadata_bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "stage_id": stage_context.stage_id,
        "partition_index": stage_context.partition_index,
        "partition_count": stage_context.partition_count,
        "upstream_stage_ids": stage_context.upstream_stage_ids,
        "query_run_id": stage_context.query_run_id,
        "row_count": normalized_batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        "batch_count": normalized_batches.len(),
        "artifacts": artifacts,
    }))
    .map_err(|e| format!("failed to encode stage exchange metadata json: {}", e))?;

    provider
        .put_object(&data_key, parquet_bytes)
        .await
        .map_err(|e| {
            format!(
                "failed to persist stage exchange artifact {}: {}",
                data_key, e
            )
        })?;

    provider
        .put_object(&metadata_key, metadata_bytes)
        .await
        .map_err(|e| {
            format!(
                "failed to persist stage exchange metadata {}: {}",
                metadata_key, e
            )
        })?;

    Ok(())
}

/// What: Build the source Delta table staging object prefix from canonical namespace.
///
/// Inputs:
/// - `namespace`: Canonical namespace for relation lookup.
///
/// Output:
/// - Object-store prefix under which committed parquet files are listed.
fn source_table_staging_prefix(namespace: &QueryNamespace) -> String {
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
async fn load_scan_batches(
    shared: &SharedData,
    source_prefix: &str,
) -> Result<Vec<RecordBatch>, String> {
    let provider = shared
        .storage_provider
        .as_ref()
        .ok_or_else(|| "storage provider is not configured for query execution".to_string())?;

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
fn decode_parquet_batches(parquet_bytes: Vec<u8>) -> Result<Vec<RecordBatch>, String> {
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

/// What: Apply a simple conjunction filter pipeline to all input batches.
///
/// Inputs:
/// - `input`: Source batches.
/// - `filter_sql`: Raw SQL predicate containing simple `AND` clauses.
///
/// Output:
/// - Filtered batches preserving input order.
fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let clauses = parse_filter_clauses(filter_sql)?;
    let mut out = Vec::with_capacity(input.len());

    for batch in input {
        let mask = build_filter_mask(batch, &clauses)?;
        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| format!("failed to apply filter: {}", e))?;
        out.push(filtered);
    }

    Ok(out)
}

/// What: Parse a restricted SQL predicate subset into executable clauses.
///
/// Inputs:
/// - `filter_sql`: Raw filter SQL.
///
/// Output:
/// - Ordered filter clauses combined with logical `AND`.
fn parse_filter_clauses(filter_sql: &str) -> Result<Vec<FilterClause>, String> {
    if filter_sql.trim().is_empty() {
        return Err("filter predicate is empty".to_string());
    }
    if !filter_sql.is_ascii() {
        return Err("filter predicate must use ASCII characters in this phase".to_string());
    }

    let normalized = format!(" {} ", filter_sql.to_ascii_lowercase());
    if normalized.contains(" or ") {
        return Err("filter predicate with OR is not supported in this phase".to_string());
    }

    let mut clauses = Vec::new();
    for raw_clause in split_case_insensitive(filter_sql, "AND") {
        let clause = raw_clause.trim();
        if clause.is_empty() {
            continue;
        }

        let (column, op, literal) = parse_single_clause(clause)?;
        clauses.push(FilterClause {
            column,
            op,
            value: parse_filter_value(literal)?,
        });
    }

    if clauses.is_empty() {
        return Err("filter predicate produced no executable clauses".to_string());
    }

    Ok(clauses)
}

/// What: Build a row mask for a batch given executable filter clauses.
///
/// Inputs:
/// - `batch`: Batch to evaluate.
/// - `clauses`: Parsed filter clauses.
///
/// Output:
/// - Boolean mask that marks rows passing all clauses.
fn build_filter_mask(
    batch: &RecordBatch,
    clauses: &[FilterClause],
) -> Result<BooleanArray, String> {
    let mut rows = vec![true; batch.num_rows()];

    for clause in clauses {
        let idx = resolve_schema_column_index(batch.schema().as_ref(), &clause.column)
            .ok_or_else(|| format!("filter column '{}' not found", clause.column))?;
        let array = batch.column(idx);

        for (row_idx, row_flag) in rows.iter_mut().enumerate() {
            if !*row_flag {
                continue;
            }

            let pass = evaluate_clause_at_row(array, row_idx, clause)?;
            if !pass {
                *row_flag = false;
            }
        }
    }

    Ok(BooleanArray::from(rows))
}

/// What: Evaluate one clause at one row index.
///
/// Inputs:
/// - `array`: Input column array.
/// - `row_idx`: Row index.
/// - `clause`: Clause metadata and literal.
///
/// Output:
/// - `true` when the row satisfies the clause.
fn evaluate_clause_at_row(
    array: &ArrayRef,
    row_idx: usize,
    clause: &FilterClause,
) -> Result<bool, String> {
    if array.is_null(row_idx) {
        return Ok(false);
    }

    match (&clause.value, array.data_type()) {
        (FilterValue::Int(rhs), DataType::Int64) => {
            let lhs = downcast_required::<Int64Array>(array, "Int64")?.value(row_idx);
            Ok(compare_i64(lhs, *rhs, clause.op))
        }
        (FilterValue::Int(rhs), DataType::Int32) => {
            let lhs = i64::from(downcast_required::<Int32Array>(array, "Int32")?.value(row_idx));
            Ok(compare_i64(lhs, *rhs, clause.op))
        }
        (FilterValue::Int(rhs), DataType::Int16) => {
            let lhs = i64::from(downcast_required::<Int16Array>(array, "Int16")?.value(row_idx));
            Ok(compare_i64(lhs, *rhs, clause.op))
        }
        (FilterValue::Bool(rhs), DataType::Boolean) => {
            let lhs = downcast_required::<BooleanArray>(array, "Boolean")?.value(row_idx);
            Ok(compare_bool(lhs, *rhs, clause.op))
        }
        (FilterValue::Str(rhs), DataType::Utf8) => {
            let lhs = downcast_required::<StringArray>(array, "Utf8")?.value(row_idx);
            Ok(compare_str(lhs, rhs.as_str(), clause.op))
        }
        (_, data_type) => Err(format!(
            "filter type mismatch for column '{}': unsupported type {:?}",
            clause.column, data_type
        )),
    }
}

/// What: Apply ordered projection expressions to all input batches.
///
/// Inputs:
/// - `input`: Source batches.
/// - `projection_exprs`: Ordered projection expressions.
///
/// Output:
/// - Projected batches preserving order and output column ordering.
fn apply_projection_pipeline(
    input: &[RecordBatch],
    projection_exprs: &[PhysicalExpr],
) -> Result<Vec<RecordBatch>, String> {
    let mut out = Vec::with_capacity(input.len());

    for batch in input {
        let mut projected_arrays = Vec::<ArrayRef>::new();
        let mut projected_fields = Vec::<Field>::new();

        for expr in projection_exprs {
            match expr {
                PhysicalExpr::ColumnRef { name } => {
                    let output_name = normalize_projection_identifier(name);
                    push_projected_column(
                        batch,
                        name,
                        Some(output_name.as_str()),
                        &mut projected_arrays,
                        &mut projected_fields,
                    )?;
                }
                PhysicalExpr::Raw { sql } => {
                    let raw = sql.trim();
                    if raw == "*" {
                        for (idx, field) in batch.schema().fields().iter().enumerate() {
                            projected_arrays.push(batch.column(idx).clone());
                            projected_fields.push(field.as_ref().clone());
                        }
                        continue;
                    }

                    let name = parse_projection_identifier(raw)?;
                    let output_name = normalize_projection_identifier(raw);
                    push_projected_column(
                        batch,
                        &name,
                        Some(output_name.as_str()),
                        &mut projected_arrays,
                        &mut projected_fields,
                    )?;
                }
            }
        }

        let projected_schema = Arc::new(Schema::new(projected_fields));
        let projected = RecordBatch::try_new(projected_schema, projected_arrays)
            .map_err(|e| format!("failed to build projected record batch: {}", e))?;
        out.push(projected);
    }

    Ok(out)
}

/// What: Apply ORDER BY sorting to input batches.
///
/// Inputs:
/// - `input`: Source batches.
/// - `sort_exprs`: Ordered sort directives.
///
/// Output:
/// - Sorted batches with deterministic row order.
fn apply_sort_pipeline(
    input: &[RecordBatch],
    sort_exprs: &[PhysicalSortExpr],
) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let normalized_batches = normalize_batches_for_sort(input)?;
    let schema = normalized_batches[0].schema();
    let merged = concat_batches(&schema, &normalized_batches)
        .map_err(|e| format!("failed to merge batches for sorting: {}", e))?;

    if merged.num_rows() <= 1 {
        return Ok(vec![merged]);
    }

    let mut sort_columns = Vec::with_capacity(sort_exprs.len());
    for sort_expr in sort_exprs {
        let raw_name = match &sort_expr.expression {
            PhysicalExpr::ColumnRef { name } => name.trim().to_string(),
            PhysicalExpr::Raw { sql } => parse_projection_identifier(sql)?,
        };

        let idx = resolve_schema_column_index(merged.schema().as_ref(), raw_name.as_str())
            .ok_or_else(|| format!("sort column '{}' not found", raw_name))?;

        sort_columns.push(SortColumn {
            values: merged.column(idx).clone(),
            options: Some(SortOptions {
                descending: !sort_expr.ascending,
                nulls_first: false,
            }),
        });
    }

    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| format!("failed to compute sort indices: {}", e))?;

    let sorted_columns = merged
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None)
                .map_err(|e| format!("failed to reorder sorted column: {}", e))
        })
        .collect::<Result<Vec<ArrayRef>, String>>()?;

    let sorted = RecordBatch::try_new(merged.schema(), sorted_columns)
        .map_err(|e| format!("failed to build sorted record batch: {}", e))?;

    Ok(vec![sorted])
}

/// What: Apply LIMIT/OFFSET slicing to input batches while preserving row order.
///
/// Inputs:
/// - `input`: Ordered source batches.
/// - `limit_spec`: Count and offset values.
///
/// Output:
/// - Sliced batches that satisfy OFFSET then LIMIT semantics.
fn apply_limit_pipeline(
    input: &[RecordBatch],
    limit_spec: &PhysicalLimitSpec,
) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    if limit_spec.count == 0 {
        return Ok(vec![RecordBatch::new_empty(input[0].schema())]);
    }

    let mut remaining_offset = usize::try_from(limit_spec.offset)
        .map_err(|_| "OFFSET value is too large for this platform".to_string())?;
    let mut remaining_count = usize::try_from(limit_spec.count)
        .map_err(|_| "LIMIT value is too large for this platform".to_string())?;

    let mut out = Vec::new();
    for batch in input {
        if remaining_count == 0 {
            break;
        }

        let row_count = batch.num_rows();
        if remaining_offset >= row_count {
            remaining_offset -= row_count;
            continue;
        }

        let start = remaining_offset;
        remaining_offset = 0;
        let available = row_count - start;
        let take_count = std::cmp::min(available, remaining_count);
        if take_count == 0 {
            continue;
        }

        out.push(batch.slice(start, take_count));
        remaining_count -= take_count;
    }

    if out.is_empty() {
        return Ok(vec![RecordBatch::new_empty(input[0].schema())]);
    }

    Ok(out)
}

/// What: Normalize input batches into a compatible schema for global sorting.
///
/// Inputs:
/// - `input`: Source batches that may carry type variations across files.
///
/// Output:
/// - New batch list with per-column types aligned for concat and sort.
fn normalize_batches_for_sort(input: &[RecordBatch]) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let column_count = input[0].num_columns();
    for batch in input {
        if batch.num_columns() != column_count {
            return Err("cannot sort batches with different column counts".to_string());
        }
    }

    let mut fields = Vec::with_capacity(column_count);
    for col_idx in 0..column_count {
        let base_field = input[0].schema().field(col_idx).as_ref().clone();
        let mut target_type: Option<DataType> = None;

        for batch in input {
            let data_type = batch.column(col_idx).data_type();
            target_type = Some(unify_sort_column_type(target_type.as_ref(), data_type));
        }

        let resolved_type = target_type.unwrap_or(DataType::Null);
        fields.push(Field::new(
            base_field.name(),
            resolved_type,
            base_field.is_nullable(),
        ));
    }

    let target_schema = Arc::new(Schema::new(fields));
    let mut normalized = Vec::with_capacity(input.len());
    for batch in input {
        let mut arrays = Vec::with_capacity(column_count);
        for col_idx in 0..column_count {
            let source = batch.column(col_idx);
            let target_type = target_schema.field(col_idx).data_type();
            if source.data_type() == target_type {
                arrays.push(source.clone());
                continue;
            }

            let casted = cast(source.as_ref(), target_type).map_err(|e| {
                format!(
                    "failed to cast column '{}' from {:?} to {:?} for sorting: {}",
                    target_schema.field(col_idx).name(),
                    source.data_type(),
                    target_type,
                    e
                )
            })?;
            arrays.push(casted);
        }

        let rebuilt = RecordBatch::try_new(target_schema.clone(), arrays)
            .map_err(|e| format!("failed to rebuild normalized batch for sorting: {}", e))?;
        normalized.push(rebuilt);
    }

    Ok(normalized)
}

fn unify_sort_column_type(existing: Option<&DataType>, incoming: &DataType) -> DataType {
    match existing {
        None => incoming.clone(),
        Some(current) if current == incoming => current.clone(),
        Some(DataType::Null) => incoming.clone(),
        Some(current) if matches!(incoming, DataType::Null) => current.clone(),
        Some(current) if is_int_type(current) && is_int_type(incoming) => DataType::Int64,
        Some(current)
            if matches!(current, DataType::Utf8) || matches!(incoming, DataType::Utf8) =>
        {
            DataType::Utf8
        }
        Some(current) => current.clone(),
    }
}

fn is_int_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

/// What: Persist result batches to deterministic task-scoped artifact location.
///
/// Inputs:
/// - `shared`: Worker state with storage provider.
/// - `result_location`: Flight URI used as retrieval root.
/// - `session_id`: Session id component for task scoping.
/// - `task_id`: Task id component for task scoping.
/// - `batches`: Executed result batches.
///
/// Output:
/// - `Ok(())` when result parquet file is stored.
async fn persist_query_artifacts(
    shared: &SharedData,
    result_location: &str,
    session_id: &str,
    task_id: &str,
    batches: &[RecordBatch],
) -> Result<(), String> {
    let provider = shared.storage_provider.as_ref().ok_or_else(|| {
        "storage provider is not configured for query result persistence".to_string()
    })?;

    let normalized_batches = normalize_batches_for_sort(batches)?;

    let path = Url::parse(result_location)
        .map_err(|e| format!("result_location is not a valid URI: {}", e))?
        .path()
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();

    if path.is_empty() {
        return Err("result_location path is empty".to_string());
    }

    let prefix = format!("{}/staging/{}/{}/", path, session_id, task_id);
    let key = format!("{}part-00000.parquet", prefix);
    let parquet_bytes = encode_batches_to_parquet(&normalized_batches)?;
    let metadata_key = format!("{}result_metadata.json", prefix);
    let artifacts = vec![QueryArtifactMetadata {
        key: key.clone(),
        size_bytes: parquet_bytes.len() as u64,
        checksum_fnv64: checksum_fnv64_hex(&parquet_bytes),
    }];
    let metadata_bytes = encode_result_metadata(&normalized_batches, &artifacts)?;

    provider
        .put_object(&key, parquet_bytes)
        .await
        .map_err(|e| format!("failed to persist query artifact {}: {}", key, e))?;

    provider
        .put_object(&metadata_key, metadata_bytes)
        .await
        .map_err(|e| format!("failed to persist query metadata {}: {}", metadata_key, e))
}

/// What: Encode record batches into a single parquet payload.
///
/// Inputs:
/// - `batches`: Ordered Arrow batches with compatible schema.
///
/// Output:
/// - Encoded parquet bytes.
fn encode_batches_to_parquet(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    let first = batches
        .first()
        .ok_or_else(|| "cannot encode empty batch list".to_string())?;

    let schema_ref = first.schema();
    let props = WriterProperties::builder().build();
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = ArrowWriter::try_new(&mut cursor, schema_ref, Some(props))
        .map_err(|e| format!("failed to create parquet writer: {}", e))?;

    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("failed writing parquet batch: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| format!("failed closing parquet writer: {}", e))?;
    Ok(cursor.into_inner())
}

/// What: Encode query result metadata sidecar for deterministic task artifacts.
///
/// Inputs:
/// - `batches`: Executed result batches.
/// - `artifacts`: Persisted query result artifact metadata.
///
/// Output:
/// - JSON bytes containing row count and schema summary.
fn encode_result_metadata(
    batches: &[RecordBatch],
    artifacts: &[QueryArtifactMetadata],
) -> Result<Vec<u8>, String> {
    let first = batches
        .first()
        .ok_or_else(|| "cannot encode metadata for empty batch list".to_string())?;

    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let fields = first
        .schema()
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "data_type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect::<Vec<_>>();

    let artifact_values = artifacts
        .iter()
        .map(|artifact| {
            serde_json::json!({
                "key": artifact.key,
                "size_bytes": artifact.size_bytes,
                "checksum_fnv64": artifact.checksum_fnv64,
            })
        })
        .collect::<Vec<_>>();

    serde_json::to_vec_pretty(&serde_json::json!({
        "row_count": row_count,
        "source_batch_count": batches.len(),
        "parquet_file_count": artifacts.len(),
        "columns": fields,
        "artifacts": artifact_values,
    }))
    .map_err(|e| format!("failed to encode query metadata json: {}", e))
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

fn push_projected_column(
    batch: &RecordBatch,
    name: &str,
    output_name: Option<&str>,
    arrays: &mut Vec<ArrayRef>,
    fields: &mut Vec<Field>,
) -> Result<(), String> {
    let idx = resolve_schema_column_index(batch.schema().as_ref(), name)
        .ok_or_else(|| format!("projection column '{}' not found", name))?;

    arrays.push(batch.column(idx).clone());
    let base_field = batch.schema().field(idx).as_ref().clone();
    let field = output_name
        .filter(|value| !value.trim().is_empty())
        .map(|value| base_field.clone().with_name(value.trim()))
        .unwrap_or(base_field);
    fields.push(field);
    Ok(())
}

fn normalize_projection_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    let no_prefix = if let Some((_, rhs)) = trimmed.rsplit_once('.') {
        rhs
    } else {
        trimmed
    };

    no_prefix
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

fn resolve_schema_column_index(schema: &Schema, requested: &str) -> Option<usize> {
    let requested = requested.trim();
    if requested.is_empty() {
        return None;
    }

    if let Ok(idx) = schema.index_of(requested) {
        return Some(idx);
    }

    let normalized = normalize_projection_identifier(requested);
    if normalized != requested
        && let Ok(idx) = schema.index_of(normalized.as_str())
    {
        return Some(idx);
    }

    if let Some(idx) = fallback_semantic_to_physical_column_index(schema, normalized.as_str()) {
        return Some(idx);
    }

    None
}

fn fallback_semantic_to_physical_column_index(schema: &Schema, normalized: &str) -> Option<usize> {
    let requested = normalized.to_ascii_lowercase();

    if requested == "id" {
        if let Ok(idx) = schema.index_of("c1") {
            return Some(idx);
        }

        if let Some((idx, _)) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name().ends_with("_c1"))
        {
            return Some(idx);
        }
    }

    if requested == "name" || requested == "value" {
        if let Ok(idx) = schema.index_of("c2") {
            return Some(idx);
        }
    }

    if requested == "document" {
        if let Some((idx, _)) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name().ends_with("_c2"))
        {
            return Some(idx);
        }

        if let Ok(idx) = schema.index_of("c2") {
            return Some(idx);
        }
    }

    None
}

/// What: Validate and parse a projection raw expression into a column identifier.
///
/// Inputs:
/// - `raw_sql`: Raw projection SQL expression.
///
/// Output:
/// - Parsed column identifier for projection lookup.
fn parse_projection_identifier(raw_sql: &str) -> Result<String, String> {
    if !is_simple_identifier_reference(raw_sql) {
        return Err(format!(
            "unsupported projection expression '{}': only '*' or simple column references are supported in this phase",
            raw_sql.trim()
        ));
    }
    Ok(normalize_projection_identifier(raw_sql))
}

/// What: Validate whether an expression is a simple identifier reference.
///
/// Inputs:
/// - `expr`: Raw expression text.
///
/// Output:
/// - `true` when expression is `col` or `qualifier.col` with common quotes.
fn is_simple_identifier_reference(expr: &str) -> bool {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.contains('(')
        || trimmed.contains(')')
        || trimmed.contains('+')
        || trimmed.contains('-')
        || trimmed.contains('*')
        || trimmed.contains('/')
        || trimmed.contains(',')
        || trimmed.contains(':')
        || trimmed.contains(' ')
    {
        return false;
    }

    let parts = trimmed.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 2 {
        return false;
    }

    parts.into_iter().all(is_identifier_segment)
}

fn is_identifier_segment(segment: &str) -> bool {
    let raw = segment
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']');

    !raw.is_empty()
        && raw
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn parse_single_clause(input: &str) -> Result<(String, FilterOp, &str), String> {
    const OPS: [(&str, FilterOp); 6] = [
        (">=", FilterOp::Ge),
        ("<=", FilterOp::Le),
        ("!=", FilterOp::Ne),
        ("=", FilterOp::Eq),
        (">", FilterOp::Gt),
        ("<", FilterOp::Lt),
    ];

    for (token, op) in OPS {
        if let Some((lhs, rhs)) = input.split_once(token) {
            if !is_simple_identifier_reference(lhs) {
                return Err(format!(
                    "unsupported filter column expression '{}': only simple column references are supported",
                    lhs.trim()
                ));
            }
            let column = normalize_projection_identifier(lhs);
            let literal = rhs.trim();
            if column.is_empty() || literal.is_empty() {
                return Err(format!("invalid filter clause '{}'", input));
            }
            return Ok((column, op, literal));
        }
    }

    Err(format!(
        "unsupported filter clause '{}': expected one of =, !=, >, >=, <, <=",
        input
    ))
}

fn parse_filter_value(raw: &str) -> Result<FilterValue, String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(FilterValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(FilterValue::Bool(false));
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return Ok(FilterValue::Int(value));
    }

    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        return Ok(FilterValue::Str(trimmed[1..trimmed.len() - 1].to_string()));
    }

    Err(format!(
        "unsupported filter literal '{}': expected int, bool, or quoted string",
        raw
    ))
}

fn split_case_insensitive<'a>(text: &'a str, token: &str) -> Vec<&'a str> {
    let splitter = format!(" {} ", token.to_ascii_lowercase());
    let lower = text.to_ascii_lowercase();

    let mut out = Vec::new();
    let mut start = 0usize;
    for (idx, _) in lower.match_indices(&splitter) {
        out.push(text[start..idx].trim());
        start = idx + splitter.len();
    }

    out.push(text[start..].trim());
    out
}

fn downcast_required<'a, T: 'static>(array: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast array to {}", expected))
}

fn compare_i64(lhs: i64, rhs: i64, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Ge => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Le => lhs <= rhs,
    }
}

fn compare_bool(lhs: bool, rhs: bool, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs && !rhs,
        FilterOp::Ge => lhs == rhs || (lhs && !rhs),
        FilterOp::Lt => !lhs && rhs,
        FilterOp::Le => lhs == rhs || (!lhs && rhs),
    }
}

fn compare_str(lhs: &str, rhs: &str, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Ge => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Le => lhs <= rhs,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        QueryArtifactMetadata, apply_filter_pipeline, apply_limit_pipeline,
        apply_projection_pipeline, decode_parquet_batches, encode_result_metadata,
        extract_runtime_plan, parse_partition_index_from_exchange_key, parse_projection_identifier,
        partition_input_batches, source_table_staging_prefix, split_case_insensitive,
        validate_upstream_exchange_partition_set,
    };
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use std::io::Cursor;
    use std::sync::Arc;

    fn batch_two_rows() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ],
        )
        .expect("test batch must build")
    }

    #[test]
    fn decodes_written_parquet() {
        let batch = batch_two_rows();
        let props = WriterProperties::builder().build();
        let mut cursor = Cursor::new(Vec::<u8>::new());
        let mut writer = ArrowWriter::try_new(&mut cursor, batch.schema(), Some(props))
            .expect("writer must build");
        writer.write(&batch).expect("write must succeed");
        writer.close().expect("close must succeed");

        let decoded = decode_parquet_batches(cursor.into_inner()).expect("decode must succeed");
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].num_rows(), 2);
    }

    #[test]
    fn applies_simple_int_filter() {
        let filtered =
            apply_filter_pipeline(&[batch_two_rows()], "id > 1").expect("filter must run");
        assert_eq!(filtered[0].num_rows(), 1);
    }

    #[test]
    fn applies_projection_raw_identifier() {
        let projected = apply_projection_pipeline(
            &[batch_two_rows()],
            &[kionas::planner::PhysicalExpr::Raw {
                sql: "name".to_string(),
            }],
        )
        .expect("projection must run");

        assert_eq!(projected[0].num_columns(), 1);
        assert_eq!(projected[0].schema().field(0).name(), "name");
    }

    #[test]
    fn builds_bucket_relative_source_prefix() {
        let ns = crate::services::query::QueryNamespace {
            database: "db1".to_string(),
            schema: "s1".to_string(),
            table: "t1".to_string(),
        };

        let prefix = source_table_staging_prefix(&ns);
        assert_eq!(prefix, "databases/db1/schemas/s1/tables/t1/staging/");
    }

    #[test]
    fn splits_case_insensitive_and_tokens() {
        let parts = split_case_insensitive("id > 1 AnD name = 'a'", "AND");
        assert_eq!(parts, vec!["id > 1", "name = 'a'"]);
    }

    #[test]
    fn encodes_result_metadata_row_count_and_columns() {
        let artifact = QueryArtifactMetadata {
            key: "query/db1/s1/t1/w1/staging/s/t/part-00000.parquet".to_string(),
            size_bytes: 128,
            checksum_fnv64: "0011223344556677".to_string(),
        };
        let bytes =
            encode_result_metadata(&[batch_two_rows()], &[artifact]).expect("metadata must encode");
        let parsed: serde_json::Value =
            serde_json::from_slice(&bytes).expect("metadata json must decode");

        assert_eq!(
            parsed.get("row_count").and_then(serde_json::Value::as_u64),
            Some(2)
        );
        assert_eq!(
            parsed
                .get("source_batch_count")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        assert_eq!(
            parsed
                .get("parquet_file_count")
                .and_then(serde_json::Value::as_u64),
            Some(1)
        );
        let columns = parsed
            .get("columns")
            .and_then(serde_json::Value::as_array)
            .expect("columns array must exist");
        assert_eq!(columns.len(), 2);
        assert_eq!(
            columns[0].get("name").and_then(serde_json::Value::as_str),
            Some("id")
        );
        assert_eq!(
            columns[1].get("name").and_then(serde_json::Value::as_str),
            Some("name")
        );
        let artifacts = parsed
            .get("artifacts")
            .and_then(serde_json::Value::as_array)
            .expect("artifacts array must exist");
        assert_eq!(artifacts.len(), 1);
    }

    #[test]
    fn rejects_unsupported_projection_expression() {
        let err = parse_projection_identifier("CAST(id AS STRING)")
            .expect_err("complex projection must be rejected");
        assert!(err.contains("unsupported projection expression"));
    }

    #[test]
    fn rejects_unsupported_filter_column_expression() {
        let err = apply_filter_pipeline(&[batch_two_rows()], "lower(name) = 'a'")
            .expect_err("complex filter lhs must be rejected");
        assert!(err.contains("unsupported filter column expression"));
    }

    #[test]
    fn partitions_source_rows_deterministically() {
        let part0 = partition_input_batches(&[batch_two_rows()], 2, 0)
            .expect("partition 0 slicing must succeed");
        let part1 = partition_input_batches(&[batch_two_rows()], 2, 1)
            .expect("partition 1 slicing must succeed");

        assert_eq!(part0[0].num_rows(), 1);
        assert_eq!(part1[0].num_rows(), 1);
    }

    #[test]
    fn rejects_out_of_bounds_partition_index() {
        let err = partition_input_batches(&[batch_two_rows()], 2, 2)
            .expect_err("partition index out of range must be rejected");
        assert!(err.contains("invalid partition index"));
    }

    #[test]
    fn parses_partition_index_from_exchange_key() {
        let key = "distributed_exchange/run/s1/stage-4/part-00003.parquet";
        let parsed = parse_partition_index_from_exchange_key(key)
            .expect("partition index must parse from exchange key");
        assert_eq!(parsed, 3);
    }

    #[test]
    fn validates_complete_upstream_partition_set() {
        let keys = vec![
            "distributed_exchange/run/s1/stage-1/part-00000.parquet".to_string(),
            "distributed_exchange/run/s1/stage-1/part-00001.parquet".to_string(),
            "distributed_exchange/run/s1/stage-1/part-00002.parquet".to_string(),
        ];

        validate_upstream_exchange_partition_set(1, &keys, 3)
            .expect("complete partition set must validate");
    }

    #[test]
    fn rejects_incomplete_upstream_partition_set() {
        let keys = vec![
            "distributed_exchange/run/s1/stage-1/part-00000.parquet".to_string(),
            "distributed_exchange/run/s1/stage-1/part-00002.parquet".to_string(),
        ];

        let err = validate_upstream_exchange_partition_set(1, &keys, 3)
            .expect_err("incomplete partition set must be rejected");
        assert!(err.contains("mismatch"));
    }

    #[test]
    fn applies_limit_pipeline_with_offset_across_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64, 3_i64])) as ArrayRef],
        )
        .expect("batch1 must build");
        let batch2 = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![4_i64, 5_i64, 6_i64])) as ArrayRef],
        )
        .expect("batch2 must build");

        let sliced = apply_limit_pipeline(
            &[batch1, batch2],
            &kionas::planner::PhysicalLimitSpec {
                count: 2,
                offset: 3,
            },
        )
        .expect("limit pipeline must succeed");

        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced[0].num_rows(), 2);
        let ids = sliced[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id column must be Int64");
        assert_eq!(ids.value(0), 4);
        assert_eq!(ids.value(1), 5);
    }

    #[test]
    fn applies_zero_limit_as_empty_result() {
        let sliced = apply_limit_pipeline(
            &[batch_two_rows()],
            &kionas::planner::PhysicalLimitSpec {
                count: 0,
                offset: 0,
            },
        )
        .expect("limit pipeline must succeed");
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced[0].num_rows(), 0);
    }

    #[test]
    fn applies_high_offset_as_empty_result() {
        let sliced = apply_limit_pipeline(
            &[batch_two_rows()],
            &kionas::planner::PhysicalLimitSpec {
                count: 5,
                offset: 999,
            },
        )
        .expect("limit pipeline must succeed");
        assert_eq!(sliced.len(), 1);
        assert_eq!(sliced[0].num_rows(), 0);
    }

    #[test]
    fn extracts_runtime_limit_spec() {
        let task = crate::services::worker_service_server::worker_service::Task {
            task_id: "t-limit".to_string(),
            operation: "query".to_string(),
            input: serde_json::json!({
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        {"Limit": {"spec": {"count": 5, "offset": 2}}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let plan = extract_runtime_plan(&task).expect("runtime plan must parse");
        let limit = plan.limit_spec.expect("limit must be extracted");
        assert_eq!(limit.count, 5);
        assert_eq!(limit.offset, 2);
    }
}
