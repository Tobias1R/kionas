use crate::execution::pipeline::{ScanDecisionContext, ScanPruningDecisionFields};
use crate::execution::planner::RuntimeScanHints;
use crate::execution::query::QueryNamespace;
use crate::state::SharedData;
use arrow::array::{BooleanArray, new_empty_array};
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// What: Slice input batches into deterministic partitions for fan-out execution.
///
/// Inputs:
/// - `input`: Input record batches for this stage.
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
    let pruning_outcome = crate::execution::pipeline_scan_pruning::maybe_prune_scan_keys(
        provider,
        source_prefix,
        &keys,
        scan_hints,
    )
    .await;
    let retained_files = pruning_outcome.keys.len();
    log::info!(
        "{}",
        crate::execution::pipeline::format_scan_pruning_decision_event(
            decision_ctx,
            &ScanPruningDecisionFields {
                requested_mode: crate::execution::pipeline_telemetry::runtime_scan_mode_label(
                    &scan_hints.mode,
                ),
                effective_mode: crate::execution::pipeline_telemetry::runtime_scan_mode_label(
                    &pruning_outcome.effective_mode,
                ),
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
