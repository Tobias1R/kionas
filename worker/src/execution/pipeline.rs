use crate::execution::planner::StageExecutionContext;
use crate::execution::planner::{RuntimeScanHints, RuntimeScanMode, RuntimeUnionSpec};
use crate::execution::query::QueryNamespace;
use crate::services::query_execution::apply_filter_predicate_pipeline;
use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;
#[cfg(test)]
use crate::storage::StorageProvider;
use crate::telemetry::StageExecutionTelemetry;
use arrow::record_batch::RecordBatch;
use kionas::planner::render_predicate_expr;
use std::collections::HashMap;

#[cfg(test)]
pub(crate) use crate::execution::pipeline_exchange_validation::StorageErrorClass;

#[cfg(test)]
pub(crate) fn classify_storage_error(error_message: &str) -> StorageErrorClass {
    crate::execution::pipeline_exchange_validation::classify_storage_error(error_message)
}

#[cfg(test)]
pub(crate) async fn list_stage_exchange_data_keys_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    query_run_id: &str,
    session_id: &str,
    upstream_stage_id: u32,
) -> Result<Vec<String>, String> {
    crate::execution::pipeline_exchange_validation::list_stage_exchange_data_keys_with_retry(
        provider,
        query_run_id,
        session_id,
        upstream_stage_id,
    )
    .await
}

#[cfg(test)]
pub(crate) async fn get_exchange_artifact_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    key: &str,
) -> Result<Option<Vec<u8>>, String> {
    crate::execution::pipeline_exchange_validation::get_exchange_artifact_with_retry(provider, key)
        .await
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn expected_exchange_artifact_from_metadata(
    metadata_bytes: &[u8],
    data_key: &str,
) -> Result<(u64, String), String> {
    crate::execution::pipeline_exchange_validation::expected_exchange_artifact_from_metadata(
        metadata_bytes,
        data_key,
    )
}

#[cfg(test)]
#[allow(dead_code)]
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

#[cfg(test)]
pub(crate) async fn load_validated_exchange_artifact_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    query_run_id: &str,
    session_id: &str,
    upstream_stage_id: u32,
    data_key: &str,
) -> Result<Vec<u8>, String> {
    let _ = checksum_fnv64_hex;
    crate::execution::pipeline_exchange_validation::load_validated_exchange_artifact_with_retry(
        provider,
        query_run_id,
        session_id,
        upstream_stage_id,
        data_key,
    )
    .await
}

pub(crate) struct ScanDecisionContext<'a> {
    pub(crate) query_id: &'a str,
    pub(crate) stage_id: u32,
    pub(crate) task_id: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScanPruningOutcome {
    pub(crate) keys: Vec<String>,
    pub(crate) effective_mode: RuntimeScanMode,
    pub(crate) reason: &'static str,
    pub(crate) cache_hit: Option<bool>,
}

pub(crate) struct ScanPruningDecisionFields<'a> {
    pub(crate) requested_mode: &'a str,
    pub(crate) effective_mode: &'a str,
    pub(crate) reason: &'a str,
    pub(crate) delta_version_pin: Option<u64>,
    pub(crate) cache_hit: Option<bool>,
    pub(crate) total_files: usize,
    pub(crate) retained_files: usize,
}

pub(crate) fn format_scan_pruning_decision_event(
    ctx: &ScanDecisionContext<'_>,
    fields: &ScanPruningDecisionFields<'_>,
) -> String {
    crate::execution::pipeline_telemetry::format_scan_pruning_decision_event(ctx, fields)
}

pub(crate) fn format_scan_mode_chosen_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    scan_mode: &str,
) -> String {
    crate::execution::pipeline_telemetry::format_scan_mode_chosen_event(
        query_id, stage_id, task_id, scan_mode,
    )
}

pub(crate) fn format_scan_fallback_reason_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    reason: &str,
) -> String {
    crate::execution::pipeline_telemetry::format_scan_fallback_reason_event(
        query_id, stage_id, task_id, reason,
    )
}

pub(crate) fn format_exchange_io_decision_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    partition_index: u32,
    direction: &str,
) -> String {
    crate::execution::pipeline_telemetry::format_exchange_io_decision_event(
        query_id,
        stage_id,
        task_id,
        partition_index,
        direction,
    )
}

#[cfg(test)]
pub(crate) fn format_exchange_retry_decision_event(
    query_id: &str,
    stage_id: u32,
    operation: &str,
    attempt: usize,
    outcome: &str,
    failure_class: &str,
    target: &str,
) -> String {
    crate::execution::pipeline_telemetry::format_exchange_retry_decision_event(
        query_id,
        stage_id,
        operation,
        attempt,
        outcome,
        failure_class,
        target,
    )
}

pub(crate) fn format_materialization_decision_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    output_format: &str,
) -> String {
    crate::execution::pipeline_telemetry::format_materialization_decision_event(
        query_id,
        stage_id,
        task_id,
        output_format,
    )
}

pub(crate) struct StageRuntimeTelemetryFields<'a> {
    pub(crate) operator_family: &'a str,
    pub(crate) origin: &'a str,
    pub(crate) stage_runtime_ms: u128,
    pub(crate) operator_rows_in: usize,
    pub(crate) operator_rows_out: usize,
    pub(crate) batch_count: usize,
    pub(crate) artifact_bytes: u64,
}

pub(crate) fn format_stage_runtime_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    fields: &StageRuntimeTelemetryFields<'_>,
) -> String {
    crate::execution::pipeline_telemetry::format_stage_runtime_event(
        query_id, stage_id, task_id, fields,
    )
}

#[tracing::instrument(
    name = "stage.execute",
    skip(shared, task, namespace, result_location),
    fields(
        query_id = tracing::field::Empty,
        stage_id = tracing::field::Empty,
        partition_id = tracing::field::Empty
    )
)]
pub(crate) async fn execute_query_task(
    shared: &SharedData,
    task: &worker_service::StagePartitionExecution,
    session_id: &str,
    namespace: &QueryNamespace,
    result_location: &str,
) -> Result<StageExecutionTelemetry, String> {
    crate::execution::pipeline_core::execute_query_task(
        shared,
        task,
        session_id,
        namespace,
        result_location,
    )
    .await
}

fn relation_key(namespace: &QueryNamespace) -> String {
    crate::execution::pipeline_relation_columns::relation_key(namespace)
}

fn apply_relation_column_names(
    batches: &[RecordBatch],
    relation_columns: &[String],
) -> Result<Vec<RecordBatch>, String> {
    crate::execution::pipeline_relation_columns::apply_relation_column_names(
        batches,
        relation_columns,
    )
}

#[cfg(test)]
fn build_empty_batch_from_relation_columns(
    relation_columns: &[String],
) -> Result<RecordBatch, String> {
    crate::execution::pipeline_relation_columns::build_empty_batch_from_relation_columns(
        relation_columns,
    )
}

#[cfg(test)]
async fn maybe_prune_scan_keys(
    provider: &std::sync::Arc<dyn crate::storage::StorageProvider + Send + Sync>,
    source_prefix: &str,
    keys: &[String],
    scan_hints: &RuntimeScanHints,
) -> ScanPruningOutcome {
    crate::execution::pipeline_scan_pruning::maybe_prune_scan_keys(
        provider,
        source_prefix,
        keys,
        scan_hints,
    )
    .await
}

pub(crate) async fn apply_union_pipeline(
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
    crate::execution::pipeline_union::summarize_union_operand_values(batches)
}

fn deduplicate_union_batches(batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, String> {
    crate::execution::pipeline_union::deduplicate_union_batches(batches)
}

#[cfg(test)]
pub(crate) fn parse_partition_index_from_exchange_key(key: &str) -> Option<u32> {
    crate::execution::pipeline_exchange_partitioning::parse_partition_index_from_exchange_key(key)
}

#[cfg(test)]
pub(crate) fn validate_upstream_exchange_partition_set(
    upstream_stage_id: u32,
    keys: &[String],
    expected_partition_count: u32,
) -> Result<(), String> {
    crate::execution::pipeline_exchange_partitioning::validate_upstream_exchange_partition_set(
        upstream_stage_id,
        keys,
        expected_partition_count,
    )
}

#[cfg(test)]
pub(crate) fn select_exchange_keys_for_downstream_partition(
    upstream_stage_id: u32,
    keys: &[String],
    upstream_partition_count: u32,
    downstream_partition_count: u32,
    downstream_partition_index: u32,
) -> Result<Vec<String>, String> {
    crate::execution::pipeline_exchange_partitioning::select_exchange_keys_for_downstream_partition(
        upstream_stage_id,
        keys,
        upstream_partition_count,
        downstream_partition_count,
        downstream_partition_index,
    )
}

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

    if stage_context.upstream_stage_flight_endpoints.is_empty() {
        return Err(
            "missing __upstream_stage_flight_endpoints_json for distributed upstream Flight pulls"
                .to_string(),
        );
    }

    load_upstream_flight_batches(session_id, stage_context).await
}

async fn load_upstream_flight_batches(
    session_id: &str,
    stage_context: &StageExecutionContext,
) -> Result<Vec<RecordBatch>, String> {
    crate::execution::pipeline_upstream::load_upstream_flight_batches(session_id, stage_context)
        .await
}

#[cfg(test)]
async fn load_upstream_exchange_batches(
    shared: &SharedData,
    session_id: &str,
    stage_context: &StageExecutionContext,
) -> Result<Vec<RecordBatch>, String> {
    crate::execution::pipeline_exchange_io::load_upstream_exchange_batches(
        shared,
        session_id,
        stage_context,
    )
    .await
}

pub(crate) fn partition_input_batches(
    input: &[RecordBatch],
    partition_count: u32,
    partition_index: u32,
) -> Result<Vec<RecordBatch>, String> {
    crate::execution::pipeline_scan_io::partition_input_batches(
        input,
        partition_count,
        partition_index,
    )
}

pub(crate) fn source_table_staging_prefix(namespace: &QueryNamespace) -> String {
    crate::execution::pipeline_scan_io::source_table_staging_prefix(namespace)
}

pub(crate) async fn load_scan_batches(
    shared: &SharedData,
    source_prefix: &str,
    scan_hints: &RuntimeScanHints,
    decision_ctx: &ScanDecisionContext<'_>,
) -> Result<Vec<RecordBatch>, String> {
    crate::execution::pipeline_scan_io::load_scan_batches(
        shared,
        source_prefix,
        scan_hints,
        decision_ctx,
    )
    .await
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn decode_parquet_batches(parquet_bytes: Vec<u8>) -> Result<Vec<RecordBatch>, String> {
    crate::execution::pipeline_scan_io::decode_parquet_batches(parquet_bytes)
}

#[cfg(test)]
#[path = "../tests/execution_pipeline_tests.rs"]
mod tests;
