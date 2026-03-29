use crate::execution::pipeline::{
    decode_parquet_batches, list_stage_exchange_data_keys_with_retry,
    load_validated_exchange_artifact_with_retry,
};
use crate::execution::pipeline_exchange_partitioning::{
    select_exchange_keys_for_downstream_partition, validate_upstream_exchange_partition_set,
};
use crate::execution::planner::StageExecutionContext;
use crate::state::SharedData;
use arrow::record_batch::RecordBatch;

/// What: Load batches from upstream stage exchange artifacts.
///
/// Inputs:
/// - `shared`: Worker state containing configured storage provider.
/// - `session_id`: Session identifier used for exchange scoping.
/// - `stage_context`: Stage metadata resolved from task params.
///
/// Output:
/// - Decoded upstream stage batches in deterministic key order.
///
/// Details:
/// - Validates expected partition set when upstream partition counts are known.
/// - Applies deterministic many-to-one partition selection for distributed pulls.
pub(crate) async fn load_upstream_exchange_batches(
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
