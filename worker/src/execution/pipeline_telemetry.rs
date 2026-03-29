use crate::execution::pipeline::{
    ScanDecisionContext, ScanPruningDecisionFields, StageRuntimeTelemetryFields,
};
use crate::execution::planner::RuntimeScanMode;

pub(crate) fn runtime_scan_mode_label(mode: &RuntimeScanMode) -> &'static str {
    match mode {
        RuntimeScanMode::Full => "full",
        RuntimeScanMode::MetadataPruned => "metadata_pruned",
    }
}

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
    format!(
        "event=execution.exchange_retry_decision query_id={} stage_id={} operator_family=exchange category=execution origin=worker_pipeline operation={} attempt={} outcome={} failure_class={} target={}",
        query_id, stage_id, operation, attempt, outcome, failure_class, target,
    )
}

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
