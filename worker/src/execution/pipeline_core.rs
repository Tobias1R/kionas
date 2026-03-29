use crate::execution::aggregate::{
    apply_aggregate_final_pipeline, apply_aggregate_partial_pipeline,
};
use crate::execution::artifacts::{persist_query_artifacts, persist_stage_exchange_artifacts};
use crate::execution::join::{apply_hash_join_pipeline, apply_nested_loop_join_pipeline};
use crate::execution::pipeline::{
    ScanDecisionContext, StageRuntimeTelemetryFields, apply_union_pipeline,
    format_exchange_io_decision_event, format_materialization_decision_event,
    format_scan_fallback_reason_event, format_scan_mode_chosen_event, format_stage_runtime_event,
    load_input_batches,
};
use crate::execution::pipeline_relation_columns::{
    apply_relation_column_names, build_empty_batch_from_relation_columns,
    parse_relation_columns_map, relation_key,
};
use crate::execution::pipeline_scan_io::{load_scan_batches, source_table_staging_prefix};
use crate::execution::planner::RuntimeScanHints;
use crate::execution::planner::StageExecutionContext;
use crate::execution::planner::{RuntimeJoinPlan, extract_runtime_plan, stage_execution_context};
use crate::execution::query::QueryNamespace;
use crate::execution::router::{route_batch_from_output_partitioning, route_batch_roundrobin};
use crate::execution::window::apply_window_pipeline;
use crate::services::query_execution::{
    apply_filter_predicate_pipeline, apply_limit_pipeline, apply_projection_pipeline,
    apply_sort_pipeline, normalize_batches_for_sort,
};
use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;
use crate::telemetry::{
    StageExecutionTelemetry, StageLatencyMetrics, StageMemoryMetrics, StageNetworkMetrics,
};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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
    let stage_context: StageExecutionContext = stage_execution_context(task)?;
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
    let current_span = tracing::Span::current();
    current_span.record("query_id", query_id.as_str());
    current_span.record("stage_id", stage_context.stage_id);
    current_span.record("partition_id", stage_context.partition_index);

    log::info!(
        "{}",
        format_scan_mode_chosen_event(
            &query_id,
            stage_context.stage_id,
            &task.task_id,
            crate::execution::pipeline_telemetry::runtime_scan_mode_label(
                &stage_context.scan_hints.mode
            ),
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
    let artifact_bytes_total = exchange_artifact_bytes + materialization_artifact_bytes;
    shared
        .counters
        .bytes_scanned_total
        .fetch_add(artifact_bytes_total, std::sync::atomic::Ordering::Relaxed);
    shared
        .prometheus_metrics
        .inc_bytes_scanned_total(artifact_bytes_total);
    let stage_runtime_ms_u64 = u64::try_from(stage_runtime_ms).unwrap_or(u64::MAX);
    shared
        .counters
        .total_stage_exec_ms
        .fetch_add(stage_runtime_ms_u64, std::sync::atomic::Ordering::Relaxed);
    shared
        .prometheus_metrics
        .inc_stage_exec_ms_total(stage_runtime_ms_u64);
    shared
        .prometheus_metrics
        .observe_stage_exec_ms(stage_runtime_ms_u64);
    let operator_rows_out_u64 = u64::try_from(operator_rows_out).unwrap_or(u64::MAX);
    shared
        .counters
        .total_rows_produced
        .fetch_add(operator_rows_out_u64, std::sync::atomic::Ordering::Relaxed);
    shared
        .prometheus_metrics
        .inc_rows_produced_total(operator_rows_out_u64);
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
                artifact_bytes: artifact_bytes_total,
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
