use chrono::{DateTime, Utc};
use std::sync::Arc;
use std::time::Duration;

use crate::counters::WorkerCounters;
use crate::metrics::WorkerPrometheusMetrics;
use kionas::get_local_hostname;
use kionas::monitoring::{
    WorkerSysInfoUpdateInput, collect_system_metrics, update_worker_sys_info,
};

/// What: Spawn a background task that periodically publishes worker metrics to Redis.
///
/// Inputs:
/// - `redis_pool`: Redis connection manager for monitoring writes.
/// - `worker_id`: Stable worker identifier.
/// - `cluster_id`: Cluster identifier this worker belongs to.
/// - `warehouse_pool`: Optional pool assignment for dashboard grouping.
/// - `worker_start_time`: Process start timestamp used for uptime.
///
/// Output:
/// - Spawns a detached tokio task and returns immediately.
///
/// Details:
/// - Update interval is controlled by KIONAS_WORKER_UPDATE_INTERVAL_SECS.
/// - Logs warning on transient failures without panicking.
pub fn spawn_monitoring_task(
    redis_pool: redis::aio::ConnectionManager,
    worker_id: String,
    cluster_id: String,
    warehouse_pool: Option<String>,
    worker_start_time: DateTime<Utc>,
    counters: Arc<WorkerCounters>,
    prometheus_metrics: Arc<WorkerPrometheusMetrics>,
) {
    let interval_secs = std::env::var("KIONAS_WORKER_UPDATE_INTERVAL_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(15);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));

        loop {
            interval.tick().await;

            let metrics = collect_system_metrics();
            let snapshot = counters.snapshot();
            prometheus_metrics.set_active_stages(snapshot.active_stages);
            prometheus_metrics.set_active_partitions(snapshot.active_partitions);
            prometheus_metrics.set_cpu_percent(metrics.cpu_percent);
            prometheus_metrics.set_memory_used_mb(metrics.memory_used_mb);
            prometheus_metrics.set_thread_count(metrics.thread_count);
            let hostname = get_local_hostname().unwrap_or_else(|| "unknown-worker".to_string());
            let input = WorkerSysInfoUpdateInput {
                worker_id: worker_id.clone(),
                cluster_id: cluster_id.clone(),
                hostname,
                warehouse_pool: warehouse_pool.clone(),
                metrics,
                active_stages: snapshot.active_stages,
                total_stages_executed: snapshot.total_stages_executed,
                active_partitions: snapshot.active_partitions,
                total_partitions_executed: snapshot.total_partitions_executed,
                bytes_scanned_total: snapshot.bytes_scanned_total,
                total_stage_exec_ms: snapshot.total_stage_exec_ms,
                total_rows_produced: snapshot.total_rows_produced,
                worker_start_time,
            };

            if let Err(error) = update_worker_sys_info(&redis_pool, &input).await {
                log::warn!(
                    "worker monitoring update failed for worker_id={}: {}",
                    worker_id,
                    error
                );
            }
        }
    });
}
