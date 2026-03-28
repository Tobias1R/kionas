use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tracing::{debug, warn};

use crate::monitoring::{MonitoringError, SystemMetrics};
use crate::redis_monitoring::{WorkerHealthStatus, WorkerSystemInfo};

const WORKER_INFO_TTL_SECONDS: u64 = 45;

/// What: Input payload used to publish a worker monitoring snapshot.
///
/// Inputs:
/// - Worker identity, cluster metadata, runtime counters, and metrics snapshot.
///
/// Output:
/// - Consumed by update_worker_sys_info to produce Redis payloads.
///
/// Details:
/// - Bundles all worker snapshot fields into a single object to keep call sites concise.
#[derive(Debug, Clone)]
pub struct WorkerSysInfoUpdateInput {
    pub worker_id: String,
    pub cluster_id: String,
    pub hostname: String,
    pub warehouse_pool: Option<String>,
    pub metrics: SystemMetrics,
    pub active_stages: u32,
    pub total_stages_executed: u64,
    pub active_partitions: u32,
    pub total_partitions_executed: u64,
    pub bytes_scanned_total: u64,
    pub total_stage_exec_ms: u64,
    pub total_rows_produced: u64,
    pub worker_start_time: DateTime<Utc>,
}

/// What: Persist a worker monitoring snapshot to Redis.
///
/// Inputs:
/// - `pool`: Redis connection manager.
/// - `worker_id`: Stable worker identifier.
/// - `cluster_id`: Cluster identifier.
/// - `hostname`: Worker host name.
/// - `warehouse_pool`: Optional worker pool assignment.
/// - `metrics`: Current system metrics snapshot.
/// - `active_stages`: Number of currently executing stages.
/// - `total_stages_executed`: Cumulative completed stage count.
/// - `active_partitions`: Number of currently executing partition tasks.
/// - `total_partitions_executed`: Cumulative completed partition task count.
/// - `bytes_scanned_total`: Cumulative bytes produced by stage artifacts.
/// - `total_stage_exec_ms`: Cumulative stage runtime in milliseconds.
/// - `total_rows_produced`: Cumulative operator output row count.
/// - `worker_start_time`: Worker start timestamp.
///
/// Output:
/// - Ok when worker snapshot is written to Redis.
///
/// Details:
/// - Writes to key pattern kionas:worker:{worker_id}:sys_info.
/// - Applies a 45-second TTL to avoid stale status data.
pub async fn update_worker_sys_info(
    pool: &ConnectionManager,
    input: &WorkerSysInfoUpdateInput,
) -> Result<(), MonitoringError> {
    let now = Utc::now();
    let uptime_delta = now
        .signed_duration_since(input.worker_start_time)
        .num_seconds();
    let uptime_seconds = u64::try_from(uptime_delta.max(0)).unwrap_or(0);

    let worker_status = WorkerSystemInfo {
        worker_id: input.worker_id.clone(),
        hostname: input.hostname.clone(),
        cluster_id: input.cluster_id.clone(),
        warehouse_pool: input.warehouse_pool.clone(),
        memory_used_mb: input.metrics.memory_used_mb,
        memory_total_mb: input.metrics.memory_total_mb,
        cpu_percent: input.metrics.cpu_percent,
        thread_count: input.metrics.thread_count,
        disk_used_mb: input.metrics.disk_used_mb,
        disk_total_mb: input.metrics.disk_total_mb,
        health_status: determine_worker_health(&input.metrics),
        active_stages: input.active_stages,
        total_stages_executed: input.total_stages_executed,
        active_partitions: input.active_partitions,
        total_partitions_executed: input.total_partitions_executed,
        bytes_scanned_total: input.bytes_scanned_total,
        total_stage_exec_ms: input.total_stage_exec_ms,
        total_rows_produced: input.total_rows_produced,
        started_at: input.worker_start_time,
        uptime_seconds,
        updated_at: now,
    };

    let key = format!("kionas:worker:{}:sys_info", input.worker_id);
    let payload = serde_json::to_string(&worker_status)?;
    let mut connection = pool.clone();

    let set_result = connection
        .set_ex(key, payload, WORKER_INFO_TTL_SECONDS)
        .await;
    match set_result {
        Ok::<(), redis::RedisError>(()) => {
            debug!("worker info updated: worker_id={}", input.worker_id);
            Ok(())
        }
        Err(error) => {
            warn!(
                "worker info update failed for worker_id={}: {}",
                input.worker_id, error
            );
            Err(MonitoringError::from(error))
        }
    }
}

/// What: Derive worker health status from utilization thresholds.
///
/// Inputs:
/// - `metrics`: System metrics snapshot.
///
/// Output:
/// - WorkerHealthStatus classification.
///
/// Details:
/// - Marks worker degraded on high memory or CPU utilization.
/// - Marks worker unhealthy when memory capacity is invalid.
pub(crate) fn determine_worker_health(metrics: &SystemMetrics) -> WorkerHealthStatus {
    if metrics.memory_total_mb == 0 {
        return WorkerHealthStatus::Unhealthy;
    }

    let memory_usage = metrics.memory_used_mb as f32 / metrics.memory_total_mb as f32;
    let memory_degraded_threshold = 0.85_f32;
    let cpu_degraded_threshold = 95.0_f32;

    if memory_usage > memory_degraded_threshold || metrics.cpu_percent > cpu_degraded_threshold {
        WorkerHealthStatus::Degraded
    } else {
        WorkerHealthStatus::Healthy
    }
}
