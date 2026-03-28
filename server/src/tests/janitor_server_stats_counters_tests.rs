use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::{ServerStatsAccumulator, collect_server_stats, derive_cluster_health};
use crate::warehouse::state::{SharedData, SharedState};
use chrono::Utc;
use kionas::redis_monitoring::{ClusterHealthStatus, WorkerHealthStatus, WorkerSystemInfo};
use tokio::sync::Mutex as AsyncMutex;

fn worker_info_with_health(health_status: WorkerHealthStatus) -> WorkerSystemInfo {
    let now = Utc::now();
    WorkerSystemInfo {
        worker_id: "worker-a".to_string(),
        hostname: "worker-a.local".to_string(),
        cluster_id: "cluster-1".to_string(),
        warehouse_pool: Some("compute_xl".to_string()),
        memory_used_mb: 256,
        memory_total_mb: 1024,
        cpu_percent: 20.0,
        thread_count: 8,
        disk_used_mb: 1_000,
        disk_total_mb: 4_000,
        health_status,
        active_stages: 0,
        total_stages_executed: 0,
        active_partitions: 0,
        total_partitions_executed: 0,
        bytes_scanned_total: 0,
        total_stage_exec_ms: 0,
        total_rows_produced: 0,
        started_at: now,
        uptime_seconds: 100,
        updated_at: now,
    }
}

#[tokio::test]
async fn collect_server_stats_computes_queries_per_minute_from_delta() {
    let shared_data: SharedData = Arc::new(AsyncMutex::new(SharedState::default()));
    let counters = {
        let shared = shared_data.lock().await;
        Arc::clone(&shared.query_counters)
    };

    let mut stats_accumulator = ServerStatsAccumulator::default();

    let first = collect_server_stats(&shared_data, &mut stats_accumulator).await;
    assert_eq!(first.queries_per_minute, 0.0);
    assert_eq!(first.total_queries_submitted, 0);

    counters
        .total_queries_submitted
        .fetch_add(3, Ordering::Relaxed);
    counters.active_queries.fetch_add(1, Ordering::Relaxed);

    let second = collect_server_stats(&shared_data, &mut stats_accumulator).await;
    assert_eq!(second.total_queries_submitted, 3);
    assert_eq!(second.active_queries, 1);
    assert_eq!(second.queries_per_minute, 12.0);
}

#[test]
fn derive_cluster_health_all_healthy_returns_healthy() {
    let workers = vec![
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
    ];
    let status = derive_cluster_health(2, &workers);
    assert_eq!(status, ClusterHealthStatus::Healthy);
}

#[test]
fn derive_cluster_health_one_degraded_returns_degraded() {
    let workers = vec![
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Degraded),
    ];
    let status = derive_cluster_health(5, &workers);
    assert_eq!(status, ClusterHealthStatus::Degraded);
}

#[test]
fn derive_cluster_health_one_unhealthy_returns_unhealthy() {
    let workers = vec![
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Unhealthy),
    ];
    let status = derive_cluster_health(5, &workers);
    assert_eq!(status, ClusterHealthStatus::Unhealthy);
}

#[test]
fn derive_cluster_health_zero_live_returns_unhealthy() {
    let workers: Vec<WorkerSystemInfo> = Vec::new();
    let status = derive_cluster_health(5, &workers);
    assert_eq!(status, ClusterHealthStatus::Unhealthy);
}

#[test]
fn derive_cluster_health_stale_ratio_twenty_percent_boundary_is_degraded() {
    let workers = vec![
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
    ];
    let status = derive_cluster_health(5, &workers);
    assert_eq!(status, ClusterHealthStatus::Degraded);
}

#[test]
fn derive_cluster_health_stale_ratio_above_fifty_percent_is_unhealthy() {
    let workers = vec![
        worker_info_with_health(WorkerHealthStatus::Healthy),
        worker_info_with_health(WorkerHealthStatus::Healthy),
    ];
    let status = derive_cluster_health(5, &workers);
    assert_eq!(status, ClusterHealthStatus::Unhealthy);
}
