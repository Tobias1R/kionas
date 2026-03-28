use super::{SystemMetrics, collect_system_metrics};
use crate::monitoring::worker_info::determine_worker_health;
use crate::redis_monitoring::WorkerHealthStatus;

#[test]
fn collect_system_metrics_returns_valid_ranges() {
    let metrics = collect_system_metrics();
    assert!(metrics.memory_total_mb > 0);
    assert!((0.0..=100.0).contains(&metrics.cpu_percent));
    if cfg!(target_os = "linux") {
        assert!(metrics.thread_count >= 1);
    } else {
        assert_eq!(metrics.thread_count, 0);
    }
}

#[test]
fn determine_worker_health_returns_unhealthy_for_zero_total_memory() {
    let metrics = SystemMetrics {
        memory_used_mb: 10,
        memory_total_mb: 0,
        cpu_percent: 10.0,
        thread_count: 1,
        disk_used_mb: 1,
        disk_total_mb: 100,
    };

    let status = determine_worker_health(&metrics);
    assert_eq!(status, WorkerHealthStatus::Unhealthy);
}

#[test]
fn determine_worker_health_returns_degraded_on_high_cpu() {
    let metrics = SystemMetrics {
        memory_used_mb: 200,
        memory_total_mb: 1_000,
        cpu_percent: 96.0,
        thread_count: 1,
        disk_used_mb: 1,
        disk_total_mb: 100,
    };

    let status = determine_worker_health(&metrics);
    assert_eq!(status, WorkerHealthStatus::Degraded);
}
