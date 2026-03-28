use chrono::Utc;

use crate::redis_monitoring::{
    ClusterHealthStatus, ClusterStatus, WorkerHealthStatus, WorkerSystemInfo,
};

#[test]
fn cluster_status_round_trip_serialization() {
    let now = Utc::now();
    let status = ClusterStatus {
        cluster_name: "local-dev-cluster".to_string(),
        cluster_id: "cluster-001".to_string(),
        cluster_version: "1.0.0".to_string(),
        status: ClusterHealthStatus::Healthy,
        uptime_seconds: 42,
        started_at: now,
        node_count: 4,
        updated_at: now,
    };

    let json = serde_json::to_string(&status).expect("cluster status should serialize");
    let decoded: ClusterStatus =
        serde_json::from_str(&json).expect("cluster status should deserialize");

    assert_eq!(decoded.cluster_name, "local-dev-cluster");
    assert_eq!(decoded.cluster_id, "cluster-001");
    assert_eq!(decoded.status, ClusterHealthStatus::Healthy);
}

#[test]
fn worker_system_info_round_trip_serialization() {
    let now = Utc::now();
    let info = WorkerSystemInfo {
        worker_id: "worker-1".to_string(),
        hostname: "worker-1.local".to_string(),
        cluster_id: "cluster-001".to_string(),
        warehouse_pool: Some("compute_xl".to_string()),
        memory_used_mb: 256,
        memory_total_mb: 1024,
        cpu_percent: 25.5,
        disk_used_mb: 1_000,
        disk_total_mb: 4_000,
        health_status: WorkerHealthStatus::Healthy,
        active_queries: 2,
        total_queries_processed: 10,
        started_at: now,
        uptime_seconds: 100,
        updated_at: now,
    };

    let json = serde_json::to_string(&info).expect("worker info should serialize");
    let decoded: WorkerSystemInfo =
        serde_json::from_str(&json).expect("worker info should deserialize");

    assert_eq!(decoded.worker_id, "worker-1");
    assert_eq!(decoded.cluster_id, "cluster-001");
    assert_eq!(decoded.health_status, WorkerHealthStatus::Healthy);
}
