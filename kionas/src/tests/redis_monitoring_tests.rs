use chrono::Utc;

use crate::redis_monitoring::{
    ClusterHealthStatus, ClusterStatus, QueryStatus, QuerySummary, WorkerHealthStatus,
    WorkerSystemInfo,
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
        thread_count: 12,
        disk_used_mb: 1_000,
        disk_total_mb: 4_000,
        health_status: WorkerHealthStatus::Healthy,
        active_stages: 2,
        total_stages_executed: 10,
        active_partitions: 5,
        total_partitions_executed: 18,
        bytes_scanned_total: 20_480,
        total_stage_exec_ms: 9_000,
        total_rows_produced: 1_500,
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

#[test]
fn query_summary_round_trip_serialization() {
    let now = Utc::now();
    let summary = QuerySummary {
        query_id: "query-001".to_string(),
        session_id: "session-001".to_string(),
        user_id: "analyst@kionas".to_string(),
        warehouse_id: Some("wh-001".to_string()),
        sql_digest: "SELECT * FROM t WHERE id = ?".to_string(),
        status: QueryStatus::Succeeded,
        duration_ms: Some(123),
        error: None,
        timestamp: now,
    };

    let json = serde_json::to_string(&summary).expect("query summary should serialize");
    let decoded: QuerySummary =
        serde_json::from_str(&json).expect("query summary should deserialize");

    assert_eq!(decoded.query_id, "query-001");
    assert_eq!(decoded.status, QueryStatus::Succeeded);
    assert_eq!(decoded.duration_ms, Some(123));
}
