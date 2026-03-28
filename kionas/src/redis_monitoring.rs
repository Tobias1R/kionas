use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// What: Cluster-level health status exposed to dashboard clients.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Serialized enum variant for cluster health in Redis payloads.
///
/// Details:
/// - Healthy means all expected services are online.
/// - Degraded means service is online with reduced capacity.
/// - Unhealthy means service should be considered unavailable.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum ClusterHealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// What: Worker-level health status exposed to dashboard clients.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Serialized enum variant for worker health in Redis payloads.
///
/// Details:
/// - Healthy means the worker is within resource thresholds.
/// - Degraded means the worker is under elevated resource pressure.
/// - Unhealthy means the worker is not considered available for traffic.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum WorkerHealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

/// What: Represents the health and metadata of the entire cluster stored in Redis.
///
/// Inputs:
/// - Constructed by monitoring code from runtime state and config metadata.
///
/// Output:
/// - Serialized to JSON and stored at kionas:cluster:info in Redis.
///
/// Details:
/// - Updated periodically by the server process.
/// - Includes enough metadata for the dashboard header and status widgets.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct ClusterStatus {
    pub cluster_name: String,
    pub cluster_id: String,
    pub cluster_version: String,
    pub status: ClusterHealthStatus,
    pub uptime_seconds: u64,
    pub started_at: DateTime<Utc>,
    pub node_count: usize,
    pub updated_at: DateTime<Utc>,
}

/// What: Represents a single worker system and query-health snapshot.
///
/// Inputs:
/// - Constructed by worker monitoring code from local metrics and runtime counters.
///
/// Output:
/// - Serialized to JSON and stored at kionas:worker:{worker_id}:sys_info in Redis.
///
/// Details:
/// - Updated periodically by each worker process.
/// - Designed to feed workers cards/table in the dashboard.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct WorkerSystemInfo {
    pub worker_id: String,
    pub hostname: String,
    pub cluster_id: String,
    pub warehouse_pool: Option<String>,
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub cpu_percent: f32,
    pub disk_used_mb: u64,
    pub disk_total_mb: u64,
    pub health_status: WorkerHealthStatus,
    pub active_queries: u32,
    pub total_queries_processed: u64,
    pub started_at: DateTime<Utc>,
    pub uptime_seconds: u64,
    pub updated_at: DateTime<Utc>,
}
