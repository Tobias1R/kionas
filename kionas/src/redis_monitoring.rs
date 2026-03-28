use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};

use crate::constants::{
    QUERY_HISTORY_MAX_ENTRIES_DEFAULT, QUERY_HISTORY_MAX_ENTRIES_ENV, QUERY_HISTORY_TTL_SECONDS,
    REDIS_UI_DASHBOARD_QUERY_HISTORY_KEY,
};

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

/// What: Represents a single worker system and execution-health snapshot.
///
/// Inputs:
/// - Constructed by worker monitoring code from local metrics and runtime execution counters.
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
    pub thread_count: u32,
    pub disk_used_mb: u64,
    pub disk_total_mb: u64,
    pub health_status: WorkerHealthStatus,
    pub active_stages: u32,
    pub total_stages_executed: u64,
    pub active_partitions: u32,
    pub total_partitions_executed: u64,
    pub bytes_scanned_total: u64,
    pub total_stage_exec_ms: u64,
    pub total_rows_produced: u64,
    pub started_at: DateTime<Utc>,
    pub uptime_seconds: u64,
    pub updated_at: DateTime<Utc>,
}

/// What: Query lifecycle status used for dashboard query history rows.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Serialized enum value for query state.
///
/// Details:
/// - Running indicates accepted and executing.
/// - Succeeded indicates completed without execution error.
/// - Failed indicates execution finished with an error.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum QueryStatus {
    Running,
    Succeeded,
    Failed,
}

/// What: Compact query summary item stored in Redis dashboard history list.
///
/// Inputs:
/// - Populated by server statement execution instrumentation.
///
/// Output:
/// - Serialized to JSON as one list element in query history.
///
/// Details:
/// - `sql_digest` must already be redacted before writing.
/// - `duration_ms` is optional because running events may not have a final duration.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
pub struct QuerySummary {
    pub query_id: String,
    pub session_id: String,
    pub user_id: String,
    pub warehouse_id: Option<String>,
    pub sql_digest: String,
    pub status: QueryStatus,
    pub duration_ms: Option<u64>,
    pub error: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// What: Append one query summary to dashboard history with bounded retention.
///
/// Inputs:
/// - `connection`: Redis async connection manager.
/// - `summary`: Query summary event payload.
///
/// Output:
/// - Ok when list push/trim/expire operations complete.
///
/// Details:
/// - Uses LPUSH for newest-first ordering.
/// - Uses LTRIM to enforce configurable max entry count.
/// - Uses EXPIRE so stale dashboards do not keep data forever.
pub async fn push_query_summary(
    connection: &mut redis::aio::ConnectionManager,
    summary: &QuerySummary,
) -> redis::RedisResult<()> {
    let payload = match serde_json::to_string(summary) {
        Ok(payload) => payload,
        Err(error) => {
            return Err(redis::RedisError::from((
                redis::ErrorKind::Client,
                "serialize query summary",
                error.to_string(),
            )));
        }
    };

    let max_entries = std::env::var(QUERY_HISTORY_MAX_ENTRIES_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(QUERY_HISTORY_MAX_ENTRIES_DEFAULT);
    let trim_stop = isize::try_from(max_entries.saturating_sub(1)).unwrap_or(isize::MAX);

    let _: usize = connection
        .lpush(REDIS_UI_DASHBOARD_QUERY_HISTORY_KEY, payload)
        .await?;
    let _: () = connection
        .ltrim(REDIS_UI_DASHBOARD_QUERY_HISTORY_KEY, 0, trim_stop)
        .await?;
    let _: bool = connection
        .expire(
            REDIS_UI_DASHBOARD_QUERY_HISTORY_KEY,
            QUERY_HISTORY_TTL_SECONDS as i64,
        )
        .await?;
    Ok(())
}
