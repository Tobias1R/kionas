use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use redis::aio::ConnectionManager;
use tracing::{debug, warn};

use crate::config::ClusterConfig;
use crate::monitoring::MonitoringError;
use crate::redis_monitoring::{ClusterHealthStatus, ClusterStatus};

const CLUSTER_INFO_KEY: &str = "kionas:cluster:info";
const CLUSTER_INFO_TTL_SECONDS: u64 = 45;

/// What: Compute and persist cluster monitoring snapshot in Redis.
///
/// Inputs:
/// - `pool`: Redis connection manager.
/// - `config`: AppConfig with cluster metadata.
/// - `node_count`: Number of active workers/nodes.
/// - `cluster_start_time`: Cluster process start timestamp.
///
/// Output:
/// - Ok when snapshot is successfully written.
///
/// Details:
/// - Stores JSON at kionas:cluster:info with 45-second TTL.
/// - Uses a temporary Healthy status until richer health logic is added.
pub async fn update_cluster_info(
    pool: &ConnectionManager,
    cluster_config: &ClusterConfig,
    node_count: usize,
    cluster_start_time: DateTime<Utc>,
) -> Result<(), MonitoringError> {
    let now = Utc::now();
    let uptime_delta = now.signed_duration_since(cluster_start_time).num_seconds();
    let uptime_seconds = u64::try_from(uptime_delta.max(0)).unwrap_or(0);

    let status = ClusterStatus {
        cluster_name: cluster_config.cluster_name.clone(),
        cluster_id: cluster_config.cluster_id.clone(),
        cluster_version: cluster_config.cluster_version.clone(),
        status: ClusterHealthStatus::Healthy,
        uptime_seconds,
        started_at: cluster_start_time,
        node_count,
        updated_at: now,
    };

    let payload = serde_json::to_string(&status)?;
    let mut connection = pool.clone();

    let set_result = connection
        .set_ex(CLUSTER_INFO_KEY, payload, CLUSTER_INFO_TTL_SECONDS)
        .await;

    match set_result {
        Ok::<(), redis::RedisError>(()) => {
            debug!(
                "cluster info updated: cluster_id={}, node_count={}",
                status.cluster_id, status.node_count
            );
            Ok(())
        }
        Err(error) => {
            warn!("cluster info update failed: {}", error);
            Err(MonitoringError::from(error))
        }
    }
}
