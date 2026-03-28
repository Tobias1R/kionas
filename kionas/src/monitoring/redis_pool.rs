use crate::monitoring::MonitoringError;
use redis::aio::ConnectionManager;

/// What: Initialize Redis connection manager for async monitoring operations.
///
/// Inputs:
/// - `redis_url`: Redis URL such as redis://localhost:6379/2.
///
/// Output:
/// - ConnectionManager ready for async commands.
///
/// Details:
/// - Uses Redis connection-manager to reuse underlying connections.
/// - Returns MonitoringError for consistent caller handling.
pub async fn init_redis_pool(redis_url: &str) -> Result<ConnectionManager, MonitoringError> {
    let client = redis::Client::open(redis_url)
        .map_err(|error| MonitoringError::Connection(error.to_string()))?;

    ConnectionManager::new(client)
        .await
        .map_err(MonitoringError::from)
}
