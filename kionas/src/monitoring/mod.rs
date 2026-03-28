pub mod cluster_info;
pub mod redis_pool;
pub mod system_metrics;
pub mod worker_info;

pub use cluster_info::*;
pub use redis_pool::*;
pub use system_metrics::*;
pub use worker_info::*;

use std::fmt::{Display, Formatter};

/// What: Error type used by Redis monitoring operations.
///
/// Inputs:
/// - Built from lower-level redis and serde_json errors.
///
/// Output:
/// - Unified error surface for monitoring callers.
///
/// Details:
/// - Keeps monitoring integration resilient and easy to log.
#[derive(Debug)]
pub enum MonitoringError {
    Redis(redis::RedisError),
    Serialization(serde_json::Error),
    Connection(String),
}

impl Display for MonitoringError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Redis(error) => write!(f, "redis error: {error}"),
            Self::Serialization(error) => write!(f, "serialization error: {error}"),
            Self::Connection(message) => write!(f, "connection error: {message}"),
        }
    }
}

impl std::error::Error for MonitoringError {}

impl From<redis::RedisError> for MonitoringError {
    fn from(value: redis::RedisError) -> Self {
        Self::Redis(value)
    }
}

impl From<serde_json::Error> for MonitoringError {
    fn from(value: serde_json::Error) -> Self {
        Self::Serialization(value)
    }
}

#[cfg(test)]
mod tests;
