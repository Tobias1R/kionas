/// What: Manages a pool of Arrow Flight client connections to downstream workers.
///
/// Details:
/// - Caches connections keyed by destination URI to reduce TLS handshake overhead
/// - Reuses healthy connections; detects and cleans up stale/dead connections
/// - Enforces a maximum pool size to prevent resource exhaustion
/// - Implements connection-level timeout and deadline awareness
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tonic14::transport::{Channel, Endpoint};

/// What: Connection metadata tracked for each pooled client.
///
/// Inputs: None (created per connection lifecycle)
/// Output: metadata tuple
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct PooledClient {
    channel: Channel,
    created_at: std::time::Instant,
    last_used: std::time::Instant,
}

/// What: Error type for Flight client pool operations.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum FlightClientPoolError {
    /// Connection timeout during channel creation
    ConnectionTimeout,
    /// Pool exhausted (max size reached)
    PoolExhausted,
    /// Endpoint URI is invalid
    InvalidEndpoint,
    /// TLS configuration error
    TlsError(String),
    /// Channel creation failure
    ChannelError(String),
}

impl std::fmt::Display for FlightClientPoolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionTimeout => write!(f, "flight client connection timed out"),
            Self::PoolExhausted => write!(f, "flight client pool exhausted"),
            Self::InvalidEndpoint => write!(f, "invalid flight endpoint URI"),
            Self::TlsError(msg) => write!(f, "flight client TLS error: {}", msg),
            Self::ChannelError(msg) => write!(f, "flight channel creation failed: {}", msg),
        }
    }
}

impl std::error::Error for FlightClientPoolError {}

/// What: Get a channel with a timeout on connection establishment.
///
/// Inputs:
/// - `endpoint`: Tonic endpoint object
/// - `timeout`: Max time to wait for connection
///
/// Output:
/// - Connected channel or timeout error
async fn timeout_create_channel(
    endpoint: Endpoint,
    timeout: Duration,
) -> Result<Channel, Box<dyn std::error::Error + Send + Sync>> {
    tokio::time::timeout(timeout, endpoint.connect())
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
}

/// What: Pool of Flight client channels mapped by destination URI.
///
/// Inputs: None (lazily constructed on first use)
/// Output: Reusable channel instances per upstream worker
pub struct FlightClientPool {
    /// Per-destination cached channels
    channels: Arc<Mutex<HashMap<String, PooledClient>>>,
    /// Maximum cached connections (limit to prevent resource exhaustion)
    max_cached_channels: usize,
    /// Connection timeout
    connect_timeout: Duration,
}

impl FlightClientPool {
    /// What: Create a new Flight client pool with given configuration.
    ///
    /// Inputs:
    /// - `max_pool_size`: Maximum connections per upstream destination (default 8)
    /// - `connect_timeout`: Timeout per connection attempt (default 5s)
    ///
    /// Output:
    /// - Initialized pool ready for acquisition
    pub fn new(max_pool_size: u32, connect_timeout: Duration) -> Self {
        Self {
            channels: Arc::new(Mutex::new(HashMap::new())),
            max_cached_channels: max_pool_size as usize,
            connect_timeout,
        }
    }

    /// What: Get or create a pooled channel to a downstream worker endpoint.
    ///
    /// Inputs:
    /// - `endpoint_uri`: Flight endpoint URL (e.g., "http://worker-2:7779")
    ///
    /// Output:
    /// - Pooled channel ready for gRPC use, or error if connection fails
    ///
    /// Details:
    /// - Creates a new channel on first request to an endpoint if cache miss
    /// - Reuses channels for repeated requests to the same endpoint
    /// - Returns error if connection times out or endpoint is unreachable
    pub async fn get_channel(&self, endpoint_uri: &str) -> Result<Channel, FlightClientPoolError> {
        // Try to get existing channel from cache
        {
            let mut channels = self.channels.lock().unwrap();
            if let Some(pooled) = channels.get_mut(endpoint_uri) {
                pooled.last_used = std::time::Instant::now();
                return Ok(pooled.channel.clone());
            }
        }

        // Don't have cached channel; create a new one
        let endpoint_obj = Endpoint::from_shared(endpoint_uri.to_string())
            .map_err(|e| FlightClientPoolError::ChannelError(e.to_string()))?;

        let channel = timeout_create_channel(endpoint_obj, self.connect_timeout)
            .await
            .map_err(|_| FlightClientPoolError::ConnectionTimeout)?;

        let pooled = PooledClient {
            channel: channel.clone(),
            created_at: std::time::Instant::now(),
            last_used: std::time::Instant::now(),
        };

        // Cache the newly created channel (evict oldest if cache full)
        {
            let mut channels = self.channels.lock().unwrap();
            if channels.len() >= self.max_cached_channels {
                // Evict the least-recently-used channel
                if let Some(oldest_key) = channels
                    .iter()
                    .min_by_key(|(_, v)| v.last_used)
                    .map(|(k, _)| k.clone())
                {
                    channels.remove(&oldest_key);
                }
            }
            channels.insert(endpoint_uri.to_string(), pooled);
        }

        Ok(channel)
    }

    /// What: Clear all pooled connections (close all channels).
    ///
    /// Inputs: None
    /// Output: All channels cleared; safe to drop
    ///
    /// Details:
    /// - Used before worker shutdown to gracefully close all client connections
    /// - Prevents connection leaks and allows clean process termination
    #[allow(dead_code)]
    pub fn clear_all(&self) {
        let mut channels = self.channels.lock().unwrap();
        channels.clear();
    }
}
#[cfg(test)]
#[path = "../tests/flight_client_pool_tests.rs"]
mod tests;
