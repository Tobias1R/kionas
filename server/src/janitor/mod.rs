use crate::auth::persistence::Persistence;
use crate::warehouse::state::SharedData;
use chrono::Utc;
use deadpool::managed::Pool;
use kionas::config::load_cluster_config;
use kionas::constants::{
    CLEANUP_JOB_INTERVAL_ENV, CLEANUP_JOB_INTERVAL_SECONDS, QUERY_RESULT_RETENTION_ENV,
    QUERY_RESULT_RETENTION_SECONDS, REDIS_POOL_SIZE_ENV, REDIS_SESSION_TTL_ENV,
    REDIS_SESSION_TTL_SECONDS, REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY,
    REDIS_UI_DASHBOARD_SERVER_STATS_KEY, REDIS_UI_DASHBOARD_SESSIONS_KEY,
    REDIS_UI_DASHBOARD_TOKENS_KEY, REDIS_UI_DASHBOARD_WORKERS_KEY, REDIS_URL_ENV,
    STAGE_EXCHANGE_RETENTION_ENV, STAGE_EXCHANGE_RETENTION_SECONDS,
    TRANSACTION_STAGING_RETENTION_ENV, TRANSACTION_STAGING_RETENTION_SECONDS,
};
use kionas::monitoring::{init_redis_pool, update_cluster_info};
use kionas::redis_monitoring::{ClusterHealthStatus, WorkerHealthStatus, WorkerSystemInfo};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{CpuExt, ProcessExt, System, SystemExt};

#[cfg(test)]
#[path = "../tests/janitor_server_stats_counters_tests.rs"]
mod tests;

const DEFAULT_REDIS_URL: &str = "redis://kionas-redis:6379/2";
const DASHBOARD_TTL_SECONDS: u64 = 120;
const FRESHNESS_TARGET_SECONDS: u64 = 15;
const DATA_VERSION: u32 = 1;
const DEFAULT_REDIS_POOL_SIZE: usize = 8;

type RedisPool = Pool<RedisConnectionManager>;

struct RedisConnectionManager {
    redis_url: String,
}

impl deadpool::managed::Manager for RedisConnectionManager {
    type Type = redis::aio::MultiplexedConnection;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> impl std::future::Future<Output = Result<Self::Type, Self::Error>> + Send {
        let redis_url = self.redis_url.clone();
        async move {
            let client = redis::Client::open(redis_url.as_str())?;
            let conn = client.get_multiplexed_async_connection().await?;
            Ok(conn)
        }
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &deadpool::managed::Metrics,
    ) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        Ok(())
    }
}

#[derive(Serialize)]
struct DashboardEnvelope<T: Serialize> {
    generated_at: String,
    freshness_target_seconds: u64,
    source_component: &'static str,
    partial_failure: bool,
    data_version: u32,
    error: Option<String>,
    data: Option<T>,
}

#[derive(Serialize)]
struct ServerStatsPayload {
    counter: u32,
    warehouses: Vec<String>,
    worker_pools: Vec<String>,
    sessions: usize,
    memory_mb: u64,
    virtual_memory_mb: u64,
    cpu_usage_percent: f32,
    active_queries: u32,
    total_queries_submitted: u64,
    total_queries_succeeded: u64,
    total_queries_failed: u64,
    queries_per_minute: f32,
}

#[derive(Default)]
struct ServerStatsAccumulator {
    last_queries_submitted: u64,
}

#[derive(Serialize)]
struct SessionSnapshot {
    id: String,
    role: String,
    warehouse: String,
    remote_addr: String,
    is_authenticated: bool,
    last_active: u64,
    use_database: String,
    query_count: u64,
    last_query_at: u64,
    total_query_duration_ms: u64,
    error_count: u64,
}

/// What: Session data structure for deserializing from Redis JSON during cleanup.
///
/// Details:
/// - Matches the actual Redis session JSON format produced by Session struct
/// - Used only for cleanup operations, not dashboard publishing
/// - Includes all fields from the serialized Session to avoid deserialization errors
#[derive(Serialize, Deserialize)]
struct RedisSessionData {
    id: String,
    auth_token: String,
    is_authenticated: bool,
    role: String,
    warehouse_name: String,
    #[serde(default)]
    pool_members: Vec<String>,
    remote_addr: String,
    last_active: u64,
    use_database: String,
}

#[derive(Serialize)]
struct TokenSnapshot {
    username: String,
    exp: usize,
}

#[derive(Serialize)]
struct WorkerSnapshot {
    key: String,
    warehouse_name: String,
    host: String,
    port: u32,
    last_heartbeat: String,
}

#[derive(Serialize)]
struct ConsulSummaryPayload {
    nodes: Vec<String>,
    master: String,
    storage_type: String,
}

/// What: Derive cluster-level health from registered and live worker health snapshots.
///
/// Inputs:
/// - `registered_count`: Number of workers tracked in server shared state.
/// - `live_workers`: Worker health snapshots currently present in monitoring Redis.
///
/// Output:
/// - `ClusterHealthStatus` derived from worker health and stale-worker ratio.
///
/// Details:
/// - Unhealthy when there are no live workers, any live worker is unhealthy,
///   or stale ratio is greater than 50%.
/// - Degraded when stale ratio is at least 20% or any live worker is degraded.
/// - Healthy only when all live workers are healthy and stale ratio is below 20%.
fn derive_cluster_health(
    registered_count: usize,
    live_workers: &[WorkerSystemInfo],
) -> ClusterHealthStatus {
    if live_workers.is_empty() {
        return ClusterHealthStatus::Unhealthy;
    }

    let live_count = live_workers.len();
    let stale_count = registered_count.saturating_sub(live_count);
    let stale_ratio = stale_count as f32 / registered_count.max(1) as f32;

    if stale_ratio > 0.50 {
        return ClusterHealthStatus::Unhealthy;
    }

    let has_unhealthy = live_workers
        .iter()
        .any(|worker| worker.health_status == WorkerHealthStatus::Unhealthy);
    if has_unhealthy {
        return ClusterHealthStatus::Unhealthy;
    }

    if stale_ratio >= 0.20 {
        return ClusterHealthStatus::Degraded;
    }

    let has_degraded = live_workers
        .iter()
        .any(|worker| worker.health_status == WorkerHealthStatus::Degraded);
    if has_degraded {
        return ClusterHealthStatus::Degraded;
    }

    ClusterHealthStatus::Healthy
}

/// What: Load currently live worker monitoring snapshots from Redis.
///
/// Inputs:
/// - `connection_manager`: Monitoring Redis connection manager (status DB).
///
/// Output:
/// - `Ok(Vec<WorkerSystemInfo>)` with successfully parsed live worker snapshots.
/// - `Err(String)` when Redis key scan/read fails.
///
/// Details:
/// - Keys are scanned via `kionas:worker:*:sys_info` pattern.
/// - Individual JSON parse failures are logged and skipped.
async fn collect_live_worker_infos(
    connection_manager: &redis::aio::ConnectionManager,
) -> Result<Vec<WorkerSystemInfo>, String> {
    let mut connection = connection_manager.clone();
    let keys: Vec<String> = connection
        .keys("kionas:worker:*:sys_info")
        .await
        .map_err(|error| format!("failed to list worker sys_info keys: {}", error))?;

    let mut workers = Vec::with_capacity(keys.len());
    for key in keys {
        let payload: String = match connection.get(&key).await {
            Ok(payload) => payload,
            Err(error) => {
                log::warn!("failed reading worker sys_info key '{}': {}", key, error);
                continue;
            }
        };

        match serde_json::from_str::<WorkerSystemInfo>(&payload) {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                log::warn!(
                    "failed parsing worker sys_info payload for key '{}': {}",
                    key,
                    error
                );
            }
        }
    }

    Ok(workers)
}

/// What: Start the janitor loop responsible for periodic Redis dashboard cache updates.
///
/// Inputs:
/// - `shared_data`: Shared server state used as the source for dashboard domain snapshots.
///
/// Output:
/// - Spawns a detached task and returns immediately.
///
/// Details:
/// - Performs a warm publish on startup and then refreshes every 15 seconds.
/// - Domain writes are isolated so one failure does not block others.
/// - Replaces the legacy /metrics HTTP loop with Redis-backed snapshot publication.
pub(crate) fn start(shared_data: SharedData) {
    // Clone for cleanup task
    let cleanup_shared_data = Arc::clone(&shared_data);

    tokio::spawn(async move {
        let janitor_start_time = Utc::now();
        let mut server_stats_accumulator = ServerStatsAccumulator::default();
        let status_redis_url = resolved_status_redis_url();
        let redis_pool = match redis_pool() {
            Ok(pool) => pool,
            Err(error) => {
                log::warn!("Janitor failed to initialize redis pool: {}", error);
                return;
            }
        };

        let monitoring_redis_pool = match init_redis_pool(status_redis_url.as_str()).await {
            Ok(pool) => Some(pool),
            Err(error) => {
                log::warn!(
                    "Janitor failed to initialize monitoring redis connection manager: {}",
                    error
                );
                None
            }
        };

        log::info!("Janitor loop started");
        publish_all_domains(
            &shared_data,
            &redis_pool,
            monitoring_redis_pool.as_ref(),
            janitor_start_time,
            &mut server_stats_accumulator,
        )
        .await;
        let mut interval = tokio::time::interval(Duration::from_secs(FRESHNESS_TARGET_SECONDS));
        loop {
            interval.tick().await;
            publish_all_domains(
                &shared_data,
                &redis_pool,
                monitoring_redis_pool.as_ref(),
                janitor_start_time,
                &mut server_stats_accumulator,
            )
            .await;
        }
    });

    // Start artifact cleanup background loop
    start_artifact_cleanup(cleanup_shared_data);
}

/// What: Publish all dashboard domains to Redis with per-domain fault isolation.
///
/// Inputs:
/// - `shared_data`: Shared server state used to collect current snapshots.
///
/// Output:
/// - None.
///
/// Details:
/// - Uses one shared Redis pool with multiplexed connections.
/// - Errors are logged and converted into partial-failure envelopes per domain.
async fn publish_all_domains(
    shared_data: &SharedData,
    redis_pool: &RedisPool,
    monitoring_redis_pool: Option<&redis::aio::ConnectionManager>,
    janitor_start_time: chrono::DateTime<Utc>,
    server_stats_accumulator: &mut ServerStatsAccumulator,
) {
    log::debug!("Janitor publish cycle started");
    publish_server_stats(shared_data, redis_pool, server_stats_accumulator).await;
    publish_sessions(shared_data, redis_pool).await;
    publish_tokens(shared_data, redis_pool).await;
    publish_workers(shared_data, redis_pool).await;
    publish_cluster_info(shared_data, monitoring_redis_pool, janitor_start_time).await;
    publish_consul_summary(shared_data, redis_pool).await;
    log::debug!("Janitor publish cycle completed");
}

/// What: Build the Redis pool used for dashboard snapshot publication.
///
/// Inputs:
/// - None.
///
/// Output:
/// - `Ok(RedisPool)` when pool construction succeeds.
/// - `Err(String)` with actionable details when initialization fails.
///
/// Details:
/// - Uses `REDIS_URL` when provided, otherwise defaults to status DB index 2.
fn redis_pool() -> Result<RedisPool, String> {
    let redis_url = resolved_status_redis_url();

    let pool_size = env::var(REDIS_POOL_SIZE_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_REDIS_POOL_SIZE);

    log::info!(
        "Janitor redis config resolved: url='{}', pool_size={}",
        redis_url,
        pool_size
    );

    let manager = RedisConnectionManager { redis_url };
    deadpool::managed::Pool::builder(manager)
        .max_size(pool_size)
        .build()
        .map_err(|error| format!("redis pool build error: {error}"))
}

/// What: Resolve the Redis URL used for status/dashboard data.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Redis URL pointing to DB index 2.
///
/// Details:
/// - Reads REDIS_URL and forces status DB semantics.
/// - Falls back to the janitor default when REDIS_URL is unset or invalid.
fn resolved_status_redis_url() -> String {
    env::var(REDIS_URL_ENV)
        .ok()
        .and_then(|url| to_status_db_url(&url))
        .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string())
}

/// What: Convert a Redis URL to target DB index 2 (status cache).
///
/// Inputs:
/// - `url`: Input Redis URL, with or without an explicit DB path.
///
/// Output:
/// - `Some(String)` when conversion succeeds.
/// - `None` when URL parsing fails.
///
/// Details:
/// - Preserves scheme, host, port, and credentials when present.
fn to_status_db_url(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let mut rebuilt = format!("{}://", parsed.scheme());
    if !parsed.username().is_empty() {
        rebuilt.push_str(parsed.username());
        if let Some(password) = parsed.password() {
            rebuilt.push(':');
            rebuilt.push_str(password);
        }
        rebuilt.push('@');
    }

    let host = parsed.host_str()?;
    rebuilt.push_str(host);
    if let Some(port) = parsed.port() {
        rebuilt.push(':');
        rebuilt.push_str(port.to_string().as_str());
    }

    rebuilt.push_str("/2");
    Some(rebuilt)
}

/// What: Publish current server stats envelope to Redis.
///
/// Inputs:
/// - `shared_data`: Shared server state with counters and registries.
/// - `client`: Redis client for writing the domain key.
///
/// Output:
/// - None.
///
/// Details:
/// - Captures memory and CPU values via sysinfo.
async fn publish_server_stats(
    shared_data: &SharedData,
    redis_pool: &RedisPool,
    server_stats_accumulator: &mut ServerStatsAccumulator,
) {
    let payload = collect_server_stats(shared_data, server_stats_accumulator).await;
    let result = publish_success(redis_pool, REDIS_UI_DASHBOARD_SERVER_STATS_KEY, payload).await;
    if let Err(error) = result {
        log::warn!("Janitor failed publishing server stats: {}", error);
    }
}

/// What: Publish current session list envelope to Redis.
///
/// Inputs:
/// - `shared_data`: Shared server state with session manager.
/// - `client`: Redis client for writing the domain key.
///
/// Output:
/// - None.
///
/// Details:
/// - Uses SessionManager as source-of-truth for active sessions.
async fn publish_sessions(shared_data: &SharedData, redis_pool: &RedisPool) {
    let sessions = collect_sessions(shared_data).await;
    let result = publish_success(redis_pool, REDIS_UI_DASHBOARD_SESSIONS_KEY, sessions).await;
    if let Err(error) = result {
        log::warn!("Janitor failed publishing sessions: {}", error);
    }
}

/// What: Publish current token list envelope to Redis.
///
/// Inputs:
/// - `shared_data`: Shared server state to resolve token parquet location.
/// - `client`: Redis client for writing the domain key.
///
/// Output:
/// - None.
///
/// Details:
/// - Writes a partial-failure envelope when token persistence is unavailable.
async fn publish_tokens(shared_data: &SharedData, redis_pool: &RedisPool) {
    match collect_tokens(shared_data).await {
        Ok(tokens) => {
            if let Err(error) =
                publish_success(redis_pool, REDIS_UI_DASHBOARD_TOKENS_KEY, tokens).await
            {
                log::warn!("Janitor failed publishing tokens: {}", error);
            }
        }
        Err(error) => {
            if let Err(publish_error) = publish_failure::<Vec<TokenSnapshot>>(
                redis_pool,
                REDIS_UI_DASHBOARD_TOKENS_KEY,
                error,
            )
            .await
            {
                log::warn!(
                    "Janitor failed publishing token failure envelope: {}",
                    publish_error
                );
            }
        }
    }
}

/// What: Publish current worker registry envelope to Redis.
///
/// Inputs:
/// - `shared_data`: Shared server state with worker registry.
/// - `client`: Redis client for writing the domain key.
///
/// Output:
/// - None.
///
/// Details:
/// - Includes worker key, endpoint metadata, and last heartbeat timestamp.
async fn publish_workers(shared_data: &SharedData, redis_pool: &RedisPool) {
    let workers = collect_workers(shared_data).await;
    let result = publish_success(redis_pool, REDIS_UI_DASHBOARD_WORKERS_KEY, workers).await;
    if let Err(error) = result {
        log::warn!("Janitor failed publishing workers: {}", error);
    }
}

/// What: Publish cluster summary envelope to Redis.
///
/// Inputs:
/// - `shared_data`: Shared server state used to resolve Consul address.
/// - `client`: Redis client for writing the domain key.
///
/// Output:
/// - None.
///
/// Details:
/// - Summary is read-only and excludes sensitive configuration data.
async fn publish_consul_summary(shared_data: &SharedData, redis_pool: &RedisPool) {
    match collect_consul_summary(shared_data).await {
        Ok(summary) => {
            if let Err(error) =
                publish_success(redis_pool, REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY, summary).await
            {
                log::warn!("Janitor failed publishing consul summary: {}", error);
            }
        }
        Err(error) => {
            if let Err(publish_error) = publish_failure::<ConsulSummaryPayload>(
                redis_pool,
                REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY,
                error,
            )
            .await
            {
                log::warn!(
                    "Janitor failed publishing consul failure envelope: {}",
                    publish_error
                );
            }
        }
    }
}

/// What: Publish cluster identity and health metadata for dashboard use.
///
/// Inputs:
/// - `shared_data`: Shared state with app config for cluster source lookup.
/// - `redis_pool`: Redis pool used for envelope writes.
/// - `janitor_start_time`: Janitor start timestamp used for uptime approximation.
///
/// Output:
/// - None.
///
/// Details:
/// - Publishes to the dedicated cluster_info domain key.
/// - Uses a static healthy status until richer health checks are implemented.
async fn publish_cluster_info(
    shared_data: &SharedData,
    monitoring_redis_pool: Option<&redis::aio::ConnectionManager>,
    janitor_start_time: chrono::DateTime<Utc>,
) {
    let Some(connection_manager) = monitoring_redis_pool else {
        log::debug!("Skipping cluster info publish because monitoring redis is unavailable");
        return;
    };

    let consul_host = {
        let shared = shared_data.lock().await;
        let config = match shared.config.as_ref() {
            Some(cfg) => cfg,
            None => {
                log::warn!("Skipping cluster info publish: missing shared config");
                return;
            }
        };
        config.consul_host.clone()
    };

    let consul_ref = if consul_host.is_empty() {
        None
    } else {
        Some(consul_host.as_str())
    };

    let cluster_config = match load_cluster_config(consul_ref).await {
        Ok(config) => config,
        Err(error) => {
            log::warn!(
                "Skipping cluster info publish: failed to load cluster config: {}",
                error
            );
            return;
        }
    };

    let registered_count = {
        let shared = shared_data.lock().await;
        let workers = shared.workers.lock().await;
        workers.len()
    };

    let live_worker_infos = match collect_live_worker_infos(connection_manager).await {
        Ok(workers) => workers,
        Err(error) => {
            log::warn!(
                "failed collecting live worker snapshots for cluster health derivation: {}",
                error
            );
            Vec::new()
        }
    };

    let health_status = derive_cluster_health(registered_count, &live_worker_infos);

    if let Err(error) = update_cluster_info(
        connection_manager,
        &cluster_config,
        registered_count,
        janitor_start_time,
        health_status,
    )
    .await
    {
        log::warn!(
            "Janitor failed publishing cluster info through monitoring API: {}",
            error
        );
    }
}

/// What: Collect server stats from in-memory state and system metrics.
///
/// Inputs:
/// - `shared_data`: Shared state containing counters and registries.
///
/// Output:
/// - `ServerStatsPayload` with state counts and sys metrics.
///
/// Details:
/// - Uses process memory/virtual memory and global CPU usage percentage.
async fn collect_server_stats(
    shared_data: &SharedData,
    server_stats_accumulator: &mut ServerStatsAccumulator,
) -> ServerStatsPayload {
    let (counter, warehouses, worker_pools, sessions, query_counters, prometheus_metrics) = {
        let shared = shared_data.lock().await;
        let counter = *shared.counter.lock().await;
        let warehouses_map = shared.warehouses.lock().await;
        let warehouses = warehouses_map.keys().cloned().collect::<Vec<_>>();
        let worker_pools_map = shared.worker_pools.lock().await;
        let worker_pools = worker_pools_map.keys().cloned().collect::<Vec<_>>();
        let sessions = shared.session_manager.list_sessions().await.len();
        let query_counters = Arc::clone(&shared.query_counters);
        let prometheus_metrics = Arc::clone(&shared.prometheus_metrics);
        (
            counter,
            warehouses,
            worker_pools,
            sessions,
            query_counters,
            prometheus_metrics,
        )
    };

    let query_snapshot = query_counters.snapshot();
    let query_delta = query_snapshot
        .total_queries_submitted
        .saturating_sub(server_stats_accumulator.last_queries_submitted);
    let queries_per_minute = (query_delta as f32) * (60.0_f32 / FRESHNESS_TARGET_SECONDS as f32);
    server_stats_accumulator.last_queries_submitted = query_snapshot.total_queries_submitted;

    let mut sys = System::new_all();
    sys.refresh_cpu();
    sys.refresh_processes();

    let (memory_mb, virtual_memory_mb) = match sysinfo::get_current_pid() {
        Ok(pid) => {
            if let Some(process) = sys.process(pid) {
                (
                    process.memory() / 1024 / 1024,
                    process.virtual_memory() / 1024 / 1024,
                )
            } else {
                (0, 0)
            }
        }
        Err(_) => (0, 0),
    };

    let cpu_usage_percent = sys.global_cpu_info().cpu_usage();

    prometheus_metrics.set_sessions_active(sessions);
    prometheus_metrics.set_process_cpu_percent(cpu_usage_percent);
    prometheus_metrics.set_process_memory_mb(memory_mb);

    ServerStatsPayload {
        counter,
        warehouses,
        worker_pools,
        sessions,
        memory_mb,
        virtual_memory_mb,
        cpu_usage_percent,
        active_queries: query_snapshot.active_queries,
        total_queries_submitted: query_snapshot.total_queries_submitted,
        total_queries_succeeded: query_snapshot.total_queries_succeeded,
        total_queries_failed: query_snapshot.total_queries_failed,
        queries_per_minute,
    }
}

/// What: Collect current session snapshots from SessionManager.
///
/// Inputs:
/// - `shared_data`: Shared state with session manager.
///
/// Output:
/// - Vector of `SessionSnapshot` rows.
///
/// Details:
/// - Snapshot is intentionally read-only and strips auth token value.
async fn collect_sessions(shared_data: &SharedData) -> Vec<SessionSnapshot> {
    let session_manager = {
        let shared = shared_data.lock().await;
        shared.session_manager.clone()
    };

    let sessions = session_manager.list_sessions().await;
    sessions
        .into_iter()
        .map(|session| SessionSnapshot {
            id: session.get_id(),
            role: session.get_role(),
            warehouse: session.get_warehouse(),
            remote_addr: session.get_remote_addr(),
            is_authenticated: session.is_authenticated(),
            last_active: session.get_last_active(),
            use_database: session.get_use_database(),
            query_count: session.get_query_count(),
            last_query_at: session.get_last_query_at(),
            total_query_duration_ms: session.get_total_query_duration_ms(),
            error_count: session.get_error_count(),
        })
        .collect()
}

/// What: Collect current token snapshots from token parquet persistence.
///
/// Inputs:
/// - `shared_data`: Shared state used to resolve security data path.
///
/// Output:
/// - `Ok(Vec<TokenSnapshot>)` when token data can be loaded.
/// - `Err(String)` when path or read operations fail.
///
/// Details:
/// - Token strings are not published; only username and expiry are included.
async fn collect_tokens(shared_data: &SharedData) -> Result<Vec<TokenSnapshot>, String> {
    let data_path = token_data_path(shared_data).await?;
    let file_path = format!("{}/tokens.parquet", data_path);
    if !Path::new(file_path.as_str()).exists() {
        return Ok(Vec::new());
    }

    let tokens = Persistence::load_tokens(file_path.as_str())
        .map_err(|error| format!("load tokens failed: {error}"))?;

    Ok(tokens
        .into_iter()
        .map(|token| TokenSnapshot {
            username: token.username,
            exp: token.exp,
        })
        .collect())
}

/// What: Resolve the configured security data path used for token persistence.
///
/// Inputs:
/// - `shared_data`: Shared state with optional app config.
///
/// Output:
/// - `Ok(String)` with security data path.
/// - `Err(String)` when security config is unavailable.
///
/// Details:
/// - Reads immutable config snapshot from shared state.
async fn token_data_path(shared_data: &SharedData) -> Result<String, String> {
    let shared = shared_data.lock().await;
    let config = shared
        .config
        .as_ref()
        .ok_or_else(|| "missing shared config".to_string())?;

    let security = config
        .services
        .security
        .as_ref()
        .ok_or_else(|| "missing services.security configuration".to_string())?;

    Ok(security.data_path.clone())
}

/// What: Collect current worker snapshots from registry state.
///
/// Inputs:
/// - `shared_data`: Shared state with worker map.
///
/// Output:
/// - Vector of `WorkerSnapshot` rows.
///
/// Details:
/// - Includes host/port and heartbeat recency data required by dashboard.
async fn collect_workers(shared_data: &SharedData) -> Vec<WorkerSnapshot> {
    let workers = {
        let shared = shared_data.lock().await;
        let workers_map = shared.workers.lock().await;
        workers_map.values().cloned().collect::<Vec<_>>()
    };

    workers
        .into_iter()
        .map(|worker| WorkerSnapshot {
            key: worker.key,
            warehouse_name: worker.warehouse.get_name(),
            host: worker.warehouse.get_host(),
            port: worker.warehouse.get_port(),
            last_heartbeat: worker.last_heartbeat.to_rfc3339(),
        })
        .collect()
}

/// What: Collect read-only Consul cluster summary fields for dashboard display.
///
/// Inputs:
/// - `shared_data`: Shared state with app config (Consul address).
///
/// Output:
/// - `Ok(ConsulSummaryPayload)` when cluster config can be loaded.
/// - `Err(String)` when Consul/file config load fails.
///
/// Details:
/// - Uses existing cluster config loader with consul-first fallback semantics.
async fn collect_consul_summary(shared_data: &SharedData) -> Result<ConsulSummaryPayload, String> {
    let consul_host = {
        let shared = shared_data.lock().await;
        let config = shared
            .config
            .as_ref()
            .ok_or_else(|| "missing shared config".to_string())?;
        config.consul_host.clone()
    };

    let consul_ref = if consul_host.is_empty() {
        None
    } else {
        Some(consul_host.as_str())
    };

    let cluster = load_cluster_config(consul_ref)
        .await
        .map_err(|error| format!("load cluster config failed: {error}"))?;

    Ok(ConsulSummaryPayload {
        nodes: cluster.nodes,
        master: cluster.master,
        storage_type: cluster.storage.storage_type,
    })
}

/// What: Publish a successful domain envelope to Redis with expiration.
///
/// Inputs:
/// - `redis_pool`: Redis connection pool.
/// - `key`: Domain key to write.
/// - `payload`: Domain payload body.
///
/// Output:
/// - `Ok(())` on successful write.
/// - `Err(String)` when serialization or Redis operations fail.
///
/// Details:
/// - Enforces a unified envelope shape for all dashboard domains.
async fn publish_success<T: Serialize>(
    redis_pool: &RedisPool,
    key: &str,
    payload: T,
) -> Result<(), String> {
    let envelope = DashboardEnvelope {
        generated_at: Utc::now().to_rfc3339(),
        freshness_target_seconds: FRESHNESS_TARGET_SECONDS,
        source_component: "server-janitor",
        partial_failure: false,
        data_version: DATA_VERSION,
        error: None,
        data: Some(payload),
    };

    publish_envelope(redis_pool, key, envelope).await
}

/// What: Publish a partial-failure envelope to Redis for a domain.
///
/// Inputs:
/// - `redis_pool`: Redis connection pool.
/// - `key`: Domain key to write.
/// - `error`: Actionable error summary.
///
/// Output:
/// - `Ok(())` on successful write.
/// - `Err(String)` when serialization or Redis operations fail.
///
/// Details:
/// - Keeps domain key available even when source extraction fails.
async fn publish_failure<T: Serialize>(
    redis_pool: &RedisPool,
    key: &str,
    error: String,
) -> Result<(), String> {
    let envelope = DashboardEnvelope::<T> {
        generated_at: Utc::now().to_rfc3339(),
        freshness_target_seconds: FRESHNESS_TARGET_SECONDS,
        source_component: "server-janitor",
        partial_failure: true,
        data_version: DATA_VERSION,
        error: Some(error),
        data: None,
    };

    publish_envelope(redis_pool, key, envelope).await
}

/// What: Serialize and write a dashboard envelope to Redis with TTL.
///
/// Inputs:
/// - `redis_pool`: Redis connection pool.
/// - `key`: Domain key to write.
/// - `envelope`: Prepared envelope payload.
///
/// Output:
/// - `Ok(())` on successful write.
/// - `Err(String)` when serialization or Redis operations fail.
///
/// Details:
/// - Uses `SETEX` semantics to ensure stale key eviction.
async fn publish_envelope<T: Serialize>(
    redis_pool: &RedisPool,
    key: &str,
    envelope: DashboardEnvelope<T>,
) -> Result<(), String> {
    let body = serde_json::to_string(&envelope)
        .map_err(|error| format!("serialize dashboard envelope failed: {error}"))?;

    let mut connection = redis_pool
        .get()
        .await
        .map_err(|error| format!("redis pooled connection failed: {error}"))?;

    connection
        .set_ex::<&str, String, ()>(key, body, DASHBOARD_TTL_SECONDS)
        .await
        .map_err(|error| format!("redis set_ex failed for {key}: {error}"))?;

    log::debug!(
        "Janitor published redis key '{}' with ttl={}s",
        key,
        DASHBOARD_TTL_SECONDS
    );
    Ok(())
}

/// What: Get the configured retention threshold for stage exchange artifacts.
///
/// Inputs:
/// - None
///
/// Output:
/// - Retention threshold in seconds
///
/// Details:
/// - Checks KIONAS_STAGE_EXCHANGE_RETENTION_SECONDS environment variable
/// - Falls back to STAGE_EXCHANGE_RETENTION_SECONDS constant if not set
fn get_stage_exchange_retention() -> u64 {
    env::var(STAGE_EXCHANGE_RETENTION_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(STAGE_EXCHANGE_RETENTION_SECONDS)
}

/// What: Get the configured retention threshold for query result artifacts.
///
/// Inputs:
/// - None
///
/// Output:
/// - Retention threshold in seconds
///
/// Details:
/// - Checks KIONAS_QUERY_RESULT_RETENTION_SECONDS environment variable
/// - Falls back to QUERY_RESULT_RETENTION_SECONDS constant if not set
fn get_query_result_retention() -> u64 {
    env::var(QUERY_RESULT_RETENTION_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(QUERY_RESULT_RETENTION_SECONDS)
}

/// What: Get the configured retention threshold for transaction staging artifacts.
///
/// Inputs:
/// - None
///
/// Output:
/// - Retention threshold in seconds
///
/// Details:
/// - Checks KIONAS_TRANSACTION_STAGING_RETENTION_SECONDS environment variable
/// - Falls back to TRANSACTION_STAGING_RETENTION_SECONDS constant if not set
#[allow(dead_code)]
fn get_transaction_staging_retention() -> u64 {
    env::var(TRANSACTION_STAGING_RETENTION_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(TRANSACTION_STAGING_RETENTION_SECONDS)
}

/// What: Get the configured cleanup job interval.
///
/// Inputs:
/// - None
///
/// Output:
/// - Cleanup interval in seconds (0 = disabled)
///
/// Details:
/// - Checks KIONAS_CLEANUP_JOB_INTERVAL_SECONDS environment variable
/// - Falls back to CLEANUP_JOB_INTERVAL_SECONDS constant if not set
/// - Return value of 0 means cleanup is disabled
fn get_cleanup_interval() -> u64 {
    env::var(CLEANUP_JOB_INTERVAL_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(CLEANUP_JOB_INTERVAL_SECONDS)
}

/// What: Get the configured retention threshold for session artifacts.
///
/// Inputs:
/// - None
///
/// Output:
/// - Retention threshold in seconds
///
/// Details:
/// - Checks KIONAS_SESSION_TTL_SECONDS environment variable
/// - Falls back to REDIS_SESSION_TTL_SECONDS constant if not set
/// - Used to identify sessions without explicit TTL that need cleanup
fn get_session_retention() -> u64 {
    env::var(REDIS_SESSION_TTL_ENV)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(REDIS_SESSION_TTL_SECONDS)
}

/// What: Get a Redis connection to DB 0 (sessions database).
///
/// Inputs:
/// - None
///
/// Output:
/// - `Ok(redis::aio::MultiplexedConnection)` on success
/// - `Err(String)` if connection fails
///
/// Details:
/// - Parses REDIS_URL environment variable
/// - Replaces or appends DB path with "/0" for session DB
/// - Establishes async multiplexed connection
async fn get_session_redis_connection() -> Result<redis::aio::MultiplexedConnection, String> {
    let redis_url = env::var(REDIS_URL_ENV)
        .ok()
        .and_then(|url| to_session_db_url(&url))
        .unwrap_or_else(|| "redis://kionas-redis:6379/0".to_string());

    let client = redis::Client::open(redis_url.as_str())
        .map_err(|e| format!("Failed to create Redis client: {e}"))?;

    client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| format!("Failed to connect to Redis: {e}"))
}

/// What: Convert a Redis URL to target DB index 0 (sessions database).
///
/// Inputs:
/// - `url`: Input Redis URL, with or without an explicit DB path.
///
/// Output:
/// - `Some(String)` when conversion succeeds.
/// - `None` when URL parsing fails.
///
/// Details:
/// - Preserves scheme, host, port, and credentials when present.
fn to_session_db_url(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let mut rebuilt = format!("{}://", parsed.scheme());
    if !parsed.username().is_empty() {
        rebuilt.push_str(parsed.username());
        if let Some(password) = parsed.password() {
            rebuilt.push(':');
            rebuilt.push_str(password);
        }
        rebuilt.push('@');
    }

    let host = parsed.host_str()?;
    rebuilt.push_str(host);
    if let Some(port) = parsed.port() {
        rebuilt.push(':');
        rebuilt.push_str(port.to_string().as_str());
    }

    rebuilt.push_str("/0");
    Some(rebuilt)
}

pub const SESSION_KEY_PREFIX: &str = "kionas:session:";

/// What: Clean up stale Redis sessions that lack TTL or are older than retention period.
///
/// Inputs:
/// - None (reads from Redis DB 0 and environment)
///
/// Output:
/// - `Ok((count, deleted))` with number of sessions scanned and deleted
/// - `Err(String)` if operation fails
///
/// Details:
/// - Scans all keys matching `kionas:session:*` pattern
/// - Deserializes JSON and checks `last_active` timestamp
/// - Deletes sessions where:
///   - `last_active == 0` (legacy sessions without TTL, created before Phase 1)
///   - `last_active < cutoff` (sessions older than retention period)
/// - Handles pagination gracefully to avoid blocking Redis
/// - Continues on individual delete errors to maximize cleanup
async fn cleanup_stale_sessions() -> Result<(u64, u64), String> {
    let mut conn = get_session_redis_connection().await?;

    let retention_seconds = get_session_retention();
    let now_u64 = Utc::now().timestamp() as u64;
    let cutoff = now_u64.saturating_sub(retention_seconds);

    let mut deleted_count = 0u64;
    let mut cursor = 0u64;
    let mut scanned_count = 0u64;
    let scan_batch_size = 100usize;

    loop {
        let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(format!("{}*", SESSION_KEY_PREFIX))
            .arg("COUNT")
            .arg(scan_batch_size)
            .query_async(&mut conn)
            .await
            .map_err(|e| format!("Redis SCAN error: {e}"))?;

        scanned_count += keys.len() as u64;

        for key in keys {
            match conn.get::<_, String>(&key).await {
                Ok(session_json) => match serde_json::from_str::<RedisSessionData>(&session_json) {
                    Ok(session) => {
                        // Delete if:
                        // 1. last_active == 0 (legacy session without TTL from before Phase 1)
                        // 2. last_active is older than retention threshold
                        let should_delete =
                            session.last_active == 0 || session.last_active < cutoff;

                        if should_delete {
                            if let Err(e) = conn.del::<_, ()>(&key).await {
                                log::warn!("Failed to delete session {}: {}", key, e);
                            } else {
                                deleted_count += 1;
                                let age_msg = if session.last_active == 0 {
                                    "legacy (no TTL)".to_string()
                                } else {
                                    format!("age={} seconds", now_u64 - session.last_active)
                                };
                                log::debug!("Cleaned session: {} ({})", session.id, age_msg);
                            }
                        }
                    }
                    Err(e) => {
                        log::warn!("Failed to parse session JSON for {}: {}", key, e);
                    }
                },
                Err(e) => {
                    log::warn!("Failed to retrieve session {}: {}", key, e);
                }
            }
        }

        cursor = next_cursor;
        if cursor == 0 {
            break;
        }
    }

    if deleted_count > 0 {
        log::info!(
            "Session cleanup: scanned={} sessions, deleted={}",
            scanned_count,
            deleted_count
        );
    }

    Ok((scanned_count, deleted_count))
}

/// What: Start the artifact cleanup background loop.
///
/// Inputs:
/// - `shared_data`: Shared server state with cluster config
///
/// Output:
/// - Spawns a detached task and returns immediately.
///
/// Details:
/// - Runs cleanup job on configured interval (default: 1 hour)
/// - Skips cleanup if interval is 0 (disabled)
/// - Cleans both MinIO artifacts and stale Redis sessions
/// - Deletes artifacts older than configured retention thresholds
/// - Logs cleanup operations and errors with counters and sizes
pub(crate) fn start_artifact_cleanup(shared_data: SharedData) {
    tokio::spawn(async move {
        let interval_secs = get_cleanup_interval();
        if interval_secs == 0 {
            log::info!("Artifact cleanup is disabled (cleanup_interval=0)");
            return;
        }

        log::info!(
            "Artifact cleanup loop started with interval={}s",
            interval_secs
        );

        let mut interval = tokio::time::interval(Duration::from_secs(interval_secs));
        loop {
            interval.tick().await;

            // Lock shared state to get config
            match shared_data.lock().await.config.clone() {
                Some(_config) => {
                    if let Err(error) = cleanup_aged_artifacts().await {
                        log::warn!("Artifact cleanup failed: {}", error);
                    }
                }
                None => {
                    log::warn!("Artifact cleanup skipped: no config available");
                }
            }
        }
    });
}

/// What: Execute a single cleanup cycle for aged MinIO artifacts.
///
/// Inputs:
/// - None (reads from environment and cluster config)
///
/// Output:
/// - `Ok(())` on success, `Err(String)` with error details
///
/// Details:
/// - Loads cluster config to get MinIO connection details
/// - Lists objects by prefix and checks last_modified timestamp
/// - Deletes objects older than retention threshold
/// - Processes three prefixes: distributed_exchange/, staging/
/// - Logs cleanup statistics: object count, bytes freed, errors
async fn cleanup_aged_artifacts() -> Result<(), String> {
    let cluster_cfg = load_cluster_config(None)
        .await
        .map_err(|e| format!("Failed to load cluster config: {e}"))?;

    let now = chrono::Utc::now();
    let mut total_deleted = 0u64;
    let mut total_bytes_freed = 0u64;
    let mut total_errors = 0u64;

    // Cleanup stale Redis sessions
    match cleanup_stale_sessions().await {
        Ok((scanned, deleted)) => {
            if deleted > 0 {
                log::info!(
                    "Cleaned stale sessions: scanned={}, deleted={}",
                    scanned,
                    deleted
                );
            }
        }
        Err(error) => {
            log::warn!("Failed to cleanup stale sessions: {}", error);
            total_errors += 1;
        }
    }

    // Cleanup stage exchanges (distributed_exchange/)
    let stage_retention = get_stage_exchange_retention();
    let stage_cutoff = now - chrono::Duration::seconds(stage_retention as i64);
    match cleanup_prefix("distributed_exchange/", stage_cutoff, &cluster_cfg).await {
        Ok((deleted, bytes)) => {
            total_deleted += deleted;
            total_bytes_freed += bytes;
            if deleted > 0 {
                log::info!(
                    "Cleaned distributed_exchange/: deleted={}, bytes_freed={}",
                    deleted,
                    bytes
                );
            }
        }
        Err(error) => {
            log::warn!("Failed to cleanup distributed_exchange/: {}", error);
            total_errors += 1;
        }
    }

    // Cleanup query results (staging/ with query result retention)
    let query_retention = get_query_result_retention();
    let query_cutoff = now - chrono::Duration::seconds(query_retention as i64);
    match cleanup_prefix("staging/", query_cutoff, &cluster_cfg).await {
        Ok((deleted, bytes)) => {
            total_deleted += deleted;
            total_bytes_freed += bytes;
            if deleted > 0 {
                log::info!(
                    "Cleaned staging/ (queries): deleted={}, bytes_freed={}",
                    deleted,
                    bytes
                );
            }
        }
        Err(error) => {
            log::warn!("Failed to cleanup staging/ (queries): {}", error);
            total_errors += 1;
        }
    }

    log::info!(
        "Artifact cleanup cycle completed: total_deleted={}, total_bytes_freed={}, errors={}",
        total_deleted,
        total_bytes_freed,
        total_errors
    );

    Ok(())
}

/// What: Delete all objects in a MinIO prefix that are older than a cutoff timestamp.
///
/// Inputs:
/// - `prefix`: S3 prefix to scan (e.g., "distributed_exchange/")
/// - `cutoff`: Timestamp before which objects should be deleted
/// - `cluster_cfg`: Cluster configuration with MinIO connection details
///
/// Output:
/// - `Ok((count, bytes))` with number of objects deleted and total bytes freed
/// - `Err(String)` if operation fails
///
/// Details:
/// - Lists objects paginated to handle large prefixes
/// - Compares object.last_modified() against cutoff time
/// - Deletes objects one at a time for safety
/// - Continues on individual delete errors to maximize cleanup
/// - TODO: This requires MinIO API exposure in worker module
async fn cleanup_prefix(
    prefix: &str,
    cutoff: chrono::DateTime<chrono::Utc>,
    _cluster_cfg: &kionas::config::ClusterConfig,
) -> Result<(u64, u64), String> {
    let deleted_count = 0u64;
    let bytes_freed = 0u64;

    log::debug!(
        "Cleanup scan for prefix '{}' with cutoff before {}",
        prefix,
        cutoff
    );

    // TODO: Phase 2 follow-up
    // This function needs access to MinIO list_objects_v2 and delete_object APIs.
    // Current implementation is a skeleton that logs the intent.
    // To complete:
    // 1. Add public list_objects_v2 method to MinioProvider in worker crate
    // 2. Add public delete_object method to MinioProvider
    // 3. Instantiate MinioProvider using cluster_cfg.storage
    // 4. Iterate through paginated results with last_modified comparison
    // 5. Accumulate and execute deletions

    log::info!(
        "Cleanup placeholder for '{}': cutoff={} (actual deletion requires MinIO API exposure)",
        prefix,
        cutoff
    );

    Ok((deleted_count, bytes_freed))
}
