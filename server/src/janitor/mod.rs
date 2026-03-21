use crate::auth::persistence::Persistence;
use crate::warehouse::state::SharedData;
use chrono::Utc;
use deadpool::managed::Pool;
use kionas::config::load_cluster_config;
use kionas::constants::{
    REDIS_POOL_SIZE_ENV, REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY,
    REDIS_UI_DASHBOARD_SERVER_STATS_KEY, REDIS_UI_DASHBOARD_SESSIONS_KEY,
    REDIS_UI_DASHBOARD_TOKENS_KEY, REDIS_UI_DASHBOARD_WORKERS_KEY, REDIS_URL_ENV,
};
use redis::AsyncCommands;
use serde::Serialize;
use std::env;
use std::path::Path;
use std::time::Duration;
use sysinfo::{CpuExt, ProcessExt, System, SystemExt};

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
    tokio::spawn(async move {
        let redis_pool = match redis_pool() {
            Ok(pool) => pool,
            Err(error) => {
                log::warn!("Janitor failed to initialize redis pool: {}", error);
                return;
            }
        };

        log::info!("Janitor loop started");
        publish_all_domains(&shared_data, &redis_pool).await;
        let mut interval = tokio::time::interval(Duration::from_secs(FRESHNESS_TARGET_SECONDS));
        loop {
            interval.tick().await;
            publish_all_domains(&shared_data, &redis_pool).await;
        }
    });
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
async fn publish_all_domains(shared_data: &SharedData, redis_pool: &RedisPool) {
    log::debug!("Janitor publish cycle started");
    publish_server_stats(shared_data, redis_pool).await;
    publish_sessions(shared_data, redis_pool).await;
    publish_tokens(shared_data, redis_pool).await;
    publish_workers(shared_data, redis_pool).await;
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
    let redis_url = env::var(REDIS_URL_ENV)
        .ok()
        .and_then(|url| to_status_db_url(&url))
        .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string());

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
async fn publish_server_stats(shared_data: &SharedData, redis_pool: &RedisPool) {
    let payload = collect_server_stats(shared_data).await;
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
async fn collect_server_stats(shared_data: &SharedData) -> ServerStatsPayload {
    let (counter, warehouses, worker_pools, sessions) = {
        let shared = shared_data.lock().await;
        let counter = *shared.counter.lock().await;
        let warehouses_map = shared.warehouses.lock().await;
        let warehouses = warehouses_map.keys().cloned().collect::<Vec<_>>();
        let worker_pools_map = shared.worker_pools.lock().await;
        let worker_pools = worker_pools_map.keys().cloned().collect::<Vec<_>>();
        let sessions = shared.session_manager.list_sessions().await.len();
        (counter, warehouses, worker_pools, sessions)
    };

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

    ServerStatsPayload {
        counter,
        warehouses,
        worker_pools,
        sessions,
        memory_mb,
        virtual_memory_mb,
        cpu_usage_percent,
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
