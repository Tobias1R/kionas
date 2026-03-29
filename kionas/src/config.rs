use crate::parse_env_vars;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::error::Error;
use std::fs;

use crate::constants::{CONSUL_CLUSTER_KEY, CONSUL_NODE_CONFIG_PREFIX};

const DEFAULT_OBJECT_STORE_POOL_SIZE: usize = 20;
const DEFAULT_INTEROPS_POOL_SIZE: usize = 20;
const DEFAULT_WORKER_EXECUTE_TIMEOUT_SECS: u64 = 120;
const DEFAULT_CONSTRAINT_CACHE_TTL_SECS: u64 = 300;

/// Typed application config used by all binaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /*
    This struct is designed to be flexible and support various deployment scenarios.
    The `mode` field can be used to indicate the role of the instance
    (e.g., "gateway", "server", "worker", "metastore"),
    allowing for different configurations based on the mode.
    The TLS certificate and key fields can be overridden by environment variables or
    Consul values, providing flexibility in how certificates are managed across different environments.
     */
    pub mode: String, // gateway, server, worker, metastore
    // Consul
    pub consul_host: String,
    // logging
    pub logging: LoggingConfig,
    // service endpoints
    pub services: ServicesConfig,
    // session configuration
    #[serde(default)]
    pub session: Option<SessionConfig>,
    // cleanup configuration
    #[serde(default)]
    pub cleanup: Option<CleanupConfig>,
    // prometheus metrics endpoint configuration
    #[serde(default)]
    pub metrics: Option<MetricsConfig>,
    #[serde(default)]
    pub write_performance: Option<WritePerformanceConfig>,
    #[serde(default)]
    pub worker_execute_timeout_secs: Option<u64>,
    #[serde(default)]
    pub constraint_cache_ttl_secs: Option<u64>,
}

/// What: Worker write-path performance tuning values loaded from config.
///
/// Inputs:
/// - `object_store_pool_size`: Max concurrent object-store clients in worker write path.
/// - `interops_pool_size`: Max concurrent interops clients for task updates.
///
/// Output:
/// - Deserialized write performance configuration used by worker runtime.
///
/// Details:
/// - Missing values are resolved via `AppConfig::resolved_write_performance_config`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WritePerformanceConfig {
    pub object_store_pool_size: usize,
    pub interops_pool_size: usize,
}

impl Default for WritePerformanceConfig {
    fn default() -> Self {
        Self {
            object_store_pool_size: DEFAULT_OBJECT_STORE_POOL_SIZE,
            interops_pool_size: DEFAULT_INTEROPS_POOL_SIZE,
        }
    }
}

/// What: Metrics endpoint configuration shared by server and worker processes.
///
/// Inputs:
/// - `enabled`: Whether the Prometheus endpoint should be started.
/// - `port`: TCP port used by the metrics HTTP endpoint.
///
/// Output:
/// - Deserialized metrics settings consumed by binary startup wiring.
///
/// Details:
/// - Defaults are mode-aware through `default_for_mode`.
/// - Environment overrides are applied by the binaries at runtime.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub port: u16,
}

impl MetricsConfig {
    /// What: Build default metrics configuration based on binary mode.
    ///
    /// Inputs:
    /// - `mode`: App mode string such as server or worker.
    ///
    /// Output:
    /// - Metrics configuration with enabled endpoint and mode-specific port.
    ///
    /// Details:
    /// - Server default port: 9091.
    /// - Worker default port: 9092.
    /// - Unknown modes fall back to server default.
    pub fn default_for_mode(mode: &str) -> Self {
        let port = if mode.eq_ignore_ascii_case("worker") {
            9092
        } else {
            9091
        };
        Self {
            enabled: true,
            port,
        }
    }
}

impl AppConfig {
    /// What: Resolve effective metrics configuration for this app config.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Effective `MetricsConfig` with mode-specific defaults when omitted.
    ///
    /// Details:
    /// - If `metrics` is absent in config payload, defaults are derived from `mode`.
    pub fn resolved_metrics_config(&self) -> MetricsConfig {
        self.metrics
            .clone()
            .unwrap_or_else(|| MetricsConfig::default_for_mode(self.mode.as_str()))
    }

    /// What: Resolve worker write performance config with safe defaults.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Effective `WritePerformanceConfig` for worker pool sizing.
    ///
    /// Details:
    /// - Falls back to object/interops pool size defaults of 20.
    pub fn resolved_write_performance_config(&self) -> WritePerformanceConfig {
        self.write_performance.clone().unwrap_or_default()
    }

    /// What: Resolve per-worker execute timeout used by server dispatch.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Effective timeout in seconds.
    ///
    /// Details:
    /// - Defaults to 120 seconds when unset.
    pub fn resolved_worker_execute_timeout_secs(&self) -> u64 {
        self.worker_execute_timeout_secs
            .unwrap_or(DEFAULT_WORKER_EXECUTE_TIMEOUT_SECS)
    }

    /// What: Resolve table-constraint cache TTL for server-side metadata cache.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Effective TTL in seconds.
    ///
    /// Details:
    /// - Defaults to 300 seconds when unset.
    pub fn resolved_constraint_cache_ttl_secs(&self) -> u64 {
        self.constraint_cache_ttl_secs
            .unwrap_or(DEFAULT_CONSTRAINT_CACHE_TTL_SECS)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InteropsConfig {
    pub host: String,
    pub port: u16,
    pub tls_cert: String,
    pub tls_key: String,
    pub ca_cert: String,
    pub mode: String,      // http or https
    pub operation: String, // interops or metastore
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehouseConfig {
    pub host: String,
    pub port: u16,
    pub tls_cert: String,
    pub tls_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServicesConfig {
    pub security: Option<SecurityConfig>,
    pub interops: Option<InteropsConfig>,
    pub warehouse: Option<WarehouseConfig>,
    pub postgres: Option<PostgresServiceConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
    pub output: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub token: String,
    pub secret: String,
    pub data_path: String,
}

/// What: Session configuration for Redis TTL and expiration.
///
/// Inputs:
/// - `ttl_seconds`: Session expiration time in seconds
/// - `description`: Documentation of the setting
///
/// Output:
/// - Deserialized session config used by session manager
///
/// Details:
/// - TTL can be overridden via KIONAS_SESSION_TTL_SECONDS environment variable
/// - Default: 604800 seconds (7 days)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionConfig {
    pub ttl_seconds: Option<u64>,
    #[serde(default)]
    pub description: Option<String>,
}

/// What: Cleanup configuration for aged artifact retention and job scheduling.
///
/// Inputs:
/// - `stage_exchange_retention_seconds`: How long to keep distributed_exchange/ artifacts
/// - `query_result_retention_seconds`: How long to keep staging/ query results
/// - `transaction_staging_retention_seconds`: How long to keep transaction staging objects
/// - `cleanup_interval_seconds`: How often to run cleanup job (0 = disabled)
/// - `description`: Documentation of the policy
///
/// Output:
/// - Deserialized cleanup config used by janitor cleanup task
///
/// Details:
/// - Retention settings can be overridden via environment variables
/// - Set cleanup_interval_seconds to 0 to disable cleanup entirely
/// - Defaults: 3 days for exchanges, 7 days for results, 1 day for txn staging, 1 hour interval
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CleanupConfig {
    pub stage_exchange_retention_seconds: Option<u64>,
    pub query_result_retention_seconds: Option<u64>,
    pub transaction_staging_retention_seconds: Option<u64>,
    pub cleanup_interval_seconds: Option<u64>,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub storage_type: String,
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}

/// What: Warehouse pool tier template configuration loaded from cluster config.
///
/// Inputs:
/// - `tier`: Tier identifier such as xs, s, m, or xl
/// - `min_members`: Minimum number of workers required in this tier
/// - `max_members`: Maximum number of workers allowed in this tier
/// - `default`: Whether this tier is the default pool tier
/// - `description`: Human-readable description of the tier purpose
///
/// Output:
/// - Deserialized tier template used by server startup and worker registration
///
/// Details:
/// - Tier templates are cluster-scoped and shared by all workers
/// - Validation for min/max/default constraints is done by server startup logic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WarehousePoolTierConfig {
    pub tier: String,
    pub min_members: usize,
    pub max_members: usize,
    pub default: bool,
    pub description: String,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub cluster_name: String,
    pub cluster_id: String,
    #[serde(alias = "version")]
    pub cluster_version: String,
    pub nodes: Vec<String>,
    pub master: String,
    pub storage: StorageConfig,
    #[serde(default, alias = "warehouse_pools_types")]
    pub warehouse_pools_tiers: Vec<WarehousePoolTierConfig>,
}

fn validate_cluster_config(config: &ClusterConfig) -> Result<(), Box<dyn Error + Send + Sync>> {
    if config.cluster_name.trim().is_empty() {
        return Err("cluster_name must be non-empty".into());
    }

    if config.cluster_id.trim().is_empty() {
        return Err("cluster_id must be non-empty".into());
    }

    Ok(())
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PostgresServiceConfig {
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_db: String,
    pub postgres_user: String,
    pub postgres_password: String,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

/// Try to fetch a raw value from Consul for the given key. Returns Ok(None) if key not found.
pub async fn fetch_consul_raw(
    consul_addr: &str,
    key: &str,
) -> Result<Option<String>, Box<dyn Error + Send + Sync>> {
    let base = consul_addr.trim_end_matches('/');
    let url = format!("{}/v1/kv/{}?raw", base, key);
    let resp = reqwest::get(&url).await?;
    match resp.status().as_u16() {
        200 => {
            let txt = resp.text().await?;
            Ok(Some(txt))
        }
        404 => Ok(None),
        s => Err(format!("consul returned unexpected status {} for {}", s, url).into()),
    }
}

/// Load an `AppConfig` for a hostname. Resolution order:
/// 1. Consul at `consul_addr` key `configs/<hostname>` (if `consul_addr` provided)
/// 2. Local file `/workspace/configs/<hostname>.json` or `.toml`
/// 3. Error
pub async fn load_for_host(
    consul_addr: Option<&str>,
    hostname: &str,
) -> Result<AppConfig, Box<dyn Error + Send + Sync>> {
    // Helper to walk a JSON value and substitute ${ENV} in all strings
    fn substitute_env(v: JsonValue) -> JsonValue {
        match v {
            JsonValue::String(s) => JsonValue::String(parse_env_vars(&s)),
            JsonValue::Array(arr) => {
                JsonValue::Array(arr.into_iter().map(substitute_env).collect())
            }
            JsonValue::Object(map) => {
                let mapped = map
                    .into_iter()
                    .map(|(k, val)| (k, substitute_env(val)))
                    .collect();
                JsonValue::Object(mapped)
            }
            other => other,
        }
    }

    // Try consul
    if let Some(ca) = consul_addr
        && let Ok(Some(raw)) =
            fetch_consul_raw(ca, &format!("{}/{}", CONSUL_NODE_CONFIG_PREFIX, hostname)).await
    {
        let trimmed = raw.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            let j: JsonValue = serde_json::from_str(&raw)?;
            let j = substitute_env(j);
            let cfg: AppConfig = serde_json::from_value(j)?;
            Ok(cfg)
        } else {
            // parse TOML then convert to JSON value for substitution
            let t: toml::Value = toml::from_str(&raw)?;
            let j = serde_json::to_value(t)?;
            let j = substitute_env(j);
            let cfg: AppConfig = serde_json::from_value(j)?;
            Ok(cfg)
        }
    } else {
        // Local fallback (use CLI arg --config or default)
        #[derive(Parser, Debug)]
        #[command(author, version, about, long_about = None)]
        struct Args {
            #[arg(short, long, default_value = "configs/server.toml")]
            config: String,
        }

        let args = Args::parse();
        let path = args.config;
        let content = fs::read_to_string(&path)?;
        let trimmed = content.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            let j: JsonValue = serde_json::from_str(&content)?;
            let j = substitute_env(j);
            let cfg: AppConfig = serde_json::from_value(j)?;
            Ok(cfg)
        } else {
            let t: toml::Value = toml::from_str(&content)?;
            let j = serde_json::to_value(t)?;
            let j = substitute_env(j);
            let cfg: AppConfig = serde_json::from_value(j)?;
            Ok(cfg)
        }
    }
}

/// Load cluster-wide config: tries consul at `cluster/<key>` then `/workspace/configs/cluster.json`.
pub async fn load_cluster_config(
    consul_addr: Option<&str>,
) -> Result<ClusterConfig, Box<dyn Error + Send + Sync>> {
    if let Some(ca) = consul_addr
        && let Ok(Some(raw)) = fetch_consul_raw(ca, CONSUL_CLUSTER_KEY).await
    {
        let trimmed = raw.trim();
        if trimmed.starts_with('{') || trimmed.starts_with('[') {
            let cfg: ClusterConfig = serde_json::from_str(&raw)?;
            validate_cluster_config(&cfg)?;
            return Ok(cfg);
        } else {
            let cfg: ClusterConfig = toml::from_str(&raw)?;
            validate_cluster_config(&cfg)?;
            return Ok(cfg);
        }
    }

    let json_path = "/workspace/configs/cluster.json";
    if let Ok(content) = fs::read_to_string(json_path) {
        let cfg: ClusterConfig = serde_json::from_str(&content)?;
        validate_cluster_config(&cfg)?;
        return Ok(cfg);
    }

    Err("failed to load cluster config from consul or /workspace/configs/cluster.json".into())
}
