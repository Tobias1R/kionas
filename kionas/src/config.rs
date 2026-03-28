use crate::parse_env_vars;
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::error::Error;
use std::fs;

use crate::constants::{CONSUL_CLUSTER_KEY, CONSUL_NODE_CONFIG_PREFIX};

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
    pub nodes: Vec<String>,
    pub master: String,
    pub storage: StorageConfig,
    #[serde(default, alias = "warehouse_pools_types")]
    pub warehouse_pools_tiers: Vec<WarehousePoolTierConfig>,
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
            return Ok(cfg);
        } else {
            let cfg: ClusterConfig = toml::from_str(&raw)?;
            return Ok(cfg);
        }
    }

    let json_path = "/workspace/configs/cluster.json";
    if let Ok(content) = fs::read_to_string(json_path) {
        let cfg: ClusterConfig = serde_json::from_str(&content)?;
        return Ok(cfg);
    }

    Err("failed to load cluster config from consul or /workspace/configs/cluster.json".into())
}
