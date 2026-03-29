use crate::counters::ServerCounters;
use crate::metrics::ServerPrometheusMetrics;
use crate::session;
use crate::tasks::TaskManager;
use crate::warehouse::Warehouse;
use crate::warehouse::pool::{PoolHealth, WarehousePool};
use crate::workers_pool::WorkerPool;
use chrono::{DateTime, Utc};
use kionas::config::AppConfig;
use kionas::config::WarehousePoolTierConfig;
use kionas::parse_env_vars;
use std::collections::HashMap;
use std::collections::hash_map::Entry as HashEntry;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tonic::transport::Channel;

#[cfg(test)]
#[path = "../tests/warehouse_state_query_counters_tests.rs"]
mod tests;
/*
Shared state structure

Must be shared between all services and master server components
Must be serializable and sendable between threads
*/
#[derive(Debug)]
pub struct SharedState {
    pub counter: Arc<AsyncMutex<u32>>,
    pub query_counters: Arc<ServerCounters>,
    pub prometheus_metrics: Arc<ServerPrometheusMetrics>,
    pub warehouses: Arc<AsyncMutex<HashMap<String, Warehouse>>>,
    pub session_manager: Arc<session::SessionManager>,
    pub worker_pools: Arc<AsyncMutex<HashMap<String, WorkerPool>>>,
    pub task_manager: Arc<TaskManager>,
    pub workers: Arc<AsyncMutex<HashMap<String, WorkerEntry>>>,
    pub pool_tier_templates: Arc<AsyncMutex<HashMap<String, WarehousePoolTierConfig>>>,
    pub warehouse_pools: Arc<AsyncMutex<HashMap<String, WarehousePool>>>,
    pub metastore_channel: Arc<AsyncMutex<Option<Channel>>>,
    pub config: Option<AppConfig>,
}

impl SharedState {
    pub fn new(config: AppConfig) -> Self {
        let worker_pools: HashMap<String, WorkerPool> = HashMap::new();
        SharedState {
            counter: Arc::new(AsyncMutex::new(0)),
            query_counters: ServerCounters::new(),
            prometheus_metrics: ServerPrometheusMetrics::new(),
            warehouses: Arc::new(AsyncMutex::new(HashMap::new())),
            session_manager: Arc::new(session::SessionManager::new()),
            worker_pools: Arc::new(AsyncMutex::new(worker_pools)),
            task_manager: Arc::new(TaskManager::new()),
            workers: Arc::new(AsyncMutex::new(HashMap::new())),
            pool_tier_templates: Arc::new(AsyncMutex::new(HashMap::new())),
            warehouse_pools: Arc::new(AsyncMutex::new(HashMap::new())),
            metastore_channel: Arc::new(AsyncMutex::new(None)),
            config: Some(config),
        }
    }
}

impl Default for SharedState {
    fn default() -> Self {
        let worker_pools: HashMap<String, WorkerPool> = HashMap::new();
        SharedState {
            counter: Arc::new(AsyncMutex::new(0)),
            query_counters: ServerCounters::new(),
            prometheus_metrics: ServerPrometheusMetrics::new(),
            warehouses: Arc::new(AsyncMutex::new(HashMap::new())),
            session_manager: Arc::new(session::SessionManager::new()),
            worker_pools: Arc::new(AsyncMutex::new(worker_pools)),
            task_manager: Arc::new(TaskManager::new()),
            workers: Arc::new(AsyncMutex::new(HashMap::new())),
            pool_tier_templates: Arc::new(AsyncMutex::new(HashMap::new())),
            warehouse_pools: Arc::new(AsyncMutex::new(HashMap::new())),
            metastore_channel: Arc::new(AsyncMutex::new(None)),
            config: None,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WorkerEntry {
    pub key: String,
    pub warehouse: Warehouse,
    pub pool: Option<WorkerPool>,
    pub ca_bytes: Option<Vec<u8>>,
    pub last_heartbeat: DateTime<Utc>,
}

impl WorkerEntry {
    pub fn new(key: String, warehouse: Warehouse, ca_bytes: Option<Vec<u8>>) -> Self {
        WorkerEntry {
            key,
            warehouse,
            pool: None,
            ca_bytes,
            last_heartbeat: Utc::now(),
        }
    }
}

impl SharedState {
    /// What: Return current members for a named warehouse pool.
    ///
    /// Inputs:
    /// - `pool_name`: Active pool name such as compute_xl
    ///
    /// Output:
    /// - `Ok(Vec<String>)` with worker names, otherwise an error string
    ///
    /// Details:
    /// - Returns a snapshot clone of members at read time
    pub async fn get_pool_members(&self, pool_name: &str) -> Result<Vec<String>, String> {
        let pools = self.warehouse_pools.lock().await;
        log::info!(
            "get_pool_members: looking for pool '{}', available pools: {:?}",
            pool_name,
            pools.keys().collect::<Vec<_>>()
        );

        match pools.get(pool_name) {
            Some(pool) => {
                log::info!(
                    "get_pool_members: found pool '{}' with members: {:?}",
                    pool_name,
                    pool.members
                );
                Ok(pool.members.clone())
            }
            None => {
                log::warn!("get_pool_members: pool '{}' not found", pool_name);
                Err(format!("Pool '{}' not found", pool_name))
            }
        }
    }

    /// What: Resolve the default pool name derived from tier templates.
    ///
    /// Inputs:
    /// - None
    ///
    /// Output:
    /// - `Ok(String)` with default pool name
    ///
    /// Details:
    /// - Default pool names are materialized as `compute_<tier>`
    pub async fn get_default_pool_name(&self) -> Result<String, String> {
        let templates = self.pool_tier_templates.lock().await;
        templates
            .values()
            .find(|template| template.default)
            .map(|template| format!("compute_{}", template.tier))
            .ok_or_else(|| "No default pool defined".to_string())
    }

    /// What: List all active warehouse pool names.
    ///
    /// Inputs:
    /// - None
    ///
    /// Output:
    /// - Vector of active pool names
    ///
    /// Details:
    /// - Active pools are created lazily as workers register
    #[allow(dead_code)]
    pub async fn list_pools(&self) -> Vec<String> {
        let pools = self.warehouse_pools.lock().await;
        pools.keys().cloned().collect()
    }

    /// What: Build health status for a named pool.
    ///
    /// Inputs:
    /// - `pool_name`: Active pool name to inspect
    ///
    /// Output:
    /// - `Ok(PoolHealth)` when pool and tier exist
    ///
    /// Details:
    /// - Health compares current members against tier min/max bounds
    #[allow(dead_code)]
    pub async fn get_pool_health(&self, pool_name: &str) -> Result<PoolHealth, String> {
        let pools = self.warehouse_pools.lock().await;
        let pool = pools
            .get(pool_name)
            .ok_or_else(|| format!("Pool '{}' not found", pool_name))?
            .clone();
        drop(pools);

        let templates = self.pool_tier_templates.lock().await;
        let tier = templates
            .get(&pool.tier)
            .ok_or_else(|| format!("Tier '{}' not found", pool.tier))?
            .clone();
        let member_count = pool.members.len();
        let is_healthy = pool.is_healthy(&tier);

        Ok(PoolHealth {
            pool_name: pool.name.clone(),
            tier: pool.tier.clone(),
            current_members: member_count,
            min_members: tier.min_members,
            max_members: tier.max_members,
            is_healthy,
        })
    }

    /// Resolve the worker key (digest) for a given session id.
    /// Uses session affinity first, falls back to fuzzy warehouse name matching.
    /// If warehouse name is a pool name, resolves to first member of that pool.
    pub async fn resolve_worker_key(&self, session_id: &str) -> Option<String> {
        // Get session
        let session_opt = self
            .session_manager
            .get_session(session_id.to_string())
            .await;
        let session = match session_opt {
            Some(s) => s,
            None => {
                log::warn!("resolve_worker_key: session {} not found", session_id);
                return None;
            }
        };
        let worker_uuid = session.get_warehouse_uuid();
        let plain = session.get_warehouse();

        log::info!(
            "resolve_worker_key: session_id={}, warehouse={}, warehouse_uuid={}",
            session_id,
            plain,
            worker_uuid
        );

        // Exact match on UUID
        {
            let warehouses = self.warehouses.lock().await;
            if warehouses.contains_key(&worker_uuid) {
                log::info!(
                    "resolve_worker_key: found exact uuid match for worker_uuid={}",
                    worker_uuid
                );
                return Some(worker_uuid);
            }
        }

        // Fuzzy match by warehouse name or host
        log::info!(
            "resolve_worker_key: trying fuzzy match on warehouse name: {}",
            plain
        );
        let warehouses = self.warehouses.lock().await;
        for (k, w) in warehouses.iter() {
            let wname = w.get_name();
            let whost = w.get_host();
            if wname == plain || wname.ends_with(&plain) || wname.contains(&plain) || whost == plain
            {
                log::info!(
                    "resolve_worker_key: found fuzzy match - warehouse_name={}, worker_key={}",
                    wname,
                    k
                );
                return Some(k.clone());
            }
        }
        log::info!("resolve_worker_key: no fuzzy match found for: {}", plain);
        drop(warehouses);

        // Check if warehouse name is a pool name
        log::info!("resolve_worker_key: checking if {} is a pool name", plain);
        let pools = self.warehouse_pools.lock().await;
        log::info!(
            "resolve_worker_key: available pools: {:?}",
            pools.keys().collect::<Vec<_>>()
        );

        if let Some(pool) = pools.get(&plain).filter(|p| !p.members.is_empty()) {
            log::info!(
                "resolve_worker_key: found pool '{}' with members: {:?}",
                plain,
                pool.members
            );
            // Resolve first pool member to worker key
            let first_member = pool.members[0].clone();
            log::info!(
                "resolve_worker_key: resolving to first pool member: {}",
                first_member
            );
            drop(pools);

            let warehouses = self.warehouses.lock().await;
            log::info!(
                "resolve_worker_key: available warehouses: {:?}",
                warehouses
                    .iter()
                    .map(|(k, w)| (k.clone(), w.get_name()))
                    .collect::<Vec<_>>()
            );

            for (k, w) in warehouses.iter() {
                let wname = w.get_name();
                if wname == first_member
                    || wname.ends_with(&first_member)
                    || wname.contains(&first_member)
                {
                    log::info!(
                        "resolve_worker_key: matched pool member '{}' to worker_key={}",
                        first_member,
                        k
                    );
                    return Some(k.clone());
                }
            }
            log::warn!(
                "resolve_worker_key: pool member '{}' not found in warehouses",
                first_member
            );
        } else {
            log::info!(
                "resolve_worker_key: '{}' is not a pool, or pool is empty",
                plain
            );
        }

        log::warn!(
            "resolve_worker_key: failed to resolve warehouse '{}' for session {}",
            plain,
            session_id
        );
        None
    }

    /// Return an existing pool for `key` or create it from warehouse metadata and config.
    /// Double-checks insertion to avoid races.
    pub async fn get_or_create_pool_for_key(
        &self,
        key: &str,
    ) -> Result<WorkerPool, std::io::Error> {
        // Fast path: check existing pool
        {
            let pools = self.worker_pools.lock().await;
            if let Some(p) = pools.get(key) {
                return Ok(p.clone());
            }
        }

        // Need warehouse metadata
        let warehouse_opt = {
            let warehouses = self.warehouses.lock().await;
            warehouses.get(key).cloned()
        };
        let warehouse = match warehouse_opt {
            Some(w) => w,
            None => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "warehouse metadata not found for key",
                ));
            }
        };

        // Build pool address
        let pool_addr = if warehouse.get_port() == 443 {
            format!("https://{}:{}", warehouse.get_host(), warehouse.get_port())
        } else {
            format!("http://{}:{}", warehouse.get_host(), warehouse.get_port())
        };

        // Read CA from config if present
        let ca_bytes = if let Some(cfg) = &self.config {
            if let Some(iops) = cfg.services.interops.as_ref() {
                if !iops.ca_cert.is_empty() {
                    match std::fs::read(parse_env_vars(&iops.ca_cert).as_str()) {
                        Ok(b) => Some(b),
                        Err(e) => {
                            log::warn!("Failed to read interops ca_cert {}: {}", iops.ca_cert, e);
                            None
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Create new pool
        let new_pool = crate::workers_pool::get_new_pool(pool_addr, warehouse.get_name(), ca_bytes);

        // Insert with double-check
        let mut pools = self.worker_pools.lock().await;
        match pools.entry(key.to_string()) {
            HashEntry::Occupied(o) => Ok(o.get().clone()),
            HashEntry::Vacant(v) => {
                v.insert(new_pool.clone());
                Ok(new_pool)
            }
        }
    }
}

pub(crate) type SharedData = Arc<AsyncMutex<SharedState>>;
