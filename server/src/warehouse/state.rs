use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use crate::config::AppConfig;
use crate::warehouse::Warehouse;
use crate::workers_pool::WorkerPool;
use crate::session;
use chrono::{DateTime, Utc};
use std::collections::hash_map::Entry as HashEntry;
use crate::tasks::TaskManager;
use kionas::parse_env_vars;
/*
Shared state structure

Must be shared between all services and master server components
Must be serializable and sendable between threads
*/
#[derive(Debug)]
pub struct SharedState {
    pub counter: Arc<AsyncMutex<u32>>,
    pub warehouses: Arc<AsyncMutex<HashMap<String, Warehouse>>>,
    pub session_manager: Arc<session::SessionManager>,
    pub worker_pools: Arc<AsyncMutex<HashMap<String,WorkerPool>>>,
    pub task_manager: Arc<TaskManager>,
    pub workers: Arc<AsyncMutex<HashMap<String, WorkerEntry>>>,
    pub config: Option<AppConfig>,
}

impl SharedState {
    pub fn new(config: AppConfig) -> Self {
        let worker_pools:HashMap<String,WorkerPool> = HashMap::new();
        SharedState {
            counter: Arc::new(AsyncMutex::new(0)),
            warehouses: Arc::new(AsyncMutex::new(HashMap::new())),
            session_manager: Arc::new(session::SessionManager::new()),
            worker_pools: Arc::new(AsyncMutex::new(worker_pools)),
            task_manager: Arc::new(TaskManager::new()),
            workers: Arc::new(AsyncMutex::new(HashMap::new())),
            config: Some(config),
        }
    }
}

impl Default for SharedState {
    fn default() -> Self {
        let worker_pools:HashMap<String,WorkerPool> = HashMap::new();
        SharedState {
            counter: Arc::new(AsyncMutex::new(0)),
            warehouses: Arc::new(AsyncMutex::new(HashMap::new())),
            session_manager: Arc::new(session::SessionManager::new()),
            worker_pools: Arc::new(AsyncMutex::new(worker_pools)),
            task_manager: Arc::new(TaskManager::new()),
            workers: Arc::new(AsyncMutex::new(HashMap::new())),
            config: None,
        }
    }
}

#[derive(Debug, Clone)]
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
    /// Resolve the worker key (digest) for a given session id.
    /// Uses session affinity first, falls back to fuzzy warehouse name matching.
    pub async fn resolve_worker_key(&self, session_id: &str) -> Option<String> {
        // Get session
        let session_opt = self.session_manager.get_session(session_id.to_string()).await;
        let session = match session_opt {
            Some(s) => s,
            None => return None,
        };
        let worker_uuid = session.get_warehouse_uuid();

        // Exact match
        {
            let warehouses = self.warehouses.lock().await;
            if warehouses.contains_key(&worker_uuid) {
                return Some(worker_uuid);
            }
        }

        // Fuzzy match by warehouse name or host
        let plain = session.get_warehouse();
        let warehouses = self.warehouses.lock().await;
        for (k, w) in warehouses.iter() {
            let wname = w.get_name();
            let whost = w.get_host();
            if wname == plain || wname.ends_with(&plain) || wname.contains(&plain) || whost == plain {
                return Some(k.clone());
            }
        }
        None
    }

    /// Return an existing pool for `key` or create it from warehouse metadata and config.
    /// Double-checks insertion to avoid races.
    pub async fn get_or_create_pool_for_key(&self, key: &str) -> Result<WorkerPool, std::io::Error> {
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
            None => return Err(std::io::Error::new(std::io::ErrorKind::NotFound, "warehouse metadata not found for key")),
        };

        // Build pool address
        let pool_addr = if warehouse.get_port() == 443 {
            format!("https://{}:{}", warehouse.get_host(), warehouse.get_port())
        } else {
            format!("http://{}:{}", warehouse.get_host(), warehouse.get_port())
        };

        // Read CA from config if present
        let ca_bytes = if let Some(cfg) = &self.config {
            if !cfg.interops.ca_cert.is_empty() {
                match std::fs::read(parse_env_vars(&cfg.interops.ca_cert).as_str()) {
                    Ok(b) => Some(b),
                    Err(e) => {
                        log::warn!("Failed to read interops ca_cert {}: {}", cfg.interops.ca_cert, e);
                        None
                    }
                }
            } else { None }
        } else { None };

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
