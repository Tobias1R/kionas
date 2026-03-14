use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use crate::config::AppConfig;
use crate::warehouse::Warehouse;
use crate::workers_pool::WorkerPool;
use crate::session;
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
            config: None,
        }
    }
}

pub(crate) type SharedData = Arc<AsyncMutex<SharedState>>;
