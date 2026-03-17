use crate::interops::manager::InteropsManager;
use crate::storage::StorageProvider;
use deadpool::managed::Pool;
use kionas::consul::ClusterInfo;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

#[derive(Deserialize, Serialize, Clone, Default, Debug)]
pub struct WorkerInformation {
    pub worker_id: String,
    pub host: String,
    pub port: u32,
    pub server_url: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub ca_cert_path: String,
}

#[derive(Deserialize, Serialize, Clone, Default)]
pub struct SharedData {
    pub worker_info: WorkerInformation,
    pub cluster_info: ClusterInfo,
    #[serde(skip)]
    pub storage_provider: Option<Arc<dyn StorageProvider + Send + Sync>>,
    #[serde(skip)]
    pub master_pool: Arc<tokio::sync::Mutex<Option<Arc<Pool<InteropsManager>>>>>,
}

impl fmt::Debug for SharedData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedData")
            .field("worker_info", &self.worker_info)
            .field("cluster_info", &self.cluster_info)
            .finish()
    }
}

impl SharedData {
    pub fn new(worker_info: WorkerInformation, cluster_info: ClusterInfo) -> Self {
        SharedData {
            worker_info,
            cluster_info,
            storage_provider: None,
            master_pool: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn set_storage_provider(&mut self, prov: Arc<dyn StorageProvider + Send + Sync>) {
        self.storage_provider = Some(prov);
    }

    pub fn set_master_pool(&mut self, pool: Arc<Pool<InteropsManager>>) {
        // best-effort synchronous set for initialization paths
        let mutex = self.master_pool.clone();
        // spawn a task to set it since we can't block here
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            let mut guard = mutex.lock().await;
            *guard = Some(pool_clone);
        });
    }
}
