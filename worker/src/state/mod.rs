

use core::hash;
use std::sync::Arc;
use kionas::consul::ClusterInfo;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use crate::storage::StorageProvider;
use std::fmt;


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
            cluster_info
            , storage_provider: None
        }
    }

    pub fn set_storage_provider(&mut self, prov: Arc<dyn StorageProvider + Send + Sync>) {
        self.storage_provider = Some(prov);
    }
}