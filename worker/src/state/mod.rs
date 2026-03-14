

use core::hash;
use std::sync::Arc;
use kionas::consul::ClusterInfo;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;


#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct WorkerInformation {
    pub worker_id: String,
    pub host: String,
    pub port: u32,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct SharedData {
    pub worker_info: WorkerInformation,
    pub cluster_info: ClusterInfo
}

impl SharedData {
    pub fn new(worker_info: WorkerInformation, cluster_info: ClusterInfo) -> Self {
        SharedData {
            worker_info,
            cluster_info
        }
    }
}