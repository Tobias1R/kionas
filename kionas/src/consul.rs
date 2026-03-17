use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::constants::{CONSUL_CLUSTER_KEY, CONSUL_NODE_CONFIG_PREFIX};


#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct ClusterInfo {
    pub nodes: Vec<String>,
    pub master: String,
    pub storage: serde_json::Value,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct WorkerInfo {
    pub worker_id: String,
    pub server_url: String,
    pub consul_url: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub ca_cert_path: String,
    pub interops_port: u16,
    pub interops_host: String,
}

pub async fn download_cluster_info(consul_url: &str) -> Result<ClusterInfo, Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();
    let url = format!("{}/v1/kv/{}?raw", consul_url, CONSUL_CLUSTER_KEY);
    println!("[DEBUG] Requesting cluster info from: {}", url);
    let resp = client.get(&url).send().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?.text().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    println!("[DEBUG] Raw response: {}", resp);
    let info: ClusterInfo = match serde_json::from_str(&resp) {
        Ok(i) => {
            println!("[DEBUG] Parsed ClusterInfo: {:?}", i);
            i
        },
        Err(e) => {
            println!("[ERROR] Failed to parse ClusterInfo: {}", e);
            return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
        }
    };
    Ok(info)
}

// Download worker information. Worker configs are populate inside the key configs/<worker_id>
pub async fn download_worker_info(consul_url: &str, worker_id: &str) -> Result<WorkerInfo, Box<dyn std::error::Error + Send + Sync>> {
    let client = Client::new();
    let url = format!("{}/v1/kv/{}{}?raw", consul_url, CONSUL_NODE_CONFIG_PREFIX, worker_id);
    println!("[DEBUG] Requesting worker info from: {}", url);
    let resp = client.get(&url).send().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?.text().await.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    println!("[DEBUG] Raw response: {}", resp);
    let info: WorkerInfo = match serde_json::from_str(&resp) {
        Ok(i) => {
            println!("[DEBUG] Parsed Worker WorkerInfo: {:?}", i);
            i
        },
        Err(e) => {
            println!("[ERROR] Failed to parse Worker WorkerInfo: {}", e);
            return Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>);
        }
    };
    Ok(info)
}
