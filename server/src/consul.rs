use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use kionas::config::AppConfig;

#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub listen_address: String,
    pub listen_port: u16,
}

pub struct ConsulClient {
    base_url: String,
    client: Client,
}

impl ConsulClient {
    pub fn new(consul_host: &str) -> Self {
        Self {
            base_url: format!("http://{}:8500/v1/kv", consul_host),
            client: Client::new(),
        }
    }

    pub async fn put_config(&self, key: &str, config: &AppConfig) -> Result<(), reqwest::Error> {
        let url = format!("{}/{}", self.base_url, key);
        let body = serde_json::to_vec(config).unwrap();
        self.client.put(&url).body(body).send().await?;
        Ok(())
    }

    pub async fn get_config(&self, table: &str, key: &str) -> Result<Option<AppConfig>, reqwest::Error> {
        let url = format!("{}/{}/{}", self.base_url, table, key);
        let resp = self.client.get(&url).send().await?;
        if resp.status().is_success() {
            let config = resp.json::<AppConfig>().await?;
            Ok(Some(config))
        } else {
            Ok(None)
        }
    }
}

impl ClusterConfig {
    pub fn from_args(listen_address: String, listen_port: u16) -> Self {
        Self {
            listen_address,
            listen_port,
        }
    }
}
