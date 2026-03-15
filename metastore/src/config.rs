use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MetastoreConfig {
    pub grpc_host: String,
    pub grpc_port: u16,
    pub postgres_host: String,
    pub postgres_port: u16,
    pub postgres_db: String,
    pub postgres_user: String,
    pub postgres_password: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
}

impl MetastoreConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let content = std::fs::read_to_string(path).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        let config: MetastoreConfig = toml::from_str(&content).map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
        Ok(config)
    }
}
