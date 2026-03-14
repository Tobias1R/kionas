use serde::{Deserialize, Serialize};
use std::fs;

use clap::Parser;

use kionas::utils::{resolve_hostname};


#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AppConfig {
    pub server: ServerConfig,
    pub logging: LoggingConfig,
    pub security: SecurityConfig,
    pub storage: StorageConfig,
    pub warehouse: WarehouseConfig,
    pub interops: InteropsConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct InteropsConfig {
    pub host: String,
    pub port: u16,
    pub tls_cert: String,
    pub tls_key: String,
    pub ca_cert: String,
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WarehouseConfig {
    pub host: String,
    pub port: u16,
    pub tls_cert: String,
    pub tls_key: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen_address: String,
    pub listen_port: u16,
    pub consul_host: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
    pub output: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SecurityConfig {
    pub token: String,
    pub secret: String,
    pub data_path: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StorageConfig {
    pub storage_type: String,
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key: String,
    pub secret_key: String,
}


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value = "configs/server.toml")]
    pub config: String,
}

impl AppConfig {
    pub async fn from_toml(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let mut config: AppConfig = toml::from_str(&content)?;
        if !config.server.listen_address.parse::<std::net::IpAddr>().is_ok() {
            let resolved = resolve_hostname(&config.server.listen_address, config.server.listen_port).await;
            match resolved {
                Ok(addr) => {
                    println!("Resolved {} to {}", config.server.listen_address, addr);
                    config.server.listen_address = addr.ip().to_string();
                }
                Err(e) => {
                    eprintln!("Failed to resolve hostname {}: {}", config.server.listen_address, e);
                    return Err(e);
                }
            }
        }
        Ok(config)
    }

    pub async fn from_args() -> Self {
        let args = Args::parse();
        AppConfig::from_toml(&args.config)
            .await
            .unwrap_or_else(|e| panic!("Failed to load configuration from {}: {}", &args.config, e))
    }
}
