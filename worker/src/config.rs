use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct WorkerConfig {
    pub worker_id: String,
    pub server_url: String,
    pub consul_url: String,
    pub tls_cert_path: String,
    pub tls_key_path: String,
    pub ca_cert_path: String,
    pub interops_port: u16,
    pub interops_host: String,
}

impl WorkerConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        println!("Loaded config from {}: {}", path, content);
        let config: WorkerConfig = toml::from_str(&content)?;
        Ok(config)
    }
}
