mod config;
mod services;

use crate::config::MetastoreConfig;
use crate::services::metastore_service::{MetastoreService, metastore_service};
use crate::services::provider::postgres::PostgresProvider;
use kionas::utils::resolve_hostname;
use tonic::transport::{Identity, ServerTlsConfig, Server};
use std::error::Error;
use std::sync::Arc;

use kionas::{parse_env_vars, };

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    // Initialize logging
    env_logger::init();

    // Load config
    let metastore_config_path: String = "${KIONAS_HOME}/configs/metastore.toml".to_string();
    let config = MetastoreConfig::from_file(parse_env_vars(metastore_config_path.as_str()).as_str())?;
    let cert = std::fs::read(parse_env_vars(&config.tls_cert_path))?;
    let key = std::fs::read(parse_env_vars(&config.tls_key_path))?;
    let identity = Identity::from_pem(cert, key);
    // TLS setup
    let tls_config =ServerTlsConfig::new().identity(identity);

    // Initialize Postgres provider
    let pg_provider = Arc::new(PostgresProvider::new(&config)?);

    // Service implementation (pass provider to service)
    let svc = MetastoreService::new(pg_provider.clone());

    let addr = resolve_hostname(config.grpc_host.as_str(), config.grpc_port).await?;

    // Start gRPC server
    Server::builder()
        .tls_config(tls_config)?
        .add_service(metastore_service::metastore_service_server::MetastoreServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
