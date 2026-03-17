
mod services;

use kionas::config;
use serde_json::Value as JsonValue;
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

    // Load a unified AppConfig (Consul -> local) and use its sections directly
    let consul_url = std::env::var("CONSUL_URL").ok();
    let hostname = kionas::get_local_hostname().unwrap_or_else(|| "metastore".to_string());

    let app_cfg = kionas::config::load_for_host(consul_url.as_deref(), &hostname).await?;

    // Map AppConfig -> use sections directly
    let interops = app_cfg.services.interops.ok_or("missing services.interops in AppConfig")?;
    let pg = app_cfg.services.postgres.ok_or("missing services.postgres in AppConfig")?;

    // TLS for metastore gRPC server comes from services.interops
    let cert = std::fs::read(kionas::parse_env_vars(&interops.tls_cert))?;
    let key = std::fs::read(kionas::parse_env_vars(&interops.tls_key))?;
    let identity = Identity::from_pem(cert, key);
    // TLS setup
    let tls_config =ServerTlsConfig::new().identity(identity);

    // Initialize Postgres provider using shared PostgresServiceConfig
    let pg_provider = Arc::new(PostgresProvider::new(&pg)?);

    // Service implementation (pass provider to service)
    let svc = MetastoreService::new(pg_provider.clone());

    let addr = resolve_hostname(interops.host.as_str(), interops.port).await?;

    // Start gRPC server
    Server::builder()
        .tls_config(tls_config)?
        .add_service(metastore_service::metastore_service_server::MetastoreServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
