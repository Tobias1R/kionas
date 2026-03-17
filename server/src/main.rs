

/*
 * Main entry point for the Kionas Warehouse Server
 * Initializes configuration, logging, TLS, and starts gRPC servers
 */
mod tls;
mod server;
mod consul;
mod auth;
mod handlers;
mod services;
mod workers_pool;
mod warehouse;
mod session;
mod auth_setup;
mod statement_handler;
mod tasks;
mod workers;
mod core;
mod transactions;

use kionas::config::AppConfig;
use kionas::config as kconfig;
use kionas::get_local_hostname;
use std::env;

 #[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Try to load host-specific config from Consul or /workspace/configs
    let consul_url = env::var("CONSUL_URL").ok();
    let hostname = get_local_hostname().unwrap_or_else(|| "server".to_string());

    // Load the portable kionas config (consul/local/cli) and pass it through.
    let config: AppConfig = kconfig::load_for_host(consul_url.as_deref(), &hostname).await?;

    server::run(config).await
}
