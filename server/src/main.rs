/*
 * Main entry point for the Kionas Warehouse Server
 * Initializes configuration, logging, TLS, and starts gRPC servers
 */
mod auth;
mod auth_setup;
mod consul;
mod core;
mod handlers;
mod server;
mod services;
mod session;
mod statement_handler;
mod tasks;
mod tls;
mod transactions;
mod warehouse;
mod workers;
mod workers_pool;

use kionas::config as kconfig;
use kionas::config::AppConfig;
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
