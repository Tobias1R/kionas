/*
 * Main entry point for the Kionas Warehouse Server
 * Initializes configuration, logging, TLS, and starts gRPC servers
 */
mod auth;
mod auth_setup;
mod consul;
//mod core;
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

fn try_install_rustls_crypto_provider() {
    let res = std::panic::catch_unwind(|| {
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let _ = provider.install_default();
    });

    match res {
        Ok(_) => log::debug!("rustls CryptoProvider installed default successfully"),
        Err(_) => log::debug!("rustls CryptoProvider.install_default() panicked or unavailable"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    try_install_rustls_crypto_provider();

    // Try to load host-specific config from Consul or /workspace/configs
    let consul_url = env::var("CONSUL_URL").ok();
    let hostname = get_local_hostname().unwrap_or_else(|| "server".to_string());

    // Load the portable kionas config (consul/local/cli) and pass it through.
    let config: AppConfig = kconfig::load_for_host(consul_url.as_deref(), &hostname).await?;

    server::run(config).await
}
