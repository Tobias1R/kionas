/*
 * Main entry point for the Kionas Warehouse Server
 * Initializes configuration, logging, TLS, and starts gRPC servers
 */
mod auth;
mod auth_setup;
mod consul;
mod counters;
mod planner;
mod providers;
//mod core;
mod handlers;
mod janitor;
mod parser;
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
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// What: Initialize structured tracing for server logs and spans.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Installs global tracing subscriber and log facade bridge.
///
/// Details:
/// - Uses `RUST_LOG` when present, otherwise defaults to `info`.
/// - Emits JSON logs and span close events to include timing metadata.
fn init_tracing() {
    let _ = tracing_log::LogTracer::init();

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::registry()
        .with(env_filter)
        .with(
            tracing_subscriber::fmt::layer()
                .json()
                .with_span_events(FmtSpan::CLOSE),
        )
        .try_init();
}

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
    init_tracing();
    try_install_rustls_crypto_provider();

    // Try to load host-specific config from Consul or /workspace/configs
    let consul_url = env::var("CONSUL_URL").ok();
    let hostname = get_local_hostname().unwrap_or_else(|| "server".to_string());

    // Load the portable kionas config (consul/local/cli) and pass it through.
    let config: AppConfig = kconfig::load_for_host(consul_url.as_deref(), &hostname).await?;

    server::run(config).await
}
