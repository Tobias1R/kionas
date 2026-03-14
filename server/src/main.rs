

/*
 * Main entry point for the Kionas Warehouse Server
 * Initializes configuration, logging, TLS, and starts gRPC servers
 */
mod tls;
mod server;
mod consul;
mod config;
mod auth;
mod handlers;
mod services;
mod workers_pool;
mod warehouse;
mod session;
mod auth_setup;
mod statement_handler;

use crate::config::AppConfig;

 #[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = AppConfig::from_args().await;
    server::run(config).await
}
