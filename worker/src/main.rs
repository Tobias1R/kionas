mod authz;
mod counters;
mod execution;
mod flight;
mod init;
mod interops;
mod metrics;
mod monitoring;
mod services;
mod state;
mod storage;
mod telemetry;
mod transactions;
mod txn;

pub mod interops_service {
    tonic::include_proto!("interops_service");
}

use chrono::Utc;
use clap::Parser;
use kionas::config;
use kionas::get_local_hostname;
use kionas::monitoring::init_redis_pool;
use kionas::utils::resolve_hostname;
use std::env;
use std::error::Error;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Parser)]
#[command(author, version, about = "kionas worker process")]
struct WorkerArgs {
    #[arg(long)]
    worker_id: Option<String>,
    #[arg(index = 1)]
    worker_id_positional: Option<String>,
}

fn resolve_worker_id(args: &WorkerArgs) -> String {
    if let Some(worker_id) = args.worker_id.as_ref()
        && !worker_id.trim().is_empty()
    {
        return worker_id.clone();
    }

    if let Some(worker_id) = args.worker_id_positional.as_ref()
        && !worker_id.trim().is_empty()
    {
        return worker_id.clone();
    }

    get_local_hostname().unwrap_or_else(|| "worker".to_string())
}

// Try to install a default rustls CryptoProvider at startup to avoid runtime
// ambiguity when multiple provider features are compiled into dependencies.
// This is the recommended approach from rustls when both 'ring' and
// 'aws-lc-rs' may appear in the dependency graph.
fn try_install_rustls_crypto_provider() {
    // Construct the crate-default provider depending on which rustls feature
    // was enabled at compile time, and install it as the process default.
    // This avoids the runtime panic about ambiguous provider selection.
    let res = std::panic::catch_unwind(|| {
        // Prefer aws_lc_rs provider explicitly. Worker crate depends on rustls
        // with the aws_lc_rs feature enabled, so this symbol is available
        // and can be installed as the process default before any TLS uses it.
        let provider = rustls::crypto::aws_lc_rs::default_provider();
        let _ = provider.install_default();
    });

    match res {
        Ok(_) => log::debug!("rustls CryptoProvider installed default successfully"),
        Err(_) => log::debug!("rustls CryptoProvider.install_default() panicked or unavailable"),
    }
}

fn resolve_worker_flight_port(default_worker_port: u16) -> u16 {
    std::env::var("WORKER_FLIGHT_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|p| *p > 0)
        .unwrap_or(default_worker_port.saturating_add(1))
}

fn resolve_worker_metrics_port(default_port: u16) -> u16 {
    std::env::var("KIONAS_WORKER_METRICS_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port > 0)
        .unwrap_or(default_port)
}

/// What: Initialize structured tracing for worker logs and spans.
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

/// What: Convert a Redis URL to target DB index 2 used for monitoring status data.
///
/// Inputs:
/// - `url`: Source Redis URL that may or may not include an explicit DB path.
///
/// Output:
/// - `Some(String)` when conversion succeeds.
/// - `None` when URL parsing fails.
///
/// Details:
/// - Preserves scheme, host, port, and credentials.
fn to_status_db_url(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let mut rebuilt = format!("{}://", parsed.scheme());

    if !parsed.username().is_empty() {
        rebuilt.push_str(parsed.username());
        if let Some(password) = parsed.password() {
            rebuilt.push(':');
            rebuilt.push_str(password);
        }
        rebuilt.push('@');
    }

    let host = parsed.host_str()?;
    rebuilt.push_str(host);
    if let Some(port) = parsed.port() {
        rebuilt.push(':');
        rebuilt.push_str(port.to_string().as_str());
    }

    rebuilt.push_str("/2");
    Some(rebuilt)
}

/// What: Resolve Redis URL used by worker monitoring status publishers.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Redis URL guaranteed to target DB index 2.
///
/// Details:
/// - Prefers KIONAS_REDIS_URL, then REDIS_URL, then default local service URL.
fn resolve_monitoring_redis_url() -> String {
    env::var("KIONAS_REDIS_URL")
        .ok()
        .or_else(|| env::var("REDIS_URL").ok())
        .and_then(|value| to_status_db_url(value.as_str()))
        .unwrap_or_else(|| "redis://kionas-redis:6379/2".to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    init_tracing();
    let args = WorkerArgs::parse();
    let consul_url = env::var("CONSUL_URL").ok();
    let worker_id = resolve_worker_id(&args);
    // Install rustls provider before any TLS code runs
    try_install_rustls_crypto_provider();
    // Try to load unified AppConfig for this worker
    let app_cfg = match config::load_for_host(consul_url.as_deref(), &worker_id).await {
        Ok(cfg) => {
            println!(
                "Loaded AppConfig for worker {}: consul_host={}",
                worker_id, cfg.consul_host
            );
            cfg
        }
        Err(_) => {
            println!(
                "No AppConfig found for worker {}, continuing with defaults or existing env vars",
                worker_id
            );
            std::process::exit(1);
        }
    };

    let cluster_info = init::init_worker(&worker_id, app_cfg.clone()).await?;

    println!("Worker {} initialized successfully", worker_id);
    // print a line ----------
    println!("-----------------------------");
    // debug worker config
    println!("Worker config: {:?}", &app_cfg);
    let metrics_cfg = app_cfg.resolved_metrics_config();
    let interops = app_cfg
        .services
        .interops
        .ok_or("missing services.interops in AppConfig")?;
    println!("Interops config: {:?}", interops);
    println!("Cluster info: {:?}", cluster_info);
    // Build TLS config
    let cert = std::fs::read(&interops.tls_cert)?;
    let key = std::fs::read(&interops.tls_key)?;
    let ca_cert = std::fs::read(&interops.ca_cert)?;
    let identity = tonic::transport::Identity::from_pem(cert, key);
    let ca = tonic::transport::Certificate::from_pem(ca_cert);
    let _tls_config = tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca);
    println!(
        "TLS configuration built successfully for worker {}",
        worker_id
    );

    let addr = resolve_hostname(&interops.host, interops.port).await?;

    // Shared state
    let worker_info = crate::state::WorkerInformation {
        worker_id: worker_id.to_string(),
        host: interops.host.clone(),
        port: interops.port as u32,
        // Use cluster master endpoint for interops callbacks (TaskUpdate),
        // not this worker's bind host/port.
        server_url: cluster_info.master.clone(),
        tls_cert_path: interops.tls_cert.clone(),
        tls_key_path: interops.tls_key.clone(),
        ca_cert_path: interops.ca_cert.clone(),
    };
    println!(
        "Configured interops callback target (master): {}",
        worker_info.server_url
    );
    let mut shared_data = crate::state::SharedData::new(worker_info, cluster_info);

    let metrics_port = resolve_worker_metrics_port(metrics_cfg.port);
    if metrics_cfg.enabled {
        let metrics_registry = shared_data.prometheus_metrics.registry.clone();
        tokio::spawn(async move {
            if let Err(error) =
                crate::metrics::serve_metrics_endpoint(metrics_registry, metrics_port).await
            {
                log::warn!("worker metrics endpoint failed: {}", error);
            }
        });
        log::info!("worker metrics endpoint enabled on port {}", metrics_port);
    } else {
        log::info!("worker metrics endpoint disabled by config");
    }

    let worker_start_time = Utc::now();
    let redis_url = resolve_monitoring_redis_url();

    match init_redis_pool(redis_url.as_str()).await {
        Ok(redis_pool) => {
            log::info!("worker monitoring redis pool initialized at {}", redis_url);
            crate::monitoring::spawn_monitoring_task(
                redis_pool,
                worker_id.clone(),
                shared_data.cluster_info.cluster_id.clone(),
                env::var("WORKER_POOL_NAME").ok(),
                worker_start_time,
                shared_data.counters.clone(),
                shared_data.prometheus_metrics.clone(),
            );
        }
        Err(error) => {
            log::warn!(
                "worker monitoring disabled: redis init failed for {}: {}",
                redis_url,
                error
            );
        }
    }

    // Build storage provider from cluster info and attach to shared state (best-effort)
    match crate::storage::build_provider_from_cluster(&shared_data.cluster_info.storage).await {
        Ok(prov) => {
            shared_data.set_storage_provider(prov);
            println!("Attached storage provider to shared state");
        }
        Err(e) => {
            eprintln!(
                "Warning: failed to build storage provider: {}. continuing without it",
                e
            );
        }
    }

    // NOTE: Pool creation is performed lazily inside the transactions maestro.
    // Avoid building the interops pool at startup to prevent duplicate init
    // paths and allow the worker to start even if master is temporarily
    // unreachable. The pool will be created on demand when needed.
    println!("Interops pool will be lazily initialized by transactions::maestro when required");

    // Start interops_server

    let flight_port = resolve_worker_flight_port(interops.port);
    let flight_addr = resolve_hostname(&interops.host, flight_port).await?;
    let flight_shared = shared_data.clone();
    tokio::spawn(async move {
        log::info!("Starting internal Flight server on {}", flight_addr);
        if let Err(e) = crate::flight::server::serve_flight(flight_shared, flight_addr).await {
            log::error!("Internal Flight server stopped with error: {}", e);
        }
    });

    let svc = services::worker_service_server::WorkerService {
        shared_data,
        authorizer: std::sync::Arc::new(crate::authz::WorkerAuthorizer::new()),
    };
    println!("Starting interops_server on {}", addr);
    tonic::transport::Server::builder()
        //.tls_config(tls_config)?
        .add_service(services::worker_service_server::worker_service::worker_service_server::WorkerServiceServer::new(svc))
        .serve(addr)
        .await?;

    Ok(())
}
