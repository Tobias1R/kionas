mod services;
mod state;
mod init;

pub mod interops_service {
    tonic::include_proto!("interops_service");
}


use std::error::Error;
use kionas::utils::{resolve_hostname,};
use kionas::config;
use kionas::get_local_hostname;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <worker_id>", args[0]);
        std::process::exit(1);
    }
    let consul_url = env::var("CONSUL_URL").ok();
    let worker_id = get_local_hostname().unwrap_or_else(|| "worker".to_string());
    // Try to load unified AppConfig for this worker
    let app_cfg = match config::load_for_host(consul_url.as_deref(), &worker_id).await {
        Ok(cfg) => {
            println!("Loaded AppConfig for worker {}: consul_host={}", worker_id, cfg.consul_host);
            cfg
        }
        Err(_) => {
            println!("No AppConfig found for worker {}, continuing with defaults or existing env vars", worker_id);
            std::process::exit(1);
        }
    };

    let cluster_info = init::init_worker(&worker_id, app_cfg.clone()).await?;

    println!("Worker {} initialized successfully", worker_id);
    // print a line ----------
    println!("-----------------------------");    
    // debug worker config
    println!("Worker config: {:?}", &app_cfg);
    let interops = app_cfg.services.interops.ok_or("missing services.interops in AppConfig")?;
    println!("Interops config: {:?}", interops);
    println!("Cluster info: {:?}", cluster_info);
    // Build TLS config
    let cert = std::fs::read(&interops.tls_cert)?;
    let key = std::fs::read(&interops.tls_key)?;
    let ca_cert = std::fs::read(&interops.ca_cert)?;
    let identity = tonic::transport::Identity::from_pem(cert, key);
    let ca = tonic::transport::Certificate::from_pem(ca_cert);
    let tls_config = tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca);
    println!("TLS configuration built successfully for worker {}", worker_id);

    let addr = resolve_hostname(&interops.host, interops.port).await?;

    // Shared state
    let worker_info = crate::state::WorkerInformation {
        worker_id: worker_id.to_string(),
        host: interops.host.clone(),
        port: interops.port as u32,
        server_url: interops.host.clone(),
        tls_cert_path: interops.tls_cert.clone(),
        tls_key_path: interops.tls_key.clone(),
        ca_cert_path: interops.ca_cert.clone()
    };
    let shared_data = crate::state::SharedData::new(worker_info, cluster_info);

    // Start interops_server
    
    let svc = services::worker_service_server::WorkerService { shared_data };
    println!("Starting interops_server on {}", addr);
    if interops.port == 443 {
        println!("Interops server will use TLS on port 443");
        tonic::transport::Server::builder()
        .tls_config(tls_config)?
        .add_service(services::worker_service_server::worker_service::worker_service_server::WorkerServiceServer::new(svc))
        .serve(addr)
        .await?;
    } else {
        println!("Interops server will use port {}", interops.port);
        tonic::transport::Server::builder()
        .add_service(services::worker_service_server::worker_service::worker_service_server::WorkerServiceServer::new(svc))
        .serve(addr)
        .await?;
    }
    
    Ok(())
}
