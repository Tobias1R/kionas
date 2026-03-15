mod config;
mod services;
mod state;
mod init;

pub mod interops_service {
    tonic::include_proto!("interops_service");
}


use std::error::Error;
use log;
use kionas::utils::resolve_hostname;


#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <worker_id>", args[0]);
        std::process::exit(1);
    }
    let worker_id = &args[1];
    let (worker_config, cluster_info) = init::init_worker(worker_id).await?;

    println!("Worker {} initialized successfully", worker_id);
    // print a line ----------
    println!("-----------------------------");    
    // debug worker config
    println!("Worker config: {:?}", worker_config);
    // Build TLS config
    let cert = std::fs::read(&worker_config.tls_cert_path)?;
    let key = std::fs::read(&worker_config.tls_key_path)?;
    let ca_cert = std::fs::read(&worker_config.ca_cert_path)?;
    let identity = tonic::transport::Identity::from_pem(cert, key);
    let ca = tonic::transport::Certificate::from_pem(ca_cert);
    let tls_config = tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca);
    println!("TLS configuration built successfully for worker {}", worker_id);

    // Shared state
    let worker_info = crate::state::WorkerInformation {
        worker_id: worker_id.to_string(),
        host: worker_config.interops_host.clone(),
        port: worker_config.interops_port as u32,
    };
    let shared_data = crate::state::SharedData::new(worker_info, cluster_info);

    // Start interops_server
    let addr = resolve_hostname(&worker_config.interops_host, worker_config.interops_port).await?;
    let svc = services::worker_service_server::WorkerService { shared_data };
    println!("Starting interops_server on {}", addr);
    if worker_config.interops_port == 443 {
        println!("Interops server will use TLS on port 443");
        tonic::transport::Server::builder()
        .tls_config(tls_config)?
        .add_service(services::worker_service_server::worker_service::worker_service_server::WorkerServiceServer::new(svc))
        .serve(addr)
        .await?;
    } else {
        println!("Interops server will use port {}", worker_config.interops_port);
        tonic::transport::Server::builder()
        .add_service(services::worker_service_server::worker_service::worker_service_server::WorkerServiceServer::new(svc))
        .serve(addr)
        .await?;
    }
    
    Ok(())
}
