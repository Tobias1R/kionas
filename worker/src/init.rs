use crate::state;
use crate::interops_service::interops_service_client::InteropsServiceClient;

use tonic::{transport::Identity, transport::ClientTlsConfig, Request};
use std::error::Error;
use kionas::{get_local_ip, get_digest, get_local_hostname};
use tonic::transport::Channel;

use crate::interops_service::{RegisterWorkerRequest, RegisterWorkerResponse};

use std::env;

use kionas::consul::{download_cluster_info, download_worker_info};


pub async fn init_worker(worker_id: &str) -> Result<(kionas::consul::WorkerInfo, kionas::consul::ClusterInfo), Box<dyn Error + Send + Sync>> {
    let consul_url = std::env::var("CONSUL_URL").unwrap_or_else(|_| "http://kionas-consul:8500".to_string());
    let cluster_info = download_cluster_info(&consul_url).await?;
    println!("Cluster info: {:?}", cluster_info);
    let log_level = cluster_info.logging["level"].clone().to_string();
    let log_output = cluster_info.logging["output"].clone().to_string();
    let log_format = cluster_info.logging["format"].clone().to_string();
    // Initialize logging
    if let Err(e) = kionas::logging::init_logging(&log_level, &log_output, &log_format) {
        eprintln!("Failed to initialize logging: {}", e);
    } else {
        println!("Logging initialized with level: {}, output: {}, format: {}", log_level, log_output, log_format);
    }

    let worker_config = download_worker_info(&consul_url, worker_id).await?;
    println!("Worker config: {:?}", worker_config.clone());

    let tls_cert_path = worker_config.tls_cert_path.clone();
    let tls_key_path = worker_config.tls_key_path.clone();
    let ca_cert_path = worker_config.ca_cert_path.clone();
    let worker_port = worker_config.interops_port.clone();
    let master_addr = worker_config.server_url.clone();

    let cert = std::fs::read(tls_cert_path.clone())?;
    let key = std::fs::read(tls_key_path.clone())?;
    let ca_cert = std::fs::read(ca_cert_path.clone())?;
    let identity = Identity::from_pem(cert, key);
    let ca = tonic::transport::Certificate::from_pem(ca_cert);
    let master_tls_config = ClientTlsConfig::new().identity(identity).ca_certificate(ca);

    let local_ip = get_local_ip().unwrap();
    let local_hostname = get_local_hostname().unwrap_or_else(|| "unknown".to_string());
    println!("Worker local IP: {}, hostname: {}", local_ip, local_hostname);
    let worker_id_hash = get_digest(&local_hostname);

    let worker_info = state::WorkerInformation {
        worker_id: worker_id_hash.clone(),
        host: local_hostname.clone(),
        port: worker_port as u32,
        server_url: master_addr.clone().to_string(),
        tls_cert_path: tls_cert_path,
        tls_key_path: tls_key_path,
        ca_cert_path: ca_cert_path
    };
    let shared_data = state::SharedData::new(worker_info.clone(), cluster_info.clone());

    let channel = Channel::from_shared(master_addr.to_string())
        .unwrap()
        .tls_config(master_tls_config)?
        .connect()
        .await?;
    println!("Channel opened to master server: {}", master_addr);
    
    // Create a client for the InteropsService
    let mut client = InteropsServiceClient::new(channel);

    // Register worker with server
    let request = tonic::Request::new(RegisterWorkerRequest {
        name: local_hostname.clone(),
        host: local_hostname.clone(),
        port: worker_port as u32,
        warehouse_type: "kionas".to_string(),
    });
    println!("Registering worker with master server...");
    let response = client.register_worker(request).await?;
    println!("Worker registration response: {:?}", response.into_inner());

    Ok((worker_config, cluster_info))
}
