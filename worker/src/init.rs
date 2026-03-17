use crate::interops_service::interops_service_client::InteropsServiceClient;
use crate::state;

use kionas::{get_digest, get_local_hostname, get_local_ip};
use std::error::Error;
use tonic::transport::Channel;
use tonic::transport::{ClientTlsConfig, Identity};

use crate::interops_service::RegisterWorkerRequest;

use kionas::config::AppConfig;
use kionas::consul::download_cluster_info;

pub async fn init_worker(
    worker_id: &str,
    app_cfg: AppConfig,
) -> Result<kionas::consul::ClusterInfo, Box<dyn Error + Send + Sync>> {
    let consul_url =
        std::env::var("CONSUL_URL").unwrap_or_else(|_| "http://kionas-consul:8500".to_string());
    let cluster_info = download_cluster_info(&consul_url).await?;

    println!("Cluster info: {:?}", cluster_info);

    // Determine worker config: prefer provided AppConfig, fallback to consul-stored worker info
    let cfg = app_cfg;
    // Construct a WorkerInfo from AppConfig.interops and consul info
    let interops = cfg
        .services
        .interops
        .ok_or("missing services.interops in AppConfig")?;
    let mut wi = kionas::consul::WorkerInfo::default();
    wi.worker_id = worker_id.to_string();
    wi.consul_url = cfg.consul_host.clone();
    wi.tls_cert_path = interops.tls_cert.clone();
    wi.tls_key_path = interops.tls_key.clone();
    wi.ca_cert_path = interops.ca_cert.clone();
    wi.interops_port = interops.port;
    wi.interops_host = interops.host.clone();
    // derive server_url (master) from cluster_info.master if available
    //let host = cluster_info.master.as_str();

    let log_level = cfg.logging.level.clone();
    let log_output = cfg.logging.output.clone();
    let log_format = cfg.logging.format.clone();
    // Initialize logging
    if let Err(e) = kionas::logging::init_logging(&log_level, &log_output, &log_format) {
        eprintln!("Failed to initialize logging: {}", e);
    } else {
        println!(
            "Logging initialized with level: {}, output: {}, format: {}",
            log_level, log_output, log_format
        );
    }

    //let worker_config = cfg.clone();

    //println!("Worker config: {:?}", worker_config.clone());

    let tls_cert_path = interops.tls_cert.clone();
    let tls_key_path = interops.tls_key.clone();
    let ca_cert_path = interops.ca_cert.clone();
    let worker_port = interops.port;
    let master_addr = cluster_info.master.clone();

    // Build TLS client configuration to securely connect to master server.
    let cert = std::fs::read(tls_cert_path.clone())?;
    let key = std::fs::read(tls_key_path.clone())?;
    let ca_cert = std::fs::read(ca_cert_path.clone())?;
    let identity = Identity::from_pem(cert, key);
    let ca = tonic::transport::Certificate::from_pem(ca_cert);
    let master_tls_config = ClientTlsConfig::new().identity(identity).ca_certificate(ca);

    let local_ip = get_local_ip().unwrap();
    let local_hostname = get_local_hostname().unwrap_or_else(|| "unknown".to_string());
    println!(
        "Worker local IP: {}, hostname: {}",
        local_ip, local_hostname
    );
    let worker_id_hash = get_digest(&local_hostname);

    let worker_info = state::WorkerInformation {
        worker_id: worker_id_hash.clone(),
        host: local_hostname.clone(),
        port: worker_port as u32,
        server_url: master_addr.clone().to_string(),
        tls_cert_path: tls_cert_path,
        tls_key_path: tls_key_path,
        ca_cert_path: ca_cert_path,
    };
    let _shared_data = state::SharedData::new(worker_info.clone(), cluster_info.clone());

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

    Ok(cluster_info)
}
