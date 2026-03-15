use crate::state::SharedData;

pub mod worker_service {
    tonic::include_proto!("worker_service");
}

#[derive(Default)]
pub struct WorkerService {
    pub shared_data: SharedData,
}

#[tonic::async_trait]
impl worker_service::worker_service_server::WorkerService for WorkerService {
    async fn register_worker(
        &self,
        request: tonic::Request<worker_service::RegisterWorkerRequest>,
    ) -> Result<tonic::Response<worker_service::RegisterWorkerResponse>, tonic::Status> {
        let req = request.into_inner();
        // TODO: Register worker logic
        let resp = worker_service::RegisterWorkerResponse {
            status: "ok".to_string(),
            uuid: req.worker_id.clone(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<worker_service::HeartbeatRequest>,
    ) -> Result<tonic::Response<worker_service::HeartbeatResponse>, tonic::Status> {
        let req = request.into_inner();
        // TODO: Heartbeat logic
        let resp = worker_service::HeartbeatResponse {
            status: "alive".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn execute_task(
        &self,
        request: tonic::Request<worker_service::TaskRequest>,
    ) -> Result<tonic::Response<worker_service::TaskResponse>, tonic::Status> {
        let req = request.into_inner();
        // TODO: Task execution logic
        // print something
        log::info!("Received task: {}", req.session_id);

        // Spawn a background task to simulate work and report completion to the master
        let task_id = req.tasks.get(0).map(|t| t.task_id.clone()).unwrap_or_else(|| "".to_string());
        let result_location = "arrow-flight-endpoint".to_string();
        let shared = self.shared_data.clone();
        tokio::spawn(async move {
            // simulate work
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            
            // Read master address and TLS settings from shared.worker_info
            let interops = &shared.worker_info;
            let host = interops.server_url.clone();
            
            let ca_path_opt = Some(interops.ca_cert_path.clone());
            // construct master URI
            
            let master = host.to_string(); // assume host includes port, e.g. "master:50051"

            // create channel and client; support TLS when master uses https.
            match tonic::transport::Channel::from_shared(master.clone()) {
                Ok(base_ep) => {
                    // build a configured endpoint; if TLS config fails, fall back to base_ep
                    let configured_endpoint = if master.starts_with("https") {
                        if let Some(ca_path) = ca_path_opt.as_ref() {
                            if let Ok(ca_bytes) = std::fs::read(ca_path) {
                                let ca_cert = tonic::transport::Certificate::from_pem(ca_bytes);
                                let mut tls = tonic::transport::ClientTlsConfig::new().ca_certificate(ca_cert);
                                if !interops.tls_cert_path.is_empty() && !interops.tls_key_path.is_empty() {
                                    match (std::fs::read(kionas::parse_env_vars(&interops.tls_cert_path)), std::fs::read(kionas::parse_env_vars(&interops.tls_key_path))) {
                                        (Ok(cert), Ok(key)) => {
                                            let id = tonic::transport::Identity::from_pem(cert, key);
                                            tls = tls.identity(id);
                                        }
                                        _ => log::warn!("Failed to read interops tls_cert/tls_key, proceeding without client identity"),
                                    }
                                }
                                match base_ep.clone().tls_config(tls) {
                                    Ok(e) => e,
                                    Err(err) => {
                                        log::error!("Failed to apply tls_config for master {}: {}", master, err);
                                        base_ep.clone()
                                    }
                                }
                            } else {
                                log::error!("Failed to read CA path {}", ca_path);
                                base_ep.clone()
                            }
                        } else {
                            log::error!("No CA path configured in shared.cluster_info.interops; cannot validate master TLS for {}", master);
                            base_ep.clone()
                        }
                    } else {
                        base_ep.clone()
                    };

                    match configured_endpoint.connect().await {
                        Ok(chan) => {
                            let mut client = crate::interops_service::interops_service_client::InteropsServiceClient::new(chan);
                            let update = crate::interops_service::TaskUpdateRequest {
                                task_id: task_id.clone(),
                                status: "succeeded".to_string(),
                                result_location: result_location.clone(),
                                error: String::new(),
                            };
                            match client.task_update(tonic::Request::new(update)).await {
                                Ok(resp) => {
                                    log::info!("Sent TaskUpdate for {} -> {:?}", task_id, resp.into_inner());
                                }
                                Err(e) => {
                                    log::error!("Failed to send TaskUpdate for {}: {}", task_id, e);
                                }
                            }
                        }
                        Err(e) => log::error!("Failed to connect to master {}: {}", master, e),
                    }
                }
                Err(e) => log::error!("Invalid master address {}: {}", master, e),
            }
        });

        let resp = worker_service::TaskResponse {
            status: "ok".to_string(),
            error: String::new(),
            result_location: "arrow-flight-endpoint".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn get_flight_info(
        &self,
        request: tonic::Request<worker_service::FlightInfoRequest>,
    ) -> Result<tonic::Response<worker_service::FlightInfoResponse>, tonic::Status> {
        let req = request.into_inner();
        // TODO: Arrow Flight info logic
        let resp = worker_service::FlightInfoResponse {
            endpoint: "arrow-flight-endpoint".to_string(),
            schema: "arrow-schema".to_string(),
            ticket: req.task_id.clone(),
        };
        Ok(tonic::Response::new(resp))
    }
}