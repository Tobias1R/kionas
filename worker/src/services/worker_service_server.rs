use crate::state::SharedData;
use tokio::fs as tokio_fs;
use serde_json::json;

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

    async fn prepare(
        &self,
        request: tonic::Request<worker_service::PrepareRequest>,
    ) -> Result<tonic::Response<worker_service::PrepareResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("Prepare called for tx={} staging_prefix={}", req.tx_id, req.staging_prefix);

        // When a storage provider is configured, store staged tasks there
        // under `staging/{staging_prefix}/{tx_id}/...`. Otherwise fall back
        // to using the local filesystem at `worker_storage/staging/...`.
        if let Some(provider) = self.shared_data.storage_provider.clone() {
            let base_key_prefix = format!("staging/{}/{}/", req.staging_prefix, req.tx_id);
            // Put each task descriptor as an object
            for t in req.tasks.iter() {
                let key = format!("{}{}.json", base_key_prefix, t.task_id);
                let payload = json!({
                    "task_id": t.task_id.clone(),
                    "operation": t.operation.clone(),
                    "input": t.input.clone(),
                    "output": t.output.clone(),
                    "params": t.params.clone(),
                });
                let bytes = serde_json::to_vec_pretty(&payload).unwrap_or_else(|_| payload.to_string().into_bytes());
                if let Err(e) = provider.put_object(&key, bytes).await {
                    log::error!("failed to put staged object {}: {}", key, e);
                    return Err(tonic::Status::internal(format!("failed to put staged object: {}", e)));
                }
            }

            // Write manifest object into the staging folder
            let manifest = json!({
                "tx_id": req.tx_id.clone(),
                "staging_prefix": req.staging_prefix.clone(),
                "tasks": req.tasks.iter().map(|t| t.task_id.clone()).collect::<Vec<_>>(),
                "created_at": chrono::Utc::now().to_rfc3339(),
            });
            let manifest_key = format!("{}{}/manifest_{}.json", "staging/", req.staging_prefix, req.tx_id);
            let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap_or_else(|_| manifest.to_string().into_bytes());
            if let Err(e) = provider.put_object(&manifest_key, manifest_bytes).await {
                log::error!("failed to put staging manifest {}: {}", manifest_key, e);
                return Err(tonic::Status::internal(format!("failed to put staging manifest: {}", e)));
            }
        } else {
            // Build staging path: ./worker_storage/staging/{staging_prefix}/{tx_id}
            let staging_dir = format!("worker_storage/staging/{}/{}", req.staging_prefix, req.tx_id);

            // Create staging directory
            if let Err(e) = tokio_fs::create_dir_all(&staging_dir).await {
                log::error!("failed to create staging dir {}: {}", staging_dir, e);
                return Err(tonic::Status::internal(format!("failed to create staging dir: {}", e)));
            }

            // For each task, write a JSON file containing the task descriptor
            for t in req.tasks.iter() {
                let task_file = format!("{}/{}.json", staging_dir, t.task_id);
                let payload = json!({
                    "task_id": t.task_id.clone(),
                    "operation": t.operation.clone(),
                    "input": t.input.clone(),
                    "output": t.output.clone(),
                    "params": t.params.clone(),
                });
                match tokio_fs::write(&task_file, serde_json::to_vec_pretty(&payload).unwrap_or_else(|_| payload.to_string().into_bytes())).await {
                    Ok(_) => {}
                    Err(e) => {
                        log::error!("failed to write staged task file {}: {}", task_file, e);
                        return Err(tonic::Status::internal(format!("failed to write staged task file: {}", e)));
                    }
                }
            }

            // Write a manifest describing the staged tx
            let manifest = json!({
                "tx_id": req.tx_id.clone(),
                "staging_prefix": req.staging_prefix.clone(),
                "tasks": req.tasks.iter().map(|t| t.task_id.clone()).collect::<Vec<_>>(),
                "created_at": chrono::Utc::now().to_rfc3339(),
            });
            let manifest_path = format!("{}/manifest_{}.json", staging_dir, req.tx_id);
            if let Err(e) = tokio_fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest).unwrap_or_else(|_| manifest.to_string().into_bytes())).await {
                log::error!("failed to write manifest {}: {}", manifest_path, e);
                return Err(tonic::Status::internal(format!("failed to write manifest: {}", e)));
            }
        }

        let resp = worker_service::PrepareResponse { success: true, message: "staged".to_string() };
        Ok(tonic::Response::new(resp))
    }

    async fn commit(
        &self,
        request: tonic::Request<worker_service::CommitRequest>,
    ) -> Result<tonic::Response<worker_service::CommitResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("Commit called for tx={} staging_prefix={}", req.tx_id, req.staging_prefix);

        let staging_prefix = format!("staging/{}/{}/", req.staging_prefix, req.tx_id);
        let final_prefix = format!("final/{}/{}/", req.staging_prefix, req.tx_id);

        if let Some(provider) = self.shared_data.storage_provider.clone() {
            // List objects under staging prefix
            match provider.list_objects(&staging_prefix).await {
                Ok(keys) => {
                    for key in keys.iter() {
                        // read object
                        match provider.get_object(key).await {
                            Ok(Some(bytes)) => {
                                // compute destination key by replacing staging_prefix with final_prefix
                                let dest_key = key.replacen(&staging_prefix, &final_prefix, 1);
                                if let Err(e) = provider.put_object(&dest_key, bytes).await {
                                    log::error!("failed to copy staging {} -> {}: {}", key, dest_key, e);
                                    return Err(tonic::Status::internal(format!("failed to copy staged object: {}", e)));
                                }
                                // delete original
                                if let Err(e) = provider.delete_object(key).await {
                                    log::warn!("failed to delete staging object {}: {}", key, e);
                                }
                            }
                            Ok(None) => log::warn!("staged object {} disappeared during commit", key),
                            Err(e) => {
                                log::error!("failed to read staged object {}: {}", key, e);
                                return Err(tonic::Status::internal(format!("failed to read staged object: {}", e)));
                            }
                        }
                    }
                }
                Err(e) => return Err(tonic::Status::internal(format!("failed to list staging objects: {}", e))),
            }

            // write final manifest into final parent for visibility
            let manifest = json!({
                "tx_id": req.tx_id.clone(),
                "final_prefix": final_prefix.clone(),
                "committed_at": chrono::Utc::now().to_rfc3339(),
            });
            let manifest_key = format!("final/{}/manifest_{}.json", req.staging_prefix, req.tx_id);
            let manifest_bytes = serde_json::to_vec_pretty(&manifest).unwrap_or_else(|_| manifest.to_string().into_bytes());
            if let Err(e) = provider.put_object(&manifest_key, manifest_bytes).await {
                log::error!("failed to put final manifest {}: {}", manifest_key, e);
                return Err(tonic::Status::internal(format!("failed to put final manifest: {}", e)));
            }
        } else {
            let staging_dir = format!("worker_storage/staging/{}/{}", req.staging_prefix, req.tx_id);
            let final_parent = format!("worker_storage/final/{}", req.staging_prefix);
            let final_dir = format!("{}/{}", final_parent, req.tx_id);

            // Ensure final parent exists
            if let Err(e) = tokio_fs::create_dir_all(&final_parent).await {
                log::error!("failed to create final parent {}: {}", final_parent, e);
                return Err(tonic::Status::internal(format!("failed to create final parent: {}", e)));
            }

            // Move (rename) staging_dir -> final_dir (atomic on same fs)
            if let Err(e) = tokio_fs::rename(&staging_dir, &final_dir).await {
                log::error!("failed to promote staging {} -> {}: {}", staging_dir, final_dir, e);
                return Err(tonic::Status::internal(format!("failed to promote staged objects: {}", e)));
            }

            // Create/Write a manifest pointer into final parent for visibility
            let manifest = json!({
                "tx_id": req.tx_id.clone(),
                "final_path": final_dir.clone(),
                "committed_at": chrono::Utc::now().to_rfc3339(),
            });
            let manifest_path = format!("{}/manifest_{}.json", final_parent, req.tx_id);
            if let Err(e) = tokio_fs::write(&manifest_path, serde_json::to_vec_pretty(&manifest).unwrap_or_else(|_| manifest.to_string().into_bytes())).await {
                log::error!("failed to write final manifest {}: {}", manifest_path, e);
                return Err(tonic::Status::internal(format!("failed to write final manifest: {}", e)));
            }
        }

        let resp = worker_service::CommitResponse { success: true, message: "committed".to_string() };
        Ok(tonic::Response::new(resp))
    }

    async fn abort(
        &self,
        request: tonic::Request<worker_service::AbortRequest>,
    ) -> Result<tonic::Response<worker_service::AbortResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!("Abort called for tx={} staging_prefix={}", req.tx_id, req.staging_prefix);

        let staging_prefix = format!("staging/{}/{}/", req.staging_prefix, req.tx_id);
        if let Some(provider) = self.shared_data.storage_provider.clone() {
            match provider.list_objects(&staging_prefix).await {
                Ok(keys) => {
                    for k in keys.iter() {
                        if let Err(e) = provider.delete_object(k).await {
                            log::warn!("failed to delete staged object {}: {}", k, e);
                        }
                    }
                    let resp = worker_service::AbortResponse { success: true, message: "aborted".to_string() };
                    return Ok(tonic::Response::new(resp));
                }
                Err(e) => return Err(tonic::Status::internal(format!("failed to list staging objects for abort: {}", e))),
            }
        } else {
            let staging_dir = format!("worker_storage/staging/{}/{}", req.staging_prefix, req.tx_id);
            match tokio_fs::remove_dir_all(&staging_dir).await {
                Ok(_) => {
                    let resp = worker_service::AbortResponse { success: true, message: "aborted".to_string() };
                    Ok(tonic::Response::new(resp))
                }
                Err(e) => {
                    // If missing, consider it successful cleanup
                    if e.kind() == std::io::ErrorKind::NotFound {
                        let resp = worker_service::AbortResponse { success: true, message: "nothing to abort".to_string() };
                        Ok(tonic::Response::new(resp))
                    } else {
                        log::error!("failed to remove staging dir {}: {}", staging_dir, e);
                        Err(tonic::Status::internal(format!("failed to remove staging dir: {}", e)))
                    }
                }
            }
        }
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