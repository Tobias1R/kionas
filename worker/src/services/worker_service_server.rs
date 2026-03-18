use crate::state::SharedData;
use base64::Engine;

use serde_json::Value;

pub mod worker_service {
    tonic::include_proto!("worker_service");
}

#[derive(Default)]
pub struct WorkerService {
    pub shared_data: SharedData,
}

fn resolve_flight_port(default_worker_port: u32) -> u32 {
    std::env::var("WORKER_FLIGHT_PORT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|p| *p > 0)
        .unwrap_or(default_worker_port.saturating_add(1))
}

fn resolve_flight_endpoint(worker_host: &str, worker_port: u32) -> String {
    let flight_port = resolve_flight_port(worker_port);
    format!("https://{}:{}", worker_host, flight_port)
}

fn build_flight_ticket(session_id: &str, task_id: &str, worker_id: &str) -> String {
    let raw = format!("{}:{}:{}", session_id, task_id, worker_id);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw.as_bytes())
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
        let _req = request.into_inner();
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
        log::info!("Received task: {}", req.session_id);

        let resp =
            crate::transactions::maestro::handle_execute_task(self.shared_data.clone(), req).await;
        Ok(tonic::Response::new(resp))
    }

    async fn prepare(
        &self,
        request: tonic::Request<worker_service::PrepareRequest>,
    ) -> Result<tonic::Response<worker_service::PrepareResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!(
            "Prepare called for tx={} staging_prefix={}",
            req.tx_id,
            req.staging_prefix
        );
        let tasks_json: Vec<Value> = req
            .tasks
            .iter()
            .map(|t| {
                serde_json::json!({
                    "task_id": t.task_id.clone(),
                    "operation": t.operation.clone(),
                    "input": t.input.clone(),
                    "output": t.output.clone(),
                    "params": t.params.clone(),
                })
            })
            .collect();

        if let Err(e) = crate::transactions::maestro::prepare_tx(
            self.shared_data.clone(),
            &req.tx_id,
            &req.staging_prefix,
            &tasks_json,
        )
        .await
        {
            log::error!("failed to stage tx {}: {}", req.tx_id, e);
            return Err(tonic::Status::internal(format!(
                "failed to stage tx: {}",
                e
            )));
        }

        let resp = worker_service::PrepareResponse {
            success: true,
            message: "staged".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn commit(
        &self,
        request: tonic::Request<worker_service::CommitRequest>,
    ) -> Result<tonic::Response<worker_service::CommitResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!(
            "Commit called for tx={} staging_prefix={}",
            req.tx_id,
            req.staging_prefix
        );
        if let Err(e) = crate::transactions::maestro::commit_tx(
            self.shared_data.clone(),
            &req.tx_id,
            &req.staging_prefix,
        )
        .await
        {
            log::error!("failed to promote tx {}: {}", req.tx_id, e);
            return Err(tonic::Status::internal(format!(
                "failed to promote staged objects: {}",
                e
            )));
        }

        let resp = worker_service::CommitResponse {
            success: true,
            message: "committed".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn abort(
        &self,
        request: tonic::Request<worker_service::AbortRequest>,
    ) -> Result<tonic::Response<worker_service::AbortResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!(
            "Abort called for tx={} staging_prefix={}",
            req.tx_id,
            req.staging_prefix
        );
        if let Err(e) = crate::transactions::maestro::abort_tx(
            self.shared_data.clone(),
            &req.tx_id,
            &req.staging_prefix,
        )
        .await
        {
            log::error!("failed to abort tx {}: {}", req.tx_id, e);
            return Err(tonic::Status::internal(format!(
                "failed to abort staged objects: {}",
                e
            )));
        }
        let resp = worker_service::AbortResponse {
            success: true,
            message: "aborted".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn get_flight_info(
        &self,
        request: tonic::Request<worker_service::FlightInfoRequest>,
    ) -> Result<tonic::Response<worker_service::FlightInfoResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.task_id.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("task_id is required"));
        }
        if req.session_id.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("session_id is required"));
        }

        let endpoint = resolve_flight_endpoint(
            &self.shared_data.worker_info.host,
            self.shared_data.worker_info.port,
        );
        let ticket = build_flight_ticket(
            &req.session_id,
            &req.task_id,
            &self.shared_data.worker_info.worker_id,
        );

        let resp = worker_service::FlightInfoResponse {
            endpoint,
            // Schema negotiation will be filled when DoGet is implemented.
            schema: "{}".to_string(),
            ticket,
        };
        Ok(tonic::Response::new(resp))
    }
}
