use crate::state::SharedData;

use serde_json::Value;

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
        // TODO: Arrow Flight info logic
        let resp = worker_service::FlightInfoResponse {
            endpoint: "arrow-flight-endpoint".to_string(),
            schema: "arrow-schema".to_string(),
            ticket: req.task_id.clone(),
        };
        Ok(tonic::Response::new(resp))
    }
}
