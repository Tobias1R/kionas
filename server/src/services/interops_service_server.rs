use crate::warehouse::Warehouse;
use crate::warehouse::state::{SharedData, WorkerEntry};
use kionas::{get_digest, parse_env_vars};
use std::sync::Arc;
use tonic::{Request, Response, Status};
// Second gRPC service
pub mod interops_service {
    tonic::include_proto!("interops_service");
}

#[derive(Default)]
pub struct InteropsService {
    shared_data: SharedData,
}

#[tonic::async_trait]
impl interops_service::interops_service_server::InteropsService for InteropsService {
    async fn register_worker(
        &self,
        request: Request<interops_service::RegisterWorkerRequest>,
    ) -> Result<Response<interops_service::RegisterWorkerResponse>, Status> {
        let worker = request.into_inner();
        let shared_data = self.shared_data.lock().await;
        let warehouses = Arc::clone(&shared_data.warehouses);
        let mut warehouses = warehouses.lock().await;
        let digested = get_digest(worker.name.clone().as_str());
        warehouses.insert(
            digested.clone(),
            Warehouse::new(
                worker.name.clone(),
                digested.clone(),
                worker.host.clone(),
                worker.port,
                worker.warehouse_type.clone(),
            ),
        );

        // create a connection pool address to the worker (include scheme)
        let pool_addr = if worker.port == 443 {
            format!("https://{}:{}", worker.host, worker.port)
        } else {
            format!("http://{}:{}", worker.host, worker.port)
        };
        log::info!(
            "Registering worker {} at {} (pool_addr={})",
            worker.name,
            worker.host,
            pool_addr
        );

        // attempt to read CA cert from config (if available) to trust worker TLS
        let cnf = shared_data.config.clone();
        let ca_bytes = if let Some(cfg) = cnf {
            if let Some(iops) = cfg.services.interops.as_ref() {
                if !iops.ca_cert.is_empty() {
                    match std::fs::read(parse_env_vars(&iops.ca_cert).as_str()) {
                        Ok(b) => Some(b),
                        Err(e) => {
                            log::warn!("Failed to read interops ca_cert {}: {}", iops.ca_cert, e);
                            None
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Create a WorkerEntry and insert into SharedState.workers for lazy pool creation later
        let _warehouses = Arc::clone(&shared_data.warehouses);
        let warehouse = Warehouse::new(
            worker.name.clone(),
            digested.clone(),
            worker.host.clone(),
            worker.port,
            worker.warehouse_type.clone(),
        );
        let entry = WorkerEntry::new(digested.clone(), warehouse.clone(), ca_bytes.clone());
        shared_data
            .workers
            .lock()
            .await
            .insert(digested.clone(), entry);
        log::info!(
            "Inserted worker entry for {} (key={})",
            worker.name,
            digested.clone()
        );

        let response = interops_service::RegisterWorkerResponse {
            status: "ok".into(),
            uuid: digested.clone(),
        };
        log::info!("Worker registered: {:?}", digested);
        Ok(Response::new(response))
    }

    async fn heartbeat(
        &self,
        request: Request<interops_service::HeartbeatRequest>,
    ) -> Result<Response<interops_service::HeartbeatResponse>, Status> {
        let heartbeat = request.into_inner();
        let shared_data = self.shared_data.lock().await;
        let warehouses = Arc::clone(&shared_data.warehouses);
        let mut warehouses = warehouses.lock().await;
        log::debug!("Heartbeat received {}", heartbeat.uuid);
        // print warehouses available
        for (key, value) in warehouses.iter() {
            log::debug!("{}: {:?}", key, value);
        }
        let warehouse = warehouses.get_mut(&heartbeat.uuid);
        if warehouse.is_none() {
            return Err(Status::not_found("Warehouse not found"));
        }
        let warehouse = warehouse.unwrap();
        let now = chrono::Utc::now();
        warehouse.set_last_heartbeat(now);
        let response = interops_service::HeartbeatResponse {
            status: "Heartbeat received".into(),
        };
        Ok(Response::new(response))
    }

    async fn task_update(
        &self,
        request: Request<interops_service::TaskUpdateRequest>,
    ) -> Result<Response<interops_service::TaskUpdateResponse>, Status> {
        let upd = request.into_inner();
        log::info!(
            "Task update received: {} status={} stage_id={:?} partition_completed={:?}/{:?}",
            upd.task_id,
            upd.status,
            upd.stage_id,
            upd.partition_completed,
            upd.partition_count
        );

        let shared = self.shared_data.lock().await;
        let tm = shared.task_manager.clone();

        if tm.get_task(&upd.task_id).await.is_some() {
            let s = upd.status.to_lowercase();
            let (state, result_loc, err) = match s.as_str() {
                "running" => (crate::tasks::TaskState::Running, None, None),
                "succeeded" | "success" | "ok" => {
                    let rl = if !upd.result_location.is_empty() {
                        Some(upd.result_location.clone())
                    } else {
                        None
                    };
                    (crate::tasks::TaskState::Succeeded, rl, None)
                }
                "failed" | "error" => {
                    let er = if !upd.error.is_empty() {
                        Some(upd.error.clone())
                    } else {
                        None
                    };
                    (crate::tasks::TaskState::Failed, None, er)
                }
                "cancelled" | "canceled" => (crate::tasks::TaskState::Cancelled, None, None),
                _ => (
                    crate::tasks::TaskState::Failed,
                    None,
                    Some(format!("unknown status from worker: {}", upd.status)),
                ),
            };

            let applied = tm
                .update_from_worker_with_stage_progress(
                    &upd.task_id,
                    state,
                    result_loc,
                    err,
                    upd.stage_id.clone(),
                    upd.partition_count,
                    upd.partition_completed,
                    upd.metadata.clone(),
                )
                .await;

            if !applied {
                log::info!(
                    "Ignored idempotent/rejected task update for task_id={} status={} stage_id={:?}",
                    upd.task_id,
                    upd.status,
                    upd.stage_id
                );
            }
        } else {
            log::warn!("Task update for unknown task id: {}", upd.task_id);
            // Proceed, but indicate not-found; still return ok to avoid worker retries
            let resp = interops_service::TaskUpdateResponse {
                status: "not_found".into(),
            };
            return Ok(Response::new(resp));
        }

        let resp = interops_service::TaskUpdateResponse {
            status: "ok".into(),
        };
        Ok(Response::new(resp))
    }
}

impl InteropsService {
    pub fn new(shared_data: SharedData) -> Self {
        Self {
            shared_data: Arc::clone(&shared_data),
        }
    }
}
