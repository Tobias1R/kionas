

use tonic::{Request, Response, Status};
use kionas::{get_digest, parse_env_vars};
use std::sync::Arc;
use crate::warehouse::state::{SharedData};
use crate::warehouse::Warehouse;
use crate::workers_pool::get_new_pool;
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
                worker.port.clone() as u32,
                worker.warehouse_type.clone(),
            ),
        );

        // create a connection pool to the worker (include scheme)
        let pool_addr = if worker.port == 443 {
            format!("https://{}:{}", worker.host, worker.port)
        } else {
            format!("http://{}:{}", worker.host, worker.port)
        };

        // attempt to read CA cert from config (if available) to trust worker TLS
        let cnf = shared_data.config.clone();
        let ca_bytes = if let Some(cfg) = cnf {
            if !cfg.interops.ca_cert.is_empty() {
                match std::fs::read(parse_env_vars(&cfg.interops.ca_cert).as_str()) {
                    Ok(b) => Some(b),
                    Err(e) => {
                        log::warn!("Failed to read interops ca_cert {}: {}", cfg.interops.ca_cert, e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        let pool = get_new_pool(pool_addr, worker.name.clone(), ca_bytes);
        shared_data.worker_pools.lock().await.insert(digested.clone(), pool);
        

        let response = interops_service::RegisterWorkerResponse {
            status: "ok".into(),
            uuid: digested.clone()
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
}

impl InteropsService {
    pub fn new(shared_data: SharedData) -> Self {
        Self { shared_data: Arc::clone(&shared_data) }
    }
}