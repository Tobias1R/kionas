use kionas::parser::datafusion_sql::sqlparser::ast::SchemaName;
use crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient;
use crate::services::worker_service_client::worker_service::{TaskRequest, Task};
use tonic::transport::Channel;
use std::collections::HashMap;
use crate::warehouse::state::SharedData;

use std::error::Error;
use tokio::time::timeout;
use std::time::Duration;

pub async fn handle(schema_name: &SchemaName, session_id: &str, shared_data: &SharedData) -> Result<String, Box<dyn Error>> {
    // Build task fragment for schema creation
    let task = TaskRequest {
        session_id: session_id.to_string(),
        tasks: vec![Task {
            task_id: "create_schema".to_string(),
            input: schema_name.to_string(),
            operation: "write".to_string(),
            output: "yada".to_string(),
            params: HashMap::new()
        }],
    };

    // Determine appropriate worker pool from shared_data (scoped locks)
    let pool_opt = {
        let state = shared_data.lock().await;
        let session_opt = state.session_manager.get_session(session_id.to_string()).await;
        let session = match session_opt {
            Some(s) => s,
            None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "session not found"))),
        };

        let worker_uuid = session.get_warehouse_uuid();

        // Try exact pool lookup by digested uuid
        let worker_pools = state.worker_pools.lock().await;
        if let Some(p) = worker_pools.get(&worker_uuid) {
            Some(p.clone())
        } else {
            // Fallback: try to find a matching warehouse name and use its pool
            let warehouses_map = state.warehouses.lock().await;
            let mut matched_key: Option<String> = None;
            for (k, w) in warehouses_map.iter() {
                let plain = session.get_warehouse();
                let wname = w.get_name();
                let whost = w.get_host();
                if wname == plain || wname.ends_with(&plain) || wname.contains(&plain) || whost == plain {
                    matched_key = Some(k.clone());
                    break;
                }
            }
            if let Some(key) = matched_key {
                match state.worker_pools.lock().await.get(&key) {
                    Some(p2) => Some(p2.clone()),
                    None => None,
                }
            } else {
                None
            }
        }
    };

    let pool = match pool_opt {
        Some(p) => p,
        None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "worker pool not found"))),
    };

    // Use the pool to get a channel and dispatch (with timeouts for diagnostics)
    log::info!("Attempting to acquire pooled connection to worker for session {}", session_id);
    let conn = match timeout(Duration::from_secs(10), pool.get()).await {
        Ok(Ok(c)) => c,
        Ok(Err(e)) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("pool.get() error: {:?}", e)))),
        Err(_) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out getting pooled connection"))),
    };
    log::info!("Acquired pooled connection for session {}", session_id);
    let channel = conn.clone();
    let mut client = WorkerServiceClient::new(channel);

    log::info!("Dispatching TaskRequest to worker for session {}", session_id);
    let exec_resp = match timeout(Duration::from_secs(30), client.execute_task(tonic::Request::new(task))).await {
        Ok(Ok(resp)) => resp,
        Ok(Err(e)) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("execute_task error: {:?}", e)))),
        Err(_) => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out executing task on worker"))),
    };
    log::info!("Received response from worker for session {}", session_id);
    let result = exec_resp.into_inner();

    if result.status == "ok" {
        Ok(format!("Schema created successfully: {}", result.result_location))
    } else {
        Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, result.error)))
    }
}
