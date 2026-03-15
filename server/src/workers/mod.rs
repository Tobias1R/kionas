use std::error::Error;
use tokio::time::{timeout, Duration};
use deadpool::managed::Object;

use crate::workers_pool::{WorkerPool, WorkerConnectionManager};

pub type PooledConn = Object<WorkerConnectionManager>;

pub async fn acquire_channel_with_heartbeat(pool: &WorkerPool, worker_id: &str, timeout_secs: u64) -> Result<PooledConn, Box<dyn Error + Send + Sync>> {
    match timeout(Duration::from_secs(timeout_secs), pool.get()).await {
        Ok(Ok(conn)) => {
            // perform heartbeat
            let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
            let hb = crate::services::worker_service_client::worker_service::HeartbeatRequest { worker_id: worker_id.to_string() };
            match timeout(Duration::from_secs(5), client.heartbeat(tonic::Request::new(hb))).await {
                Ok(Ok(_)) => Ok(conn),
                Ok(Err(e)) => Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("heartbeat error: {:?}", e)))),
                Err(_) => Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out during heartbeat"))),
            }
        }
        Ok(Err(e)) => Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("pool.get() error: {:?}", e)))),
        Err(_) => Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out getting pooled connection"))),
    }
}

pub async fn send_task_to_worker(conn: PooledConn, req: crate::services::worker_service_client::worker_service::TaskRequest, timeout_secs: u64) -> Result<crate::services::worker_service_client::worker_service::TaskResponse, Box<dyn Error + Send + Sync>> {
    let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
    match timeout(Duration::from_secs(timeout_secs), client.execute_task(tonic::Request::new(req))).await {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, format!("execute_task error: {:?}", e)))),
        Err(_) => Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out executing task"))),
    }
}
