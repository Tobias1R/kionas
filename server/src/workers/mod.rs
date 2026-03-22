use deadpool::managed::Object;
use std::error::Error;
use tokio::time::{Duration, timeout};

use crate::workers_pool::{WorkerConnectionManager, WorkerPool};

pub type PooledConn = Object<WorkerConnectionManager>;

pub async fn acquire_channel_with_heartbeat(
    pool: &WorkerPool,
    worker_id: &str,
    timeout_secs: u64,
) -> Result<PooledConn, Box<dyn Error + Send + Sync>> {
    match timeout(Duration::from_secs(timeout_secs), pool.get()).await {
        Ok(Ok(conn)) => {
            // perform heartbeat
            let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
            let hb = crate::services::worker_service_client::worker_service::HeartbeatRequest {
                worker_id: worker_id.to_string(),
            };
            match timeout(
                Duration::from_secs(5),
                client.heartbeat(tonic::Request::new(hb)),
            )
            .await
            {
                Ok(Ok(_)) => Ok(conn),
                Ok(Err(e)) => Err(Box::new(std::io::Error::other(
                    format!("heartbeat error: {:?}", e),
                ))),
                Err(_) => Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out during heartbeat",
                ))),
            }
        }
        Ok(Err(e)) => Err(Box::new(std::io::Error::other(
            format!("pool.get() error: {:?}", e),
        ))),
        Err(_) => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out getting pooled connection",
        ))),
    }
}

pub async fn send_task_to_worker(
    conn: PooledConn,
    req: crate::services::worker_service_client::worker_service::TaskRequest,
    auth_ctx: Option<&crate::statement_handler::shared::helpers::DispatchAuthContext>,
    timeout_secs: u64,
) -> Result<
    crate::services::worker_service_client::worker_service::TaskResponse,
    Box<dyn Error + Send + Sync>,
> {
    let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
    let mut request = tonic::Request::new(req);
    if let Ok(value) =
        tonic::metadata::MetadataValue::try_from(request.get_ref().session_id.as_str())
    {
        request.metadata_mut().insert("session_id", value);
    }
    if let Some(ctx) = auth_ctx {
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(ctx.rbac_user.as_str()) {
            request.metadata_mut().insert("rbac_user", value);
        }
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(ctx.rbac_role.as_str()) {
            request.metadata_mut().insert("rbac_role", value);
        }
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(ctx.scope.as_str()) {
            request.metadata_mut().insert("auth_scope", value);
        }
        if let Ok(value) = tonic::metadata::MetadataValue::try_from(ctx.query_id.as_str()) {
            request.metadata_mut().insert("query_id", value);
        }
    }
    match timeout(
        Duration::from_secs(timeout_secs),
        client.execute_task(request),
    )
    .await
    {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(Box::new(std::io::Error::other(
            format!("execute_task error: {:?}", e),
        ))),
        Err(_) => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out executing task",
        ))),
    }
}

#[allow(dead_code)]
pub async fn send_prepare_to_worker(
    conn: PooledConn,
    req: crate::services::worker_service_client::worker_service::PrepareRequest,
    timeout_secs: u64,
) -> Result<
    crate::services::worker_service_client::worker_service::PrepareResponse,
    Box<dyn Error + Send + Sync>,
> {
    let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
    match timeout(
        Duration::from_secs(timeout_secs),
        client.prepare(tonic::Request::new(req)),
    )
    .await
    {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(Box::new(std::io::Error::other(
            format!("prepare error: {:?}", e),
        ))),
        Err(_) => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out during prepare",
        ))),
    }
}

#[allow(dead_code)]
pub async fn send_commit_to_worker(
    conn: PooledConn,
    req: crate::services::worker_service_client::worker_service::CommitRequest,
    timeout_secs: u64,
) -> Result<
    crate::services::worker_service_client::worker_service::CommitResponse,
    Box<dyn Error + Send + Sync>,
> {
    let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
    match timeout(
        Duration::from_secs(timeout_secs),
        client.commit(tonic::Request::new(req)),
    )
    .await
    {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(Box::new(std::io::Error::other(
            format!("commit error: {:?}", e),
        ))),
        Err(_) => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out during commit",
        ))),
    }
}

#[allow(dead_code)]
pub async fn send_abort_to_worker(
    conn: PooledConn,
    req: crate::services::worker_service_client::worker_service::AbortRequest,
    timeout_secs: u64,
) -> Result<
    crate::services::worker_service_client::worker_service::AbortResponse,
    Box<dyn Error + Send + Sync>,
> {
    let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
    match timeout(
        Duration::from_secs(timeout_secs),
        client.abort(tonic::Request::new(req)),
    )
    .await
    {
        Ok(Ok(resp)) => Ok(resp.into_inner()),
        Ok(Err(e)) => Err(Box::new(std::io::Error::other(
            format!("abort error: {:?}", e),
        ))),
        Err(_) => Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "timed out during abort",
        ))),
    }
}
