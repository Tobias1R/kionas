use deadpool::managed::{Manager, Metrics, Pool, RecycleResult};
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

#[derive(Debug, Default)]
pub struct WorkerConnectionManager {
    endpoint: String,
    worker_id: String,
    ca_cert: Option<Vec<u8>>,
}

impl WorkerConnectionManager {
    pub fn new(endpoint: String, worker_id: String, ca_cert: Option<Vec<u8>>) -> Self {
        Self {
            endpoint,
            worker_id,
            ca_cert,
        }
    }
}

impl Manager for WorkerConnectionManager {
    type Type = Channel;
    type Error = tonic::transport::Error;

    async fn create(&self) -> Result<Channel, tonic::transport::Error> {
        log::info!(
            "WorkerConnectionManager creating channel to {} (worker_id={}) ca_cert_present={}",
            self.endpoint,
            self.worker_id,
            self.ca_cert.is_some()
        );
        let mut ep = Endpoint::from_shared(self.endpoint.clone())?;
        if let Some(ca) = &self.ca_cert {
            let ca_cert = tonic::transport::Certificate::from_pem(ca.clone());
            let tls = tonic::transport::ClientTlsConfig::new()
                .ca_certificate(ca_cert)
                .domain_name(self.worker_id.clone());
            ep = ep.tls_config(tls)?;
        }
        ep = ep.connect_timeout(Duration::from_secs(5));
        // Use lazy connect so the pool can be created before the worker is actually accepting connections.
        Ok(ep.connect_lazy())
    }

    async fn recycle(
        &self,
        conn: &mut Channel,
        _: &Metrics,
    ) -> RecycleResult<tonic::transport::Error> {
        let mut client = crate::services::worker_service_client::worker_service::worker_service_client::WorkerServiceClient::new(conn.clone());
        let request = tonic::Request::new(
            crate::services::worker_service_client::worker_service::HeartbeatRequest {
                worker_id: self.worker_id.clone(),
            },
        );
        let _ = client.heartbeat(request).await;
        Ok(())
    }
}

pub fn get_new_pool(
    endpoint: String,
    worker_id: String,
    ca_cert: Option<Vec<u8>>,
) -> Pool<WorkerConnectionManager> {
    let manager = WorkerConnectionManager::new(endpoint, worker_id, ca_cert);
    Pool::builder(manager).max_size(10).build().unwrap()
}

pub type WorkerPool = Pool<WorkerConnectionManager>;
