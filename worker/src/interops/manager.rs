use deadpool::managed::{Manager, Metrics};
use std::time::Duration;

pub struct InteropsManager {
    pub addr: String,
    pub ca_cert_path: Option<String>,
    pub tls_cert_path: Option<String>,
    pub tls_key_path: Option<String>,
}

impl Manager for InteropsManager {
    type Type = crate::interops_service::interops_service_client::InteropsServiceClient<
        tonic::transport::Channel,
    >;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> impl std::future::Future<Output = Result<Self::Type, Self::Error>> + Send {
        let master = self.addr.clone();
        let ca = self.ca_cert_path.clone();
        let cert = self.tls_cert_path.clone();
        let key = self.tls_key_path.clone();
        async move {
            let base_ep = tonic::transport::Channel::from_shared(master.clone())?;
            let configured_endpoint = if master.starts_with("https") {
                if let Some(ca_path) = ca.as_ref() {
                    let ca_bytes = std::fs::read(ca_path)?;
                    let ca = tonic::transport::Certificate::from_pem(ca_bytes);
                    let mut tls = tonic::transport::ClientTlsConfig::new().ca_certificate(ca);
                    if let (Some(cert_path), Some(key_path)) = (cert.as_ref(), key.as_ref())
                        && let (Ok(cert), Ok(key)) =
                            (std::fs::read(cert_path), std::fs::read(key_path))
                    {
                        let id = tonic::transport::Identity::from_pem(cert, key);
                        tls = tls.identity(id);
                    }
                    match base_ep.clone().tls_config(tls) {
                        Ok(e) => e,
                        Err(_) => base_ep.clone(),
                    }
                } else {
                    base_ep.clone()
                }
            } else {
                base_ep.clone()
            };

            let chan = tokio::time::timeout(Duration::from_secs(10), configured_endpoint.connect())
                .await??;
            Ok(crate::interops_service::interops_service_client::InteropsServiceClient::new(chan))
        }
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &Metrics,
    ) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        Ok(())
    }
}
