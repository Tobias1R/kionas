use crate::warehouse::state::SharedData;
use std::error::Error;
use tonic::Request;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[allow(dead_code, clippy::enum_variant_names)]
pub mod metastore_service {
    tonic::include_proto!("metastore_service");
}

use metastore_service::metastore_service_client::MetastoreServiceClient;
use metastore_service::{MetastoreRequest, MetastoreResponse};

#[derive(Clone)]
pub struct MetastoreClient {
    inner: MetastoreServiceClient<Channel>,
}

impl MetastoreClient {
    /// Create a new client from a URI. TLS can be configured via Optional CA bytes in shared config.
    pub async fn connect_with_shared(
        shared: &SharedData,
    ) -> Result<Self, Box<dyn Error + Send + Sync>> {
        // Grab config and the shared metastore_channel Arc so we can reuse a channel
        let cfg = {
            let s = shared.lock().await;
            s.config.clone()
        };

        let shared_chan = {
            let s = shared.lock().await;
            s.metastore_channel.clone()
        };

        // Try to reuse existing channel
        {
            let opt_chan = shared_chan.lock().await;
            if let Some(chan) = opt_chan.as_ref() {
                log::info!("Reusing cached metastore channel");
                return Ok(Self {
                    inner: MetastoreServiceClient::new(chan.clone()),
                });
            }
        }

        // No cached channel — build a fresh endpoint and connect
        let addr = "https://kionas-metastore:443".to_string();
        let mut ep = Endpoint::from_shared(addr.clone())?;

        if let Some(c) = cfg
            && let Some(iops) = c.services.interops.as_ref()
            && !iops.ca_cert.is_empty()
            && let Ok(ca_bytes) = std::fs::read(kionas::parse_env_vars(&iops.ca_cert).as_str())
        {
            let ca = tonic::transport::Certificate::from_pem(ca_bytes);
            let tls = ClientTlsConfig::new().ca_certificate(ca);
            ep = ep.tls_config(tls)?;
        }

        log::info!("Creating new metastore channel to {}", addr);
        let chan = ep.connect().await?;

        // Cache the channel for future requests
        {
            let mut opt_chan = shared_chan.lock().await;
            *opt_chan = Some(chan.clone());
            log::info!("Cached metastore channel for reuse");
        }

        Ok(Self {
            inner: MetastoreServiceClient::new(chan),
        })
    }

    pub async fn execute(
        &mut self,
        req: MetastoreRequest,
    ) -> Result<MetastoreResponse, Box<dyn Error + Send + Sync>> {
        let resp = self.inner.execute(Request::new(req)).await?;
        Ok(resp.into_inner())
    }
}
