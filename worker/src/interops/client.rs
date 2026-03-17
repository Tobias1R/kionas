use tokio::task::JoinHandle;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

/// Spawn a background task that connects to `master` and sends a TaskUpdate.
/// This mirrors the previous inline logic but centralizes it for reuse.
pub fn notify_task_update_in_background(
    master: String,
    ca_cert_path: Option<String>,
    tls_cert_path: Option<String>,
    tls_key_path: Option<String>,
    update: crate::interops_service::TaskUpdateRequest,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        match Channel::from_shared(master.clone()) {
            Ok(base_ep) => {
                let configured_endpoint = if master.starts_with("https") {
                    if let Some(ca_path) = ca_cert_path.as_ref() {
                        match std::fs::read(ca_path) {
                            Ok(ca_bytes) => {
                                let ca = Certificate::from_pem(ca_bytes);
                                let mut tls = ClientTlsConfig::new().ca_certificate(ca);
                                if let (Some(cert_path), Some(key_path)) =
                                    (tls_cert_path.as_ref(), tls_key_path.as_ref())
                                {
                                    if let (Ok(cert), Ok(key)) =
                                        (std::fs::read(cert_path), std::fs::read(key_path))
                                    {
                                        let id = Identity::from_pem(cert, key);
                                        tls = tls.identity(id);
                                    } else {
                                        log::warn!(
                                            "Failed to read interops tls_cert/tls_key, proceeding without client identity"
                                        );
                                    }
                                }
                                match base_ep.clone().tls_config(tls) {
                                    Ok(e) => e,
                                    Err(err) => {
                                        log::error!(
                                            "Failed to apply tls_config for master {}: {}",
                                            master,
                                            err
                                        );
                                        base_ep.clone()
                                    }
                                }
                            }
                            Err(e) => {
                                log::error!("Failed to read CA path {}: {}", ca_path, e);
                                base_ep.clone()
                            }
                        }
                    } else {
                        log::error!(
                            "No CA path configured; cannot validate master TLS for {}",
                            master
                        );
                        base_ep.clone()
                    }
                } else {
                    base_ep.clone()
                };

                match configured_endpoint.connect().await {
                    Ok(chan) => {
                        let mut client = crate::interops_service::interops_service_client::InteropsServiceClient::new(chan);
                        match client.task_update(tonic::Request::new(update)).await {
                            Ok(resp) => log::info!("Sent TaskUpdate -> {:?}", resp.into_inner()),
                            Err(e) => log::error!("Failed to send TaskUpdate: {}", e),
                        }
                    }
                    Err(e) => log::error!("Failed to connect to master {}: {}", master, e),
                }
            }
            Err(e) => log::error!("Invalid master address {}: {}", master, e),
        }
    })
}
