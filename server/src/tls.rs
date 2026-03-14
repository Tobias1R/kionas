use std::error::Error;
use tonic::transport::{Identity, ServerTlsConfig, Certificate};
use tokio::fs;
use kionas::parse_env_vars;
use crate::{config::AppConfig, tls};

pub async fn build_server_tls(cfg: &AppConfig) -> Result<ServerTlsConfig, Box<dyn Error>> {
    let default_tls_cert = parse_env_vars("${KIONAS_HOME}/certs/kionas/kionas.crt");
    let default_tls_key = parse_env_vars("${KIONAS_HOME}/certs/kionas/kionas.key");
    let wh_info = cfg.warehouse.clone();


    let wh_tls_cert_value = if wh_info.tls_cert.is_empty() {
        default_tls_cert
    } else {
        wh_info.tls_cert
    };
    let wh_tls_key_value = if wh_info.tls_key.is_empty() {
        default_tls_key
    } else {
        wh_info.tls_key
    };

    let wh_tls_cert = parse_env_vars(wh_tls_cert_value.as_str());
    let wh_tls_key = parse_env_vars(wh_tls_key_value.as_str());

    let cert = fs::read(wh_tls_cert.clone()).await?;
    let key = fs::read(wh_tls_key.clone()).await?;
    let wh_identity = Identity::from_pem(cert, key);
    Ok(ServerTlsConfig::new().identity(wh_identity))
}


pub async fn build_interops_tls(cfg: &AppConfig) -> Result<(ServerTlsConfig, Certificate), Box<dyn Error>> {
    let default_tls_cert = "${KIONAS_HOME}/certs/interopsserver.crt";
    let default_tls_key = "${KIONAS_HOME}/certs/interopsserver.key";
    let iops_info = cfg.interops.clone();

    let iops_tls_cert_value = if iops_info.tls_cert.is_empty() {
        default_tls_cert.to_string()
    } else {
        iops_info.tls_cert
    };
    let iops_tls_key_value = if iops_info.tls_key.is_empty() {
        default_tls_key.to_string()
    } else {
        iops_info.tls_key
    };    

    let iops_tls_cert = parse_env_vars(iops_tls_cert_value.as_str());
    let iops_tls_key = parse_env_vars(iops_tls_key_value.as_str());

    let cert = fs::read(iops_tls_cert.clone()).await?;
    let key = fs::read(iops_tls_key.clone()).await?;
    let iops_identity = Identity::from_pem(cert.clone(), key);

    // ca cert
    let default_ca_cert_file = "${KIONAS_HOME}/certs/Kionas-RootCA/Kionas-RootCA.crt";
    let ca_cert_file = if iops_info.ca_cert.is_empty() {
        default_ca_cert_file.to_string()
    } else {
        iops_info.ca_cert
    };
    
    let ca_cert_file = parse_env_vars(ca_cert_file.as_str());
    let ca_pem = fs::read(ca_cert_file.clone()).await?;
    let ca_cert = Certificate::from_pem(ca_pem);

    let iops_tls_config = ServerTlsConfig::new()
        .client_ca_root(ca_cert.clone())
        .client_auth_optional(true)
        .identity(iops_identity);

    Ok((iops_tls_config, ca_cert))
}