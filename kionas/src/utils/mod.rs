use chrono;
use regex::Regex;
use sysinfo::{ProcessExt, System, SystemExt};

pub async fn resolve_hostname(
    hostname: &str,
    port: u16,
) -> Result<std::net::SocketAddr, Box<dyn std::error::Error + Send + Sync>> {
    let addr = format!("{}:{}", hostname, port);
    let mut addrs = tokio::net::lookup_host(addr)
        .await
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;
    addrs.next().ok_or_else(|| {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "No address found",
        )) as Box<dyn std::error::Error + Send + Sync>
    })
}

pub async fn check_tcp_addr(host: &str, port: u16, name: &str) -> bool {
    let addr = format!("{}:{}", host, port);
    match tokio::time::timeout(
        std::time::Duration::from_secs(3),
        tokio::net::TcpStream::connect(addr),
    )
    .await
    {
        Ok(Ok(_stream)) => {
            log::info!("{} reachable at {}:{}", name, host, port);
            true
        }
        Ok(Err(e)) => {
            log::warn!("{} connection error to {}:{} - {}", name, host, port, e);
            false
        }
        Err(_) => {
            log::warn!("{} connection timed out to {}:{}", name, host, port);
            false
        }
    }
}

pub fn parse_endpoint(endpoint: &str) -> Option<(String, u16)> {
    // naive parsing: strip scheme, take host:port part
    let without_scheme = if let Some(idx) = endpoint.find("://") {
        &endpoint[idx + 3..]
    } else {
        endpoint
    };
    let host_port = without_scheme.split('/').next().unwrap_or("");
    let mut parts = host_port.split(':');
    let host = parts.next()?.to_string();
    let port = parts
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(80u16);
    Some((host, port))
}

pub fn print_memory_usage() {
    let mut sys = System::new_all();
    sys.refresh_all();
    let process = sys.process(sysinfo::get_current_pid().unwrap()).unwrap();
    log::info!("Memory used: {} MB", process.memory() / 1024 / 1024);
    log::info!(
        "Virtual memory used: {} MB",
        process.virtual_memory() / 1024 / 1024
    );
}

pub fn print_server_info() {
    let sys = System::new_all();
    let long_string = r#"
    __ __ _                       
   / //_/(_)___  ____  ____ ______
  / ,<  / / __ \/ __ \/ __ `/ ___/
 / /| |/ / /_/ / / / / /_/ (__  ) 
/_/ |_/_/\____/_/ /_/\__,_/____/  
"#;
    let line = "---------------------------------------------------------------";
    log::info!("{}", long_string);
    log::info!("Server version: 0.1.0");
    log::info!("{}", line);
    log::info!(
        "OS Name: {:?}",
        sys.name().unwrap_or_else(|| "Unknown".to_string())
    );
    log::info!(
        "OS Version: {:?}",
        sys.long_os_version()
            .unwrap_or_else(|| "Unknown".to_string())
    );
    log::info!(
        "OS Hostname: {:?}",
        sys.host_name().unwrap_or_else(|| "Unknown".to_string())
    );
    log::info!(
        "OS Kernel Version: {:?}",
        sys.kernel_version()
            .unwrap_or_else(|| "Unknown".to_string())
    );

    log::info!("{}", line);
    log::info!("Starting Kionas server at: {}", chrono::Utc::now());
    log::info!("{}", line);
}

/// What: Produce a redacted SQL digest safe for dashboard history display.
///
/// Inputs:
/// - `sql`: Raw SQL text.
///
/// Output:
/// - SQL with string and numeric literals redacted, truncated to 200 chars.
///
/// Details:
/// - Uses conservative regex substitutions and does not attempt full SQL parsing.
/// - Prevents obvious literal leakage in monitoring payloads.
pub fn redact_sql(sql: &str) -> String {
    let single_quoted = match Regex::new(r"'(?:''|[^'])*'") {
        Ok(regex) => regex,
        Err(_) => return sql.chars().take(200).collect(),
    };
    let double_quoted = match Regex::new(r#""(?:""|[^"])*""#) {
        Ok(regex) => regex,
        Err(_) => return sql.chars().take(200).collect(),
    };
    let numeric = match Regex::new(r"\b\d+(?:\.\d+)?\b") {
        Ok(regex) => regex,
        Err(_) => return sql.chars().take(200).collect(),
    };

    let redacted_single = single_quoted.replace_all(sql, "'?'");
    let redacted_double = double_quoted.replace_all(&redacted_single, "\"?\"");
    let redacted_numeric = numeric.replace_all(&redacted_double, "?");
    redacted_numeric.chars().take(200).collect()
}
