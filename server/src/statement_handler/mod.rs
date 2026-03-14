use crate::warehouse::state::SharedData;
use crate::session::Session;
use crate::warehouse::Warehouse;

/// Discover worker address tied to session
pub async fn get_worker_addr_for_session(shared_data: &SharedData, session_id: &str) -> Option<String> {
    let state = shared_data.lock().await;
    // Get session
    let session_opt = state.session_manager.get_session(session_id.to_string()).await;
    let session = match session_opt {
        Some(s) => s,
        None => {
            log::warn!("Session not found: {}", session_id);
            return None;
        }
    };

    // Try by digested warehouse uuid first
    let worker_uuid = session.get_warehouse_uuid();
    let warehouses_map = state.warehouses.lock().await;
    log::info!("Resolving worker for session {} -> warehouse='{}' uuid={}", session_id, session.get_warehouse(), worker_uuid);
    // Dump registered warehouses for diagnostics
    for (k, w) in warehouses_map.iter() {
        log::info!("Registered warehouse key={} name={} host={} port={}", k, w.get_name(), w.get_host(), w.get_port());
    }
    // Dump worker pools keys
    let worker_pools_map = state.worker_pools.lock().await;
    for (k, _) in worker_pools_map.iter() {
        log::info!("Worker pool key={}", k);
    }
    if let Some(warehouse) = warehouses_map.get(&worker_uuid) {
        let scheme = if warehouse.get_port() == 443 { "https" } else { "http" };
        let uri = format!("{}://{}:{}", scheme, warehouse.get_host(), warehouse.get_port());
        return Some(uri);
    }

    // Fallback: try to find by plain warehouse name
    for (_k, w) in warehouses_map.iter() {
        let plain = session.get_warehouse();
        let wname = w.get_name();
        let whost = w.get_host();
        if wname == plain || wname.ends_with(&plain) || wname.contains(&plain) || whost == plain {
            log::info!("Fuzzy matched warehouse '{}' to session warehouse '{}'", wname, plain);
            let scheme = if w.get_port() == 443 { "https" } else { "http" };
            let uri = format!("{}://{}:{}", scheme, w.get_host(), w.get_port());
            return Some(uri);
        }
    }

    log::warn!("Warehouse not found for session {} (uuid={})", session_id, worker_uuid);
    None
}
pub mod create_schema;

use kionas::parser::datafusion_sql::sqlparser::ast::{ObjectName, Statement};

pub async fn handle_statement(stmt: &Statement, session_id: &str, shared_data: &SharedData) -> String {
    match stmt {
        Statement::CreateSchema { schema_name, .. } => {
            match create_schema::handle(schema_name, session_id, shared_data).await {
                Ok(s) => s,
                Err(e) => e.to_string(),
            }
        }
        // Add more statement handlers here
        _ => "Unsupported statement".to_string(),
    }
}
