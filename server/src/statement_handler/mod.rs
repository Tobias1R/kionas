pub(crate) mod helpers;
pub mod use_warehouse;

use kionas::parser::datafusion_sql::sqlparser::ast::{ObjectName, Statement};
use crate::services::metastore_client::metastore_service as ms;
use crate::services::metastore_client::MetastoreClient;
use crate::warehouse::state::SharedData;
use crate::core::DomainService;
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


pub async fn handle_statement(stmt: &Statement, session_id: &str, shared_data: &SharedData) -> String {
    match stmt {
        Statement::CreateSchema { schema_name, .. } => {
            // Build domain object from AST and validate
            let owner = {
                let state = shared_data.lock().await;
                match state.session_manager.get_session(session_id.to_string()).await {
                    Some(s) => s.get_role(),
                    None => "unknown".to_string(),
                }
            };
            match DomainService::from_create_schema(schema_name, &owner) {
                Ok(_catalog) => {
                    match helpers::run_task_for_input(shared_data, session_id, "create_schema", schema_name.to_string(), 30).await {
                        Ok(loc) => {
                            // Persist to metastore (use domain-derived request in future)
                            let mreq = ms::MetastoreRequest { action: Some(ms::metastore_request::Action::CreateSchema(ms::CreateSchemaRequest { schema_name: schema_name.to_string() })) };
                            match MetastoreClient::connect_with_shared(shared_data).await {
                                Ok(mut client) => {
                                    match client.execute(mreq).await {
                                        Ok(resp) => log::info!("Metastore Execute response for CreateSchema: {:?}", resp),
                                        Err(e) => log::error!("Metastore Execute failed for CreateSchema: {}", e),
                                    }
                                }
                                Err(e) => log::error!("Failed to connect to metastore for CreateSchema: {}", e),
                            }
                            format!("Schema created successfully: {}", loc)
                        }
                        Err(e) => e.to_string(),
                    }
                }
                Err(e) => format!("Domain validation failed: {}", e),
            }
        }
        Statement::CreateTable(create_table) => {
            // `CreateTable` is a tuple variant carrying a `CreateTable` struct; extract its `name`.
            let name = &create_table.name;
            // Build domain object from AST
            let default_schema = {
                let state = shared_data.lock().await;
                match state.session_manager.get_session(session_id.to_string()).await {
                    Some(s) => s.get_use_database(),
                    None => "default".to_string(),
                }
            };
            match DomainService::from_create_table(create_table, &default_schema) {
                Ok(_table) => {
                    match helpers::run_task_for_input(shared_data, session_id, "create_table", name.to_string(), 30).await {
                        Ok(loc) => {
                            let table_name = name.to_string();
                            let creq = ms::CreateTableRequest {
                                schema_name: String::new(),
                                table_name: table_name.clone(),
                                engine: String::new(),
                                columns: Vec::new(),
                            };
                            let mreq = ms::MetastoreRequest { action: Some(ms::metastore_request::Action::CreateTable(creq)) };
                            match MetastoreClient::connect_with_shared(shared_data).await {
                                Ok(mut client) => {
                                    match client.execute(mreq).await {
                                        Ok(resp) => log::info!("Metastore Execute response for CreateTable {}: {:?}", table_name, resp),
                                        Err(e) => log::error!("Metastore Execute failed for CreateTable {}: {}", table_name, e),
                                    }
                                }
                                Err(e) => log::error!("Failed to connect to metastore for CreateTable {}: {}", table_name, e),
                            }
                            format!("Table created successfully: {}", loc)
                        }
                        Err(e) => e.to_string(),
                    }
                }
                Err(e) => format!("Domain validation failed: {}", e),
            }
        }
        // Add more statement handlers here
        _ => "Unsupported statement".to_string(),
    }
}

/// Check for simple, service-level commands that don't parse into the
/// standard SQL AST (for example `USE WAREHOUSE <name>`). If handled,
/// return Some(message) to short-circuit normal parsing; otherwise
/// return None to continue normal processing.
pub async fn maybe_handle_direct_command(query: &str, session_id: &str, shared_data: &SharedData) -> Option<String> {
    let q_trim = query.trim();
    let q_lower = q_trim.to_lowercase();
    if q_lower.starts_with("use warehouse") {
        match use_warehouse::handle_use_warehouse(q_trim, session_id, shared_data).await {
            Ok(msg) => return Some(msg),
            Err(e) => return Some(format!("ERROR: {}", e)),
        }
    }
    None
}
