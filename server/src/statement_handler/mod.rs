pub(crate) mod helpers;
pub mod use_warehouse;

use kionas::parser::datafusion_sql::sqlparser::ast::{ObjectName, Statement};
use crate::services::metastore_client::metastore_service as ms;
use crate::services::metastore_client::MetastoreClient;
use crate::warehouse::state::SharedData;
use crate::core::DomainService;
use crate::session::Session;
use crate::warehouse::Warehouse;
use crate::transactions::Maestro;
use crate::transactions::Participant;

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
                Ok(catalog) => {
                    // Transactional create: coordinate with Maestro
                    match helpers::resolve_session_and_key(shared_data, session_id).await {
                        Ok((_session, key)) => {
                            let participant = Participant { id: key.clone(), target: key.clone(), staging_prefix: format!("schema-{}", schema_name.to_string()) };
                            let maestro = Maestro::new(shared_data.clone());
                            match maestro.execute_transaction(vec![participant], 3, 120).await {
                                Ok(tx) => {
                                    // Persist schema metadata
                                    let mreq = ms::MetastoreRequest { action: Some(ms::metastore_request::Action::CreateSchema(ms::CreateSchemaRequest { schema_name: schema_name.to_string() })) };
                                    match MetastoreClient::connect_with_shared(shared_data).await {
                                        Ok(mut client) => match client.execute(mreq).await {
                                            Ok(_) => format!("Schema created successfully (tx={}): {}", tx, schema_name),
                                            Err(e) => format!("Schema created but failed to persist in metastore: {}", e),
                                        },
                                        Err(e) => format!("Schema created but failed to connect to metastore: {}", e),
                                    }
                                }
                                Err(e) => format!("Failed to create schema transactionally: {}", e),
                            }
                        }
                        Err(e) => format!("Failed to resolve worker for schema create: {}", e),
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
                Ok(table) => {
                    // Resolve participant (single-worker for now)
                    match helpers::resolve_session_and_key(shared_data, session_id).await {
                        Ok((_session, key)) => {
                            let table_name = table.name.to_string();
                            let participant = Participant { id: key.clone(), target: key.clone(), staging_prefix: format!("table-{}", table_name) };
                            let maestro = Maestro::new(shared_data.clone());
                            match maestro.execute_transaction(vec![participant], 3, 180).await {
                                Ok(tx) => {
                                    // Persist table metadata to metastore
                                    let creq = ms::CreateTableRequest {
                                        schema_name: default_schema.clone(),
                                        table_name: table_name.clone(),
                                        engine: String::new(),
                                        columns: Vec::new(),
                                    };
                                    let mreq = ms::MetastoreRequest { action: Some(ms::metastore_request::Action::CreateTable(creq)) };
                                    match MetastoreClient::connect_with_shared(shared_data).await {
                                        Ok(mut client) => match client.execute(mreq).await {
                                            Ok(_) => format!("Table created successfully (tx={}): {}", tx, table_name),
                                            Err(e) => format!("Table committed but failed to persist in metastore: {}", e),
                                        },
                                        Err(e) => format!("Table committed but failed to connect to metastore: {}", e),
                                    }
                                }
                                Err(e) => format!("Failed to create table transactionally: {}", e),
                            }
                        }
                        Err(e) => format!("Failed to resolve worker for table create: {}", e),
                    }
                }
                Err(e) => format!("Domain validation failed: {}", e),
            }
        }
        
        Statement::CreateDatabase {
            db_name, 
            if_not_exists, 
            location,
            managed_location,
            or_replace,
            transient,
            clone,
            data_retention_time_in_days,
            max_data_extension_time_in_days,
            external_volume,
            catalog,
            replace_invalid_characters,
            default_ddl_collation,
            storage_serialization_policy,
            comment,
            catalog_sync,
            catalog_sync_namespace_mode,
            catalog_sync_namespace_flatten_delimiter,
            with_tags,
            with_contacts
             , .. } => {
            // Build domain object from AST and validate
            let owner = {
                let state = shared_data.lock().await;
                match state.session_manager.get_session(session_id.to_string()).await {
                    Some(s) => s.get_role(),
                    None => "unknown".to_string(),
                }
            };
            match DomainService::from_create_database(db_name, &owner) {
                Ok(db) => {
                    match helpers::resolve_session_and_key(shared_data, session_id).await {
                        Ok((_session, key)) => {
                            let participant = Participant { id: key.clone(), target: key.clone(), staging_prefix: format!("database-{}", db_name.to_string()) };
                            let maestro = Maestro::new(shared_data.clone());
                            match maestro.execute_transaction(vec![participant], 3, 180).await {
                                Ok(tx) => {
                                    let mreq = ms::MetastoreRequest { action: Some(ms::metastore_request::Action::CreateSchema(ms::CreateSchemaRequest { schema_name: db_name.to_string() })) };
                                    match MetastoreClient::connect_with_shared(shared_data).await {
                                        Ok(mut client) => match client.execute(mreq).await {
                                            Ok(_) => format!("Database created successfully (tx={}): {}", tx, db_name),
                                            Err(e) => format!("Database committed but failed to persist in metastore: {}", e),
                                        },
                                        Err(e) => format!("Database committed but failed to connect to metastore: {}", e),
                                    }
                                }
                                Err(e) => format!("Failed to create database transactionally: {}", e),
                            }
                        }
                        Err(e) => format!("Failed to resolve worker for database create: {}", e),
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
