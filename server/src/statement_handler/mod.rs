pub(crate) mod create_database;
pub(crate) mod create_schema;
pub(crate) mod create_table;
pub(crate) mod distributed_dag;
pub(crate) mod helpers;
pub(crate) mod query_select;
pub(crate) mod rbac;
pub mod use_warehouse;

use crate::services::request_context::RequestContext;
use crate::warehouse::state::SharedData;
use kionas::parser::datafusion_sql::sqlparser::ast::Statement;
use std::collections::HashMap;

fn canonicalize_insert_table_name(raw_table_name: &str, default_schema: &str) -> String {
    let cleaned = raw_table_name.trim().trim_matches('"').trim_matches('`');
    if cleaned.is_empty() {
        return cleaned.to_string();
    }
    if cleaned.contains('.') {
        cleaned.to_string()
    } else {
        format!("deltalake.{}.{}", default_schema, cleaned)
    }
}

/// Discover worker address tied to session
pub async fn get_worker_addr_for_session(
    shared_data: &SharedData,
    session_id: &str,
) -> Option<String> {
    let state = shared_data.lock().await;
    // Get session
    let session_opt = state
        .session_manager
        .get_session(session_id.to_string())
        .await;
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
    log::info!(
        "Resolving worker for session {} -> warehouse='{}' uuid={}",
        session_id,
        session.get_warehouse(),
        worker_uuid
    );
    // Dump registered warehouses for diagnostics
    for (k, w) in warehouses_map.iter() {
        log::info!(
            "Registered warehouse key={} name={} host={} port={}",
            k,
            w.get_name(),
            w.get_host(),
            w.get_port()
        );
    }
    // Dump worker pools keys
    let worker_pools_map = state.worker_pools.lock().await;
    for (k, _) in worker_pools_map.iter() {
        log::info!("Worker pool key={}", k);
    }
    if let Some(warehouse) = warehouses_map.get(&worker_uuid) {
        let scheme = if warehouse.get_port() == 443 {
            "https"
        } else {
            "http"
        };
        let uri = format!(
            "{}://{}:{}",
            scheme,
            warehouse.get_host(),
            warehouse.get_port()
        );
        return Some(uri);
    }

    // Fallback: try to find by plain warehouse name
    for (_k, w) in warehouses_map.iter() {
        let plain = session.get_warehouse();
        let wname = w.get_name();
        let whost = w.get_host();
        if wname == plain || wname.ends_with(&plain) || wname.contains(&plain) || whost == plain {
            log::info!(
                "Fuzzy matched warehouse '{}' to session warehouse '{}'",
                wname,
                plain
            );
            let scheme = if w.get_port() == 443 { "https" } else { "http" };
            let uri = format!("{}://{}:{}", scheme, w.get_host(), w.get_port());
            return Some(uri);
        }
    }

    log::warn!(
        "Warehouse not found for session {} (uuid={})",
        session_id,
        worker_uuid
    );
    None
}

pub async fn handle_statement(
    stmt: &Statement,
    session_id: &str,
    ctx: &RequestContext,
    shared_data: &SharedData,
) -> String {
    match stmt {
        Statement::Insert(insert_stmt) => {
            // Server phase for INSERT: accept statement and dispatch a worker task.
            // We send the normalized SQL text as payload and keep operation explicit.
            let payload = stmt.to_string();
            let default_schema = {
                let state = shared_data.lock().await;
                match state
                    .session_manager
                    .get_session(session_id.to_string())
                    .await
                {
                    Some(s) => s.get_use_database(),
                    None => "default".to_string(),
                }
            };
            let mut params = HashMap::new();
            let table_name =
                canonicalize_insert_table_name(&insert_stmt.table.to_string(), &default_schema);
            params.insert("table_name".to_string(), table_name);

            match helpers::run_task_for_input_with_params(
                shared_data,
                session_id,
                "insert",
                payload,
                params,
                None,
                30,
            )
            .await
            {
                Ok(result_location) => {
                    format!("Insert dispatched successfully: {}", result_location)
                }
                Err(e) => format!("Failed to dispatch insert: {}", e),
            }
        }
        Statement::Query(_) => {
            let ast = match query_select::select_ast_from_statement(stmt) {
                Ok(parsed_ast) => parsed_ast,
                Err(e) => {
                    return format!("RESULT|VALIDATION|INVALID_QUERY_STATEMENT|{}", e);
                }
            };
            query_select::handle_select_query(shared_data, session_id, ctx, ast).await
        }
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        } => {
            create_schema::handle_create_schema(
                shared_data,
                session_id,
                create_schema::CreateSchemaAst {
                    schema_name,
                    if_not_exists: *if_not_exists,
                    with: with.as_ref(),
                    options: options.as_ref(),
                    default_collate_spec: default_collate_spec.as_ref(),
                    clone: clone.as_ref(),
                },
            )
            .await
        }
        Statement::CreateTable(create_table) => {
            create_table::handle_create_table(
                shared_data,
                session_id,
                create_table::CreateTableAst { create_table },
            )
            .await
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
            with_contacts,
            ..
        } => {
            create_database::handle_create_database(
                shared_data,
                session_id,
                create_database::CreateDatabaseAst {
                    db_name,
                    if_not_exists: *if_not_exists,
                    location: location.as_ref(),
                    managed_location: managed_location.as_ref(),
                    or_replace: *or_replace,
                    transient: *transient,
                    clone: clone.as_ref(),
                    data_retention_time_in_days: *data_retention_time_in_days,
                    max_data_extension_time_in_days: *max_data_extension_time_in_days,
                    external_volume: external_volume.as_ref(),
                    catalog: catalog.as_ref(),
                    replace_invalid_characters: *replace_invalid_characters,
                    default_ddl_collation: default_ddl_collation.as_ref(),
                    storage_serialization_policy: storage_serialization_policy.as_ref(),
                    comment: comment.as_ref(),
                    catalog_sync: catalog_sync.as_ref(),
                    catalog_sync_namespace_mode: catalog_sync_namespace_mode.as_ref(),
                    catalog_sync_namespace_flatten_delimiter:
                        catalog_sync_namespace_flatten_delimiter.as_ref(),
                    with_tags: with_tags.as_ref(),
                    with_contacts: with_contacts.as_ref(),
                },
            )
            .await
        }
        // Add more statement handlers here
        _ => "Unsupported statement".to_string(),
    }
}

/// Check for simple, service-level commands that don't parse into the
/// standard SQL AST (for example `USE WAREHOUSE <name>`). If handled,
/// return Some(message) to short-circuit normal parsing; otherwise
/// return None to continue normal processing.
pub async fn maybe_handle_direct_command(
    query: &str,
    session_id: &str,
    shared_data: &SharedData,
) -> Option<String> {
    if let Some(msg) = rbac::maybe_handle_rbac_direct_command(query, session_id, shared_data).await
    {
        return Some(msg);
    }

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
