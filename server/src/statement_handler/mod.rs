pub(crate) mod ddl;
pub(crate) mod dml;
pub(crate) mod query;
pub(crate) mod shared;
pub(crate) mod utility;

use crate::counters::{QueryGuard, QueryOutcome};
use crate::metrics::{PrometheusQueryGuard, PrometheusQueryStatus};
use crate::parser::datafusion_sql::sqlparser::ast::Statement;
use crate::services::request_context::RequestContext;
use crate::warehouse::state::SharedData;
use chrono::Utc;
use kionas::constants::REDIS_URL_ENV;
use kionas::redis_monitoring::{QueryStatus, QuerySummary, push_query_summary};
use kionas::utils::redact_sql;
use tokio::time::Instant;

/// What: Determine whether a statement should be counted as a dispatched query.
///
/// Inputs:
/// - `stmt`: Parsed SQL statement.
///
/// Output:
/// - `true` for statements routed through server query execution paths.
///
/// Details:
/// - Excludes utility statements (e.g. USE/SET) which are handled in direct-command path.
fn should_track_query_counter(stmt: &Statement) -> bool {
    matches!(stmt, Statement::Query(_) | Statement::Insert(_))
}

/// What: Evaluate whether a statement handler response encodes success.
///
/// Inputs:
/// - `result`: Structured or legacy handler output string.
///
/// Output:
/// - `true` when output prefix/category is `RESULT|SUCCESS|...`.
///
/// Details:
/// - Non-structured and non-success categories are treated as failed outcomes.
fn outcome_is_success(result: &str) -> bool {
    let mut parts = result.splitn(4, '|');
    matches!(
        (parts.next(), parts.next()),
        (Some("RESULT"), Some("SUCCESS"))
    )
}

/// What: Publish a redacted query history entry for dashboard playback.
///
/// Inputs:
/// - `ctx`: Request context containing query/session/user metadata.
/// - `stmt`: Parsed SQL statement.
/// - `result`: Statement handler output string.
/// - `duration_ms`: Optional query duration in milliseconds.
///
/// Output:
/// - No return value; failures are logged and ignored.
///
/// Details:
/// - This path is best-effort and must not block query responses.
async fn record_query_history(
    ctx: &RequestContext,
    stmt: &Statement,
    result: &str,
    duration_ms: Option<u64>,
) {
    let redis_url = std::env::var(REDIS_URL_ENV).unwrap_or_else(|_| kionas::get_redis_url_status());

    let client = match redis::Client::open(redis_url.as_str()) {
        Ok(client) => client,
        Err(error) => {
            log::warn!("failed to open Redis client for query history: {}", error);
            return;
        }
    };

    let mut connection = match client.get_connection_manager().await {
        Ok(connection) => connection,
        Err(error) => {
            log::warn!(
                "failed to acquire Redis connection manager for query history: {}",
                error
            );
            return;
        }
    };

    let succeeded = outcome_is_success(result);
    let status = if succeeded {
        QueryStatus::Succeeded
    } else {
        QueryStatus::Failed
    };

    let error_message = if succeeded {
        None
    } else {
        Some(result.chars().take(200).collect::<String>())
    };

    let summary = QuerySummary {
        query_id: ctx.query_id.clone(),
        session_id: ctx.session_id.clone(),
        user_id: ctx.rbac_user.clone(),
        warehouse_id: (!ctx.warehouse_name.is_empty()).then_some(ctx.warehouse_name.clone()),
        sql_digest: redact_sql(&stmt.to_string()),
        status,
        duration_ms,
        error: error_message,
        timestamp: Utc::now(),
    };

    if let Err(error) = push_query_summary(&mut connection, &summary).await {
        log::warn!("failed to push query summary to Redis history: {}", error);
    }
}

/// Discover worker address tied to session
#[allow(dead_code)]
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
    log::debug!(
        "Resolving worker for session {} -> warehouse='{}' uuid={}",
        session_id,
        session.get_warehouse(),
        worker_uuid
    );
    // Dump registered warehouses for diagnostics
    for (k, w) in warehouses_map.iter() {
        log::debug!(
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
        log::debug!("Worker pool key={}", k);
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
            log::debug!(
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

#[tracing::instrument(
    name = "query.dispatch",
    skip(stmt, ctx, shared_data),
    fields(
        query_id = tracing::field::Empty,
        session_id = %ctx.session_id,
        warehouse = %ctx.warehouse_name
    )
)]
pub async fn handle_statement(
    stmt: &Statement,
    session_id: &str,
    ctx: &RequestContext,
    shared_data: &SharedData,
) -> String {
    let should_track = should_track_query_counter(stmt);
    tracing::Span::current().record("query_id", ctx.query_id.as_str());

    let query_start = should_track.then(Instant::now);

    let (query_counters, session_manager, prometheus_metrics) = if should_track {
        let shared_data_ref = shared_data.lock().await;
        (
            Some(shared_data_ref.query_counters.clone()),
            Some(shared_data_ref.session_manager.clone()),
            Some(shared_data_ref.prometheus_metrics.clone()),
        )
    } else {
        (None, None, None)
    };

    let mut query_guard = query_counters.as_ref().map(QueryGuard::acquire);
    let mut prometheus_query_guard = prometheus_metrics
        .as_ref()
        .map(|metrics| PrometheusQueryGuard::acquire(metrics.clone(), ctx.warehouse_name.as_str()));

    let result = match stmt {
        Statement::Insert(insert_stmt) => {
            dml::insert::handle_insert_statement(shared_data, session_id, stmt, insert_stmt).await
        }
        Statement::Query(_) => {
            let ast = match query::select::select_ast_from_statement(stmt) {
                Ok(parsed_ast) => parsed_ast,
                Err(e) => {
                    return format!("RESULT|VALIDATION|INVALID_QUERY_STATEMENT|{}", e);
                }
            };
            query::select::handle_select_query(shared_data, session_id, ctx, ast).await
        }
        Statement::CreateSchema {
            schema_name,
            if_not_exists,
            with,
            options,
            default_collate_spec,
            clone,
        } => {
            ddl::create_schema::handle_create_schema(
                shared_data,
                session_id,
                ddl::create_schema::CreateSchemaAst {
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
            ddl::create_table::handle_create_table(
                shared_data,
                session_id,
                ddl::create_table::CreateTableAst { create_table },
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
            ddl::create_database::handle_create_database(
                shared_data,
                session_id,
                ddl::create_database::CreateDatabaseAst {
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
    };

    let succeeded = outcome_is_success(result.as_str());

    if let Some(guard) = query_guard.as_mut()
        && succeeded
    {
        guard.outcome = QueryOutcome::Succeeded;
    }

    if let Some(guard) = prometheus_query_guard.as_mut()
        && succeeded
    {
        guard.status = PrometheusQueryStatus::Success;
    }

    let duration_ms = query_start
        .as_ref()
        .map(|start| u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX));

    if let (Some(start), Some(session_manager)) = (query_start, session_manager) {
        let duration_ms = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);

        if let Some(mut session) = session_manager.get_session(ctx.session_id.clone()).await {
            session.record_query_completion(duration_ms, succeeded);
            if let Err(error) = session_manager
                .update_session(session_id.to_string(), &session)
                .await
            {
                log::warn!(
                    "failed to update session observability for session_id={}: {}",
                    session_id,
                    error
                );
            }
        }
    }

    if should_track {
        record_query_history(ctx, stmt, result.as_str(), duration_ms).await;
    }

    result
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
    if let Some(msg) =
        utility::rbac::maybe_handle_rbac_direct_command(query, session_id, shared_data).await
    {
        return Some(msg);
    }

    let q_trim = query.trim();
    let q_lower = q_trim.to_lowercase();
    if q_lower.starts_with("use warehouse") {
        match utility::use_warehouse::handle_use_warehouse(q_trim, session_id, shared_data).await {
            Ok(msg) => return Some(msg),
            Err(e) => return Some(format!("ERROR: {}", e)),
        }
    }
    None
}
