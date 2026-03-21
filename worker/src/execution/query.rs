#![allow(dead_code)]

use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;

/// What: Canonical query namespace extracted from server-generated payload.
///
/// Inputs:
/// - `database`: Canonical database identifier.
/// - `schema`: Canonical schema identifier.
/// - `table`: Canonical table identifier.
///
/// Output:
/// - Structured namespace values used to compose deterministic result locations.
#[derive(Debug)]
pub(crate) struct QueryNamespace {
    pub(crate) database: String,
    pub(crate) schema: String,
    pub(crate) table: String,
}

/// What: Normalize an identifier into a canonical lowercase value.
///
/// Inputs:
/// - `raw`: Unnormalized identifier text.
///
/// Output:
/// - Canonical identifier stripped from common quote wrappers.
fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

/// What: Validate namespace naming rules for query namespace parts.
///
/// Inputs:
/// - `name`: Canonical database, schema, or table name.
///
/// Output:
/// - `true` when the name is valid.
fn is_valid_namespace_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

/// What: Extract canonical operator names from a serialized physical plan payload.
///
/// Inputs:
/// - `operators`: JSON array of physical operators.
///
/// Output:
/// - Ordered operator name list.
///
/// Details:
/// - Physical operators use serde enum encoding:
///   - data-bearing variants serialize as externally tagged objects
///   - unit variants (for example `Materialize`) serialize as plain strings.
fn operator_names_from_payload(operators: &[serde_json::Value]) -> Result<Vec<String>, String> {
    let mut names = Vec::with_capacity(operators.len());
    for op in operators {
        if let Some(name) = op.as_str() {
            names.push(name.to_string());
            continue;
        }

        let obj = op
            .as_object()
            .ok_or_else(|| "physical_plan operator must be an object or string".to_string())?;
        let name = obj
            .keys()
            .next()
            .cloned()
            .ok_or_else(|| "physical_plan operator object cannot be empty".to_string())?;
        names.push(name);
    }
    Ok(names)
}

/// What: Validate that physical operators belong to the supported Phase 2 set.
///
/// Inputs:
/// - `names`: Ordered operator names extracted from payload.
///
/// Output:
/// - `Ok(())` when all operators are supported in this phase.
/// - `Err(message)` describing the first unsupported operator.
fn validate_supported_operator_set(names: &[String]) -> Result<(), String> {
    for name in names {
        match name.as_str() {
            "TableScan" | "Filter" | "Projection" | "HashJoin" | "Sort" | "Limit"
            | "AggregatePartial" | "AggregateFinal" | "Materialize" => {}
            other => {
                return Err(format!(
                    "physical operator '{}' is not supported in this phase",
                    other
                ));
            }
        }
    }
    Ok(())
}

/// What: Detect deferred predicate families in raw predicate SQL strings.
///
/// Inputs:
/// - `sql`: Raw predicate SQL text from payload.
///
/// Output:
/// - `Some(label)` for unsupported predicate family names.
/// - `None` for supported predicate subset.
fn detect_deferred_predicate_label(sql: &str) -> Option<&'static str> {
    let normalized = sql.to_ascii_lowercase();
    let deferred = [
        (" between ", "BETWEEN"),
        (" in (", "IN list"),
        (" like ", "LIKE"),
        (" ilike ", "ILIKE"),
        (" exists", "EXISTS"),
        (" any ", "ANY"),
        (" all ", "ALL"),
        (" over (", "window function"),
    ];

    for (needle, label) in deferred {
        if normalized.contains(needle) {
            return Some(label);
        }
    }
    None
}

/// What: Validate filter predicate payloads against the supported Phase 2 subset.
///
/// Inputs:
/// - `operators`: Physical operators from payload.
///
/// Output:
/// - `Ok(())` when all filter predicates are supported.
/// - `Err(message)` when a deferred predicate family is detected.
fn validate_filter_predicates(operators: &[serde_json::Value]) -> Result<(), String> {
    for op in operators {
        if op.is_string() {
            continue;
        }

        let obj = op
            .as_object()
            .ok_or_else(|| "physical_plan operator must be an object or string".to_string())?;
        if let Some(filter_payload) = obj.get("Filter") {
            let predicate = filter_payload
                .get("predicate")
                .ok_or_else(|| "Filter operator missing predicate".to_string())?;
            if let Some(sql) = predicate
                .get("Raw")
                .and_then(|raw| raw.get("sql"))
                .and_then(serde_json::Value::as_str)
                && let Some(label) = detect_deferred_predicate_label(sql)
            {
                return Err(format!(
                    "predicate is not supported in this phase: {}",
                    label
                ));
            }
        }
    }

    Ok(())
}

/// What: Validate the phase-2 physical pipeline shape encoded in the query payload.
///
/// Inputs:
/// - `operators`: Ordered physical operator array from `physical_plan.operators`.
///
/// Output:
/// - `Ok(())` when the payload follows phase-2 linear pipeline invariants.
/// - `Err(message)` with actionable validation details otherwise.
fn validate_physical_pipeline_shape(operators: &[serde_json::Value]) -> Result<(), String> {
    let names = operator_names_from_payload(operators)?;
    validate_supported_operator_set(&names)?;

    if names.first().map(std::string::String::as_str) != Some("TableScan") {
        return Err("physical_plan must start with TableScan".to_string());
    }
    if names.last().map(std::string::String::as_str) != Some("Materialize") {
        return Err("physical_plan must end with Materialize".to_string());
    }

    let projection_count = names
        .iter()
        .filter(|name| name.as_str() == "Projection")
        .count();
    if projection_count != 1 {
        return Err("physical_plan must include exactly one Projection operator".to_string());
    }

    let sort_count = names.iter().filter(|name| name.as_str() == "Sort").count();
    if sort_count > 1 {
        return Err("physical_plan must include at most one Sort operator".to_string());
    }

    if let Some(sort_index) = names.iter().position(|name| name.as_str() == "Sort") {
        let projection_index = names
            .iter()
            .position(|name| name.as_str() == "Projection")
            .ok_or_else(|| "physical_plan must include Projection".to_string())?;
        if sort_index < projection_index {
            return Err("physical_plan Sort must appear after Projection".to_string());
        }
    }

    let hash_join_count = names
        .iter()
        .filter(|name| name.as_str() == "HashJoin")
        .count();
    if hash_join_count > 1 {
        return Err("physical_plan must include at most one HashJoin operator".to_string());
    }

    let aggregate_partial_count = names
        .iter()
        .filter(|name| name.as_str() == "AggregatePartial")
        .count();
    if aggregate_partial_count > 1 {
        return Err("physical_plan must include at most one AggregatePartial operator".to_string());
    }

    let aggregate_final_count = names
        .iter()
        .filter(|name| name.as_str() == "AggregateFinal")
        .count();
    if aggregate_final_count > 1 {
        return Err("physical_plan must include at most one AggregateFinal operator".to_string());
    }

    if aggregate_partial_count != aggregate_final_count {
        return Err(
            "physical_plan AggregatePartial and AggregateFinal operators must appear together"
                .to_string(),
        );
    }

    if let Some(aggregate_partial_index) = names
        .iter()
        .position(|name| name.as_str() == "AggregatePartial")
    {
        let aggregate_final_index = names
            .iter()
            .position(|name| name.as_str() == "AggregateFinal")
            .ok_or_else(|| {
                "physical_plan AggregateFinal is required when AggregatePartial is present"
                    .to_string()
            })?;

        let projection_index = names
            .iter()
            .position(|name| name.as_str() == "Projection")
            .ok_or_else(|| "physical_plan must include Projection".to_string())?;

        if aggregate_partial_index >= aggregate_final_index {
            return Err(
                "physical_plan AggregatePartial must appear before AggregateFinal".to_string(),
            );
        }

        if aggregate_final_index >= projection_index {
            return Err("physical_plan AggregateFinal must appear before Projection".to_string());
        }

        if let Some(hash_join_index) = names.iter().position(|name| name.as_str() == "HashJoin")
            && aggregate_partial_index < hash_join_index
        {
            return Err("physical_plan AggregatePartial must appear after HashJoin".to_string());
        }
    }

    if let Some(hash_join_index) = names.iter().position(|name| name.as_str() == "HashJoin") {
        let projection_index = names
            .iter()
            .position(|name| name.as_str() == "Projection")
            .ok_or_else(|| "physical_plan must include Projection".to_string())?;
        if hash_join_index > projection_index {
            return Err("physical_plan HashJoin must appear before Projection".to_string());
        }

        if let Some(sort_index) = names.iter().position(|name| name.as_str() == "Sort")
            && hash_join_index > sort_index
        {
            return Err("physical_plan HashJoin must appear before Sort".to_string());
        }
    }

    let limit_count = names.iter().filter(|name| name.as_str() == "Limit").count();
    if limit_count > 1 {
        return Err("physical_plan must include at most one Limit operator".to_string());
    }

    if let Some(limit_index) = names.iter().position(|name| name.as_str() == "Limit") {
        let projection_index = names
            .iter()
            .position(|name| name.as_str() == "Projection")
            .ok_or_else(|| "physical_plan must include Projection".to_string())?;
        if limit_index < projection_index {
            return Err("physical_plan Limit must appear after Projection".to_string());
        }

        if let Some(sort_index) = names.iter().position(|name| name.as_str() == "Sort")
            && limit_index < sort_index
        {
            return Err("physical_plan Limit must appear after Sort".to_string());
        }
    }

    validate_filter_predicates(operators)?;

    Ok(())
}

/// What: Resolve flight port for query result references.
///
/// Inputs:
/// - `default_worker_port`: Worker interops port.
///
/// Output:
/// - Worker flight port with env override fallback.
fn resolve_worker_flight_port(default_worker_port: u32) -> u32 {
    std::env::var("WORKER_FLIGHT_PORT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|p| *p > 0)
        .unwrap_or(default_worker_port.saturating_add(1))
}

/// What: Resolve query-handle endpoint host and port with optional proxy overrides.
///
/// Inputs:
/// - `proxy_host`: Optional proxy host override.
/// - `proxy_port`: Optional proxy port override.
/// - `default_host`: Worker host used when proxy host is absent.
/// - `default_worker_port`: Worker interops port used to derive worker Flight default.
///
/// Output:
/// - `(host, port)` endpoint tuple for query-handle generation.
fn resolve_query_result_endpoint_with_overrides(
    proxy_host: Option<&str>,
    proxy_port: Option<u32>,
    default_host: &str,
    default_worker_port: u32,
) -> (String, u32) {
    let host = proxy_host
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(default_host)
        .to_string();

    let port = proxy_port
        .filter(|p| *p > 0)
        .unwrap_or_else(|| resolve_worker_flight_port(default_worker_port));

    (host, port)
}

/// What: Resolve query-handle endpoint host and port from environment.
///
/// Inputs:
/// - `default_host`: Worker host fallback.
/// - `default_worker_port`: Worker interops port fallback.
///
/// Output:
/// - `(host, port)` endpoint tuple.
///
/// Details:
/// - `FLIGHT_PROXY_HOST` and `FLIGHT_PROXY_PORT` allow routing retrieval through proxy.
/// - Falls back to worker Flight endpoint when overrides are not set.
fn resolve_query_result_endpoint(default_host: &str, default_worker_port: u32) -> (String, u32) {
    let proxy_host = std::env::var("FLIGHT_PROXY_HOST").ok();
    let proxy_port = std::env::var("FLIGHT_PROXY_PORT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok());

    resolve_query_result_endpoint_with_overrides(
        proxy_host.as_deref(),
        proxy_port,
        default_host,
        default_worker_port,
    )
}

/// What: Resolve canonical namespace from stage task params.
///
/// Inputs:
/// - `task`: Query task carrying stage params.
///
/// Output:
/// - Canonical namespace values used for deterministic result paths.
fn resolve_query_namespace_from_params(
    task: &worker_service::Task,
) -> Result<QueryNamespace, String> {
    let database = task
        .params
        .get("database_name")
        .map(|value| normalize_identifier(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "query stage task missing database_name param".to_string())?;
    let schema = task
        .params
        .get("schema_name")
        .map(|value| normalize_identifier(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "query stage task missing schema_name param".to_string())?;
    let table = task
        .params
        .get("table_name")
        .map(|value| normalize_identifier(value))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "query stage task missing table_name param".to_string())?;

    for value in [&database, &schema, &table] {
        if !is_valid_namespace_name(value) {
            return Err(format!(
                "invalid namespace value '{}': only [a-zA-Z0-9_-] are allowed",
                value
            ));
        }
    }

    Ok(QueryNamespace {
        database,
        schema,
        table,
    })
}

/// What: Determine whether the task is dispatched as a distributed stage.
///
/// Inputs:
/// - `task`: Query task metadata and params.
///
/// Output:
/// - `true` when stage params are present.
fn is_stage_query_task(task: &worker_service::Task) -> bool {
    task.params
        .get("stage_id")
        .is_some_and(|value| !value.trim().is_empty())
}

/// What: Parse and validate canonical SELECT query payload.
///
/// Inputs:
/// - `task`: Worker task carrying server-generated canonical query payload.
///
/// Output:
/// - Canonical `QueryNamespace` used to build deterministic result locations.
///
/// Details:
/// - Enforces payload invariants needed by this phase:
///   - `version` must be `2`.
///   - `statement` must be `Select`.
///   - `namespace.database/schema/table` must exist and be valid.
///   - `logical_plan` must be present as an object.
///   - `physical_plan` must be present as an object with a non-empty `operators` array.
fn resolve_query_namespace(task: &worker_service::Task) -> Result<QueryNamespace, String> {
    let parsed: serde_json::Value =
        serde_json::from_str(&task.input).map_err(|e| format!("invalid query payload: {}", e))?;

    let version = parsed
        .get("version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "query payload missing version".to_string())?;
    if version != 2 {
        return Err(format!("unsupported query payload version: {}", version));
    }

    let statement = parsed
        .get("statement")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "query payload missing statement".to_string())?;
    if statement != "Select" {
        return Err(format!(
            "unsupported query statement '{}': only Select is allowed",
            statement
        ));
    }

    let namespace = parsed
        .get("namespace")
        .ok_or_else(|| "query payload missing namespace".to_string())?;

    if !parsed
        .get("logical_plan")
        .is_some_and(serde_json::Value::is_object)
    {
        return Err("query payload missing logical_plan object".to_string());
    }

    let physical_plan = parsed
        .get("physical_plan")
        .filter(|v| v.is_object())
        .ok_or_else(|| "query payload missing physical_plan object".to_string())?;

    let operators = physical_plan
        .get("operators")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "query payload missing physical_plan.operators array".to_string())?;
    if operators.is_empty() {
        return Err("query payload has empty physical_plan.operators".to_string());
    }
    validate_physical_pipeline_shape(operators)?;

    let read_part = |key: &str| {
        namespace
            .get(key)
            .and_then(serde_json::Value::as_str)
            .map(normalize_identifier)
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| format!("query payload missing namespace.{}", key))
    };

    let database = read_part("database")?;
    let schema = read_part("schema")?;
    let table = read_part("table")?;

    for value in [&database, &schema, &table] {
        if !is_valid_namespace_name(value) {
            return Err(format!(
                "invalid namespace value '{}': only [a-zA-Z0-9_-] are allowed",
                value
            ));
        }
    }

    Ok(QueryNamespace {
        database,
        schema,
        table,
    })
}

/// What: Build deterministic query result location for the current phase.
///
/// Inputs:
/// - `shared`: Worker state carrying worker identity and ports.
/// - `task`: Worker task metadata.
/// - `session_id`: Session id associated with the task request.
/// - `namespace`: Canonical query namespace.
///
/// Output:
/// - Flight-style deterministic result location string.
fn build_result_location(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
    namespace: &QueryNamespace,
) -> String {
    let (endpoint_host, endpoint_port) =
        resolve_query_result_endpoint(&shared.worker_info.host, shared.worker_info.port);

    let ticket_opt = task.params.get("__auth_scope").and_then(|scope| {
        let rbac_user = task.params.get("__rbac_user")?.clone();
        let rbac_role = task.params.get("__rbac_role")?.clone();
        let ctx = crate::authz::DispatchAuthContext {
            session_id: session_id.to_string(),
            rbac_user,
            rbac_role,
            auth_scope: scope.clone(),
            query_id: task.params.get("__query_id").cloned().unwrap_or_default(),
        };
        crate::authz::WorkerAuthorizer::new()
            .issue_signed_flight_ticket(&ctx, &task.task_id, &shared.worker_info.worker_id)
            .ok()
    });

    let mut handle = format!(
        "flight://{}:{}/query/{}/{}/{}/{}?session_id={}&task_id={}",
        endpoint_host,
        endpoint_port,
        namespace.database,
        namespace.schema,
        namespace.table,
        shared.worker_info.worker_id,
        session_id,
        task.task_id
    );

    if let Some(ticket) = ticket_opt {
        handle.push_str("&ticket=");
        handle.push_str(&ticket);
    }

    handle
}

/// What: Execute query task with local worker pipeline and deterministic result handle.
///
/// Inputs:
/// - `shared`: Worker state containing worker identity and flight endpoint config.
/// - `task`: Worker task containing canonical SELECT payload.
/// - `session_id`: Session id propagated from task request.
///
/// Output:
/// - `Ok(result_location)` when payload is validated and local execution artifacts are persisted.
/// - `Err(message)` when payload validation or execution fails.
///
/// Details:
/// - Preserves existing query handle contract while replacing stub behavior.
/// - Runtime execution supports scan/filter/projection/sort/limit/materialize subset only.
pub(crate) async fn execute_query_task_stub(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
) -> Result<String, String> {
    let namespace = if is_stage_query_task(task) {
        resolve_query_namespace_from_params(task)?
    } else {
        resolve_query_namespace(task)?
    };
    let result_location = build_result_location(shared, task, session_id, &namespace);

    crate::execution::pipeline::execute_query_task(
        shared,
        task,
        session_id,
        &namespace,
        &result_location,
    )
    .await?;

    Ok(result_location)
}

#[cfg(test)]
#[path = "../tests/execution_query_tests.rs"]
mod tests;
