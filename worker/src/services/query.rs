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
struct QueryNamespace {
    database: String,
    schema: String,
    table: String,
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
    let flight_port = resolve_worker_flight_port(shared.worker_info.port);
    format!(
        "flight://{}:{}/query/{}/{}/{}/{}?session_id={}&task_id={}",
        shared.worker_info.host,
        flight_port,
        namespace.database,
        namespace.schema,
        namespace.table,
        shared.worker_info.worker_id,
        session_id,
        task.task_id
    )
}

/// What: Execute query task stub for initial server-to-worker integration.
///
/// Inputs:
/// - `shared`: Worker state containing worker identity and flight endpoint config.
/// - `task`: Worker task containing canonical SELECT payload.
/// - `session_id`: Session id propagated from task request.
///
/// Output:
/// - `Ok(result_location)` when payload is accepted and deterministic result endpoint is built.
/// - `Err(message)` when payload validation fails.
///
/// Details:
/// - This phase does not execute scans/shuffles/reductions yet.
/// - It validates server payload contract and returns a deterministic query handle.
pub(crate) async fn execute_query_task_stub(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
) -> Result<String, String> {
    let namespace = resolve_query_namespace(task)?;
    Ok(build_result_location(shared, task, session_id, &namespace))
}

#[cfg(test)]
mod tests {
    use super::resolve_query_namespace;

    #[test]
    fn rejects_non_select_statement() {
        let task = crate::services::worker_service_server::worker_service::Task {
            task_id: "t1".to_string(),
            operation: "query".to_string(),
            input: serde_json::json!({
                "version": 2,
                "statement": "Insert",
                "namespace": {
                    "database": "db1",
                    "schema": "s1",
                    "table": "t1"
                },
                "logical_plan": {}
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err = resolve_query_namespace(&task).expect_err("must reject non-select statement");
        assert!(err.contains("only Select"));
    }

    #[test]
    fn accepts_valid_select_namespace() {
        let task = crate::services::worker_service_server::worker_service::Task {
            task_id: "t1".to_string(),
            operation: "query".to_string(),
            input: serde_json::json!({
                "version": 2,
                "statement": "Select",
                "namespace": {
                    "database": "DB1",
                    "schema": "Public",
                    "table": "Users"
                },
                "logical_plan": {}
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let namespace = resolve_query_namespace(&task).expect("must parse namespace");
        assert_eq!(namespace.database, "db1");
        assert_eq!(namespace.schema, "public");
        assert_eq!(namespace.table, "users");
    }

    #[test]
    fn rejects_missing_logical_plan_for_v2() {
        let task = crate::services::worker_service_server::worker_service::Task {
            task_id: "t1".to_string(),
            operation: "query".to_string(),
            input: serde_json::json!({
                "version": 2,
                "statement": "Select",
                "namespace": {
                    "database": "db1",
                    "schema": "s1",
                    "table": "t1"
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err =
            resolve_query_namespace(&task).expect_err("must reject missing logical_plan object");
        assert!(err.contains("missing logical_plan"));
    }
}
