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
            "TableScan" | "Filter" | "Projection" | "Materialize" => {}
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
            {
                if let Some(label) = detect_deferred_predicate_label(sql) {
                    return Err(format!(
                        "predicate is not supported in this phase: {}",
                        label
                    ));
                }
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
/// - Runtime execution supports scan/filter/projection/materialize subset only.
pub(crate) async fn execute_query_task_stub(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
) -> Result<String, String> {
    let namespace = resolve_query_namespace(task)?;
    let result_location = build_result_location(shared, task, session_id, &namespace);

    crate::services::query_execution::execute_query_task(
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
mod tests {
    use super::resolve_query_namespace;

    #[test]
    fn accepts_unit_variant_string_operator_shape() {
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
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let namespace = resolve_query_namespace(&task).expect("must accept unit variant shape");
        assert_eq!(namespace.database, "db1");
    }

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
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
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
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "public", "table": "users"}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
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

    #[test]
    fn rejects_missing_physical_plan_for_v2() {
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
                },
                "logical_plan": {}
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err =
            resolve_query_namespace(&task).expect_err("must reject missing physical_plan object");
        assert!(err.contains("missing physical_plan"));
    }

    #[test]
    fn rejects_empty_physical_plan_operators_for_v2() {
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
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": []
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err = resolve_query_namespace(&task)
            .expect_err("must reject empty physical_plan operators for phase 2 payloads");
        assert!(err.contains("empty physical_plan.operators"));
    }

    #[test]
    fn rejects_physical_plan_without_tablescan_first() {
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
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err = resolve_query_namespace(&task)
            .expect_err("must reject non-conforming phase 2 pipeline shape");
        assert!(err.contains("start with TableScan"));
    }

    #[test]
    fn rejects_physical_plan_without_materialize_last() {
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
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}}
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err = resolve_query_namespace(&task)
            .expect_err("must reject pipeline missing final materialize");
        assert!(err.contains("end with Materialize"));
    }

    #[test]
    fn rejects_physical_plan_without_single_projection() {
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
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err =
            resolve_query_namespace(&task).expect_err("must reject pipeline without projection");
        assert!(err.contains("exactly one Projection"));
    }

    #[test]
    fn rejects_deferred_operator_variant_in_worker_payload() {
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
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        {"HashJoin": {"on": []}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err = resolve_query_namespace(&task)
            .expect_err("must reject deferred physical operator in worker payload");
        assert!(err.contains("HashJoin"));
    }

    #[test]
    fn rejects_deferred_predicate_variant_in_worker_payload() {
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
                },
                "logical_plan": {},
                "physical_plan": {
                    "operators": [
                        {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                        {"Filter": {"predicate": {"Raw": {"sql": "name LIKE 'a%'"}}}},
                        {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                        "Materialize"
                    ]
                }
            })
            .to_string(),
            output: String::new(),
            params: std::collections::HashMap::new(),
        };

        let err = resolve_query_namespace(&task)
            .expect_err("must reject deferred predicate in worker payload");
        assert!(err.contains("predicate is not supported"));
        assert!(err.contains("LIKE"));
    }
}
