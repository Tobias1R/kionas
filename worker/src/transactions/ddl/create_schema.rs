use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;

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

/// What: Validate namespace naming rules for schema creation.
///
/// Inputs:
/// - `name`: Canonical database or schema name.
///
/// Output:
/// - `true` when the name is valid.
fn is_valid_namespace_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

/// What: Resolve canonical `(database_name, schema_name)` from worker task payload.
///
/// Inputs:
/// - `task`: Worker task containing serialized create_schema payload.
///
/// Output:
/// - Canonical `(database_name, schema_name)` tuple.
fn resolve_namespace(
    task: &worker_service::StagePartitionExecution,
) -> Result<(String, String), String> {
    let parsed: serde_json::Value = serde_json::from_str(&task.input)
        .map_err(|e| format!("invalid create_schema payload: {}", e))?;

    let database_name = parsed
        .get("namespace")
        .and_then(|v| v.get("database"))
        .and_then(serde_json::Value::as_str)
        .map(normalize_identifier)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "create_schema payload missing namespace.database".to_string())?;

    let schema_name = parsed
        .get("namespace")
        .and_then(|v| v.get("schema"))
        .and_then(serde_json::Value::as_str)
        .map(normalize_identifier)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "create_schema payload missing namespace.schema".to_string())?;

    if !is_valid_namespace_name(&database_name) {
        return Err(format!(
            "invalid database name '{}': only [a-zA-Z0-9_-] are allowed",
            database_name
        ));
    }
    if !is_valid_namespace_name(&schema_name) {
        return Err(format!(
            "invalid schema name '{}': only [a-zA-Z0-9_-] are allowed",
            schema_name
        ));
    }

    Ok((database_name, schema_name))
}

/// What: Execute create_schema storage initialization on the worker.
///
/// Inputs:
/// - `shared`: Worker shared state containing configured storage provider and cluster info.
/// - `task`: Worker task payload containing typed CREATE SCHEMA fields.
///
/// Output:
/// - `Ok(location)` when schema marker/path was initialized or already exists.
/// - `Err(message)` when payload is invalid or path invariants fail.
///
/// Details:
/// - The schema marker is always nested under the parent database path.
/// - Existing markers are treated as idempotent success.
pub(crate) async fn execute_create_schema_task(
    shared: &SharedData,
    task: &worker_service::StagePartitionExecution,
) -> Result<String, String> {
    let (database_name, schema_name) = resolve_namespace(task)?;
    let marker_key = format!(
        "databases/{}/schemas/{}/_kionas_schema.json",
        database_name, schema_name
    );

    if let Some(provider) = &shared.storage_provider {
        match provider.get_object(&marker_key).await {
            Ok(Some(_)) => return Ok(format!("storage://{}", marker_key)),
            Ok(None) => {}
            Err(e) => {
                log::warn!(
                    "create_schema marker pre-check failed for '{}': {}; proceeding with create",
                    marker_key,
                    e
                );
            }
        }

        let marker = serde_json::json!({
            "database_name": database_name,
            "schema_name": schema_name,
            "task_id": task.task_id,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|e| format!("marker encode failed: {}", e))?;

        provider
            .put_object(&marker_key, marker_bytes)
            .await
            .map_err(|e| format!("failed to create schema marker '{}': {}", marker_key, e))?;

        return Ok(format!("storage://{}", marker_key));
    }

    let schema_path = format!(
        "worker_storage/databases/{}/schemas/{}",
        database_name, schema_name
    );
    tokio::fs::create_dir_all(&schema_path)
        .await
        .map_err(|e| format!("failed to create schema directory '{}': {}", schema_path, e))?;

    let marker_path = format!("{}/_kionas_schema.json", schema_path);
    if tokio::fs::metadata(&marker_path).await.is_err() {
        let marker = serde_json::json!({
            "database_name": database_name,
            "schema_name": schema_name,
            "task_id": task.task_id,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|e| format!("marker encode failed: {}", e))?;

        tokio::fs::write(&marker_path, marker_bytes)
            .await
            .map_err(|e| format!("failed to write schema marker '{}': {}", marker_path, e))?;
    }

    Ok(schema_path)
}
