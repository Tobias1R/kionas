use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;

/// What: Normalize an identifier into a canonical lowercase value.
///
/// Inputs:
/// - `raw`: Unnormalized identifier text.
///
/// Output:
/// - Canonical identifier stripped from common quote wrappers.
///
/// Details:
/// - This keeps server and worker path derivation behavior deterministic.
fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

/// What: Validate database naming rules used by the first create-database implementation.
///
/// Inputs:
/// - `name`: Canonical database name.
///
/// Output:
/// - `true` if the name is acceptable.
///
/// Details:
/// - Accepted characters are `[a-zA-Z0-9_-]`.
fn is_valid_database_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

/// What: Extract a canonical database name from worker task payload.
///
/// Inputs:
/// - `task`: Worker task containing serialized create_database payload.
///
/// Output:
/// - Canonical database name.
///
/// Details:
/// - Prefers `db_name.normalized`, then falls back to `db_name.raw` or `database_name`.
fn resolve_database_name(task: &worker_service::StagePartitionExecution) -> Result<String, String> {
    let parsed: serde_json::Value = serde_json::from_str(&task.input)
        .map_err(|e| format!("invalid create_database payload: {}", e))?;

    let normalized = parsed
        .get("db_name")
        .and_then(|v| v.get("normalized"))
        .and_then(serde_json::Value::as_str)
        .map(normalize_identifier)
        .filter(|s| !s.trim().is_empty());

    let fallback = parsed
        .get("db_name")
        .and_then(|v| v.get("raw"))
        .and_then(serde_json::Value::as_str)
        .map(normalize_identifier)
        .or_else(|| {
            parsed
                .get("database_name")
                .and_then(serde_json::Value::as_str)
                .map(normalize_identifier)
        })
        .filter(|s| !s.trim().is_empty());

    let database_name = normalized.or(fallback).ok_or_else(|| {
        "create_database payload missing db_name.normalized or db_name.raw".to_string()
    })?;

    if !is_valid_database_name(&database_name) {
        return Err(format!(
            "invalid database name '{}': only [a-zA-Z0-9_-] are allowed",
            database_name
        ));
    }

    Ok(database_name)
}

/// What: Execute create_database storage initialization on the worker.
///
/// Inputs:
/// - `shared`: Worker shared state containing configured storage provider and cluster info.
/// - `task`: Worker task payload that contains serialized CREATE DATABASE AST fields.
///
/// Output:
/// - `Ok(location)` when storage marker/path has been initialized or already exists.
/// - `Err(message)` when payload is invalid or storage invariants fail.
///
/// Details:
/// - This operation is idempotent: existing marker/path is treated as success.
/// - A marker file is used as the durable signal that the database path was initialized.
pub(crate) async fn execute_create_database_task(
    shared: &SharedData,
    task: &worker_service::StagePartitionExecution,
) -> Result<String, String> {
    let database_name = resolve_database_name(task)?;
    let marker_key = format!("databases/{}/_kionas_database.json", database_name);

    if let Some(provider) = &shared.storage_provider {
        match provider.get_object(&marker_key).await {
            Ok(Some(_)) => {
                return Ok(format!("storage://{}", marker_key));
            }
            Ok(None) => {}
            Err(e) => {
                log::warn!(
                    "create_database marker pre-check failed for '{}': {}; proceeding with create",
                    marker_key,
                    e
                );
            }
        }

        let marker = serde_json::json!({
            "database_name": database_name,
            "task_id": task.task_id,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|e| format!("marker encode failed: {}", e))?;

        provider
            .put_object(&marker_key, marker_bytes)
            .await
            .map_err(|e| format!("failed to create database marker '{}': {}", marker_key, e))?;

        return Ok(format!("storage://{}", marker_key));
    }

    let base_path = format!("worker_storage/databases/{}", database_name);
    tokio::fs::create_dir_all(&base_path)
        .await
        .map_err(|e| format!("failed to create database directory '{}': {}", base_path, e))?;

    let marker_path = format!("{}/_kionas_database.json", base_path);
    if tokio::fs::metadata(&marker_path).await.is_err() {
        let marker = serde_json::json!({
            "database_name": database_name,
            "task_id": task.task_id,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|e| format!("marker encode failed: {}", e))?;

        tokio::fs::write(&marker_path, marker_bytes)
            .await
            .map_err(|e| format!("failed to write database marker '{}': {}", marker_path, e))?;
    }

    Ok(base_path)
}
