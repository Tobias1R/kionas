use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// What: Normalize an identifier into canonical lowercase representation.
///
/// Inputs:
/// - `raw`: Unnormalized identifier text.
///
/// Output:
/// - Canonical lowercase identifier with common quotes removed.
fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

/// What: Validate namespace segment names.
///
/// Inputs:
/// - `name`: Canonical segment value.
///
/// Output:
/// - `true` when the segment is valid.
fn is_valid_namespace_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
}

#[derive(Clone, Debug)]
struct ParsedColumn {
    name: String,
    data_type: String,
    nullable: bool,
}

/// What: Resolve canonical namespace from typed create_table payload.
///
/// Inputs:
/// - `task`: Worker task containing serialized create_table payload.
///
/// Output:
/// - Canonical `(database_name, schema_name, table_name)` tuple.
fn resolve_namespace(
    task: &worker_service::StagePartitionExecution,
) -> Result<(String, String, String), String> {
    let parsed: serde_json::Value = serde_json::from_str(&task.input)
        .map_err(|e| format!("invalid create_table payload: {}", e))?;

    let database_name = parsed
        .get("namespace")
        .and_then(|v| v.get("database"))
        .and_then(serde_json::Value::as_str)
        .map(normalize_identifier)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "create_table payload missing namespace.database".to_string())?;

    let schema_name = parsed
        .get("namespace")
        .and_then(|v| v.get("schema"))
        .and_then(serde_json::Value::as_str)
        .map(normalize_identifier)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "create_table payload missing namespace.schema".to_string())?;

    let table_name = parsed
        .get("namespace")
        .and_then(|v| v.get("table"))
        .and_then(serde_json::Value::as_str)
        .map(normalize_identifier)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| "create_table payload missing namespace.table".to_string())?;

    for part in [&database_name, &schema_name, &table_name] {
        if !is_valid_namespace_name(part) {
            return Err(format!(
                "invalid namespace segment '{}': only [a-zA-Z0-9_-] are allowed",
                part
            ));
        }
    }

    Ok((database_name, schema_name, table_name))
}

/// What: Parse typed column definitions from CREATE TABLE payload.
///
/// Inputs:
/// - `task`: Worker task containing serialized create_table payload.
///
/// Output:
/// - Parsed typed column list used for delta initialization.
fn resolve_columns(
    task: &worker_service::StagePartitionExecution,
) -> Result<Vec<ParsedColumn>, String> {
    let parsed: serde_json::Value = serde_json::from_str(&task.input)
        .map_err(|e| format!("invalid create_table payload: {}", e))?;

    let raw_columns = parsed
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "create_table payload missing columns".to_string())?;

    if raw_columns.is_empty() {
        return Err("create_table payload has no columns".to_string());
    }

    let mut columns = Vec::with_capacity(raw_columns.len());
    for col in raw_columns {
        let name = col
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map(normalize_identifier)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| "create_table payload column missing name".to_string())?;
        let data_type = col
            .get("data_type")
            .and_then(serde_json::Value::as_str)
            .map(std::string::ToString::to_string)
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| "create_table payload column missing data_type".to_string())?;
        let nullable = col
            .get("nullable")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(true);
        columns.push(ParsedColumn {
            name,
            data_type,
            nullable,
        });
    }

    Ok(columns)
}

/// What: Build a delta table URI from storage config and namespace parts.
///
/// Inputs:
/// - `shared`: Worker shared state.
/// - `database_name`: Canonical database name.
/// - `schema_name`: Canonical schema name.
/// - `table_name`: Canonical table name.
///
/// Output:
/// - Fully qualified `s3://` delta table URI.
fn derive_table_uri(
    shared: &SharedData,
    database_name: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<String, String> {
    let bucket = shared
        .cluster_info
        .storage
        .get("bucket")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "worker storage config missing bucket for create_table".to_string())?;

    Ok(format!(
        "s3://{}/databases/{}/schemas/{}/tables/{}",
        bucket, database_name, schema_name, table_name
    ))
}

/// What: Convert parsed create_table columns into Arrow schema.
///
/// Inputs:
/// - `columns`: Typed column descriptors from payload.
///
/// Output:
/// - Arrow schema used for delta table initialization.
fn build_arrow_schema(columns: &[ParsedColumn]) -> Result<Schema, String> {
    if columns.is_empty() {
        return Err("create_table payload has no columns".to_string());
    }

    let fields = columns
        .iter()
        .map(|col| {
            let arrow_type = resolve_arrow_type_for_declared_type(&col.data_type)?;
            Ok(Field::new(col.name.clone(), arrow_type, col.nullable))
        })
        .collect::<Result<Vec<_>, String>>()?;

    Ok(Schema::new(fields))
}

/// What: Resolve an Arrow type from one declared SQL datatype token.
///
/// Inputs:
/// - `declared_type`: Declared SQL datatype text.
///
/// Output:
/// - Arrow datatype for known FOUNDATION mappings.
/// - Error when no explicit mapping exists (to avoid silent fallback).
fn resolve_arrow_type_for_declared_type(declared_type: &str) -> Result<DataType, String> {
    let dt = declared_type.to_ascii_lowercase();

    if dt.contains("bigint") || dt == "int8" || dt == "long" {
        return Ok(DataType::Int64);
    }
    if dt.contains("smallint") || dt == "int2" {
        return Ok(DataType::Int16);
    }
    if dt.contains("int") || dt == "integer" || dt == "int4" {
        return Ok(DataType::Int32);
    }
    if dt.contains("boolean") || dt == "bool" {
        return Ok(DataType::Boolean);
    }
    if dt.contains("float") || dt.contains("real") {
        return Ok(DataType::Float32);
    }
    if dt.contains("double") {
        return Ok(DataType::Float64);
    }
    if dt.contains("timestamp") {
        return Ok(DataType::Timestamp(TimeUnit::Microsecond, None));
    }
    if dt.contains("datetime") {
        return Ok(DataType::Utf8);
    }
    if dt == "date" {
        return Ok(DataType::Date32);
    }
    if dt.contains("decimal") || dt.contains("numeric") {
        return Ok(DataType::Utf8);
    }
    if dt.contains("binary") {
        return Ok(DataType::Binary);
    }
    if dt.contains("char") || dt.contains("text") || dt.contains("string") {
        return Ok(DataType::Utf8);
    }

    Err(format!(
        "create_table has unsupported declared data_type '{}' and cannot fallback implicitly",
        declared_type
    ))
}

/// What: Execute create_table storage initialization on worker.
///
/// Inputs:
/// - `shared`: Worker shared state.
/// - `task`: Worker task containing typed create_table payload.
///
/// Output:
/// - `Ok(location)` when table storage/marker was initialized or already existed.
/// - `Err(message)` for invalid payload or storage failures.
pub(crate) async fn execute_create_table_task(
    shared: &SharedData,
    task: &worker_service::StagePartitionExecution,
) -> Result<String, String> {
    let (database_name, schema_name, table_name) = resolve_namespace(task)?;
    let columns = resolve_columns(task)?;
    let table_uri = derive_table_uri(shared, &database_name, &schema_name, &table_name)?;

    let expected_segment = format!(
        "/databases/{}/schemas/{}/tables/{}/",
        database_name, schema_name, table_name
    );
    if !format!("{}/", table_uri).contains(&expected_segment) {
        return Err("create_table payload failed path invariant validation".to_string());
    }

    let marker_key = format!(
        "databases/{}/schemas/{}/tables/{}/_kionas_table.json",
        database_name, schema_name, table_name
    );

    if let Some(provider) = &shared.storage_provider {
        match provider.get_object(&marker_key).await {
            Ok(Some(_)) => return Ok(table_uri),
            Ok(None) => {}
            Err(e) => {
                log::warn!(
                    "create_table marker pre-check failed for '{}': {}; proceeding with create",
                    marker_key,
                    e
                );
            }
        }

        let schema = build_arrow_schema(&columns)?;
        crate::storage::deltalake::create_table(shared.clone(), &table_uri, &schema)
            .await
            .map_err(|e| format!("failed to initialize delta table at {}: {}", table_uri, e))?;

        let marker = serde_json::json!({
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "table_uri": table_uri,
            "task_id": task.task_id,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|e| format!("marker encode failed: {}", e))?;
        provider
            .put_object(&marker_key, marker_bytes)
            .await
            .map_err(|e| format!("failed to create table marker '{}': {}", marker_key, e))?;

        return Ok(table_uri);
    }

    let local_table_path = format!(
        "worker_storage/databases/{}/schemas/{}/tables/{}",
        database_name, schema_name, table_name
    );
    tokio::fs::create_dir_all(&local_table_path)
        .await
        .map_err(|e| {
            format!(
                "failed to create local table directory '{}': {}",
                local_table_path, e
            )
        })?;

    let marker_path = format!("{}/_kionas_table.json", local_table_path);
    if tokio::fs::metadata(&marker_path).await.is_err() {
        let marker = serde_json::json!({
            "database_name": database_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "task_id": task.task_id,
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let marker_bytes = serde_json::to_vec_pretty(&marker)
            .map_err(|e| format!("marker encode failed: {}", e))?;
        tokio::fs::write(&marker_path, marker_bytes)
            .await
            .map_err(|e| {
                format!(
                    "failed to write local table marker '{}': {}",
                    marker_path, e
                )
            })?;
    }

    Ok(local_table_path)
}
