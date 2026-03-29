use crate::state::SharedData;
use arrow::array::{StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

/// What: Build a Delta table URI from storage config and a SQL table name.
///
/// Inputs:
/// - `storage`: Cluster storage JSON config
/// - `table_name`: SQL table identifier
///
/// Output:
/// - Optional `s3://` URI that points to the table root
///
/// Details:
/// - Uses configured bucket and maps canonical `database.schema.table` to
///   `databases/<db>/schemas/<schema>/tables/<table>`.
/// - Preserves a legacy fallback path mapping for non-3-part names.
pub(crate) fn derive_table_uri_from_storage(
    storage: &JsonValue,
    table_name: &str,
) -> Option<String> {
    fn normalize_segment(raw: &str) -> String {
        raw.trim()
            .trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']')
            .to_ascii_lowercase()
    }

    let bucket = storage.get("bucket").and_then(|v| v.as_str())?.trim();
    if bucket.is_empty() {
        return None;
    }

    let clean_table = table_name.trim();
    let parts = clean_table
        .split('.')
        .map(normalize_segment)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();

    if parts.len() == 3 {
        return Some(format!(
            "s3://{}/databases/{}/schemas/{}/tables/{}",
            bucket, parts[0], parts[1], parts[2]
        ));
    }

    let clean_table = clean_table
        .trim_matches('"')
        .trim_matches('`')
        .replace('.', "/");
    if clean_table.is_empty() {
        return None;
    }
    Some(format!("s3://{}/{}", bucket, clean_table))
}

/// What: Handle task execution orchestration and async status reporting.
///
/// Inputs:
/// - `shared`: Shared worker state.
/// - `req`: Worker task request envelope.
///
/// Output:
/// - Immediate task response while actual work continues asynchronously.
pub async fn handle_execute_task(
    shared: SharedData,
    req: crate::services::worker_service_server::worker_service::TaskRequest,
) -> crate::services::worker_service_server::worker_service::TaskResponse {
    let first_task = req.tasks.first().cloned();
    let operation = first_task
        .as_ref()
        .map(|t| t.operation.to_lowercase())
        .unwrap_or_default();
    let task_id = first_task
        .as_ref()
        .map(|t| t.task_id.clone())
        .unwrap_or_default();
    let session_id = req.session_id.clone();

    if operation == "cleanup_session" {
        let cleanup_target = first_task
            .as_ref()
            .and_then(|task| task.params.get("cleanup_session_id"))
            .map(String::as_str)
            .unwrap_or(session_id.as_str())
            .trim()
            .to_string();

        if cleanup_target.is_empty() {
            return crate::services::worker_service_server::worker_service::TaskResponse {
                status: "error".to_string(),
                error: "cleanup_session requires non-empty session_id or cleanup_session_id"
                    .to_string(),
                result_location: String::new(),
            };
        }

        shared
            .cleanup_runtime_session(cleanup_target.as_str())
            .await;

        return crate::services::worker_service_server::worker_service::TaskResponse {
            status: "ok".to_string(),
            error: String::new(),
            result_location: String::new(),
        };
    }

    if operation == "create_database" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "create_database task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::transactions::ddl::create_database::execute_create_database_task(&shared, task)
            .await
        {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    if operation == "create_schema" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "create_schema task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::transactions::ddl::create_schema::execute_create_schema_task(&shared, task)
            .await
        {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    if operation == "create_table" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "create_table task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::transactions::ddl::create_table::execute_create_table_task(&shared, task).await
        {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    if operation == "query" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "query task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::services::query::execute_query_task_stub(&shared, task, &session_id).await {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    let explicit_delta_table_uri = first_task.as_ref().and_then(|task| {
        task.params
            .get("table_uri")
            .filter(|uri| !uri.trim().is_empty())
            .cloned()
            .or_else(|| {
                if task.output.trim().is_empty() {
                    None
                } else {
                    Some(task.output.clone())
                }
            })
    });
    let parsed_insert = if operation == "insert" {
        match first_task.as_ref() {
            Some(task) => match crate::transactions::insert_contract::parse_insert_payload(task) {
                Ok(parsed) => Some(parsed),
                Err(message) => {
                    return crate::services::worker_service_server::worker_service::TaskResponse {
                        status: "error".to_string(),
                        error: message,
                        result_location: String::new(),
                    };
                }
            },
            None => None,
        }
    } else {
        None
    };
    let insert_column_type_hints = if operation == "insert" {
        match first_task.as_ref() {
            Some(task) => {
                match crate::transactions::insert_contract::parse_insert_column_type_hints(task) {
                    Ok(hints) => hints,
                    Err(message) => {
                        return crate::services::worker_service_server::worker_service::TaskResponse {
                            status: "error".to_string(),
                            error: message,
                            result_location: String::new(),
                        };
                    }
                }
            }
            None => HashMap::new(),
        }
    } else {
        HashMap::new()
    };
    if operation == "insert"
        && let (Some(task), Some(parsed)) = (first_task.as_ref(), parsed_insert.as_ref())
    {
        let required_columns =
            crate::transactions::constraints::insert_not_null::parse_required_not_null_columns(
                task,
            );
        if let Err(message) =
            crate::transactions::constraints::insert_not_null::enforce_not_null_columns(
                task,
                parsed,
                &required_columns,
            )
        {
            return crate::services::worker_service_server::worker_service::TaskResponse {
                status: "error".to_string(),
                error: message,
                result_location: String::new(),
            };
        }
    }
    let table_name_override = first_task
        .as_ref()
        .and_then(|task| task.params.get("table_name").cloned())
        .filter(|s| !s.trim().is_empty());
    let derived_insert_uri = if explicit_delta_table_uri.is_none() {
        let table_name =
            table_name_override.or_else(|| parsed_insert.as_ref().map(|p| p.table_name.clone()));
        table_name
            .as_deref()
            .and_then(|name| derive_table_uri_from_storage(&shared.cluster_info.storage, name))
    } else {
        None
    };
    let delta_table_uri = explicit_delta_table_uri.or(derived_insert_uri);
    let result_location = delta_table_uri
        .clone()
        .unwrap_or_else(|| "arrow-flight-endpoint".to_string());

    let stage_id = first_task
        .as_ref()
        .and_then(|task| (task.stage_id > 0).then(|| task.stage_id.to_string()));
    let partition_count = first_task
        .as_ref()
        .and_then(|task| (task.partition_count > 0).then_some(task.partition_count));
    let upstream_stage_ids = first_task
        .as_ref()
        .map(|task| {
            serde_json::to_string(&task.upstream_stage_ids).unwrap_or_else(|_| "[]".to_string())
        })
        .unwrap_or_else(|| "[]".to_string());
    let partition_spec = first_task
        .as_ref()
        .map(|task| {
            if task.partition_spec.trim().is_empty() {
                "\"Single\"".to_string()
            } else {
                task.partition_spec.clone()
            }
        })
        .unwrap_or_else(|| "\"Single\"".to_string());

    shared
        .set_task_result_location(&session_id, &task_id, &result_location)
        .await;
    let result_location_for_spawn = result_location.clone();
    let shared_clone = shared.clone();
    let stage_id_for_spawn = stage_id.clone();
    let partition_count_for_spawn = partition_count;
    let upstream_stage_ids_for_spawn = upstream_stage_ids.clone();
    let partition_spec_for_spawn = partition_spec.clone();
    tokio::spawn(async move {
        let mut status = "succeeded".to_string();
        let mut error_message = String::new();

        if let Some(table_uri) = delta_table_uri {
            let batch_res: Result<RecordBatch, String> = if operation == "insert" {
                if let Some(parsed) = parsed_insert.as_ref() {
                    crate::transactions::insert_record_batch::build_record_batch_from_insert(
                        parsed,
                        &insert_column_type_hints,
                    )
                } else {
                    Err("failed to parse INSERT payload".to_string())
                }
            } else {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("session_id", DataType::Utf8, false),
                    Field::new("task_id", DataType::Utf8, false),
                    Field::new("result_location", DataType::Utf8, false),
                    Field::new(
                        "committed_at_ms",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                ]));

                let committed_at = chrono::Utc::now().timestamp_millis();
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec![session_id.clone()])),
                        Arc::new(StringArray::from(vec![task_id.clone()])),
                        Arc::new(StringArray::from(vec![result_location_for_spawn.clone()])),
                        Arc::new(TimestampMillisecondArray::from(vec![committed_at])),
                    ],
                )
                .map_err(|e| format!("failed to build record batch: {}", e))
            };

            match batch_res {
                Ok(batch) => {
                    if let Err(e) = crate::storage::deltalake::write_parquet_and_commit(
                        shared_clone.clone(),
                        &table_uri,
                        vec![batch],
                    )
                    .await
                    {
                        status = "failed".to_string();
                        error_message = format!("delta write/commit failed: {}", e);
                        log::error!(
                            "Failed delta write for task {} table {}: {}",
                            task_id,
                            table_uri,
                            e
                        );
                    }
                }
                Err(e) => {
                    status = "failed".to_string();
                    error_message = format!("failed to build record batch: {}", e);
                    log::error!(
                        "Failed to build Arrow record batch for task {}: {}",
                        task_id,
                        e
                    );
                }
            }
        } else {
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        if let Some(pool_arc) =
            crate::transactions::interops_pool::ensure_pool(shared_clone.clone()).await
        {
            match pool_arc.get().await {
                Ok(mut pooled_client) => {
                    let mut metadata = std::collections::HashMap::new();
                    metadata.insert(
                        "upstream_stage_ids".to_string(),
                        upstream_stage_ids_for_spawn.clone(),
                    );
                    metadata.insert(
                        "partition_spec".to_string(),
                        partition_spec_for_spawn.clone(),
                    );

                    let partition_completed = partition_count_for_spawn
                        .map(|count| if status == "succeeded" { count } else { 0 });

                    let update = crate::interops_service::TaskUpdateRequest {
                        task_id: task_id.clone(),
                        status,
                        result_location: result_location_for_spawn.clone(),
                        error: error_message,
                        stage_id: stage_id_for_spawn.clone(),
                        partition_count: partition_count_for_spawn,
                        partition_completed,
                        metadata,
                    };
                    if let Err(e) = pooled_client.task_update(tonic::Request::new(update)).await {
                        log::error!(
                            "Failed to send TaskUpdate for {} using pool: {}",
                            task_id,
                            e
                        );
                    }
                }
                Err(e) => log::error!("Failed to acquire pooled master client: {}", e),
            }
        } else {
            log::error!(
                "No interops pool available; dropping TaskUpdate for {}",
                task_id
            );
        }
    });

    crate::services::worker_service_server::worker_service::TaskResponse {
        status: "ok".to_string(),
        error: String::new(),
        result_location,
    }
}
