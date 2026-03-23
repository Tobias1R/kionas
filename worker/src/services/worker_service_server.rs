#![allow(clippy::result_large_err)]

use crate::state::SharedData;
use std::sync::Arc;

use serde_json::Value;
use url::Url;

#[allow(clippy::enum_variant_names)]
pub mod worker_service {
    tonic::include_proto!("worker_service");
}

#[cfg(test)]
#[path = "../tests/services_worker_service_server_tests.rs"]
mod tests;

#[derive(Default)]
pub struct WorkerService {
    pub shared_data: SharedData,
    pub authorizer: Arc<crate::authz::WorkerAuthorizer>,
}

const OUTCOME_PREFIX: &str = "RESULT";

fn format_outcome(category: &str, code: &str, message: &str) -> String {
    format!("{}|{}|{}|{}", OUTCOME_PREFIX, category, code, message)
}

fn parse_structured_outcome(raw: &str) -> Option<(&str, &str, &str)> {
    let mut parts = raw.splitn(4, '|');
    let prefix = parts.next()?;
    if prefix != OUTCOME_PREFIX {
        return None;
    }
    let category = parts.next()?;
    let code = parts.next()?;
    let message = parts.next().unwrap_or_default();
    Some((category, code, message))
}

fn normalize_worker_error_for_response(raw_error: &str) -> String {
    if let Some((category, code, message)) = parse_structured_outcome(raw_error)
        && matches!(
            category,
            "VALIDATION" | "CONSTRAINT" | "EXECUTION" | "INFRA"
        )
    {
        return format_outcome(category, code, message);
    }

    let lower = raw_error.to_ascii_lowercase();

    let (category, code) = if lower.contains("temporal_literal_invalid") {
        ("VALIDATION", "VALIDATION_TEMPORAL_LITERAL_INVALID")
    } else if lower.contains("datetime_timezone_not_allowed") {
        ("VALIDATION", "VALIDATION_DATETIME_TIMEZONE_NOT_ALLOWED")
    } else if lower.contains("decimal_coercion_failed") {
        ("VALIDATION", "VALIDATION_DECIMAL_COERCION_FAILED")
    } else if lower.contains("insert_type_hints_malformed") {
        ("VALIDATION", "VALIDATION_INSERT_TYPE_HINTS_MALFORMED")
    } else if lower.contains("missing not null column") {
        ("CONSTRAINT", "CONSTRAINT_NOT_NULL_COLUMNS_MISSING")
    } else if lower.contains("not null constraint violated") {
        ("CONSTRAINT", "CONSTRAINT_NOT_NULL_VIOLATION")
    } else if lower.contains("table not found") {
        ("VALIDATION", "VALIDATION_TABLE_NOT_FOUND")
    } else if lower.contains("unsupported") || lower.contains("invalid") {
        ("VALIDATION", "VALIDATION_WORKER_TASK_INVALID")
    } else if lower.contains("permission denied") || lower.contains("unauthenticated") {
        ("INFRA", "INFRA_WORKER_AUTHZ_FAILED")
    } else if lower.contains("storage")
        || lower.contains("object store")
        || lower.contains("result location")
    {
        ("INFRA", "INFRA_WORKER_STORAGE_OR_RESULT_UNAVAILABLE")
    } else {
        ("INFRA", "INFRA_WORKER_TASK_FAILED")
    };

    format_outcome(category, code, raw_error)
}

fn is_staged_query_task(task: &worker_service::StagePartitionExecution) -> bool {
    task.stage_id > 0 || task.partition_count > 1 || !task.upstream_stage_ids.is_empty()
}

fn validate_query_task_context(
    task: &worker_service::StagePartitionExecution,
) -> Result<(), tonic::Status> {
    if task.task_id.trim().is_empty() {
        return Err(tonic::Status::invalid_argument(format_outcome(
            "INFRA",
            "INFRA_WORKER_EXECUTION_TASK_CONTEXT_MISSING",
            "task_id is required for query task",
        )));
    }

    let query_id = task
        .params
        .get("__query_id")
        .map(|v| v.trim())
        .filter(|v| !v.is_empty());
    if query_id.is_none() {
        return Err(tonic::Status::failed_precondition(format_outcome(
            "INFRA",
            "INFRA_WORKER_EXECUTION_CONTEXT_MISSING",
            format!("query_id is missing for task_id={}", task.task_id).as_str(),
        )));
    }

    if is_staged_query_task(task) {
        if task.stage_id == 0 {
            return Err(tonic::Status::failed_precondition(format_outcome(
                "EXECUTION",
                "EXECUTION_WORKER_EXECUTION_STAGE_CONTEXT_MISSING",
                format!(
                    "stage_id is missing or invalid for task_id={}",
                    task.task_id
                )
                .as_str(),
            )));
        }

        if task.partition_count == 0 || task.partition_id >= task.partition_count {
            return Err(tonic::Status::failed_precondition(format_outcome(
                "EXECUTION",
                "EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING",
                format!(
                    "partition_index is missing or invalid for task_id={} stage_id={}",
                    task.task_id, task.stage_id
                )
                .as_str(),
            )));
        }
    }

    Ok(())
}

fn resolve_flight_port(default_worker_port: u32) -> u32 {
    std::env::var("WORKER_FLIGHT_PORT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|p| *p > 0)
        .unwrap_or(default_worker_port.saturating_add(1))
}

fn resolve_flight_endpoint(worker_host: &str, worker_port: u32) -> String {
    let flight_port = resolve_flight_port(worker_port);
    format!("http://{}:{}", worker_host, flight_port)
}

fn parse_scope_relation_from_task(
    task: &worker_service::StagePartitionExecution,
) -> Option<(String, String, String)> {
    if let (Some(database), Some(schema), Some(table)) = (
        task.params.get("database_name"),
        task.params.get("schema_name"),
        task.params.get("table_name"),
    ) {
        let database = database.trim().to_ascii_lowercase();
        let schema = schema.trim().to_ascii_lowercase();
        let table = table.trim().to_ascii_lowercase();
        if !database.is_empty() && !schema.is_empty() && !table.is_empty() {
            return Some((database, schema, table));
        }
    }

    let payload: serde_json::Value = serde_json::from_str(&task.input).ok()?;
    let namespace = payload.get("namespace")?;
    let database = namespace
        .get("database")?
        .as_str()?
        .trim()
        .to_ascii_lowercase();
    let schema = namespace
        .get("schema")?
        .as_str()?
        .trim()
        .to_ascii_lowercase();
    let table = namespace
        .get("table")?
        .as_str()?
        .trim()
        .to_ascii_lowercase();
    if database.is_empty() || schema.is_empty() || table.is_empty() {
        return None;
    }

    Some((database, schema, table))
}

/// What: Build the task-scoped staging prefix from a task result location.
///
/// Inputs:
/// - `result_location`: Result URI persisted for a task.
/// - `session_id`: Session identifier.
/// - `task_id`: Task identifier.
///
/// Output:
/// - Object storage prefix where query artifacts and metadata are stored.
fn to_staging_prefix(
    result_location: &str,
    session_id: &str,
    task_id: &str,
) -> Result<String, tonic::Status> {
    let parsed = Url::parse(result_location)
        .map_err(|e| tonic::Status::invalid_argument(format!("invalid result location: {}", e)))?;

    let path = parsed.path().trim_start_matches('/').trim_end_matches('/');
    if path.is_empty() {
        return Err(tonic::Status::invalid_argument(
            "result location path is empty",
        ));
    }

    Ok(format!("{}/staging/{}/{}/", path, session_id, task_id))
}

/// What: Read result metadata sidecar for a task prefix.
///
/// Inputs:
/// - `shared_data`: Worker shared state.
/// - `prefix`: Task-scoped object prefix.
///
/// Output:
/// - Parsed JSON metadata value.
async fn load_result_metadata(
    shared_data: &SharedData,
    prefix: &str,
) -> Result<Value, tonic::Status> {
    let provider = shared_data
        .storage_provider
        .as_ref()
        .ok_or_else(|| tonic::Status::failed_precondition("storage provider is not configured"))?;

    let metadata_key = format!("{}result_metadata.json", prefix);
    let bytes = provider
        .get_object(&metadata_key)
        .await
        .map_err(|e| tonic::Status::internal(format!("failed to read {}: {}", metadata_key, e)))?
        .ok_or_else(|| {
            tonic::Status::not_found(format!("metadata not found at {}", metadata_key))
        })?;

    serde_json::from_slice::<Value>(&bytes)
        .map_err(|e| tonic::Status::internal(format!("failed to parse metadata JSON: {}", e)))
}

/// What: Serialize metadata columns into stable schema JSON string.
///
/// Inputs:
/// - `metadata`: Parsed task result metadata.
///
/// Output:
/// - JSON string for `FlightInfoResponse.schema`.
fn schema_json_from_metadata(metadata: &Value) -> Result<String, tonic::Status> {
    let columns = metadata
        .get("columns")
        .ok_or_else(|| tonic::Status::failed_precondition("metadata missing columns"))?;

    serde_json::to_string(columns)
        .map_err(|e| tonic::Status::internal(format!("failed to serialize schema JSON: {}", e)))
}

#[tonic::async_trait]
impl worker_service::worker_service_server::WorkerService for WorkerService {
    async fn register_worker(
        &self,
        request: tonic::Request<worker_service::RegisterWorkerRequest>,
    ) -> Result<tonic::Response<worker_service::RegisterWorkerResponse>, tonic::Status> {
        let req = request.into_inner();
        // TODO: Register worker logic
        let resp = worker_service::RegisterWorkerResponse {
            status: "ok".to_string(),
            uuid: req.worker_id.clone(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn heartbeat(
        &self,
        request: tonic::Request<worker_service::HeartbeatRequest>,
    ) -> Result<tonic::Response<worker_service::HeartbeatResponse>, tonic::Status> {
        let _req = request.into_inner();
        // TODO: Heartbeat logic
        let resp = worker_service::HeartbeatResponse {
            status: "alive".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn execute_task(
        &self,
        request: tonic::Request<worker_service::TaskRequest>,
    ) -> Result<tonic::Response<worker_service::TaskResponse>, tonic::Status> {
        let is_query_task = request
            .get_ref()
            .tasks
            .first()
            .map(|t| t.operation.eq_ignore_ascii_case("query"))
            .unwrap_or(false);
        let request_session_id = request.get_ref().session_id.clone();

        let auth_ctx_opt = if is_query_task {
            let auth_ctx = self
                .authorizer
                .extract_dispatch_context(request.metadata(), request_session_id.as_str())?;
            self.authorizer.validate_dispatch_context(&auth_ctx).await?;
            Some(auth_ctx)
        } else {
            None
        };

        let mut req = request.into_inner();
        if let Some(auth_ctx) = &auth_ctx_opt
            && req.session_id.trim().is_empty()
        {
            req.session_id = auth_ctx.session_id.clone();
        }

        if let Some(task) = req.tasks.first()
            && task.operation.eq_ignore_ascii_case("query")
            && let Some((database, schema, table)) = parse_scope_relation_from_task(task)
            && let Some(auth_ctx) = &auth_ctx_opt
        {
            self.authorizer.validate_query_scope(
                &auth_ctx.auth_scope,
                &database,
                &schema,
                &table,
            )?;
        }

        if let Some(auth_ctx) = &auth_ctx_opt {
            for task in &mut req.tasks {
                task.params
                    .insert("__auth_scope".to_string(), auth_ctx.auth_scope.clone());
                task.params
                    .insert("__rbac_user".to_string(), auth_ctx.rbac_user.clone());
                task.params
                    .insert("__rbac_role".to_string(), auth_ctx.rbac_role.clone());
                task.params
                    .insert("__query_id".to_string(), auth_ctx.query_id.clone());
            }
        }

        for task in &req.tasks {
            if task.operation.eq_ignore_ascii_case("query") {
                validate_query_task_context(task)?;
            }
        }

        log::info!("Received task: {}", req.session_id);

        let mut resp =
            crate::transactions::maestro::handle_execute_task(self.shared_data.clone(), req).await;
        if resp.status != "ok" {
            resp.error = normalize_worker_error_for_response(resp.error.as_str());
        }
        Ok(tonic::Response::new(resp))
    }

    async fn prepare(
        &self,
        request: tonic::Request<worker_service::PrepareRequest>,
    ) -> Result<tonic::Response<worker_service::PrepareResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!(
            "Prepare called for tx={} staging_prefix={}",
            req.tx_id,
            req.staging_prefix
        );
        let tasks_json: Vec<Value> = req
            .tasks
            .iter()
            .map(|t| {
                serde_json::json!({
                    "task_id": t.task_id.clone(),
                    "operation": t.operation.clone(),
                    "input": t.input.clone(),
                    "output": t.output.clone(),
                    "params": t.params.clone(),
                })
            })
            .collect();

        if let Err(e) = crate::transactions::maestro::prepare_tx(
            self.shared_data.clone(),
            &req.tx_id,
            &req.staging_prefix,
            &tasks_json,
        )
        .await
        {
            log::error!("failed to stage tx {}: {}", req.tx_id, e);
            return Err(tonic::Status::internal(format!(
                "failed to stage tx: {}",
                e
            )));
        }

        let resp = worker_service::PrepareResponse {
            success: true,
            message: "staged".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn commit(
        &self,
        request: tonic::Request<worker_service::CommitRequest>,
    ) -> Result<tonic::Response<worker_service::CommitResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!(
            "Commit called for tx={} staging_prefix={}",
            req.tx_id,
            req.staging_prefix
        );
        if let Err(e) = crate::transactions::maestro::commit_tx(
            self.shared_data.clone(),
            &req.tx_id,
            &req.staging_prefix,
        )
        .await
        {
            log::error!("failed to promote tx {}: {}", req.tx_id, e);
            return Err(tonic::Status::internal(format!(
                "failed to promote staged objects: {}",
                e
            )));
        }

        let resp = worker_service::CommitResponse {
            success: true,
            message: "committed".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn abort(
        &self,
        request: tonic::Request<worker_service::AbortRequest>,
    ) -> Result<tonic::Response<worker_service::AbortResponse>, tonic::Status> {
        let req = request.into_inner();
        log::info!(
            "Abort called for tx={} staging_prefix={}",
            req.tx_id,
            req.staging_prefix
        );
        if let Err(e) = crate::transactions::maestro::abort_tx(
            self.shared_data.clone(),
            &req.tx_id,
            &req.staging_prefix,
        )
        .await
        {
            log::error!("failed to abort tx {}: {}", req.tx_id, e);
            return Err(tonic::Status::internal(format!(
                "failed to abort staged objects: {}",
                e
            )));
        }
        let resp = worker_service::AbortResponse {
            success: true,
            message: "aborted".to_string(),
        };
        Ok(tonic::Response::new(resp))
    }

    async fn get_flight_info(
        &self,
        request: tonic::Request<worker_service::FlightInfoRequest>,
    ) -> Result<tonic::Response<worker_service::FlightInfoResponse>, tonic::Status> {
        let request_session_id = request.get_ref().session_id.clone();
        let auth_ctx = self
            .authorizer
            .extract_dispatch_context(request.metadata(), request_session_id.as_str())?;
        self.authorizer.validate_dispatch_context(&auth_ctx).await?;

        let req = request.into_inner();
        if req.task_id.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("task_id is required"));
        }
        if req.session_id.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("session_id is required"));
        }

        let endpoint = resolve_flight_endpoint(
            &self.shared_data.worker_info.host,
            self.shared_data.worker_info.port,
        );

        let result_location = self
            .shared_data
            .get_task_result_location(&req.session_id, &req.task_id)
            .await
            .ok_or_else(|| {
                tonic::Status::not_found(format!(
                    "result location missing or expired for task_id={}",
                    req.task_id
                ))
            })?;
        let prefix = to_staging_prefix(&result_location, &req.session_id, &req.task_id)?;
        let metadata = load_result_metadata(&self.shared_data, &prefix).await?;
        let schema = schema_json_from_metadata(&metadata)?;

        let ticket = self.authorizer.issue_signed_flight_ticket(
            &auth_ctx,
            &req.task_id,
            &self.shared_data.worker_info.worker_id,
        )?;

        let resp = worker_service::FlightInfoResponse {
            endpoint,
            schema,
            ticket,
        };
        Ok(tonic::Response::new(resp))
    }
}
