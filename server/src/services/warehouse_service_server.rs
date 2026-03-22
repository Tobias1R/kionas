// Minimal, clean service implementation for `warehouse_service`.
// This file intentionally keeps handler logic simple so the crate builds.

use crate::parser::sql::parse_query;
use crate::statement_handler::handle_statement;
use crate::warehouse::state::SharedData;
use tonic::{Request, Response, Status};

pub mod warehouse_service {
    tonic::include_proto!("warehouse_service");
}

use warehouse_service::warehouse_service_server::WarehouseService as WarehouseServiceTrait;
use warehouse_service::{QueryRequest, QueryResponse, QueryStatusRequest, QueryStatusResponse};

use crate::services::request_context::RequestContext;

/// What: Map statement handler output into the public QueryResponse contract.
///
/// Inputs:
/// - `result`: Raw handler output string.
/// - `resp`: Mutable query response to update.
///
/// Output:
/// - Updates `resp.message`, `resp.status`, `resp.error_code`, and optional `resp.data` in-place.
///
/// Details:
/// - Structured outputs use `RESULT|<category>|<code>|<message>`.
/// - Legacy plain-text outputs still map to generic infra failures when applicable.
/// - For `QUERY_DISPATCHED`, response data carries the query handle bytes.
fn apply_statement_outcome(result: &str, resp: &mut QueryResponse) {
    // Ensure each statement outcome writes its own data contract and does not
    // leak bytes from previous statements in multi-statement requests.
    resp.data.clear();

    let mut parts = result.splitn(4, '|');
    let prefix = parts.next().unwrap_or_default();
    if prefix != "RESULT" {
        resp.status = "ERROR".to_string();
        resp.error_code = "INFRA_GENERIC".to_string();
        resp.message = result.to_string();
        return;
    }

    let category = parts.next().unwrap_or("INFRA");
    let code = parts.next().unwrap_or("UNKNOWN");
    let message = parts.next().unwrap_or("unknown error");

    resp.message = message.to_string();

    if code == "QUERY_DISPATCHED"
        && let Some(handle) = extract_query_handle_from_message(message)
    {
        resp.data = handle.into_bytes();
    }

    match category {
        "SUCCESS" => {
            resp.status = "OK".to_string();
            resp.error_code = "0".to_string();
        }
        "VALIDATION" | "CONSTRAINT" | "EXECUTION" | "INFRA" => {
            resp.status = "ERROR".to_string();
            if code.starts_with(category) {
                resp.error_code = code.to_string();
            } else {
                resp.error_code = format!("{}_{}", category, code);
            }
        }
        _ => {
            resp.status = "ERROR".to_string();
            resp.error_code = "INFRA_UNKNOWN_CATEGORY".to_string();
        }
    }
}

/// What: Extract a query handle location from a query success message.
///
/// Inputs:
/// - `message`: Query success message.
///
/// Output:
/// - `Some(handle)` when a `(location: <handle>)` suffix exists.
/// - `None` when message does not contain a handle suffix.
fn extract_query_handle_from_message(message: &str) -> Option<String> {
    let marker = "(location: ";
    let start = message.rfind(marker)? + marker.len();
    let end = message[start..].find(')')? + start;
    let handle = message[start..end].trim();
    if handle.is_empty() {
        return None;
    }
    Some(handle.to_string())
}

#[derive(Clone)]
pub struct WarehouseService {
    pub shared_data: SharedData,
}

impl WarehouseService {
    pub async fn initialize(shared_data: SharedData) -> Self {
        WarehouseService { shared_data }
    }
}

#[tonic::async_trait]
impl WarehouseServiceTrait for WarehouseService {
    async fn query_status(
        &self,
        request: Request<QueryStatusRequest>,
    ) -> Result<Response<QueryStatusResponse>, Status> {
        let req = request.into_inner();
        let resp = QueryStatusResponse {
            query_id: req.query_id,
            status: "OK".to_string(),
            error_code: "0".to_string(),
            execution_time: "0".to_string(),
            data: "".to_string(),
        };
        Ok(Response::new(resp))
    }

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<QueryResponse>, Status> {
        // Build a request context (query id, session, warehouse, storage) before consuming the request
        let ctx = RequestContext::from_request(self.shared_data.clone(), &request).await?;
        let req = request.into_inner();
        let query_text = req.query;
        let session_id = ctx.session_id.clone();

        // Use the query id from the context so callers can correlate work.
        let query_id = ctx.query_id.clone();
        log::debug!(
            "received query query_id={} session_id={} query_len={}",
            query_id,
            session_id,
            query_text.len()
        );

        // Prepare a single mutable response and populate it based on interpretation of the query.
        let mut resp = QueryResponse {
            message: String::new(),
            status: "OK".to_string(),
            error_code: "0".to_string(),
            execution_time: "0".to_string(),
            data: Vec::new(),
        };

        let shared_data = self.shared_data.clone();

        // Attempt to fetch session context without high-volume object dumps.
        if !ctx.session_id.is_empty() {
            let session_manager = {
                let sd = shared_data.lock().await;
                sd.session_manager.clone()
            };
            if session_manager
                .get_session(ctx.session_id.clone())
                .await
                .is_some()
            {
                log::debug!(
                    "session context available for query_id={} session_id={}",
                    query_id,
                    session_id
                );
            }
        }
        // Centralized handler for simple commands (e.g. `USE WAREHOUSE`) that
        // do not produce a normal SQL AST. The logic lives in the
        // `statement_handler` package so command handling is centralized.
        if let Some(msg) = crate::statement_handler::maybe_handle_direct_command(
            &query_text,
            &session_id,
            &shared_data,
        )
        .await
        {
            resp.message = msg;
            return Ok(Response::new(resp));
        }

        match parse_query(&query_text) {
            Ok(statements) => {
                log::debug!("Parsed statements: {:?}", statements);

                // Acquire TaskManager reference without holding shared_data lock across awaits
                let _task_manager = {
                    let state = shared_data.lock().await;
                    state.task_manager.clone()
                };

                for stmt in &statements {
                    // Execute handler synchronously (existing logic performs dispatch to worker)
                    let result = handle_statement(stmt, &session_id, &ctx, &shared_data).await;
                    log::debug!("Execution result: {}", result);
                    apply_statement_outcome(&result, &mut resp);

                    // statement_handler now performs any metastore actions after dispatch.
                }
            }
            Err(e) => {
                log::debug!("Failed to parse query: {}", e);
                resp.status = "ERROR".to_string();
                resp.error_code = "1".to_string();
                resp.message = e;
            }
        }

        Ok(Response::new(resp))
    }
}

#[cfg(test)]
#[path = "../tests/services_warehouse_service_server_tests.rs"]
mod tests;
