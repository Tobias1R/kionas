// Minimal, clean service implementation for `warehouse_service`.
// This file intentionally keeps handler logic simple so the crate builds.

use crate::statement_handler::handle_statement;
use crate::warehouse::state::SharedData;
use kionas::parser::sql::parse_query;
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
/// - Updates `resp.message`, `resp.status`, and `resp.error_code` in-place.
///
/// Details:
/// - Structured outputs use `RESULT|<category>|<code>|<message>`.
/// - Legacy plain-text outputs still map to generic infra failures when applicable.
fn apply_statement_outcome(result: &str, resp: &mut QueryResponse) {
    let mut parts = result.splitn(4, '|');
    let prefix = parts.next().unwrap_or_default();
    if prefix != "RESULT" {
        if result.starts_with("Failed")
            || result.starts_with("Domain validation failed")
            || result.starts_with("Unsupported")
        {
            resp.status = "ERROR".to_string();
            resp.error_code = "INFRA_GENERIC".to_string();
        }
        resp.message = result.to_string();
        return;
    }

    let category = parts.next().unwrap_or("INFRA");
    let code = parts.next().unwrap_or("UNKNOWN");
    let message = parts.next().unwrap_or("unknown error");

    resp.message = message.to_string();
    match category {
        "SUCCESS" => {
            resp.status = "OK".to_string();
            resp.error_code = "0".to_string();
        }
        "VALIDATION" => {
            resp.status = "ERROR".to_string();
            resp.error_code = format!("BUSINESS_{}", code);
        }
        _ => {
            resp.status = "ERROR".to_string();
            resp.error_code = format!("INFRA_{}", code);
        }
    }
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

        // print
        log::info!("Received query: {}", query_text);

        // Use the query id from the context so callers can correlate work.
        let _query_id = ctx.query_id.clone();

        // Prepare a single mutable response and populate it based on interpretation of the query.
        let mut resp = QueryResponse {
            message: String::new(),
            status: "OK".to_string(),
            error_code: "0".to_string(),
            execution_time: "0".to_string(),
            data: Vec::new(),
        };

        let shared_data = self.shared_data.clone();

        // Attempt to fetch and log the full session (if available) for better observability
        if !ctx.session_id.is_empty() {
            let session_manager = {
                let sd = shared_data.lock().await;
                sd.session_manager.clone()
            };
            if let Some(sess) = session_manager.get_session(ctx.session_id.clone()).await {
                log::info!("Session: {:?}", sess);
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
                println!("Parsed statements: {:?}", statements);

                // Acquire TaskManager reference without holding shared_data lock across awaits
                let _task_manager = {
                    let state = shared_data.lock().await;
                    state.task_manager.clone()
                };

                for stmt in &statements {
                    // Execute handler synchronously (existing logic performs dispatch to worker)
                    let result = handle_statement(stmt, &session_id, &shared_data).await;
                    println!("Execution result: {}", result);
                    apply_statement_outcome(&result, &mut resp);

                    // statement_handler now performs any metastore actions after dispatch.
                }
            }
            Err(e) => {
                println!("Failed to parse query: {}", e);
                resp.status = "ERROR".to_string();
                resp.error_code = "1".to_string();
                resp.message = e;
            }
        }

        Ok(Response::new(resp))
    }
}
