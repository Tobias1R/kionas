// Minimal, clean service implementation for `warehouse_service`.
// This file intentionally keeps handler logic simple so the crate builds.

use tonic::{Request, Response, Status};
use crate::warehouse::state::SharedData;
use kionas::parser::sql::parse_query;
use crate::statement_handler::handle_statement;

pub mod warehouse_service {
    tonic::include_proto!("warehouse_service");
}

use warehouse_service::warehouse_service_server::WarehouseService as WarehouseServiceTrait;
use warehouse_service::{
    QueryStatusRequest, QueryStatusResponse,
    QueryRequest, QueryResponse    
};

use crate::services::request_context::RequestContext;
use crate::tasks::{TaskManager, TaskState};
use chrono::Utc;

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
        let query_id = ctx.query_id.clone();

        // Prepare a single mutable response and populate it based on interpretation of the query.
        let mut resp = QueryResponse {
            message: String::new(),
            status: "OK".to_string(),
            error_code: "0".to_string(),
            execution_time: "0".to_string(),
            data: Vec::new(),
        };


        let shared_data = self.shared_data.clone();

        // Centralized handler for simple commands (e.g. `USE WAREHOUSE`) that
        // do not produce a normal SQL AST. The logic lives in the
        // `statement_handler` package so command handling is centralized.
        if let Some(msg) = crate::statement_handler::maybe_handle_direct_command(&query_text, &session_id, &shared_data).await {
            resp.message = msg;
            return Ok(Response::new(resp));
        }

        async fn handle_query(query: String, session_id: String, shared_data: SharedData, query_id: String) {
            match parse_query(&query) {
                Ok(statements) => {
                    println!("Parsed statements: {:?}", statements);

                    // Acquire TaskManager reference without holding shared_data lock across awaits
                    let task_manager = {
                        let state = shared_data.lock().await;
                        state.task_manager.clone()
                    };

                    for stmt in &statements {
                        // Create a task representing this statement
                        let payload = format!("{:?}", stmt);
                        let task_id = task_manager.create_task(query_id.clone(), session_id.clone(), "sql_statement".to_string(), payload).await;
                        // Mark scheduled then running
                        task_manager.set_state(&task_id, TaskState::Scheduled).await;
                        task_manager.set_state(&task_id, TaskState::Running).await;

                        // Execute handler synchronously (existing logic performs dispatch to worker)
                        let result = handle_statement(stmt, &session_id, &shared_data).await;
                        println!("Execution result: {}", result);

                        // Record the textual result as the result_location for now
                        if let Some(task_arc) = task_manager.get_task(&task_id).await {
                            let mut t = task_arc.lock().await;
                            t.result_location = Some(result.clone());
                            t.error = None;
                            t.state = TaskState::Succeeded;
                            t.finished_at = Some(Utc::now());
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to parse query: {}", e);
                }
            }
        }

        handle_query(query_text.clone(), session_id.clone(), shared_data, query_id.clone()).await;

        Ok(Response::new(resp))
    }

    
}
