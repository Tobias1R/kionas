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

        async fn handle_query(query: String, session_id: String, shared_data: SharedData) {
            match parse_query(&query) {
                Ok(statements) => {
                    println!("Parsed statements: {:?}", statements);
                    for stmt in &statements {
                        let result = handle_statement(stmt, &session_id, &shared_data).await;
                        println!("Execution result: {}", result);
                        // TODO: Populate response with result
                        // Arrow-flight
                    }
                }
                Err(e) => {
                    println!("Failed to parse query: {}", e);
                }
            }
        }

        handle_query(query_text.clone(), session_id.clone(), shared_data).await;

        Ok(Response::new(resp))
    }

    
}
