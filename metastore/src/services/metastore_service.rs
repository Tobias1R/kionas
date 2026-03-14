use tonic::{Request, Response, Status};
pub mod metastore_service {
    tonic::include_proto!("metastore_service");
}

use crate::services::actions;
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub struct MetastoreService {
    pub provider: Arc<dyn MetastoreProvider>,
}

impl MetastoreService {
    pub fn new(provider: Arc<dyn MetastoreProvider>) -> Self {
        Self { provider }
    }
}

#[tonic::async_trait]
impl metastore_service::metastore_service_server::MetastoreService for MetastoreService {
    async fn execute(
        &self,
        request: Request<metastore_service::MetastoreRequest>,
    ) -> Result<Response<metastore_service::MetastoreResponse>, Status> {
        let req = request.into_inner();
        let mut response = metastore_service::MetastoreResponse::default();
        match req.action {
            Some(metastore_service::metastore_request::Action::CreateTable(create)) => {
                response.result = Some(actions::create_table::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::DropTable(drop)) => {
                response.result = Some(actions::drop_table::handle(&self.provider, drop).await);
            }
            Some(metastore_service::metastore_request::Action::GetTable(get)) => {
                response.result = Some(actions::get_table::handle(&self.provider, get).await);
            }
            Some(metastore_service::metastore_request::Action::CreateSchema(create)) => {
                response.result = Some(actions::create_schema::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::DropSchema(drop)) => {
                response.result = Some(actions::drop_schema::handle(&self.provider, drop).await);
            }
            Some(metastore_service::metastore_request::Action::GetSchema(get)) => {
                response.result = Some(actions::get_schema::handle(&self.provider, get).await);
            }
            None => {
                return Err(Status::invalid_argument("No action provided"));
            }
        }
        Ok(Response::new(response))
    }

    async fn update_table_statistics(
        &self,
        request: Request<metastore_service::UpdateTableStatisticsRequest>,
    ) -> Result<Response<metastore_service::UpdateTableStatisticsResponse>, Status> {
        actions::update_table_statistics::handle(&self.provider, request.into_inner()).await
    }

    async fn get_table_statistics(
        &self,
        request: Request<metastore_service::GetTableStatisticsRequest>,
    ) -> Result<Response<metastore_service::GetTableStatisticsResponse>, Status> {
        actions::get_table_statistics::handle(&self.provider, request.into_inner()).await
    }
}

// To use in main.rs:
// let svc = MyMetastoreService::default();
// Server::builder().add_service(MetastoreServiceServer::new(svc));
