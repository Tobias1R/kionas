use tonic::{Request, Response, Status};
#[allow(clippy::module_inception)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
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
                response.result =
                    Some(actions::create_schema::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::DropSchema(drop)) => {
                response.result = Some(actions::drop_schema::handle(&self.provider, drop).await);
            }
            Some(metastore_service::metastore_request::Action::GetSchema(get)) => {
                response.result = Some(actions::get_schema::handle(&self.provider, get).await);
            }
            Some(metastore_service::metastore_request::Action::CreateDatabase(create)) => {
                response.result =
                    Some(actions::create_database::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::GetDatabase(get)) => {
                response.result = Some(actions::get_database::handle(&self.provider, get).await);
            }

            // Transaction actions
            Some(metastore_service::metastore_request::Action::CreateTransaction(create)) => {
                response.result =
                    Some(actions::create_transaction::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::UpdateTransactionState(update)) => {
                response.result =
                    Some(actions::update_transaction_state::handle(&self.provider, update).await);
            }
            Some(metastore_service::metastore_request::Action::GetTransaction(get)) => {
                response.result = Some(actions::get_transaction::handle(&self.provider, get).await);
            }
            Some(metastore_service::metastore_request::Action::CreateUser(create)) => {
                response.result = Some(actions::create_user::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::DeleteUser(delete)) => {
                response.result = Some(actions::delete_user::handle(&self.provider, delete).await);
            }
            Some(metastore_service::metastore_request::Action::CreateGroup(create)) => {
                response.result = Some(actions::create_group::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::DeleteGroup(delete)) => {
                response.result = Some(actions::delete_group::handle(&self.provider, delete).await);
            }
            Some(metastore_service::metastore_request::Action::CreateRole(create)) => {
                response.result = Some(actions::create_role::handle(&self.provider, create).await);
            }
            Some(metastore_service::metastore_request::Action::DropRole(drop)) => {
                response.result = Some(actions::drop_role::handle(&self.provider, drop).await);
            }
            Some(metastore_service::metastore_request::Action::GrantRole(grant)) => {
                response.result = Some(actions::grant_role::handle(&self.provider, grant).await);
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
