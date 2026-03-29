use tonic::{Request, Response, Status};
#[allow(clippy::module_inception)]
#[allow(dead_code)]
#[allow(clippy::enum_variant_names)]
pub mod metastore_service {
    tonic::include_proto!("metastore_service");
}

pub mod interops_service {
    tonic::include_proto!("interops_service");
}

use crate::services::actions;
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};

pub struct MetastoreService {
    pub provider: Arc<dyn MetastoreProvider>,
    pub invalidation_notifier: Option<TableSchemaInvalidationNotifier>,
}

#[derive(Clone)]
pub struct TableSchemaInvalidationNotifier {
    server_addr: String,
    tls_identity_pem: Option<(Vec<u8>, Vec<u8>)>,
    ca_cert_pem: Option<Vec<u8>>,
}

impl TableSchemaInvalidationNotifier {
    pub fn new(
        server_addr: String,
        tls_identity_pem: Option<(Vec<u8>, Vec<u8>)>,
        ca_cert_pem: Option<Vec<u8>>,
    ) -> Self {
        Self {
            server_addr,
            tls_identity_pem,
            ca_cert_pem,
        }
    }

    async fn connect(
        &self,
    ) -> Result<interops_service::interops_service_client::InteropsServiceClient<Channel>, String>
    {
        let mut endpoint = Channel::from_shared(self.server_addr.clone()).map_err(|error| {
            format!(
                "invalid interops endpoint '{}': {}",
                self.server_addr, error
            )
        })?;

        if self.server_addr.starts_with("https://") {
            let mut tls = ClientTlsConfig::new();
            if let Some((cert, key)) = &self.tls_identity_pem {
                tls = tls.identity(Identity::from_pem(cert.clone(), key.clone()));
            }
            if let Some(ca_cert) = &self.ca_cert_pem {
                tls = tls.ca_certificate(Certificate::from_pem(ca_cert.clone()));
            }
            endpoint = endpoint
                .tls_config(tls)
                .map_err(|error| format!("failed to configure interops TLS client: {}", error))?;
        }

        let channel = endpoint.connect().await.map_err(|error| {
            format!(
                "failed to connect interops endpoint '{}': {}",
                self.server_addr, error
            )
        })?;
        Ok(interops_service::interops_service_client::InteropsServiceClient::new(channel))
    }

    pub async fn invalidate_table_schema(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Result<(), String> {
        let mut client = self.connect().await?;
        client
            .invalidate_table_schema(Request::new(
                interops_service::InvalidateTableSchemaRequest {
                    database: database.to_string(),
                    schema: schema.to_string(),
                    table: table.to_string(),
                },
            ))
            .await
            .map_err(|error| format!("interops invalidate_table_schema RPC failed: {}", error))?;
        Ok(())
    }
}

impl MetastoreService {
    pub fn new(
        provider: Arc<dyn MetastoreProvider>,
        invalidation_notifier: Option<TableSchemaInvalidationNotifier>,
    ) -> Self {
        Self {
            provider,
            invalidation_notifier,
        }
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
                let database_name = create.database_name.trim().to_ascii_lowercase();
                let schema_name = create.schema_name.trim().to_ascii_lowercase();
                let table_name = create.table_name.trim().to_ascii_lowercase();
                let create_response = actions::create_table::handle(&self.provider, create).await;
                if let metastore_service::metastore_response::Result::CreateTableResponse(result) =
                    &create_response
                    && result.success
                    && let Some(notifier) = &self.invalidation_notifier
                    && let Err(error) = notifier
                        .invalidate_table_schema(
                            database_name.as_str(),
                            schema_name.as_str(),
                            table_name.as_str(),
                        )
                        .await
                {
                    log::warn!(
                        "table schema cache invalidation after create_table failed for {}.{}.{}: {}",
                        database_name,
                        schema_name,
                        table_name,
                        error
                    );
                }
                response.result = Some(create_response);
            }
            Some(metastore_service::metastore_request::Action::DropTable(drop)) => {
                let schema_name = drop.schema_name.trim().to_ascii_lowercase();
                let table_name = drop.table_name.trim().to_ascii_lowercase();
                let drop_response = actions::drop_table::handle(&self.provider, drop).await;
                if let metastore_service::metastore_response::Result::DropTableResponse(result) =
                    &drop_response
                    && result.success
                    && let Some(notifier) = &self.invalidation_notifier
                    && let Err(error) = notifier
                        .invalidate_table_schema(
                            "deltalake",
                            schema_name.as_str(),
                            table_name.as_str(),
                        )
                        .await
                {
                    log::warn!(
                        "table schema cache invalidation after drop_table failed for deltalake.{}.{}: {}",
                        schema_name,
                        table_name,
                        error
                    );
                }
                response.result = Some(drop_response);
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
