use super::super::metastore_service::{metastore_service, metastore_service::CreateTableRequest, metastore_service::metastore_response::Result};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: CreateTableRequest) -> Result {
    let resp = provider.create_table(req).await;
    Result::CreateTableResponse(resp)
}
