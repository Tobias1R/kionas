use super::super::metastore_service::{metastore_service, metastore_service::DropSchemaRequest, metastore_service::metastore_response::Result};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: metastore_service::DropSchemaRequest) -> Result {
    let resp = provider.drop_schema(req).await;
    Result::DropSchemaResponse(resp)
}
