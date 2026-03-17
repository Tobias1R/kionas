use super::super::metastore_service::{
    metastore_service, metastore_service::metastore_response::Result,
};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub async fn handle(
    provider: &Arc<dyn MetastoreProvider>,
    req: metastore_service::CreateSchemaRequest,
) -> Result {
    let resp = provider.create_schema(req).await;
    Result::CreateSchemaResponse(resp)
}
