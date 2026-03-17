use super::super::metastore_service::{
    metastore_service::GetSchemaRequest, metastore_service::metastore_response::Result,
};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: GetSchemaRequest) -> Result {
    let resp = provider.get_schema(req).await;
    Result::GetSchemaResponse(resp)
}
