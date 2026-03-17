use super::super::metastore_service::{
    metastore_service::GetTableRequest, metastore_service::metastore_response::Result,
};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: GetTableRequest) -> Result {
    let resp = provider.get_table(req).await;
    Result::GetTableResponse(resp)
}
