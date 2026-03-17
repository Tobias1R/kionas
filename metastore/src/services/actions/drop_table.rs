use super::super::metastore_service::{
    metastore_service::DropTableRequest, metastore_service::metastore_response::Result,
};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: DropTableRequest) -> Result {
    let resp = provider.drop_table(req).await;
    Result::DropTableResponse(resp)
}
