use super::super::metastore_service::{metastore_service, metastore_service::GetTransactionRequest, metastore_service::metastore_response::Result};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: metastore_service::GetTransactionRequest) -> Result {
    let resp = provider.get_transaction(req).await;
    Result::GetTransactionResponse(resp)
}
