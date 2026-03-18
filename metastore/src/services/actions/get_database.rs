use super::super::metastore_service::{
    metastore_service::GetDatabaseRequest, metastore_service::metastore_response::Result,
};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

/// What: Dispatch get_database action to the configured metastore provider.
///
/// Inputs:
/// - `provider`: Active provider implementation.
/// - `req`: Get-database request payload.
///
/// Output:
/// - Metastore oneof response variant for get-database.
///
/// Details:
/// - Provider controls the lookup semantics and message details.
pub async fn handle(provider: &Arc<dyn MetastoreProvider>, req: GetDatabaseRequest) -> Result {
    let resp = provider.get_database(req).await;
    Result::GetDatabaseResponse(resp)
}
