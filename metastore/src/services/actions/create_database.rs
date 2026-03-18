use super::super::metastore_service::{
    metastore_service, metastore_service::metastore_response::Result,
};
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

/// What: Dispatch create_database action to the configured metastore provider.
///
/// Inputs:
/// - `provider`: Active provider implementation.
/// - `req`: Create-database request payload.
///
/// Output:
/// - Metastore oneof response variant for create-database.
///
/// Details:
/// - This keeps gRPC service dispatch thin and provider-driven.
pub async fn handle(
    provider: &Arc<dyn MetastoreProvider>,
    req: metastore_service::CreateDatabaseRequest,
) -> Result {
    let resp = provider.create_database(req).await;
    Result::CreateDatabaseResponse(resp)
}
