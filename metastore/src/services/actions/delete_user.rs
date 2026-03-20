use crate::services::metastore_service::metastore_service;
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

/// What: Routes delete-user RBAC request to the metastore provider.
///
/// Inputs:
/// - `provider`: Metastore provider implementation
/// - `req`: Delete user request payload
///
/// Output:
/// - Metastore oneof result carrying `DeleteUserResponse`
///
/// Details:
/// - Keeps action dispatch thin and delegates validation/persistence to provider.
pub async fn handle(
    provider: &Arc<dyn MetastoreProvider>,
    req: metastore_service::DeleteUserRequest,
) -> metastore_service::metastore_response::Result {
    let resp = provider.delete_user(req).await;
    metastore_service::metastore_response::Result::DeleteUserResponse(resp)
}
