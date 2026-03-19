use crate::services::metastore_service::metastore_service;
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

/// What: Routes grant-role RBAC request to the metastore provider.
///
/// Inputs:
/// - `provider`: Metastore provider implementation
/// - `req`: Grant role request payload
///
/// Output:
/// - Metastore oneof result carrying `GrantRoleResponse`
///
/// Details:
/// - Keeps action dispatch thin and delegates validation/persistence to provider.
pub async fn handle(
    provider: &Arc<dyn MetastoreProvider>,
    req: metastore_service::GrantRoleRequest,
) -> metastore_service::metastore_response::Result {
    let resp = provider.grant_role(req).await;
    metastore_service::metastore_response::Result::GrantRoleResponse(resp)
}
