use crate::services::metastore_service::metastore_service;
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

/// What: Routes drop-role RBAC request to the metastore provider.
///
/// Inputs:
/// - `provider`: Metastore provider implementation
/// - `req`: Drop role request payload
///
/// Output:
/// - Metastore oneof result carrying `DropRoleResponse`
///
/// Details:
/// - Keeps action dispatch thin and delegates validation/persistence to provider.
pub async fn handle(
    provider: &Arc<dyn MetastoreProvider>,
    req: metastore_service::DropRoleRequest,
) -> metastore_service::metastore_response::Result {
    let resp = provider.drop_role(req).await;
    metastore_service::metastore_response::Result::DropRoleResponse(resp)
}
