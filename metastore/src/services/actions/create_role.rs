use crate::services::metastore_service::metastore_service;
use crate::services::provider::postgres::MetastoreProvider;
use std::sync::Arc;

/// What: Routes create-role RBAC request to the metastore provider.
///
/// Inputs:
/// - `provider`: Metastore provider implementation
/// - `req`: Create role request payload
///
/// Output:
/// - Metastore oneof result carrying `CreateRoleResponse`
///
/// Details:
/// - Keeps action dispatch thin and delegates validation/persistence to provider.
pub async fn handle(
    provider: &Arc<dyn MetastoreProvider>,
    req: metastore_service::CreateRoleRequest,
) -> metastore_service::metastore_response::Result {
    let resp = provider.create_role(req).await;
    metastore_service::metastore_response::Result::CreateRoleResponse(resp)
}
