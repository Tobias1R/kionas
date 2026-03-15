use std::error::Error;

use crate::auth::jwt::JwtInterceptor;
use crate::warehouse::state::SharedData;

/// Initialize authentication related pieces.
/// Returns (JwtInterceptor, WarehouseAuthServiceBackend)
pub async fn initialize_auth(
    shared_data: SharedData,
    jwt_secret: String,
    data_path: String,
) -> Result<(JwtInterceptor, crate::services::warehouse_auth_service::WarehouseAuthServiceBackend), Box<dyn Error + Send + Sync>> {
    let jwt_interceptor = JwtInterceptor::new(jwt_secret.clone());

    let auth_service = crate::services::warehouse_auth_service::WarehouseAuthServiceBackend::initialize(
        shared_data,
        data_path,
        jwt_secret.clone(),
    )
    .await;

    Ok((jwt_interceptor, auth_service))
}
