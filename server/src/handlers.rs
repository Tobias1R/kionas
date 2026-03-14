use std::error::Error;
use std::future::Future;
use std::pin::Pin;

use tonic::transport::Server;
use tonic::transport::ServerTlsConfig;
use tonic_reflection::server::Builder as ReflectionBuilder;

type ServerFuture = Pin<Box<dyn Future<Output = Result<(), Box<dyn Error>>> + Send>>;

pub fn build_servers<A, W, I>(
    wh_tls_config: ServerTlsConfig,
    iops_tls_config: ServerTlsConfig,
    warehouse_addr: std::net::SocketAddr,
    interops_addr: std::net::SocketAddr,
    reflection_descriptor: &'static [u8],
    auth_service: A,
    jwt_interceptor: crate::auth::jwt::JwtInterceptor,
    warehouse_service_impl: W,
    interops_service_impl: I,
) -> (ServerFuture, ServerFuture)
where
    A: crate::services::warehouse_auth_service::warehouse_auth_service::warehouse_auth_service_server::WarehouseAuthService + Send + Sync + 'static,
    W: crate::services::warehouse_service_server::warehouse_service::warehouse_service_server::WarehouseService + Send + Sync + 'static,
    I: crate::services::interops_service_server::interops_service::interops_service_server::InteropsService + Send + Sync + 'static,
{
    // Build reflection service locally from the descriptor bytes so callers don't need the concrete type
    let reflection_service = ReflectionBuilder::configure()
        .register_encoded_file_descriptor_set(reflection_descriptor)
        .build_v1()
        .unwrap();

    let wh_reflection = reflection_service.clone();

    let wh_fut: ServerFuture = Box::pin(async move {
        let srv = Server::builder()
            .tls_config(wh_tls_config)
            .unwrap()
            .add_service(wh_reflection)
            .add_service(
                crate::services::warehouse_auth_service::warehouse_auth_service::warehouse_auth_service_server::WarehouseAuthServiceServer::new(
                    auth_service,
                ),
            )
            .add_service(
                crate::services::warehouse_service_server::warehouse_service::warehouse_service_server::WarehouseServiceServer::with_interceptor(
                    warehouse_service_impl,
                    jwt_interceptor,
                ),
            )
            .serve(warehouse_addr)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error>);

        srv
    });

    let iops_fut: ServerFuture = Box::pin(async move {
        let srv = Server::builder()
            .tls_config(iops_tls_config)
            .unwrap()
            .http2_adaptive_window(Some(true))
            .add_service(reflection_service)
            .add_service(
                crate::services::interops_service_server::interops_service::interops_service_server::InteropsServiceServer::new(
                    interops_service_impl,
                ),
            )
            .serve(interops_addr)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error>);

        srv
    });

    (wh_fut, iops_fut)
}
