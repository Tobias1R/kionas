// First gRPC service
#[allow(dead_code, clippy::enum_variant_names)]
pub mod worker_service {
    tonic::include_proto!("worker_service");
}
