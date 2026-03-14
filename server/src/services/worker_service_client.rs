use crate::warehouse::state::SharedData;

// First gRPC service
pub mod worker_service {
    tonic::include_proto!("worker_service");
}

#[derive(Default)]
pub struct WorkerServiceClient {
    shared_data: SharedData,
}

