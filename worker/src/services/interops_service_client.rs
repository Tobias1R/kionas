use crate::state::SharedData;

// Second gRPC service
pub mod interops_service {
    tonic::include_proto!("interops_service");
}

#[derive(Default)]
pub struct InteropsService {
    shared_data: SharedData,
}
