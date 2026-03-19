pub mod interops;
pub mod state;
pub mod storage;

mod txn;

pub mod interops_service {
    tonic::include_proto!("interops_service");
}
