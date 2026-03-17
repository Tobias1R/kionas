use std::env;
use std::path::PathBuf;

fn main() {
    let proto_dir = &["../kionas/proto"];
    let protos = &["warehouse_service.proto", "warehouse_auth_service.proto"];
    let descriptor_path = PathBuf::from("../kionas/generated/").join("grpc.reflection.v1alpha");
    tonic_build::configure()
        .build_server(true) // Include server-side code
        .build_client(true)
        .file_descriptor_set_path(descriptor_path) // Include client-side code
        .compile_protos(protos, proto_dir)
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
