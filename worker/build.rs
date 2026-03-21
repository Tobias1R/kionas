fn main() {
    let proto_dir = &["../kionas/proto"];
    let protos = &["worker_service.proto", "interops_service.proto"];
    tonic_build::configure()
        .build_server(true) // Include server-side code
        .build_client(true)
        .compile_protos(protos, proto_dir)
        .unwrap_or_else(|e| panic!("Failed to compile protos {:?}", e));
}
