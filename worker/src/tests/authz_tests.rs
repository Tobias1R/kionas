use super::WorkerAuthorizer;
use tonic::metadata::MetadataValue;

#[tokio::test]
async fn rejects_missing_auth_metadata() {
    let authz = WorkerAuthorizer::new();
    let metadata = tonic::metadata::MetadataMap::new();
    let err = authz
        .extract_dispatch_context(&metadata, "s1")
        .expect_err("must reject missing metadata");
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

#[tokio::test]
async fn validates_signed_ticket_round_trip() {
    let authz = WorkerAuthorizer::new();
    let mut metadata = tonic::metadata::MetadataMap::new();
    metadata.insert("session_id", MetadataValue::from_static("s1"));
    metadata.insert("rbac_user", MetadataValue::from_static("alice"));
    metadata.insert("rbac_role", MetadataValue::from_static("ADMIN"));
    metadata.insert("auth_scope", MetadataValue::from_static("select:*"));

    let ctx = authz
        .extract_dispatch_context(&metadata, "s1")
        .expect("must extract context");

    let ticket = authz
        .issue_signed_flight_ticket(&ctx, "t1", "worker1")
        .expect("ticket must sign");
    let claims = authz
        .verify_signed_flight_ticket(&ticket)
        .expect("ticket must verify");

    assert_eq!(claims.sid, "s1");
    assert_eq!(claims.tid, "t1");
    assert_eq!(claims.wid, "worker1");
}
