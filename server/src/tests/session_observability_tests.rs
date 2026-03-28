use crate::session::Session;

#[test]
fn legacy_session_json_deserializes_with_zero_query_stats() {
    let raw = r#"{
        "id":"s-1",
        "auth_token":"token-1",
        "is_authenticated":true,
        "role":"admin",
        "warehouse_name":"compute_xl",
        "pool_members":[],
        "remote_addr":"127.0.0.1",
        "last_active":100,
        "use_database":"db1"
    }"#;

    let session: Session = serde_json::from_str(raw).expect("legacy session JSON should parse");

    assert_eq!(session.get_query_count(), 0);
    assert_eq!(session.get_last_query_at(), 0);
    assert_eq!(session.get_total_query_duration_ms(), 0);
    assert_eq!(session.get_error_count(), 0);
}

#[test]
fn record_query_completion_updates_success_and_error_counters() {
    let mut session = Session::new(
        "s-1".to_string(),
        "token".to_string(),
        "admin".to_string(),
        "compute_xl".to_string(),
        Vec::new(),
        "127.0.0.1".to_string(),
    );

    session.record_query_completion(25, true);
    let first_last_query_at = session.get_last_query_at();

    assert_eq!(session.get_query_count(), 1);
    assert!(first_last_query_at > 0);
    assert_eq!(session.get_total_query_duration_ms(), 25);
    assert_eq!(session.get_error_count(), 0);

    session.record_query_completion(15, false);

    assert_eq!(session.get_query_count(), 2);
    assert!(session.get_last_query_at() >= first_last_query_at);
    assert_eq!(session.get_total_query_duration_ms(), 40);
    assert_eq!(session.get_error_count(), 1);
}

#[test]
fn record_query_completion_uses_saturating_add_for_duration_and_counts() {
    let raw = r#"{
        "id":"s-2",
        "auth_token":"token-2",
        "is_authenticated":true,
        "role":"admin",
        "warehouse_name":"compute_xl",
        "pool_members":[],
        "remote_addr":"127.0.0.1",
        "last_active":100,
        "use_database":"db1",
        "query_count":18446744073709551615,
        "last_query_at":0,
        "total_query_duration_ms":18446744073709551615,
        "error_count":18446744073709551615
    }"#;

    let mut session: Session = serde_json::from_str(raw).expect("seeded session JSON should parse");
    session.record_query_completion(10, false);

    assert_eq!(session.get_query_count(), u64::MAX);
    assert_eq!(session.get_total_query_duration_ms(), u64::MAX);
    assert_eq!(session.get_error_count(), u64::MAX);
}
