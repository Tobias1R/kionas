use super::{map_insert_dispatch_error, normalize_insert_error_message};

#[test]
fn maps_known_insert_validation_and_constraint_errors() {
    assert_eq!(
        map_insert_dispatch_error("temporal_literal_invalid: bad timestamp"),
        ("VALIDATION", "VALIDATION_TEMPORAL_LITERAL_INVALID")
    );
    assert_eq!(
        map_insert_dispatch_error("not null constraint violated for column id"),
        ("CONSTRAINT", "CONSTRAINT_NOT_NULL_VIOLATION")
    );
    assert_eq!(
        map_insert_dispatch_error("missing not null column: id"),
        ("CONSTRAINT", "CONSTRAINT_NOT_NULL_COLUMNS_MISSING")
    );
}

#[test]
fn maps_known_insert_infra_and_table_resolution_errors() {
    assert_eq!(
        map_insert_dispatch_error("register_object_store failed for s3://bucket/path"),
        ("INFRA", "INFRA_INSERT_OBJECT_STORE_UNAVAILABLE")
    );
    assert_eq!(
        map_insert_dispatch_error("table not found: sales.public.orders"),
        ("VALIDATION", "VALIDATION_TABLE_NOT_FOUND")
    );
}

#[test]
fn maps_unknown_insert_errors_to_infra_dispatch_failed() {
    assert_eq!(
        map_insert_dispatch_error("unexpected io timeout while dispatching"),
        ("INFRA", "INFRA_INSERT_DISPATCH_FAILED")
    );
}

#[test]
fn structured_result_errors_are_normalized_without_leaking_wrapped_prefix() {
    let code = "INFRA_INSERT_DISPATCH_FAILED";
    let wrapped = "RESULT|INFRA|WORKER_DOWNSTREAM_TIMEOUT|worker timed out at stage 2";
    assert_eq!(
        normalize_insert_error_message(code, wrapped),
        "worker timed out at stage 2"
    );
}
