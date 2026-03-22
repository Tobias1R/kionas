use super::{normalize_decimal_literal, parse_datetime_literal, parse_insert_column_type_hints};
use crate::services::worker_service_server::worker_service;
use std::collections::HashMap;

fn task_with_params(params: HashMap<String, String>) -> worker_service::Task {
    worker_service::Task {
        task_id: "t1".to_string(),
        operation: "insert".to_string(),
        input: String::new(),
        output: String::new(),
        params,
        filter_predicate: None,
    }
}

#[test]
fn rejects_datetime_literal_with_timezone_offset() {
    let err = parse_datetime_literal("'2024-01-01T00:00:00+02:00'")
        .expect_err("datetime timezone offsets must be rejected");
    assert!(err.contains("DATETIME_TIMEZONE_NOT_ALLOWED"));
}

#[test]
fn normalizes_decimal_literal_with_scale_padding() {
    let normalized = normalize_decimal_literal("'10.5'", Some((6, 3)))
        .expect("decimal coercion should normalize value");
    assert_eq!(normalized, "10.500");
}

#[test]
fn rejects_decimal_literal_exceeding_precision() {
    let err = normalize_decimal_literal("'1234.56'", Some((5, 2)))
        .expect_err("precision overflow should fail");
    assert!(err.contains("DECIMAL_COERCION_FAILED"));
    assert!(err.contains("exceeds precision"));
}

#[test]
fn rejects_missing_type_hints_in_contract_mode() {
    let mut params = HashMap::new();
    params.insert("datatype_contract_version".to_string(), "1".to_string());
    let task = task_with_params(params);

    let err = parse_insert_column_type_hints(&task)
        .expect_err("contract mode should require type hints payload");
    assert!(err.contains("INSERT_TYPE_HINTS_MALFORMED"));
}
