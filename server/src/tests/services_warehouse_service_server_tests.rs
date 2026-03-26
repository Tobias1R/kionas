use super::{apply_statement_outcome, extract_query_handle_from_message};
use crate::services::warehouse_service_server::warehouse_service::QueryResponse;

#[test]
fn extracts_query_handle_from_message_suffix() {
    let msg = "query dispatched successfully for db.schema.tbl (location: flight://worker:50052/query/db/schema/tbl/worker?session_id=s1&task_id=t1)";
    let handle = extract_query_handle_from_message(msg).expect("handle must be parsed");
    assert_eq!(
        handle,
        "flight://worker:50052/query/db/schema/tbl/worker?session_id=s1&task_id=t1"
    );
}

#[test]
fn maps_query_dispatched_handle_into_response_data() {
    let mut resp = QueryResponse {
        message: String::new(),
        status: "OK".to_string(),
        error_code: "0".to_string(),
        execution_time: "0".to_string(),
        data: Vec::new(),
    };

    apply_statement_outcome(
        "RESULT|SUCCESS|QUERY_DISPATCHED|query dispatched successfully for db.schema.tbl (location: flight://worker:50052/query/db/schema/tbl/worker?session_id=s1&task_id=t1)",
        &mut resp,
    );

    let decoded = String::from_utf8(resp.data.clone()).expect("response data must be utf8");
    assert_eq!(resp.status, "OK");
    assert_eq!(resp.error_code, "0");
    assert_eq!(
        decoded,
        "flight://worker:50052/query/db/schema/tbl/worker?session_id=s1&task_id=t1"
    );
}

#[test]
fn clears_stale_query_handle_for_non_query_outcome() {
    let mut resp = QueryResponse {
        message: String::new(),
        status: "OK".to_string(),
        error_code: "0".to_string(),
        execution_time: "0".to_string(),
        data: Vec::new(),
    };

    apply_statement_outcome(
        "RESULT|SUCCESS|QUERY_DISPATCHED|query dispatched successfully for db.schema.tbl (location: flight://worker:50052/query/db/schema/tbl/worker?session_id=s1&task_id=t1)",
        &mut resp,
    );
    assert!(!resp.data.is_empty());

    apply_statement_outcome(
        "RESULT|SUCCESS|TABLE_CREATED|table created successfully",
        &mut resp,
    );
    assert!(resp.data.is_empty());
}

#[test]
fn maps_unsupported_operator_as_validation_error() {
    let mut resp = QueryResponse {
        message: String::new(),
        status: "OK".to_string(),
        error_code: "0".to_string(),
        execution_time: "0".to_string(),
        data: Vec::new(),
    };

    apply_statement_outcome(
        "RESULT|VALIDATION|UNSUPPORTED_OPERATOR|physical operator 'HashJoin' is not supported in this phase",
        &mut resp,
    );

    assert_eq!(resp.status, "ERROR");
    assert_eq!(resp.error_code, "VALIDATION_UNSUPPORTED_OPERATOR");
    assert_eq!(
        resp.message,
        "physical operator 'HashJoin' is not supported in this phase"
    );
}

#[test]
fn maps_unsupported_predicate_as_validation_error() {
    let mut resp = QueryResponse {
        message: String::new(),
        status: "OK".to_string(),
        error_code: "0".to_string(),
        execution_time: "0".to_string(),
        data: Vec::new(),
    };

    apply_statement_outcome(
        "RESULT|VALIDATION|UNSUPPORTED_PREDICATE|predicate is not supported in this phase: LIKE",
        &mut resp,
    );

    assert_eq!(resp.status, "ERROR");
    assert_eq!(resp.error_code, "VALIDATION_UNSUPPORTED_PREDICATE");
    assert_eq!(
        resp.message,
        "predicate is not supported in this phase: LIKE"
    );
}

#[test]
fn maps_unsupported_pipeline_as_validation_error() {
    let mut resp = QueryResponse {
        message: String::new(),
        status: "OK".to_string(),
        error_code: "0".to_string(),
        execution_time: "0".to_string(),
        data: Vec::new(),
    };

    apply_statement_outcome(
        "RESULT|VALIDATION|UNSUPPORTED_PIPELINE|invalid physical pipeline: pipeline must end with materialize",
        &mut resp,
    );

    assert_eq!(resp.status, "ERROR");
    assert_eq!(resp.error_code, "VALIDATION_UNSUPPORTED_PIPELINE");
    assert_eq!(
        resp.message,
        "invalid physical pipeline: pipeline must end with materialize"
    );
}

#[test]
fn maps_select_validation_codes_to_validation_error_family() {
    let cases = vec![
        (
            "UNSUPPORTED_QUERY_SHAPE",
            "VALIDATION_UNSUPPORTED_QUERY_SHAPE",
        ),
        (
            "VALIDATION_UNSUPPORTED_QUERY_SHAPE",
            "VALIDATION_UNSUPPORTED_QUERY_SHAPE",
        ),
        ("UNSUPPORTED_OPERATOR", "VALIDATION_UNSUPPORTED_OPERATOR"),
        (
            "VALIDATION_UNSUPPORTED_OPERATOR",
            "VALIDATION_UNSUPPORTED_OPERATOR",
        ),
        ("UNSUPPORTED_PREDICATE", "VALIDATION_UNSUPPORTED_PREDICATE"),
        (
            "VALIDATION_UNSUPPORTED_PREDICATE",
            "VALIDATION_UNSUPPORTED_PREDICATE",
        ),
        ("UNSUPPORTED_PIPELINE", "VALIDATION_UNSUPPORTED_PIPELINE"),
        (
            "VALIDATION_UNSUPPORTED_PIPELINE",
            "VALIDATION_UNSUPPORTED_PIPELINE",
        ),
    ];

    for (validation_code, expected_error_code) in cases {
        let mut resp = QueryResponse {
            message: String::new(),
            status: "OK".to_string(),
            error_code: "0".to_string(),
            execution_time: "0".to_string(),
            data: Vec::new(),
        };

        apply_statement_outcome(
            &format!(
                "RESULT|VALIDATION|{}|validation message for {}",
                validation_code, validation_code
            ),
            &mut resp,
        );

        assert_eq!(resp.status, "ERROR");
        assert_eq!(resp.error_code, expected_error_code);
        assert_eq!(
            resp.message,
            format!("validation message for {}", validation_code)
        );
    }
}

#[test]
fn maps_constraint_and_execution_categories_deterministically() {
    let mut resp = QueryResponse {
        message: String::new(),
        status: "OK".to_string(),
        error_code: "0".to_string(),
        execution_time: "0".to_string(),
        data: Vec::new(),
    };

    apply_statement_outcome(
        "RESULT|CONSTRAINT|NOT_NULL_VIOLATION|missing not null column",
        &mut resp,
    );
    assert_eq!(resp.status, "ERROR");
    assert_eq!(resp.error_code, "CONSTRAINT_NOT_NULL_VIOLATION");

    apply_statement_outcome(
        "RESULT|EXECUTION|WORKER_OPERATOR_FAILED|hash join failed",
        &mut resp,
    );
    assert_eq!(resp.status, "ERROR");
    assert_eq!(resp.error_code, "EXECUTION_WORKER_OPERATOR_FAILED");
}

#[test]
fn maps_non_structured_outcome_to_infra_generic() {
    let mut resp = QueryResponse {
        message: String::new(),
        status: "OK".to_string(),
        error_code: "0".to_string(),
        execution_time: "0".to_string(),
        data: Vec::new(),
    };

    apply_statement_outcome("Unsupported statement", &mut resp);
    assert_eq!(resp.status, "ERROR");
    assert_eq!(resp.error_code, "INFRA_GENERIC");
    assert_eq!(resp.message, "Unsupported statement");
}
