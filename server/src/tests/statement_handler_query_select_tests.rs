use kionas::parser::sql::parse_query;
use kionas::sql::query_model::{
    QueryModelError, VALIDATION_CODE_UNSUPPORTED_OPERATOR, VALIDATION_CODE_UNSUPPORTED_PIPELINE,
    VALIDATION_CODE_UNSUPPORTED_PREDICATE, build_select_query_dispatch_envelope,
    validation_code_for_query_error,
};

#[test]
fn minimal_select_payload_builds() {
    let statements = parse_query("SELECT id, name FROM sales.public.users WHERE active = true")
        .expect("statement should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        kionas::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let canonical = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
        .expect("payload should build");
    let payload = canonical.payload;
    assert!(payload.contains("\"statement\":\"Select\""));
    assert!(payload.contains("\"database\":\"sales\""));
    assert!(payload.contains("\"table\":\"users\""));
}

#[test]
fn rejects_multi_table_shape() {
    let statements = parse_query("SELECT * FROM a, b").expect("statement should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        kionas::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let err = build_select_query_dispatch_envelope(query, "s1", "default", "public")
        .expect_err("should reject multi-table select");
    assert!(err.to_string().contains("exactly one table"));
}

#[test]
fn extracts_namespace_from_payload() {
    let statements =
        parse_query("SELECT id FROM sales.public.users").expect("statement should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        kionas::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let canonical = build_select_query_dispatch_envelope(query, "s1", "sales", "public")
        .expect("payload should build");
    let database = canonical.database;
    let schema = canonical.schema;
    let table = canonical.table;

    assert_eq!(database, "sales");
    assert_eq!(schema, "public");
    assert_eq!(table, "users");
}

#[test]
fn maps_capability_error_codes() {
    assert_eq!(
        validation_code_for_query_error(&QueryModelError::InvalidPhysicalPipeline(
            "pipeline must end with materialize".to_string(),
        )),
        VALIDATION_CODE_UNSUPPORTED_PIPELINE
    );
    assert_eq!(
        validation_code_for_query_error(&QueryModelError::UnsupportedPhysicalOperator(
            "HashJoin".to_string(),
        )),
        VALIDATION_CODE_UNSUPPORTED_OPERATOR
    );
    assert_eq!(
        validation_code_for_query_error(
            &QueryModelError::UnsupportedPredicate("LIKE".to_string(),)
        ),
        VALIDATION_CODE_UNSUPPORTED_PREDICATE
    );
}

#[test]
fn parse_foundation_predicate_ast_accepts_case_insensitive_and() {
    let parsed = super::parse_foundation_predicate_ast("id >= 10 aNd score < 99");
    assert!(parsed.is_some());
}

#[test]
fn parse_foundation_predicate_ast_rejects_case_insensitive_or() {
    let parsed = super::parse_foundation_predicate_ast("id >= 10 oR score < 99");
    assert!(parsed.is_none());
}
