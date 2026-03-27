use crate::planner::{BinaryOperator, ExprPhysicalExpr, ExprType, ExprValue, evaluate_expression};
use chrono::NaiveDate;
use std::collections::HashMap;

fn sample_row() -> HashMap<String, ExprValue> {
    let mut row = HashMap::new();
    row.insert("id".to_string(), ExprValue::Int(7));
    row.insert("name".to_string(), ExprValue::String("Alice".to_string()));
    row.insert(
        "created".to_string(),
        ExprValue::Date(NaiveDate::from_ymd_opt(2024, 2, 29).expect("valid leap day")),
    );
    row
}

fn sample_schema() -> HashMap<String, ExprType> {
    let mut schema = HashMap::new();
    schema.insert("id".to_string(), ExprType::Int);
    schema.insert("name".to_string(), ExprType::String);
    schema.insert("created".to_string(), ExprType::Date);
    schema
}

#[test]
fn evaluates_column_reference() {
    let expr = ExprPhysicalExpr::ColumnRef {
        name: "id".to_string(),
    };

    let value = evaluate_expression(&expr, &sample_row(), &sample_schema())
        .expect("column reference should evaluate");
    assert_eq!(value, ExprValue::Int(7));
}

#[test]
fn evaluates_literal_value() {
    let expr = ExprPhysicalExpr::Literal {
        value: ExprValue::String("hello".to_string()),
    };

    let value = evaluate_expression(&expr, &sample_row(), &sample_schema())
        .expect("literal should evaluate");
    assert_eq!(value, ExprValue::String("hello".to_string()));
}

#[test]
fn evaluates_year_function() {
    let expr = ExprPhysicalExpr::FunctionCall {
        name: "YEAR".to_string(),
        args: vec![ExprPhysicalExpr::ColumnRef {
            name: "created".to_string(),
        }],
    };

    let value =
        evaluate_expression(&expr, &sample_row(), &sample_schema()).expect("YEAR should evaluate");
    assert_eq!(value, ExprValue::Int(2024));
}

#[test]
fn evaluates_lower_function() {
    let expr = ExprPhysicalExpr::FunctionCall {
        name: "LOWER".to_string(),
        args: vec![ExprPhysicalExpr::ColumnRef {
            name: "name".to_string(),
        }],
    };

    let value =
        evaluate_expression(&expr, &sample_row(), &sample_schema()).expect("LOWER should evaluate");
    assert_eq!(value, ExprValue::String("alice".to_string()));
}

#[test]
fn returns_error_on_type_mismatch() {
    let expr = ExprPhysicalExpr::FunctionCall {
        name: "YEAR".to_string(),
        args: vec![ExprPhysicalExpr::ColumnRef {
            name: "name".to_string(),
        }],
    };

    let err = evaluate_expression(&expr, &sample_row(), &sample_schema())
        .expect_err("YEAR on string should fail");
    assert!(err.contains("expects DATE"));
}

#[test]
fn evaluates_null_handling_for_coalesce() {
    let expr = ExprPhysicalExpr::FunctionCall {
        name: "COALESCE".to_string(),
        args: vec![
            ExprPhysicalExpr::Literal {
                value: ExprValue::Null,
            },
            ExprPhysicalExpr::Literal {
                value: ExprValue::String("fallback".to_string()),
            },
        ],
    };

    let value = evaluate_expression(&expr, &sample_row(), &sample_schema())
        .expect("COALESCE should evaluate");
    assert_eq!(value, ExprValue::String("fallback".to_string()));
}

#[test]
fn evaluates_nested_expressions() {
    let expr = ExprPhysicalExpr::FunctionCall {
        name: "YEAR".to_string(),
        args: vec![ExprPhysicalExpr::ColumnRef {
            name: "created".to_string(),
        }],
    };

    let comparison = ExprPhysicalExpr::BinaryOp {
        left: Box::new(expr),
        op: BinaryOperator::Eq,
        right: Box::new(ExprPhysicalExpr::Literal {
            value: ExprValue::Int(2024),
        }),
    };

    let value = evaluate_expression(&comparison, &sample_row(), &sample_schema())
        .expect("nested expression should evaluate");
    assert_eq!(value, ExprValue::Bool(true));
}

#[test]
fn evaluates_ceil_function() {
    let expr = ExprPhysicalExpr::FunctionCall {
        name: "CEIL".to_string(),
        args: vec![ExprPhysicalExpr::Literal {
            value: ExprValue::Float(12.25),
        }],
    };

    let value =
        evaluate_expression(&expr, &sample_row(), &sample_schema()).expect("CEIL should evaluate");
    assert_eq!(value, ExprValue::Float(13.0));
}

#[test]
fn evaluates_year_with_null_argument_as_null() {
    let expr = ExprPhysicalExpr::FunctionCall {
        name: "YEAR".to_string(),
        args: vec![ExprPhysicalExpr::Literal {
            value: ExprValue::Null,
        }],
    };

    let value =
        evaluate_expression(&expr, &sample_row(), &sample_schema()).expect("YEAR should evaluate");
    assert_eq!(value, ExprValue::Null);
}
