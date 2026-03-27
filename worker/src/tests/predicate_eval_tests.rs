use super::apply_filter_predicate_pipeline;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kionas::planner::{PredicateComparisonOp, PredicateExpr, PredicateValue};
use std::sync::Arc;

fn batch_three_rows() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64, 3_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef,
        ],
    )
    .expect("test batch must build")
}

fn batch_with_nullable_ids() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("name", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![Some(1_i64), None, Some(10_i64)])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "n", "z"])) as ArrayRef,
        ],
    )
    .expect("nullable test batch must build")
}

#[test]
fn evaluates_and_predicate() {
    let predicate = PredicateExpr::And {
        clauses: vec![
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Gt,
                value: PredicateValue::Int(1),
            },
            PredicateExpr::Comparison {
                column: "name".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Str("b".to_string()),
            },
        ],
    };

    let filtered = apply_filter_predicate_pipeline(&[batch_three_rows()], &predicate, None)
        .expect("AND predicate must run");
    assert_eq!(filtered[0].num_rows(), 1);
}

#[test]
fn evaluates_or_predicate() {
    let predicate = PredicateExpr::Or {
        clauses: vec![
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Int(1),
            },
            PredicateExpr::Comparison {
                column: "name".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Str("c".to_string()),
            },
        ],
    };

    let filtered = apply_filter_predicate_pipeline(&[batch_three_rows()], &predicate, None)
        .expect("OR predicate must run");
    assert_eq!(filtered[0].num_rows(), 2);
}

#[test]
fn evaluates_not_predicate() {
    let predicate = PredicateExpr::Not {
        expr: Box::new(PredicateExpr::Comparison {
            column: "id".to_string(),
            op: PredicateComparisonOp::Eq,
            value: PredicateValue::Int(1),
        }),
    };

    let filtered = apply_filter_predicate_pipeline(&[batch_three_rows()], &predicate, None)
        .expect("NOT predicate must run");
    assert_eq!(filtered[0].num_rows(), 2);
}

#[test]
fn excludes_null_values_for_comparisons() {
    let predicate = PredicateExpr::Comparison {
        column: "id".to_string(),
        op: PredicateComparisonOp::Lt,
        value: PredicateValue::Int(5),
    };

    let filtered = apply_filter_predicate_pipeline(&[batch_with_nullable_ids()], &predicate, None)
        .expect("comparison with null values must run");
    assert_eq!(filtered[0].num_rows(), 1);
}

#[test]
fn evaluates_complex_nested_predicate() {
    let predicate = PredicateExpr::And {
        clauses: vec![
            PredicateExpr::Or {
                clauses: vec![
                    PredicateExpr::Comparison {
                        column: "id".to_string(),
                        op: PredicateComparisonOp::Eq,
                        value: PredicateValue::Int(1),
                    },
                    PredicateExpr::Comparison {
                        column: "id".to_string(),
                        op: PredicateComparisonOp::Eq,
                        value: PredicateValue::Int(2),
                    },
                ],
            },
            PredicateExpr::Not {
                expr: Box::new(PredicateExpr::Comparison {
                    column: "name".to_string(),
                    op: PredicateComparisonOp::Eq,
                    value: PredicateValue::Str("a".to_string()),
                }),
            },
        ],
    };

    let filtered = apply_filter_predicate_pipeline(&[batch_three_rows()], &predicate, None)
        .expect("complex nested predicate must run");
    assert_eq!(filtered[0].num_rows(), 1);
}

#[test]
fn and_short_circuits_when_all_rows_become_false() {
    let predicate = PredicateExpr::And {
        clauses: vec![
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Lt,
                value: PredicateValue::Int(0),
            },
            PredicateExpr::Comparison {
                column: "missing_column".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Int(1),
            },
        ],
    };

    let filtered = apply_filter_predicate_pipeline(&[batch_three_rows()], &predicate, None)
        .expect("AND should short-circuit before missing column evaluation");
    assert_eq!(filtered[0].num_rows(), 0);
}

#[test]
fn or_short_circuits_when_all_rows_become_true() {
    let predicate = PredicateExpr::Or {
        clauses: vec![
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Gt,
                value: PredicateValue::Int(0),
            },
            PredicateExpr::Comparison {
                column: "missing_column".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Int(1),
            },
        ],
    };

    let filtered = apply_filter_predicate_pipeline(&[batch_three_rows()], &predicate, None)
        .expect("OR should short-circuit before missing column evaluation");
    assert_eq!(filtered[0].num_rows(), 3);
}

#[test]
fn handles_empty_and_and_empty_or() {
    let and_predicate = PredicateExpr::And {
        clauses: Vec::new(),
    };
    let or_predicate = PredicateExpr::Or {
        clauses: Vec::new(),
    };

    let all_rows = apply_filter_predicate_pipeline(&[batch_three_rows()], &and_predicate, None)
        .expect("empty AND must run");
    let no_rows = apply_filter_predicate_pipeline(&[batch_three_rows()], &or_predicate, None)
        .expect("empty OR must run");

    assert_eq!(all_rows[0].num_rows(), 3);
    assert_eq!(no_rows[0].num_rows(), 0);
}
