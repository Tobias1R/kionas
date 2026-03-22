use super::{apply_aggregate_final_pipeline, apply_aggregate_partial_pipeline};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kionas::planner::{
    AggregateFunction, PhysicalAggregateExpr, PhysicalAggregateSpec, PhysicalExpr,
    PredicateComparisonOp, PredicateExpr, PredicateValue,
};
use std::sync::Arc;

#[test]
fn aggregates_count_and_sum_with_grouping() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("br"), Some("br"), Some("us")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), None, Some(7)])) as ArrayRef,
        ],
    )
    .expect("batch");

    let spec = PhysicalAggregateSpec {
        grouping_exprs: vec![PhysicalExpr::ColumnRef {
            name: "country".to_string(),
        }],
        aggregates: vec![
            PhysicalAggregateExpr {
                function: AggregateFunction::Count,
                input: None,
                output_name: "count".to_string(),
            },
            PhysicalAggregateExpr {
                function: AggregateFunction::Sum,
                input: Some(PhysicalExpr::ColumnRef {
                    name: "amount".to_string(),
                }),
                output_name: "sum_amount".to_string(),
            },
        ],
    };

    let output = apply_aggregate_partial_pipeline(&[batch], &spec).expect("partial");
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 2);
}

#[test]
fn finalizes_avg_from_partial_state_columns() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("avg_amount__sum", DataType::Float64, false),
        Field::new("avg_amount__count", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("br"), Some("br")])) as ArrayRef,
            Arc::new(arrow::array::Float64Array::from(vec![10.0, 20.0])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1, 1])) as ArrayRef,
        ],
    )
    .expect("batch");

    let spec = PhysicalAggregateSpec {
        grouping_exprs: vec![PhysicalExpr::ColumnRef {
            name: "country".to_string(),
        }],
        aggregates: vec![PhysicalAggregateExpr {
            function: AggregateFunction::Avg,
            input: Some(PhysicalExpr::ColumnRef {
                name: "amount".to_string(),
            }),
            output_name: "avg_amount".to_string(),
        }],
    };

    let output = apply_aggregate_final_pipeline(&[batch], &spec).expect("final");
    assert_eq!(output.len(), 1);
    assert_eq!(output[0].num_rows(), 1);
}

#[test]
fn aggregate_rejects_predicate_input_expression() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("br")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10)])) as ArrayRef,
        ],
    )
    .expect("batch");

    let spec = PhysicalAggregateSpec {
        grouping_exprs: vec![PhysicalExpr::ColumnRef {
            name: "country".to_string(),
        }],
        aggregates: vec![PhysicalAggregateExpr {
            function: AggregateFunction::Sum,
            input: Some(PhysicalExpr::Predicate {
                predicate: PredicateExpr::Comparison {
                    column: "amount".to_string(),
                    op: PredicateComparisonOp::Eq,
                    value: PredicateValue::Int(10),
                },
            }),
            output_name: "sum_amount".to_string(),
        }],
    };

    let err = apply_aggregate_partial_pipeline(&[batch], &spec)
        .expect_err("aggregate must reject predicate input expression");
    assert!(err.contains("aggregate input expression cannot be a predicate"));
}

#[test]
fn aggregate_count_and_sum_apply_null_semantics_per_group() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("amount", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("br"),
                Some("br"),
                Some("us"),
                Some("us"),
            ])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(10), None, None, Some(7)])) as ArrayRef,
        ],
    )
    .expect("batch");

    let spec = PhysicalAggregateSpec {
        grouping_exprs: vec![PhysicalExpr::ColumnRef {
            name: "country".to_string(),
        }],
        aggregates: vec![
            PhysicalAggregateExpr {
                function: AggregateFunction::Count,
                input: Some(PhysicalExpr::ColumnRef {
                    name: "amount".to_string(),
                }),
                output_name: "count_amount".to_string(),
            },
            PhysicalAggregateExpr {
                function: AggregateFunction::Sum,
                input: Some(PhysicalExpr::ColumnRef {
                    name: "amount".to_string(),
                }),
                output_name: "sum_amount".to_string(),
            },
        ],
    };

    let output = apply_aggregate_partial_pipeline(&[batch], &spec).expect("partial");
    let batch = &output[0];

    let countries = batch
        .column_by_name("country")
        .expect("country column")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("country must be utf8");
    let counts = batch
        .column_by_name("count_amount")
        .expect("count column")
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("count must be int64");
    let sums = batch
        .column_by_name("sum_amount")
        .expect("sum column")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("sum must be float64");

    assert_eq!(batch.num_rows(), 2);
    assert_eq!(countries.value(0), "br");
    assert_eq!(countries.value(1), "us");
    assert_eq!(counts.value(0), 1);
    assert_eq!(counts.value(1), 1);
    assert_eq!(sums.value(0), 10.0);
    assert_eq!(sums.value(1), 7.0);
}

#[test]
fn aggregate_rejects_non_numeric_input_type_for_sum() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("country", DataType::Utf8, true),
        Field::new("amount_text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![Some("br")])) as ArrayRef,
            Arc::new(StringArray::from(vec![Some("10")])) as ArrayRef,
        ],
    )
    .expect("batch");

    let spec = PhysicalAggregateSpec {
        grouping_exprs: vec![PhysicalExpr::ColumnRef {
            name: "country".to_string(),
        }],
        aggregates: vec![PhysicalAggregateExpr {
            function: AggregateFunction::Sum,
            input: Some(PhysicalExpr::ColumnRef {
                name: "amount_text".to_string(),
            }),
            output_name: "sum_amount".to_string(),
        }],
    };

    let err = apply_aggregate_partial_pipeline(&[batch], &spec)
        .expect_err("sum must reject non-numeric aggregate input type");
    assert!(err.contains("unsupported aggregate input type Utf8"));
}
