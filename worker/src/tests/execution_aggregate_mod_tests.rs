use super::{apply_aggregate_final_pipeline, apply_aggregate_partial_pipeline};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kionas::planner::{
    AggregateFunction, PhysicalAggregateExpr, PhysicalAggregateSpec, PhysicalExpr,
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
