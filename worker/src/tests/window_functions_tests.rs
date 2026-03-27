use crate::execution::window::apply_window_pipeline;
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kionas::planner::{
    PhysicalExpr, PhysicalSortExpr, PhysicalWindowFrameBound, PhysicalWindowFrameSpec,
    PhysicalWindowFrameUnit, PhysicalWindowFunctionSpec, PhysicalWindowSpec,
};
use std::sync::Arc;

fn sample_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("dept", DataType::Utf8, false),
        Field::new("employee_id", DataType::Int64, false),
        Field::new("salary", DataType::Int64, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "b", "b"])) as ArrayRef,
            Arc::new(Int64Array::from(vec![1_i64, 2_i64, 3_i64, 4_i64])) as ArrayRef,
            Arc::new(Int64Array::from(vec![10_i64, 20_i64, 5_i64, 15_i64])) as ArrayRef,
        ],
    )
    .expect("sample batch must build")
}

#[test]
fn window_row_number_resets_per_partition() {
    let spec = PhysicalWindowSpec {
        partition_by: vec![PhysicalExpr::ColumnRef {
            name: "dept".to_string(),
        }],
        order_by: vec![PhysicalSortExpr {
            expression: PhysicalExpr::ColumnRef {
                name: "salary".to_string(),
            },
            ascending: true,
        }],
        functions: vec![PhysicalWindowFunctionSpec {
            function_name: "ROW_NUMBER".to_string(),
            args: Vec::new(),
            output_name: "rn".to_string(),
            frame: None,
        }],
    };

    let output = apply_window_pipeline(&[sample_batch()], &spec).expect("window execution");
    assert_eq!(output.len(), 1);

    let batch = &output[0];
    assert_eq!(batch.num_columns(), 4);
    assert_eq!(batch.schema().field(3).name(), "rn");

    let dept = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("dept must be utf8");
    let rn = batch
        .column(3)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("rn must be int64");

    let mut seen_a = Vec::new();
    let mut seen_b = Vec::new();
    for idx in 0..batch.num_rows() {
        if dept.value(idx) == "a" {
            seen_a.push(rn.value(idx));
        } else {
            seen_b.push(rn.value(idx));
        }
    }

    assert_eq!(seen_a, vec![1, 2]);
    assert_eq!(seen_b, vec![1, 2]);
}

#[test]
fn window_sum_running_total_with_rows_frame() {
    let spec = PhysicalWindowSpec {
        partition_by: vec![PhysicalExpr::ColumnRef {
            name: "dept".to_string(),
        }],
        order_by: vec![PhysicalSortExpr {
            expression: PhysicalExpr::ColumnRef {
                name: "employee_id".to_string(),
            },
            ascending: true,
        }],
        functions: vec![PhysicalWindowFunctionSpec {
            function_name: "SUM".to_string(),
            args: vec![PhysicalExpr::ColumnRef {
                name: "salary".to_string(),
            }],
            output_name: "running_salary".to_string(),
            frame: Some(PhysicalWindowFrameSpec {
                unit: PhysicalWindowFrameUnit::Rows,
                start_bound: PhysicalWindowFrameBound::UnboundedPreceding,
                end_bound: PhysicalWindowFrameBound::CurrentRow,
            }),
        }],
    };

    let output = apply_window_pipeline(&[sample_batch()], &spec).expect("window execution");
    assert_eq!(output.len(), 1);

    let batch = &output[0];
    let dept = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("dept must be utf8");
    let running = batch
        .column(3)
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("running_salary must be float64");

    let mut seen_a = Vec::new();
    let mut seen_b = Vec::new();
    for idx in 0..batch.num_rows() {
        let value = running.value(idx);
        if dept.value(idx) == "a" {
            seen_a.push(value);
        } else {
            seen_b.push(value);
        }
    }

    assert_eq!(seen_a, vec![10.0, 30.0]);
    assert_eq!(seen_b, vec![5.0, 20.0]);
}
