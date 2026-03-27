use crate::execution::window_frame::ResolvedWindowFrame;
use crate::services::query_execution::resolve_schema_column_index;
use arrow::array::{
    Array, ArrayRef, Date32Array, Date64Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::record_batch::RecordBatch;
use kionas::planner::PhysicalExpr;

/// What: Supported runtime window function families in Phase A.
///
/// Inputs:
/// - Parsed from canonical SQL function names.
///
/// Output:
/// - Strongly-typed runtime function discriminator.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WindowFunctionKind {
    RowNumber,
    Rank,
    DenseRank,
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

/// What: Resolve runtime window function kind from canonical name.
///
/// Inputs:
/// - `name`: Canonical function name from physical plan.
///
/// Output:
/// - Parsed function kind, or actionable error for unsupported functions.
pub(crate) fn parse_window_function_kind(name: &str) -> Result<WindowFunctionKind, String> {
    match name.trim().to_ascii_uppercase().as_str() {
        "ROW_NUMBER" => Ok(WindowFunctionKind::RowNumber),
        "RANK" => Ok(WindowFunctionKind::Rank),
        "DENSE_RANK" => Ok(WindowFunctionKind::DenseRank),
        "COUNT" => Ok(WindowFunctionKind::Count),
        "SUM" => Ok(WindowFunctionKind::Sum),
        "AVG" => Ok(WindowFunctionKind::Avg),
        "MIN" => Ok(WindowFunctionKind::Min),
        "MAX" => Ok(WindowFunctionKind::Max),
        unsupported => Err(format!(
            "window function '{}' is not supported yet in Phase A runtime",
            unsupported
        )),
    }
}

/// What: Resolve argument column index for aggregate window functions.
///
/// Inputs:
/// - `batch`: Partition batch used for evaluation.
/// - `args`: Function arguments from physical plan.
/// - `function_name`: Function name for diagnostics.
///
/// Output:
/// - Optional column index (`None` for `COUNT(*)`).
pub(crate) fn resolve_window_argument_index(
    batch: &RecordBatch,
    args: &[PhysicalExpr],
    function_name: &str,
) -> Result<Option<usize>, String> {
    if args.is_empty() {
        return Ok(None);
    }

    let first = &args[0];
    let requested = match first {
        PhysicalExpr::ColumnRef { name } => name.trim().to_string(),
        PhysicalExpr::Raw { sql } => sql.trim().to_string(),
        PhysicalExpr::Predicate { .. } => {
            return Err(format!(
                "window function '{}' argument cannot be a predicate expression",
                function_name
            ));
        }
    };

    let idx = resolve_schema_column_index(batch.schema().as_ref(), requested.as_str()).ok_or_else(
        || {
            format!(
                "window function '{}' argument column '{}' was not found",
                function_name, requested
            )
        },
    )?;

    Ok(Some(idx))
}

/// What: Compute numeric aggregate over one resolved frame interval.
///
/// Inputs:
/// - `batch`: Partition batch.
/// - `column_index`: Optional input column index.
/// - `frame`: Optional resolved frame.
/// - `kind`: Aggregate function family.
///
/// Output:
/// - Numeric aggregate value as `f64`, or `None` for empty frames.
pub(crate) fn evaluate_numeric_window(
    batch: &RecordBatch,
    column_index: Option<usize>,
    frame: Option<ResolvedWindowFrame>,
    kind: &WindowFunctionKind,
) -> Result<Option<f64>, String> {
    let frame = match frame {
        Some(frame) => frame,
        None => {
            return Ok(match kind {
                WindowFunctionKind::Count => Some(0.0),
                _ => None,
            });
        }
    };

    match kind {
        WindowFunctionKind::Count => {
            if let Some(index) = column_index {
                let mut count = 0_i64;
                for row_idx in frame.start..=frame.end {
                    if !batch.column(index).is_null(row_idx) {
                        count += 1;
                    }
                }
                Ok(Some(count as f64))
            } else {
                let span = frame.end.saturating_sub(frame.start).saturating_add(1);
                Ok(Some(span as f64))
            }
        }
        WindowFunctionKind::Sum
        | WindowFunctionKind::Avg
        | WindowFunctionKind::Min
        | WindowFunctionKind::Max => {
            let index = column_index.ok_or_else(|| {
                "window aggregate requires an input column for SUM/AVG/MIN/MAX".to_string()
            })?;

            let mut values = Vec::<f64>::new();
            for row_idx in frame.start..=frame.end {
                if let Some(value) = numeric_array_value(batch.column(index), row_idx)? {
                    values.push(value);
                }
            }

            if values.is_empty() {
                return Ok(None);
            }

            let value = match kind {
                WindowFunctionKind::Sum => values.iter().sum::<f64>(),
                WindowFunctionKind::Avg => values.iter().sum::<f64>() / values.len() as f64,
                WindowFunctionKind::Min => values
                    .iter()
                    .copied()
                    .reduce(f64::min)
                    .ok_or_else(|| "unable to compute MIN over empty frame".to_string())?,
                WindowFunctionKind::Max => values
                    .iter()
                    .copied()
                    .reduce(f64::max)
                    .ok_or_else(|| "unable to compute MAX over empty frame".to_string())?,
                _ => {
                    return Err("unsupported aggregate evaluation path".to_string());
                }
            };

            Ok(Some(value))
        }
        _ => Err("numeric window evaluation received non-aggregate function".to_string()),
    }
}

fn numeric_array_value(array: &ArrayRef, row_idx: usize) -> Result<Option<f64>, String> {
    if array.is_null(row_idx) {
        return Ok(None);
    }

    match array.data_type() {
        arrow::datatypes::DataType::Int16 => array
            .as_any()
            .downcast_ref::<Int16Array>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast Int16 array".to_string()),
        arrow::datatypes::DataType::Int32 => array
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast Int32 array".to_string()),
        arrow::datatypes::DataType::Int64 => array
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast Int64 array".to_string()),
        arrow::datatypes::DataType::Float64 => array
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|values| Some(values.value(row_idx)))
            .ok_or_else(|| "failed to downcast Float64 array".to_string()),
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Second, _) => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast TimestampSecond array".to_string()),
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast TimestampMillisecond array".to_string()),
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, _) => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast TimestampMicrosecond array".to_string()),
        arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, _) => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast TimestampNanosecond array".to_string()),
        arrow::datatypes::DataType::Date32 => array
            .as_any()
            .downcast_ref::<Date32Array>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast Date32 array".to_string()),
        arrow::datatypes::DataType::Date64 => array
            .as_any()
            .downcast_ref::<Date64Array>()
            .map(|values| Some(values.value(row_idx) as f64))
            .ok_or_else(|| "failed to downcast Date64 array".to_string()),
        arrow::datatypes::DataType::Utf8 => {
            let values = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast Utf8 array".to_string())?;
            let parsed = values
                .value(row_idx)
                .parse::<f64>()
                .map_err(|_| "failed to parse Utf8 value as number".to_string())?;
            Ok(Some(parsed))
        }
        other => Err(format!(
            "numeric window aggregates do not support datatype {:?}",
            other
        )),
    }
}
