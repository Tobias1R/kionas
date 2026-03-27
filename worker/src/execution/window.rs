use crate::execution::window_frame::resolve_window_frame;
use crate::execution::window_functions::{
    WindowFunctionKind, evaluate_numeric_window, parse_window_function_kind,
    resolve_window_argument_index,
};
use crate::services::query_execution::{apply_sort_pipeline, normalize_batches_for_sort};
use arrow::array::{ArrayRef, Float64Array, Int64Array, UInt32Array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use kionas::planner::{PhysicalSortExpr, PhysicalWindowSpec};
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(test)]
#[path = "../tests/window_functions_tests.rs"]
mod tests;

/// What: Execute window aggregation over input batches.
///
/// Inputs:
/// - `input`: Source record batches.
/// - `spec`: Window operator specification.
///
/// Output:
/// - Windowed record batches with function outputs appended.
///
/// Details:
/// - Partitions rows by `partition_by` expressions.
/// - Sorts each partition by `order_by` expressions.
/// - Evaluates ranking and aggregate window functions.
/// - Emits one merged record batch to preserve deterministic output.
pub(crate) fn apply_window_pipeline(
    input: &[RecordBatch],
    spec: &PhysicalWindowSpec,
) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let normalized = normalize_batches_for_sort(input)?;
    let schema = normalized[0].schema();
    let merged = arrow::compute::concat_batches(&schema, &normalized)
        .map_err(|e| format!("failed to merge batches for window execution: {}", e))?;

    if merged.num_rows() == 0 {
        return Ok(vec![merged]);
    }

    let partitions = partition_indices(&merged, spec)?;
    let mut partition_batches = Vec::<RecordBatch>::with_capacity(partitions.len());

    for indices in partitions {
        let partition = take_rows(&merged, &indices)?;
        let sorted_partition = if spec.order_by.is_empty() {
            partition
        } else {
            sort_partition(&partition, &spec.order_by)?
        };

        let windowed = evaluate_partition_window_functions(&sorted_partition, spec)?;
        partition_batches.push(windowed);
    }

    if partition_batches.is_empty() {
        return Ok(vec![RecordBatch::new_empty(merged.schema())]);
    }

    let final_schema = partition_batches[0].schema();
    let final_batch = arrow::compute::concat_batches(&final_schema, &partition_batches)
        .map_err(|e| format!("failed to merge window output batches: {}", e))?;

    Ok(vec![final_batch])
}

fn partition_indices(
    batch: &RecordBatch,
    spec: &PhysicalWindowSpec,
) -> Result<Vec<Vec<u32>>, String> {
    if spec.partition_by.is_empty() {
        let all = (0..batch.num_rows())
            .map(|idx| {
                u32::try_from(idx).map_err(|_| "row index overflow for partitioning".to_string())
            })
            .collect::<Result<Vec<_>, String>>()?;
        return Ok(vec![all]);
    }

    let mut grouped = HashMap::<String, Vec<u32>>::new();
    let mut order = Vec::<String>::new();

    for row_idx in 0..batch.num_rows() {
        let mut key_parts = Vec::with_capacity(spec.partition_by.len());
        for expr in &spec.partition_by {
            let column_idx = expression_column_index(batch, expr)?;
            let value = array_value_to_string(batch.column(column_idx).as_ref(), row_idx)
                .map_err(|e| format!("failed to stringify partition key value: {}", e))?;
            key_parts.push(value);
        }

        let key = key_parts.join("\u{1F}");
        let as_u32 = u32::try_from(row_idx)
            .map_err(|_| "row index overflow for partitioning".to_string())?;
        if let Some(rows) = grouped.get_mut(&key) {
            rows.push(as_u32);
        } else {
            grouped.insert(key.clone(), vec![as_u32]);
            order.push(key);
        }
    }

    Ok(order
        .iter()
        .filter_map(|key| grouped.get(key).cloned())
        .collect::<Vec<_>>())
}

fn expression_column_index(
    batch: &RecordBatch,
    expr: &kionas::planner::PhysicalExpr,
) -> Result<usize, String> {
    let requested = match expr {
        kionas::planner::PhysicalExpr::ColumnRef { name } => name.trim().to_string(),
        kionas::planner::PhysicalExpr::Raw { sql } => sql.trim().to_string(),
        kionas::planner::PhysicalExpr::Predicate { .. } => {
            return Err("window partition expression cannot be a predicate".to_string());
        }
    };

    crate::services::query_execution::resolve_schema_column_index(
        batch.schema().as_ref(),
        &requested,
    )
    .ok_or_else(|| format!("window partition column '{}' was not found", requested))
}

fn take_rows(batch: &RecordBatch, indices: &[u32]) -> Result<RecordBatch, String> {
    let row_selector = UInt32Array::from(indices.to_vec());
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &row_selector, None)
                .map_err(|e| format!("failed to take partition rows: {}", e))
        })
        .collect::<Result<Vec<ArrayRef>, String>>()?;

    RecordBatch::try_new(batch.schema(), columns)
        .map_err(|e| format!("failed to build partition batch: {}", e))
}

fn sort_partition(
    batch: &RecordBatch,
    order_by: &[PhysicalSortExpr],
) -> Result<RecordBatch, String> {
    let sorted = apply_sort_pipeline(std::slice::from_ref(batch), order_by)?;
    sorted
        .into_iter()
        .next()
        .ok_or_else(|| "sort pipeline returned no batch for partition".to_string())
}

fn evaluate_partition_window_functions(
    partition: &RecordBatch,
    spec: &PhysicalWindowSpec,
) -> Result<RecordBatch, String> {
    let partition_len = partition.num_rows();
    let order_key_cache = build_order_key_cache(partition, &spec.order_by)?;

    let mut fields = partition
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    let mut columns = partition.columns().to_vec();

    for function in &spec.functions {
        let kind = parse_window_function_kind(&function.function_name)?;
        let arg_index =
            resolve_window_argument_index(partition, &function.args, &function.function_name)?;

        match kind {
            WindowFunctionKind::RowNumber => {
                let values = (0..partition_len)
                    .map(|idx| i64::try_from(idx + 1).unwrap_or(i64::MAX))
                    .collect::<Vec<_>>();
                fields.push(Field::new(
                    function.output_name.as_str(),
                    DataType::Int64,
                    false,
                ));
                columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
            }
            WindowFunctionKind::Rank => {
                let values = compute_rank_values(&order_key_cache, false)?;
                fields.push(Field::new(
                    function.output_name.as_str(),
                    DataType::Int64,
                    false,
                ));
                columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
            }
            WindowFunctionKind::DenseRank => {
                let values = compute_rank_values(&order_key_cache, true)?;
                fields.push(Field::new(
                    function.output_name.as_str(),
                    DataType::Int64,
                    false,
                ));
                columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
            }
            WindowFunctionKind::Count
            | WindowFunctionKind::Sum
            | WindowFunctionKind::Avg
            | WindowFunctionKind::Min
            | WindowFunctionKind::Max => {
                if matches!(kind, WindowFunctionKind::Count) {
                    let mut values = Vec::<i64>::with_capacity(partition_len);
                    for row_idx in 0..partition_len {
                        let frame =
                            resolve_window_frame(function.frame.as_ref(), row_idx, partition_len)?;
                        let value = evaluate_numeric_window(partition, arg_index, frame, &kind)?
                            .unwrap_or(0.0);
                        values.push(value as i64);
                    }
                    fields.push(Field::new(
                        function.output_name.as_str(),
                        DataType::Int64,
                        false,
                    ));
                    columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
                } else {
                    let mut values = Vec::<Option<f64>>::with_capacity(partition_len);
                    for row_idx in 0..partition_len {
                        let frame =
                            resolve_window_frame(function.frame.as_ref(), row_idx, partition_len)?;
                        values.push(evaluate_numeric_window(partition, arg_index, frame, &kind)?);
                    }
                    fields.push(Field::new(
                        function.output_name.as_str(),
                        DataType::Float64,
                        true,
                    ));
                    columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
                }
            }
        }
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|e| format!("failed to build window output batch: {}", e))
}

fn build_order_key_cache(
    batch: &RecordBatch,
    order_by: &[PhysicalSortExpr],
) -> Result<Vec<String>, String> {
    if order_by.is_empty() {
        return Ok((0..batch.num_rows())
            .map(|_| "__NO_ORDER_KEY__".to_string())
            .collect::<Vec<_>>());
    }

    let mut keys = Vec::<String>::with_capacity(batch.num_rows());
    for row_idx in 0..batch.num_rows() {
        let mut parts = Vec::<String>::with_capacity(order_by.len());
        for sort_expr in order_by {
            let requested = match &sort_expr.expression {
                kionas::planner::PhysicalExpr::ColumnRef { name } => name.trim().to_string(),
                kionas::planner::PhysicalExpr::Raw { sql } => sql.trim().to_string(),
                kionas::planner::PhysicalExpr::Predicate { .. } => {
                    return Err("window ORDER BY expression cannot be a predicate".to_string());
                }
            };
            let idx = crate::services::query_execution::resolve_schema_column_index(
                batch.schema().as_ref(),
                requested.as_str(),
            )
            .ok_or_else(|| format!("window order-by column '{}' was not found", requested))?;
            let value = array_value_to_string(batch.column(idx).as_ref(), row_idx)
                .map_err(|e| format!("failed to stringify order key value: {}", e))?;
            parts.push(value);
        }
        keys.push(parts.join("\u{1E}"));
    }

    Ok(keys)
}

fn compute_rank_values(order_keys: &[String], dense: bool) -> Result<Vec<i64>, String> {
    if order_keys.is_empty() {
        return Ok(Vec::new());
    }

    let mut values = Vec::<i64>::with_capacity(order_keys.len());
    let mut current_rank = 1_i64;
    let mut dense_rank = 1_i64;
    let mut previous_key = order_keys[0].clone();

    for (idx, key) in order_keys.iter().enumerate() {
        if idx == 0 {
            values.push(1);
            continue;
        }

        if key != &previous_key {
            previous_key = key.clone();
            if dense {
                dense_rank = dense_rank.saturating_add(1);
            } else {
                current_rank =
                    i64::try_from(idx + 1).map_err(|_| "rank index overflow".to_string())?;
            }
        }

        if dense {
            values.push(dense_rank);
        } else {
            values.push(current_rank);
        }
    }

    Ok(values)
}
