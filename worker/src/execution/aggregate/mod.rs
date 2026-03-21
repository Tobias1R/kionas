mod aggregate_avg;
mod aggregate_count;
mod aggregate_max;
mod aggregate_min;
mod aggregate_sum;

use crate::execution::aggregate::aggregate_avg::{
    finalize_avg, merge_final_avg, update_partial_avg,
};
use crate::execution::aggregate::aggregate_count::{merge_final_count, update_partial_count};
use crate::execution::aggregate::aggregate_max::{merge_final_max, update_partial_max};
use crate::execution::aggregate::aggregate_min::{merge_final_min, update_partial_min};
use crate::execution::aggregate::aggregate_sum::{merge_final_sum, update_partial_sum};
use crate::services::query_execution::resolve_schema_column_index;
use arrow::array::{
    ArrayRef, Date32Array, Date64Array, Float64Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use kionas::planner::{AggregateFunction, PhysicalAggregateSpec, PhysicalExpr};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
enum AggregateState {
    Count(i64),
    Sum(f64),
    Min(Option<f64>),
    Max(Option<f64>),
    Avg { sum: f64, count: i64 },
}

#[derive(Debug, Clone)]
struct GroupState {
    grouping_values: Vec<String>,
    aggregates: Vec<AggregateState>,
}

/// What: Execute aggregate partial stage for grouped query pipelines.
///
/// Inputs:
/// - `input`: Source record batches.
/// - `spec`: Aggregate operator specification.
///
/// Output:
/// - One batch with grouped partial aggregate state.
pub(crate) fn apply_aggregate_partial_pipeline(
    input: &[RecordBatch],
    spec: &PhysicalAggregateSpec,
) -> Result<Vec<RecordBatch>, String> {
    let grouped = aggregate_input_rows(input, spec, false)?;
    log::info!(
        "aggregation partial executed: input_rows={} groups_formed={} output_rows={}",
        input.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        grouped.len(),
        grouped.len()
    );
    build_output_batches(spec, grouped, false)
}

/// What: Execute aggregate final stage for grouped query pipelines.
///
/// Inputs:
/// - `input`: Upstream partial aggregate batches.
/// - `spec`: Aggregate operator specification.
///
/// Output:
/// - One batch with grouped finalized aggregate values.
pub(crate) fn apply_aggregate_final_pipeline(
    input: &[RecordBatch],
    spec: &PhysicalAggregateSpec,
) -> Result<Vec<RecordBatch>, String> {
    let grouped = aggregate_input_rows(input, spec, true)?;
    log::info!(
        "aggregation final executed: input_rows={} groups_formed={} output_rows={}",
        input.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        grouped.len(),
        grouped.len()
    );
    build_output_batches(spec, grouped, true)
}

fn aggregate_input_rows(
    input: &[RecordBatch],
    spec: &PhysicalAggregateSpec,
    is_final_stage: bool,
) -> Result<Vec<GroupState>, String> {
    let mut grouped = HashMap::<String, GroupState>::new();

    for batch in input {
        let group_indices = resolve_group_indices(batch, &spec.grouping_exprs)?;
        let aggregate_indices = resolve_aggregate_indices(batch, spec, is_final_stage)?;

        for row_idx in 0..batch.num_rows() {
            let grouping_values = extract_grouping_values(batch, &group_indices, row_idx)?;
            let group_key = grouping_values.join("|");

            let entry = grouped.entry(group_key).or_insert_with(|| GroupState {
                grouping_values: grouping_values.clone(),
                aggregates: initial_aggregate_states(spec),
            });

            for (agg_idx, aggregate) in spec.aggregates.iter().enumerate() {
                let state = &mut entry.aggregates[agg_idx];
                if is_final_stage {
                    merge_final_aggregate_state(
                        batch,
                        row_idx,
                        aggregate,
                        aggregate_indices[agg_idx],
                        state,
                    )?;
                } else {
                    update_partial_aggregate_state(
                        batch,
                        row_idx,
                        aggregate,
                        aggregate_indices[agg_idx],
                        state,
                    )?;
                }
            }
        }
    }

    let mut values = grouped.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| left.grouping_values.cmp(&right.grouping_values));
    Ok(values)
}

fn resolve_group_indices(
    batch: &RecordBatch,
    grouping_exprs: &[PhysicalExpr],
) -> Result<Vec<usize>, String> {
    grouping_exprs
        .iter()
        .map(|expr| {
            let name = match expr {
                PhysicalExpr::ColumnRef { name } => normalize_identifier(name),
                PhysicalExpr::Raw { sql } => normalize_identifier(sql),
            };
            resolve_schema_column_index(batch.schema().as_ref(), &name)
                .ok_or_else(|| format!("grouping column '{}' not found", name))
        })
        .collect::<Result<Vec<_>, String>>()
}

fn resolve_aggregate_indices(
    batch: &RecordBatch,
    spec: &PhysicalAggregateSpec,
    is_final_stage: bool,
) -> Result<Vec<Option<usize>>, String> {
    spec.aggregates
        .iter()
        .map(|aggregate| {
            if is_final_stage {
                match aggregate.function {
                    AggregateFunction::Count
                    | AggregateFunction::Sum
                    | AggregateFunction::Min
                    | AggregateFunction::Max => batch
                        .schema()
                        .index_of(aggregate.output_name.as_str())
                        .map(Some)
                        .map_err(|_| {
                            format!(
                                "aggregate partial output column '{}' not found",
                                aggregate.output_name
                            )
                        }),
                    AggregateFunction::Avg => Ok(None),
                }
            } else {
                match &aggregate.input {
                    Some(PhysicalExpr::ColumnRef { name }) => {
                        let normalized = normalize_identifier(name);
                        resolve_schema_column_index(batch.schema().as_ref(), normalized.as_str())
                            .map(Some)
                            .ok_or_else(|| {
                                format!(
                                    "aggregate input column '{}' not found for {}",
                                    name, aggregate.output_name
                                )
                            })
                    }
                    Some(PhysicalExpr::Raw { sql }) => {
                        let normalized = normalize_identifier(sql);
                        resolve_schema_column_index(batch.schema().as_ref(), normalized.as_str())
                            .map(Some)
                            .ok_or_else(|| {
                                format!(
                                    "aggregate input column '{}' not found for {}",
                                    sql, aggregate.output_name
                                )
                            })
                    }
                    None => Ok(None),
                }
            }
        })
        .collect::<Result<Vec<_>, String>>()
}

fn extract_grouping_values(
    batch: &RecordBatch,
    group_indices: &[usize],
    row_idx: usize,
) -> Result<Vec<String>, String> {
    group_indices
        .iter()
        .map(|index| stringify_array_value(batch.column(*index), row_idx))
        .collect::<Result<Vec<_>, String>>()
}

fn initial_aggregate_states(spec: &PhysicalAggregateSpec) -> Vec<AggregateState> {
    spec.aggregates
        .iter()
        .map(|aggregate| match aggregate.function {
            AggregateFunction::Count => AggregateState::Count(0),
            AggregateFunction::Sum => AggregateState::Sum(0.0),
            AggregateFunction::Min => AggregateState::Min(None),
            AggregateFunction::Max => AggregateState::Max(None),
            AggregateFunction::Avg => AggregateState::Avg { sum: 0.0, count: 0 },
        })
        .collect::<Vec<_>>()
}

fn update_partial_aggregate_state(
    batch: &RecordBatch,
    row_idx: usize,
    aggregate: &kionas::planner::PhysicalAggregateExpr,
    index: Option<usize>,
    state: &mut AggregateState,
) -> Result<(), String> {
    match (aggregate.function.clone(), state) {
        (AggregateFunction::Count, AggregateState::Count(count_state)) => {
            if let Some(column_index) = index {
                let include_row = !batch.column(column_index).is_null(row_idx);
                update_partial_count(count_state, include_row);
            } else {
                update_partial_count(count_state, true);
            }
            Ok(())
        }
        (AggregateFunction::Sum, AggregateState::Sum(sum_state)) => {
            let value = if let Some(column_index) = index {
                numeric_array_value(batch.column(column_index), row_idx)?
            } else {
                None
            };
            update_partial_sum(sum_state, value);
            Ok(())
        }
        (AggregateFunction::Min, AggregateState::Min(min_state)) => {
            let value = if let Some(column_index) = index {
                numeric_array_value(batch.column(column_index), row_idx)?
            } else {
                None
            };
            update_partial_min(min_state, value);
            Ok(())
        }
        (AggregateFunction::Max, AggregateState::Max(max_state)) => {
            let value = if let Some(column_index) = index {
                numeric_array_value(batch.column(column_index), row_idx)?
            } else {
                None
            };
            update_partial_max(max_state, value);
            Ok(())
        }
        (AggregateFunction::Avg, AggregateState::Avg { sum, count }) => {
            let value = if let Some(column_index) = index {
                numeric_array_value(batch.column(column_index), row_idx)?
            } else {
                None
            };
            update_partial_avg(sum, count, value);
            Ok(())
        }
        _ => Err("aggregate state/function mismatch".to_string()),
    }
}

fn merge_final_aggregate_state(
    batch: &RecordBatch,
    row_idx: usize,
    aggregate: &kionas::planner::PhysicalAggregateExpr,
    index: Option<usize>,
    state: &mut AggregateState,
) -> Result<(), String> {
    match (aggregate.function.clone(), state) {
        (AggregateFunction::Count, AggregateState::Count(count_state)) => {
            let column_index = index.ok_or_else(|| {
                format!("missing partial count column for {}", aggregate.output_name)
            })?;
            let partial = numeric_array_value(batch.column(column_index), row_idx)?.unwrap_or(0.0);
            merge_final_count(count_state, partial as i64);
            Ok(())
        }
        (AggregateFunction::Sum, AggregateState::Sum(sum_state)) => {
            let column_index = index.ok_or_else(|| {
                format!("missing partial sum column for {}", aggregate.output_name)
            })?;
            let partial = numeric_array_value(batch.column(column_index), row_idx)?.unwrap_or(0.0);
            merge_final_sum(sum_state, partial);
            Ok(())
        }
        (AggregateFunction::Min, AggregateState::Min(min_state)) => {
            let column_index = index.ok_or_else(|| {
                format!("missing partial min column for {}", aggregate.output_name)
            })?;
            let partial = numeric_array_value(batch.column(column_index), row_idx)?;
            merge_final_min(min_state, partial);
            Ok(())
        }
        (AggregateFunction::Max, AggregateState::Max(max_state)) => {
            let column_index = index.ok_or_else(|| {
                format!("missing partial max column for {}", aggregate.output_name)
            })?;
            let partial = numeric_array_value(batch.column(column_index), row_idx)?;
            merge_final_max(max_state, partial);
            Ok(())
        }
        (AggregateFunction::Avg, AggregateState::Avg { sum, count }) => {
            let sum_name = format!("{}__sum", aggregate.output_name);
            let count_name = format!("{}__count", aggregate.output_name);
            let sum_index = batch
                .schema()
                .index_of(sum_name.as_str())
                .map_err(|_| format!("missing partial AVG sum column '{}'", sum_name))?;
            let count_index = batch
                .schema()
                .index_of(count_name.as_str())
                .map_err(|_| format!("missing partial AVG count column '{}'", count_name))?;
            let upstream_sum =
                numeric_array_value(batch.column(sum_index), row_idx)?.unwrap_or(0.0);
            let upstream_count =
                numeric_array_value(batch.column(count_index), row_idx)?.unwrap_or(0.0);
            merge_final_avg(sum, count, upstream_sum, upstream_count as i64);
            Ok(())
        }
        _ => Err("aggregate state/function mismatch".to_string()),
    }
}

fn build_output_batches(
    spec: &PhysicalAggregateSpec,
    grouped: Vec<GroupState>,
    is_final_stage: bool,
) -> Result<Vec<RecordBatch>, String> {
    let mut fields = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for expr in &spec.grouping_exprs {
        let name = match expr {
            PhysicalExpr::ColumnRef { name } => normalize_identifier(name),
            PhysicalExpr::Raw { sql } => normalize_identifier(sql),
        };
        let values = grouped
            .iter()
            .map(|group| group.grouping_values[fields.len()].clone())
            .collect::<Vec<_>>();
        fields.push(Field::new(name.as_str(), DataType::Utf8, true));
        columns.push(Arc::new(StringArray::from(values)) as ArrayRef);
    }

    for (agg_idx, aggregate) in spec.aggregates.iter().enumerate() {
        match aggregate.function {
            AggregateFunction::Count => {
                let values = grouped
                    .iter()
                    .map(|group| match &group.aggregates[agg_idx] {
                        AggregateState::Count(value) => *value,
                        _ => 0,
                    })
                    .collect::<Vec<_>>();
                fields.push(Field::new(
                    aggregate.output_name.as_str(),
                    DataType::Int64,
                    false,
                ));
                columns.push(Arc::new(Int64Array::from(values)) as ArrayRef);
            }
            AggregateFunction::Sum => {
                let values = grouped
                    .iter()
                    .map(|group| match &group.aggregates[agg_idx] {
                        AggregateState::Sum(value) => *value,
                        _ => 0.0,
                    })
                    .collect::<Vec<_>>();
                fields.push(Field::new(
                    aggregate.output_name.as_str(),
                    DataType::Float64,
                    false,
                ));
                columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
            }
            AggregateFunction::Min => {
                let values = grouped
                    .iter()
                    .map(|group| match &group.aggregates[agg_idx] {
                        AggregateState::Min(value) => *value,
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                fields.push(Field::new(
                    aggregate.output_name.as_str(),
                    DataType::Float64,
                    true,
                ));
                columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
            }
            AggregateFunction::Max => {
                let values = grouped
                    .iter()
                    .map(|group| match &group.aggregates[agg_idx] {
                        AggregateState::Max(value) => *value,
                        _ => None,
                    })
                    .collect::<Vec<_>>();
                fields.push(Field::new(
                    aggregate.output_name.as_str(),
                    DataType::Float64,
                    true,
                ));
                columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
            }
            AggregateFunction::Avg => {
                if is_final_stage {
                    let values = grouped
                        .iter()
                        .map(|group| match &group.aggregates[agg_idx] {
                            AggregateState::Avg { sum, count } => finalize_avg(*sum, *count),
                            _ => None,
                        })
                        .collect::<Vec<_>>();
                    fields.push(Field::new(
                        aggregate.output_name.as_str(),
                        DataType::Float64,
                        true,
                    ));
                    columns.push(Arc::new(Float64Array::from(values)) as ArrayRef);
                } else {
                    let sum_values = grouped
                        .iter()
                        .map(|group| match &group.aggregates[agg_idx] {
                            AggregateState::Avg { sum, .. } => *sum,
                            _ => 0.0,
                        })
                        .collect::<Vec<_>>();
                    let count_values = grouped
                        .iter()
                        .map(|group| match &group.aggregates[agg_idx] {
                            AggregateState::Avg { count, .. } => *count,
                            _ => 0,
                        })
                        .collect::<Vec<_>>();
                    fields.push(Field::new(
                        format!("{}__sum", aggregate.output_name).as_str(),
                        DataType::Float64,
                        false,
                    ));
                    columns.push(Arc::new(Float64Array::from(sum_values)) as ArrayRef);
                    fields.push(Field::new(
                        format!("{}__count", aggregate.output_name).as_str(),
                        DataType::Int64,
                        false,
                    ));
                    columns.push(Arc::new(Int64Array::from(count_values)) as ArrayRef);
                }
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| format!("failed to build aggregate output batch: {}", e))?;
    Ok(vec![batch])
}

fn stringify_array_value(array: &ArrayRef, row_idx: usize) -> Result<String, String> {
    if array.is_null(row_idx) {
        return Ok("<NULL>".to_string());
    }

    match array.data_type() {
        DataType::Utf8 => Ok(array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| "failed to downcast Utf8 grouping array".to_string())?
            .value(row_idx)
            .to_string()),
        DataType::Int64 => Ok(array
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| "failed to downcast Int64 grouping array".to_string())?
            .value(row_idx)
            .to_string()),
        DataType::Int32 => Ok(array
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| "failed to downcast Int32 grouping array".to_string())?
            .value(row_idx)
            .to_string()),
        DataType::Int16 => Ok(array
            .as_any()
            .downcast_ref::<Int16Array>()
            .ok_or_else(|| "failed to downcast Int16 grouping array".to_string())?
            .value(row_idx)
            .to_string()),
        DataType::Date32 => Ok(format!(
            "d:{}",
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to downcast Date32 grouping array".to_string())?
                .value(row_idx)
        )),
        DataType::Date64 => Ok(format!(
            "d:{}",
            array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| "failed to downcast Date64 grouping array".to_string())?
                .value(row_idx)
                .div_euclid(86_400_000)
        )),
        DataType::Timestamp(unit, timezone) => {
            if timezone.is_some() {
                return Err(
                    "unsupported grouping data type Timestamp with timezone in this phase"
                        .to_string(),
                );
            }

            let nanos = timestamp_value_as_nanos(array, row_idx, unit)?;
            Ok(format!("tsn:{}", nanos))
        }
        _ => Err(format!(
            "unsupported grouping data type {:?}: only Utf8/Int16/Int32/Int64/Date32/Date64/Timestamp are supported",
            array.data_type()
        )),
    }
}

fn timestamp_value_as_nanos(
    array: &ArrayRef,
    row_idx: usize,
    unit: &TimeUnit,
) -> Result<i128, String> {
    let raw = match unit {
        TimeUnit::Second => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .ok_or_else(|| "failed to downcast TimestampSecond grouping array".to_string())?
            .value(row_idx),
        TimeUnit::Millisecond => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| "failed to downcast TimestampMillisecond grouping array".to_string())?
            .value(row_idx),
        TimeUnit::Microsecond => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| "failed to downcast TimestampMicrosecond grouping array".to_string())?
            .value(row_idx),
        TimeUnit::Nanosecond => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| "failed to downcast TimestampNanosecond grouping array".to_string())?
            .value(row_idx),
    };

    let nanos = match unit {
        TimeUnit::Second => i128::from(raw) * 1_000_000_000,
        TimeUnit::Millisecond => i128::from(raw) * 1_000_000,
        TimeUnit::Microsecond => i128::from(raw) * 1_000,
        TimeUnit::Nanosecond => i128::from(raw),
    };

    Ok(nanos)
}

fn numeric_array_value(array: &ArrayRef, row_idx: usize) -> Result<Option<f64>, String> {
    if array.is_null(row_idx) {
        return Ok(None);
    }

    match array.data_type() {
        DataType::Int64 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast Int64 aggregate input".to_string())?
                .value(row_idx) as f64,
        )),
        DataType::Int32 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast Int32 aggregate input".to_string())?
                .value(row_idx) as f64,
        )),
        DataType::Int16 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to downcast Int16 aggregate input".to_string())?
                .value(row_idx) as f64,
        )),
        DataType::Float64 => Ok(Some(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast Float64 aggregate input".to_string())?
                .value(row_idx),
        )),
        _ => Err(format!(
            "unsupported aggregate input type {:?}: only numeric types are supported",
            array.data_type()
        )),
    }
}

fn normalize_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    let no_prefix = if let Some((_, rhs)) = trimmed.rsplit_once('.') {
        rhs
    } else {
        trimmed
    };

    no_prefix
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

#[cfg(test)]
#[path = "../../tests/execution_aggregate_mod_tests.rs"]
mod tests;
