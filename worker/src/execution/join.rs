use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt32Array,
};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use kionas::planner::PhysicalJoinSpec;

/// What: Execute a constrained inner equi hash-join between left and right batches.
///
/// Inputs:
/// - `left_batches`: Left-side input batches.
/// - `right_batches`: Right-side input batches.
/// - `spec`: Physical join specification containing join keys and right relation metadata.
///
/// Output:
/// - Joined record batches preserving deterministic row expansion order.
///
/// Details:
/// - Current slice supports only INNER joins with one or more equi-key pairs.
/// - Output includes all left columns plus all right columns; duplicate right names are table-prefixed.
pub(crate) fn apply_hash_join_pipeline(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    spec: &PhysicalJoinSpec,
) -> Result<Vec<RecordBatch>, String> {
    if left_batches.is_empty() || right_batches.is_empty() {
        return Ok(vec![build_empty_join_batch(
            left_batches,
            right_batches,
            spec,
        )?]);
    }

    let left_schema = left_batches[0].schema();
    let right_schema = right_batches[0].schema();

    let left_all = arrow::compute::concat_batches(&left_schema, left_batches)
        .map_err(|e| format!("failed to concat left join batches: {}", e))?;
    let right_all = arrow::compute::concat_batches(&right_schema, right_batches)
        .map_err(|e| format!("failed to concat right join batches: {}", e))?;

    if left_all.num_rows() == 0 || right_all.num_rows() == 0 {
        return Ok(vec![build_empty_join_batch(
            left_batches,
            right_batches,
            spec,
        )?]);
    }

    let left_key_columns = resolve_join_key_indices(left_all.schema().as_ref(), &spec.keys, true)?;
    let right_key_columns =
        resolve_join_key_indices(right_all.schema().as_ref(), &spec.keys, false)?;

    let mut right_index = HashMap::<String, Vec<u32>>::new();
    for row in 0..right_all.num_rows() {
        let key = row_join_key(&right_all, row, &right_key_columns)?;
        right_index.entry(key).or_default().push(row as u32);
    }

    let mut left_indices = Vec::<u32>::new();
    let mut right_indices = Vec::<u32>::new();
    for row in 0..left_all.num_rows() {
        let key = row_join_key(&left_all, row, &left_key_columns)?;
        if let Some(matches) = right_index.get(&key) {
            for right_row in matches {
                left_indices.push(row as u32);
                right_indices.push(*right_row);
            }
        }
    }

    if left_indices.is_empty() {
        return Ok(vec![build_empty_join_batch(
            left_batches,
            right_batches,
            spec,
        )?]);
    }

    let left_take = UInt32Array::from(left_indices);
    let right_take = UInt32Array::from(right_indices);

    let mut fields = Vec::<Field>::new();
    let mut arrays = Vec::<ArrayRef>::new();

    for field in left_all.schema().fields() {
        fields.push(field.as_ref().clone());
    }

    for col in left_all.columns() {
        let taken = take(col.as_ref(), &left_take, None)
            .map_err(|e| format!("failed to take left join rows: {}", e))?;
        arrays.push(taken);
    }

    for field in right_all.schema().fields() {
        let mut right_field = field.as_ref().clone();
        if left_all
            .schema()
            .fields()
            .iter()
            .any(|left_field| left_field.name() == right_field.name())
        {
            right_field = Field::new(
                &format!("{}_{}", spec.right_relation.table, right_field.name()),
                right_field.data_type().clone(),
                right_field.is_nullable(),
            );
        }
        fields.push(right_field);
    }

    for col in right_all.columns() {
        let taken = take(col.as_ref(), &right_take, None)
            .map_err(|e| format!("failed to take right join rows: {}", e))?;
        arrays.push(taken);
    }

    let joined_schema = Arc::new(Schema::new(fields));
    let joined = RecordBatch::try_new(joined_schema, arrays)
        .map_err(|e| format!("failed to build joined batch: {}", e))?;

    Ok(vec![joined])
}

fn build_empty_join_batch(
    left_batches: &[RecordBatch],
    right_batches: &[RecordBatch],
    spec: &PhysicalJoinSpec,
) -> Result<RecordBatch, String> {
    let left_schema = left_batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| "left join input is empty".to_string())?;
    let right_schema = right_batches
        .first()
        .map(|b| b.schema())
        .ok_or_else(|| "right join input is empty".to_string())?;

    let mut fields = Vec::<Field>::new();
    for field in left_schema.fields() {
        fields.push(field.as_ref().clone());
    }

    for field in right_schema.fields() {
        let mut next = field.as_ref().clone();
        if left_schema
            .fields()
            .iter()
            .any(|left_field| left_field.name() == next.name())
        {
            next = Field::new(
                &format!("{}_{}", spec.right_relation.table, next.name()),
                next.data_type().clone(),
                next.is_nullable(),
            );
        }
        fields.push(next);
    }

    let mut arrays = Vec::<ArrayRef>::new();
    for field in &fields {
        arrays.push(empty_array_for_type(field.data_type()));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| format!("failed to build empty join batch: {}", e))
}

fn resolve_join_key_indices(
    schema: &Schema,
    keys: &[kionas::planner::JoinKeyPair],
    left_side: bool,
) -> Result<Vec<usize>, String> {
    let mut indices = Vec::with_capacity(keys.len());
    for key in keys {
        let raw = if left_side { &key.left } else { &key.right };
        let raw_trimmed = raw.trim();
        let fallback = raw_trimmed
            .rsplit('.')
            .next()
            .map(str::trim)
            .unwrap_or(raw_trimmed);

        let mapped = fallback_semantic_to_physical_column(schema, fallback);
        let index = schema
            .fields()
            .iter()
            .position(|field| {
                field.name() == raw_trimmed
                    || field.name() == fallback
                    || mapped.as_ref().is_some_and(|m| field.name() == m)
            })
            .ok_or_else(|| format!("join key '{}' is not present in schema", raw_trimmed))?;
        indices.push(index);
    }
    Ok(indices)
}

fn fallback_semantic_to_physical_column(schema: &Schema, requested: &str) -> Option<String> {
    let has_only_cn = schema.fields().iter().all(|field| {
        let name = field.name();
        name.strip_prefix('c')
            .map(|digits| !digits.is_empty() && digits.chars().all(|ch| ch.is_ascii_digit()))
            .unwrap_or(false)
    });
    if !has_only_cn {
        return None;
    }

    match requested.to_ascii_lowercase().as_str() {
        "id" => Some("c1".to_string()),
        "name" | "document" | "value" => Some("c2".to_string()),
        _ => None,
    }
}

fn row_join_key(batch: &RecordBatch, row: usize, key_columns: &[usize]) -> Result<String, String> {
    let mut key_parts = Vec::with_capacity(key_columns.len());
    for col_idx in key_columns {
        let value = scalar_to_string(batch.column(*col_idx).as_ref(), row)?;
        key_parts.push(value);
    }
    Ok(key_parts.join("|"))
}

fn scalar_to_string(array: &dyn Array, row: usize) -> Result<String, String> {
    if array.is_null(row) {
        return Ok("<NULL>".to_string());
    }

    match array.data_type() {
        DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| "failed to cast Int16 join key array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to cast Int32 join key array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to cast Int64 join key array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to cast Utf8 join key array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Boolean => {
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to cast Boolean join key array".to_string())?;
            Ok(arr.value(row).to_string())
        }
        DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| "failed to cast Date32 join key array".to_string())?;
            Ok(format!("d:{}", arr.value(row)))
        }
        DataType::Date64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| "failed to cast Date64 join key array".to_string())?;
            let days = arr.value(row).div_euclid(86_400_000);
            Ok(format!("d:{}", days))
        }
        DataType::Timestamp(unit, timezone) => {
            if timezone.is_some() {
                return Err(
                    "unsupported join key type Timestamp with timezone in this phase".to_string(),
                );
            }

            let nanos = timestamp_value_as_nanos(array, row, unit)?;
            Ok(format!("tsn:{}", nanos))
        }
        other => Err(format!("unsupported join key type {:?}", other)),
    }
}

fn timestamp_value_as_nanos(
    array: &dyn Array,
    row: usize,
    unit: &TimeUnit,
) -> Result<i128, String> {
    let raw = match unit {
        TimeUnit::Second => array
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .ok_or_else(|| "failed to cast TimestampSecond join key array".to_string())?
            .value(row),
        TimeUnit::Millisecond => array
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .ok_or_else(|| "failed to cast TimestampMillisecond join key array".to_string())?
            .value(row),
        TimeUnit::Microsecond => array
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| "failed to cast TimestampMicrosecond join key array".to_string())?
            .value(row),
        TimeUnit::Nanosecond => array
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .ok_or_else(|| "failed to cast TimestampNanosecond join key array".to_string())?
            .value(row),
    };

    let nanos = match unit {
        TimeUnit::Second => i128::from(raw) * 1_000_000_000,
        TimeUnit::Millisecond => i128::from(raw) * 1_000_000,
        TimeUnit::Microsecond => i128::from(raw) * 1_000,
        TimeUnit::Nanosecond => i128::from(raw),
    };

    Ok(nanos)
}

fn empty_array_for_type(data_type: &DataType) -> ArrayRef {
    match data_type {
        DataType::Int16 => Arc::new(Int16Array::from(Vec::<i16>::new())),
        DataType::Int32 => Arc::new(Int32Array::from(Vec::<i32>::new())),
        DataType::Int64 => Arc::new(Int64Array::from(Vec::<i64>::new())),
        DataType::Date32 => Arc::new(Date32Array::from(Vec::<i32>::new())),
        DataType::Date64 => Arc::new(Date64Array::from(Vec::<i64>::new())),
        DataType::Timestamp(TimeUnit::Second, timezone) => Arc::new(
            TimestampSecondArray::from(Vec::<i64>::new()).with_timezone_opt(timezone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, timezone) => Arc::new(
            TimestampMillisecondArray::from(Vec::<i64>::new()).with_timezone_opt(timezone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Microsecond, timezone) => Arc::new(
            TimestampMicrosecondArray::from(Vec::<i64>::new()).with_timezone_opt(timezone.clone()),
        ),
        DataType::Timestamp(TimeUnit::Nanosecond, timezone) => Arc::new(
            TimestampNanosecondArray::from(Vec::<i64>::new()).with_timezone_opt(timezone.clone()),
        ),
        DataType::Utf8 => Arc::new(StringArray::from(Vec::<String>::new())),
        DataType::Boolean => Arc::new(BooleanArray::from(Vec::<bool>::new())),
        _ => Arc::new(StringArray::from(Vec::<String>::new())),
    }
}
