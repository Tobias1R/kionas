use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, LargeStringArray,
    StringArray, UInt32Array, UInt64Array,
};
use arrow::compute::take;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use fnv::FnvHasher;
use kionas::planner::PartitionSpec;
use murmur3::murmur3_x64_128;
use serde_json::Value;
use std::cmp::Ordering;
use std::hash::Hasher;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

fn normalize_routing_column_name(raw: &str) -> String {
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
        .to_string()
}

fn resolve_routing_column_index(
    schema: &arrow::datatypes::Schema,
    requested: &str,
) -> Option<usize> {
    if let Ok(index) = schema.index_of(requested) {
        return Some(index);
    }

    let normalized = normalize_routing_column_name(requested);
    if normalized != requested
        && let Ok(index) = schema.index_of(normalized.as_str())
    {
        return Some(index);
    }

    schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, field)| field.name().eq_ignore_ascii_case(normalized.as_str()))
        .map(|(index, _)| index)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashFunction {
    Fnv,
    Murmur3,
}

fn configured_hash_function() -> HashFunction {
    match std::env::var("WORKER_HASH_FUNCTION")
        .ok()
        .map(|value| value.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("murmur3") => HashFunction::Murmur3,
        _ => HashFunction::Fnv,
    }
}

fn hash_bytes(payload: &[u8], algorithm: HashFunction) -> Result<u64, String> {
    match algorithm {
        HashFunction::Fnv => {
            let mut hasher = FnvHasher::default();
            hasher.write(payload);
            Ok(hasher.finish())
        }
        HashFunction::Murmur3 => {
            let mut cursor = Cursor::new(payload);
            let digest = murmur3_x64_128(&mut cursor, 0)
                .map_err(|e| format!("failed to compute murmur3 hash: {}", e))?;
            let lower = digest & u128::from(u64::MAX);
            u64::try_from(lower).map_err(|e| format!("failed to convert hash digest: {}", e))
        }
    }
}

fn value_at_as_json(array: &ArrayRef, row_index: usize) -> Result<Value, String> {
    if array.is_null(row_index) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Int64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| "failed to downcast Int64Array".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::Int32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| "failed to downcast Int32Array".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::UInt64 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| "failed to downcast UInt64Array".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::UInt32 => {
            let typed = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| "failed to downcast UInt32Array".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::Float64 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| "failed to downcast Float64Array".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::Float32 => {
            let typed = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| "failed to downcast Float32Array".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::Boolean => {
            let typed = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| "failed to downcast BooleanArray".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::Utf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| "failed to downcast StringArray".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        DataType::LargeUtf8 => {
            let typed = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| "failed to downcast LargeStringArray".to_string())?;
            Ok(Value::from(typed.value(row_index)))
        }
        other => Err(format!("unsupported data type for routing: {}", other)),
    }
}

fn compare_json_values(lhs: &Value, rhs: &Value) -> Result<Ordering, String> {
    match (lhs, rhs) {
        (Value::Null, Value::Null) => Ok(Ordering::Equal),
        (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
        (Value::Number(left), Value::Number(right)) => {
            let left = left
                .as_f64()
                .ok_or_else(|| "failed to parse left numeric bound".to_string())?;
            let right = right
                .as_f64()
                .ok_or_else(|| "failed to parse right numeric value".to_string())?;
            left.partial_cmp(&right)
                .ok_or_else(|| "failed to compare numeric values".to_string())
        }
        (Value::String(left), Value::String(right)) => Ok(left.cmp(right)),
        _ => Err("range bounds and row values must share comparable JSON scalar types".to_string()),
    }
}

fn partition_for_range_value(
    row_value: &Value,
    bounds: &[Value],
    downstream_partition_count: u32,
) -> Result<u32, String> {
    if downstream_partition_count == 0 {
        return Err("downstream partition count must be greater than zero".to_string());
    }

    let mut lo = 0usize;
    let mut hi = bounds.len();
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let cmp = compare_json_values(&bounds[mid], row_value)?;
        if cmp.is_le() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    let raw_partition =
        u32::try_from(lo).map_err(|e| format!("range partition index overflow: {}", e))?;
    Ok(raw_partition.min(downstream_partition_count.saturating_sub(1)))
}

fn split_batch_by_partition_indices(
    batch: &RecordBatch,
    indices_by_partition: &[Vec<u32>],
) -> Result<Vec<RecordBatch>, String> {
    let mut routed = Vec::with_capacity(indices_by_partition.len());

    for indices in indices_by_partition {
        if indices.is_empty() {
            routed.push(batch.slice(0, 0));
            continue;
        }

        let index_array = UInt32Array::from(indices.clone());
        let columns = batch
            .columns()
            .iter()
            .map(|column| {
                take(column.as_ref(), &index_array, None)
                    .map_err(|e| format!("failed to gather routed partition rows: {}", e))
            })
            .collect::<Result<Vec<_>, _>>()?;
        let routed_batch = RecordBatch::try_new(batch.schema(), columns)
            .map_err(|e| format!("failed to build routed record batch: {}", e))?;
        routed.push(routed_batch);
    }

    Ok(routed)
}

fn parse_partition_spec_raw(partitioning: &str) -> Result<Option<PartitionSpec>, String> {
    if partitioning.trim().is_empty() {
        return Ok(Some(PartitionSpec::Single));
    }

    if let Ok(spec) = serde_json::from_str::<PartitionSpec>(partitioning) {
        return Ok(Some(spec));
    }

    let normalized = partitioning.trim().trim_matches('"').to_ascii_lowercase();
    let spec = match normalized.as_str() {
        "single" | "singlepartition" | "single_partition" => Some(PartitionSpec::Single),
        "broadcast" => Some(PartitionSpec::Broadcast),
        _ => None,
    };

    Ok(spec)
}

fn parse_hash_keys_from_raw(partitioning: &str) -> Result<Vec<String>, String> {
    let payload = serde_json::from_str::<Value>(partitioning)
        .map_err(|e| format!("failed to parse hash partitioning payload: {}", e))?;

    if let Some(hash) = payload.get("Hash") {
        let keys = hash
            .get("keys")
            .or_else(|| hash.get("key_columns"))
            .and_then(Value::as_array)
            .ok_or_else(|| "hash partitioning payload is missing keys".to_string())?;
        return Ok(keys
            .iter()
            .filter_map(Value::as_str)
            .map(std::string::ToString::to_string)
            .collect::<Vec<_>>());
    }

    Err("hash partitioning payload is missing Hash object".to_string())
}

fn parse_range_from_raw(partitioning: &str) -> Result<(String, Vec<Value>), String> {
    let payload = serde_json::from_str::<Value>(partitioning)
        .map_err(|e| format!("failed to parse range partitioning payload: {}", e))?;

    let range = payload
        .get("Range")
        .ok_or_else(|| "range partitioning payload is missing Range object".to_string())?;

    let column = range
        .get("column")
        .or_else(|| range.get("partition_key_column"))
        .or_else(|| range.get("key_column"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "range partitioning payload is missing key column".to_string())?
        .to_string();

    let bounds = range
        .get("bounds")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| "range partitioning payload is missing bounds".to_string())?;

    Ok((column, bounds))
}

/// What: Route one Arrow batch using hash partitioning and stable hash selection.
///
/// Inputs:
/// - `batch`: Input record batch to route.
/// - `partition_key_columns`: Ordered key columns used for hash routing.
/// - `downstream_partition_count`: Number of destination partitions.
///
/// Output:
/// - One record batch per destination partition index.
///
/// Details:
/// - Hash algorithm is selected via `WORKER_HASH_FUNCTION` (`fnv` default, `murmur3` optional).
pub(crate) fn route_batch_hash(
    batch: &RecordBatch,
    partition_key_columns: &[String],
    downstream_partition_count: u32,
) -> Result<Vec<RecordBatch>, String> {
    if downstream_partition_count == 0 {
        return Err("downstream partition count must be greater than zero".to_string());
    }
    if partition_key_columns.is_empty() {
        return Err("hash partitioning requires at least one partition key column".to_string());
    }

    let partition_count = usize::try_from(downstream_partition_count)
        .map_err(|e| format!("invalid downstream partition count: {}", e))?;

    let schema = batch.schema();
    let key_indices = partition_key_columns
        .iter()
        .map(|column| {
            resolve_routing_column_index(schema.as_ref(), column).ok_or_else(|| {
                format!(
                    "hash partition key column '{}' is not present in output schema",
                    column
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut indices_by_partition = vec![Vec::<u32>::new(); partition_count];
    let algorithm = configured_hash_function();

    for row_index in 0..batch.num_rows() {
        let mut payload = Vec::<u8>::new();
        for key_index in &key_indices {
            let value = value_at_as_json(&batch.column(*key_index).clone(), row_index)?;
            payload.extend_from_slice(value.to_string().as_bytes());
            payload.push(0x1f);
        }

        let hash = hash_bytes(&payload, algorithm)?;
        let partition_index_u64 = hash % u64::from(downstream_partition_count);
        let partition_index = usize::try_from(partition_index_u64)
            .map_err(|e| format!("failed to convert partition index: {}", e))?;
        indices_by_partition[partition_index]
            .push(u32::try_from(row_index).map_err(|e| format!("row index overflow: {}", e))?);
    }

    split_batch_by_partition_indices(batch, &indices_by_partition)
}

/// What: Route one Arrow batch using round-robin partitioning with persisted counter state.
///
/// Inputs:
/// - `batch`: Input record batch to route.
/// - `downstream_partition_count`: Number of destination partitions.
/// - `next_partition`: Shared atomic round-robin cursor across batches.
///
/// Output:
/// - One record batch per destination partition index.
///
/// Details:
/// - Counter is incremented once per row and persisted by caller-owned `Arc<AtomicUsize>`.
pub(crate) fn route_batch_roundrobin(
    batch: &RecordBatch,
    downstream_partition_count: u32,
    next_partition: &Arc<AtomicUsize>,
) -> Result<Vec<RecordBatch>, String> {
    if downstream_partition_count == 0 {
        return Err("downstream partition count must be greater than zero".to_string());
    }

    let partition_count = usize::try_from(downstream_partition_count)
        .map_err(|e| format!("invalid downstream partition count: {}", e))?;
    let mut indices_by_partition = vec![Vec::<u32>::new(); partition_count];

    let start = next_partition.fetch_add(batch.num_rows(), AtomicOrdering::SeqCst);
    for row_index in 0..batch.num_rows() {
        let rr_index = start.saturating_add(row_index);
        let partition_index = rr_index % partition_count;
        indices_by_partition[partition_index]
            .push(u32::try_from(row_index).map_err(|e| format!("row index overflow: {}", e))?);
    }

    split_batch_by_partition_indices(batch, &indices_by_partition)
}

/// What: Route one Arrow batch using range partitioning against sorted bounds.
///
/// Inputs:
/// - `batch`: Input record batch to route.
/// - `partition_key_column`: Column containing the range key.
/// - `range_bounds`: Sorted bound values used to derive partition membership.
/// - `downstream_partition_count`: Number of destination partitions.
///
/// Output:
/// - One record batch per destination partition index.
///
/// Details:
/// - Values below first bound map to partition `0`, above last bound map to `N-1`.
pub(crate) fn route_batch_range(
    batch: &RecordBatch,
    partition_key_column: &str,
    range_bounds: &[Value],
    downstream_partition_count: u32,
) -> Result<Vec<RecordBatch>, String> {
    if downstream_partition_count == 0 {
        return Err("downstream partition count must be greater than zero".to_string());
    }

    let key_index = resolve_routing_column_index(batch.schema().as_ref(), partition_key_column)
        .ok_or_else(|| {
            format!(
                "range partition key column '{}' is not present in output schema",
                partition_key_column
            )
        })?;
    let key_array = batch.column(key_index).clone();

    let partition_count = usize::try_from(downstream_partition_count)
        .map_err(|e| format!("invalid downstream partition count: {}", e))?;
    let mut indices_by_partition = vec![Vec::<u32>::new(); partition_count];

    for row_index in 0..batch.num_rows() {
        let row_value = value_at_as_json(&key_array, row_index)?;
        let partition_index =
            partition_for_range_value(&row_value, range_bounds, downstream_partition_count)?;
        let partition_index = usize::try_from(partition_index)
            .map_err(|e| format!("failed to convert partition index: {}", e))?;
        indices_by_partition[partition_index]
            .push(u32::try_from(row_index).map_err(|e| format!("row index overflow: {}", e))?);
    }

    split_batch_by_partition_indices(batch, &indices_by_partition)
}

/// What: Route one Arrow batch to a single destination partition.
///
/// Inputs:
/// - `batch`: Input record batch to route.
/// - `downstream_partition_count`: Number of destination partitions.
///
/// Output:
/// - Batch at partition `0` and empty batches for remaining partitions.
///
/// Details:
/// - Preserves existing single-partition behavior while keeping result shape consistent.
pub(crate) fn route_batch_single(
    batch: &RecordBatch,
    downstream_partition_count: u32,
) -> Result<Vec<RecordBatch>, String> {
    if downstream_partition_count == 0 {
        return Err("downstream partition count must be greater than zero".to_string());
    }

    let partition_count = usize::try_from(downstream_partition_count)
        .map_err(|e| format!("invalid downstream partition count: {}", e))?;
    let mut routed = Vec::<RecordBatch>::with_capacity(partition_count);
    routed.push(batch.clone());
    for _ in 1..partition_count {
        routed.push(batch.slice(0, 0));
    }

    Ok(routed)
}

/// What: Dispatch batch routing based on one `PartitionSpec` strategy.
///
/// Inputs:
/// - `batch`: Input record batch to route.
/// - `partition_spec`: Partitioning strategy descriptor.
/// - `downstream_partition_count`: Number of destination partitions.
/// - `rr_counter`: Optional round-robin counter for strategies that require it.
///
/// Output:
/// - One routed record batch per destination partition.
///
/// Details:
/// - `Broadcast` currently follows single-partition behavior for compatibility.
pub(crate) fn route_batch(
    batch: &RecordBatch,
    partition_spec: &PartitionSpec,
    downstream_partition_count: u32,
    _rr_counter: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<RecordBatch>, String> {
    match partition_spec {
        PartitionSpec::Single | PartitionSpec::Broadcast => {
            route_batch_single(batch, downstream_partition_count)
        }
        PartitionSpec::Hash { keys } => route_batch_hash(batch, keys, downstream_partition_count),
    }
}

/// What: Route one batch from serialized output destination partitioning metadata.
///
/// Inputs:
/// - `batch`: Input record batch to route.
/// - `partitioning`: Serialized partitioning payload or plain partitioning kind string.
/// - `downstream_partition_count`: Number of destination partitions.
/// - `rr_counter`: Optional round-robin counter for round-robin routing.
///
/// Output:
/// - One routed record batch per destination partition.
///
/// Details:
/// - Supports `PartitionSpec` JSON, plain strings (`single`, `hash`, `round_robin_batch`),
///   and extended JSON payloads for RoundRobin/Range routing.
pub(crate) fn route_batch_from_output_partitioning(
    batch: &RecordBatch,
    partitioning: &str,
    downstream_partition_count: u32,
    rr_counter: Option<&Arc<AtomicUsize>>,
) -> Result<Vec<RecordBatch>, String> {
    if let Some(spec) = parse_partition_spec_raw(partitioning)? {
        return route_batch(batch, &spec, downstream_partition_count, rr_counter);
    }

    let normalized = partitioning.trim().trim_matches('"').to_ascii_lowercase();
    if normalized == "round_robin_batch"
        || normalized == "roundrobin"
        || normalized == "round_robin"
    {
        let counter = rr_counter
            .ok_or_else(|| "round-robin partitioning requires a shared counter".to_string())?;
        return route_batch_roundrobin(batch, downstream_partition_count, counter);
    }

    if normalized == "hash" {
        let keys = parse_hash_keys_from_raw(partitioning)?;
        return route_batch_hash(batch, &keys, downstream_partition_count);
    }

    if normalized == "range" {
        let (column, bounds) = parse_range_from_raw(partitioning)?;
        return route_batch_range(batch, &column, &bounds, downstream_partition_count);
    }

    if let Ok(raw_payload) = serde_json::from_str::<Value>(partitioning) {
        if raw_payload.get("RoundRobin").is_some() {
            let counter = rr_counter
                .ok_or_else(|| "round-robin partitioning requires a shared counter".to_string())?;
            return route_batch_roundrobin(batch, downstream_partition_count, counter);
        }

        if raw_payload.get("Hash").is_some() {
            let keys = parse_hash_keys_from_raw(partitioning)?;
            return route_batch_hash(batch, &keys, downstream_partition_count);
        }

        if raw_payload.get("Range").is_some() {
            let (column, bounds) = parse_range_from_raw(partitioning)?;
            return route_batch_range(batch, &column, &bounds, downstream_partition_count);
        }
    }

    Err(format!(
        "unsupported destination partitioning strategy: {}",
        partitioning
    ))
}

#[cfg(test)]
#[path = "../tests/router_tests.rs"]
mod tests;
