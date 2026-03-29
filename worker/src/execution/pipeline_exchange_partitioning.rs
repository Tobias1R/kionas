use std::collections::BTreeSet;

/// What: Parse exchange partition index from a stage artifact key.
///
/// Inputs:
/// - `key`: Object-store key ending with `part-xxxxx.parquet`.
///
/// Output:
/// - Partition index when the key matches expected naming.
pub(crate) fn parse_partition_index_from_exchange_key(key: &str) -> Option<u32> {
    let file_name = key.rsplit('/').next()?;
    if !file_name.ends_with(".parquet") {
        return None;
    }
    let stem = file_name.strip_suffix(".parquet")?;
    let index_raw = stem.strip_prefix("part-")?;
    index_raw.parse::<u32>().ok()
}

/// What: Validate that upstream exchange artifacts cover the expected partition set.
///
/// Inputs:
/// - `upstream_stage_id`: Upstream stage identifier.
/// - `keys`: Exchange parquet artifact keys found for the upstream stage.
/// - `expected_partition_count`: Declared upstream partition count.
///
/// Output:
/// - `Ok(())` when all partitions are present exactly once.
/// - Error when partitions are missing, duplicated, or malformed.
pub(crate) fn validate_upstream_exchange_partition_set(
    upstream_stage_id: u32,
    keys: &[String],
    expected_partition_count: u32,
) -> Result<(), String> {
    if expected_partition_count == 0 {
        return Err(format!(
            "upstream stage {} has invalid expected partition count 0",
            upstream_stage_id
        ));
    }

    if keys.len() != expected_partition_count as usize {
        return Err(format!(
            "upstream stage {} exchange artifact count mismatch: expected {}, found {}",
            upstream_stage_id,
            expected_partition_count,
            keys.len()
        ));
    }

    let mut seen = BTreeSet::<u32>::new();
    for key in keys {
        let partition_index = parse_partition_index_from_exchange_key(key).ok_or_else(|| {
            format!(
                "upstream stage {} exchange artifact has invalid key format: {}",
                upstream_stage_id, key
            )
        })?;

        if partition_index >= expected_partition_count {
            return Err(format!(
                "upstream stage {} exchange artifact partition {} out of range [0, {})",
                upstream_stage_id, partition_index, expected_partition_count
            ));
        }

        if !seen.insert(partition_index) {
            return Err(format!(
                "upstream stage {} has duplicate exchange artifact for partition {}",
                upstream_stage_id, partition_index
            ));
        }
    }

    if seen.len() != expected_partition_count as usize {
        return Err(format!(
            "upstream stage {} exchange partition coverage mismatch: expected {} unique partitions, found {}",
            upstream_stage_id,
            expected_partition_count,
            seen.len()
        ));
    }

    Ok(())
}

/// What: Select exchange artifacts for one downstream partition from an upstream stage.
///
/// Inputs:
/// - `upstream_stage_id`: Upstream stage identifier.
/// - `keys`: Candidate upstream exchange artifact keys.
/// - `upstream_partition_count`: Declared upstream partition count.
/// - `downstream_partition_count`: Declared downstream partition count.
/// - `downstream_partition_index`: Current downstream partition index.
///
/// Output:
/// - Ordered subset of upstream keys assigned to the downstream partition.
///
/// Details:
/// - Uses one-to-one mapping when upstream fanout is less than or equal to downstream fanout.
/// - Uses modulo mapping when upstream fanout is greater than downstream fanout.
pub(crate) fn select_exchange_keys_for_downstream_partition(
    upstream_stage_id: u32,
    keys: &[String],
    upstream_partition_count: u32,
    downstream_partition_count: u32,
    downstream_partition_index: u32,
) -> Result<Vec<String>, String> {
    if downstream_partition_count == 0 {
        return Err(format!(
            "downstream stage for upstream stage {} has invalid partition count 0",
            upstream_stage_id
        ));
    }
    if downstream_partition_index >= downstream_partition_count {
        return Err(format!(
            "downstream partition {} out of range [0, {}) for upstream stage {}",
            downstream_partition_index, downstream_partition_count, upstream_stage_id
        ));
    }

    if upstream_partition_count <= downstream_partition_count {
        let suffix = format!("part-{:05}.parquet", downstream_partition_index);
        return Ok(keys
            .iter()
            .filter(|key| key.ends_with(&suffix))
            .cloned()
            .collect::<Vec<_>>());
    }

    let mut selected = Vec::<String>::new();
    for key in keys {
        let upstream_partition_index =
            parse_partition_index_from_exchange_key(key).ok_or_else(|| {
                format!(
                    "upstream stage {} exchange artifact has invalid key format: {}",
                    upstream_stage_id, key
                )
            })?;

        if upstream_partition_index >= upstream_partition_count {
            return Err(format!(
                "upstream stage {} exchange artifact partition {} out of range [0, {})",
                upstream_stage_id, upstream_partition_index, upstream_partition_count
            ));
        }

        if upstream_partition_index % downstream_partition_count == downstream_partition_index {
            selected.push(key.clone());
        }
    }

    selected.sort();
    Ok(selected)
}
