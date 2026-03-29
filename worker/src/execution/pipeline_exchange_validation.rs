#[cfg(test)]
use crate::execution::pipeline::format_exchange_retry_decision_event;
#[cfg(test)]
use crate::execution::pipeline::parse_partition_index_from_exchange_key;
#[cfg(test)]
use crate::storage::StorageProvider;
#[cfg(test)]
use crate::storage::exchange::{list_stage_exchange_data_keys, stage_exchange_metadata_key};
#[cfg(test)]
use std::time::Duration;
#[cfg(test)]
use tokio::time::sleep;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(test)]
pub(crate) enum StorageErrorClass {
    Retriable,
    NonRetriable,
}

/// What: Classify storage operation failures for bounded retry decisions.
///
/// Inputs:
/// - `error_message`: Storage-layer error text.
///
/// Output:
/// - `StorageErrorClass::Retriable` for transient transport/system faults.
/// - `StorageErrorClass::NonRetriable` for permanent/semantic failures.
#[cfg(test)]
pub(crate) fn classify_storage_error(error_message: &str) -> StorageErrorClass {
    let normalized = error_message.to_ascii_lowercase();

    let retriable_signals = [
        "timeout",
        "timed out",
        "connection reset",
        "connection refused",
        "temporary",
        "temporarily unavailable",
        "unavailable",
        "too many requests",
        "throttl",
        "deadline exceeded",
        "broken pipe",
        "eof",
        "io error",
    ];

    if retriable_signals
        .iter()
        .any(|signal| normalized.contains(signal))
    {
        StorageErrorClass::Retriable
    } else {
        StorageErrorClass::NonRetriable
    }
}

#[cfg(test)]
const EXCHANGE_IO_MAX_ATTEMPTS: usize = 3;
#[cfg(test)]
const EXCHANGE_IO_RETRY_DELAY_MS: u64 = 25;

/// What: List upstream stage exchange artifact keys with bounded retry semantics.
///
/// Inputs:
/// - `provider`: Configured storage provider.
/// - `query_run_id`: Query run identifier for exchange scope.
/// - `session_id`: Session identifier.
/// - `upstream_stage_id`: Upstream stage identifier.
///
/// Output:
/// - Sorted parquet exchange keys for the upstream stage, or explicit terminal error.
#[cfg(test)]
pub(crate) async fn list_stage_exchange_data_keys_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    query_run_id: &str,
    session_id: &str,
    upstream_stage_id: u32,
) -> Result<Vec<String>, String> {
    let mut attempt = 1usize;
    loop {
        match list_stage_exchange_data_keys(provider, query_run_id, session_id, upstream_stage_id)
            .await
        {
            Ok(keys) => return Ok(keys),
            Err(error) => {
                let msg = error.to_string();
                let class = classify_storage_error(&msg);
                let should_retry =
                    class == StorageErrorClass::Retriable && attempt < EXCHANGE_IO_MAX_ATTEMPTS;
                let failure_class = match class {
                    StorageErrorClass::Retriable => "retriable",
                    StorageErrorClass::NonRetriable => "non_retriable",
                };

                if should_retry {
                    log::warn!(
                        "{} reason={}",
                        format_exchange_retry_decision_event(
                            query_run_id,
                            upstream_stage_id,
                            "list",
                            attempt,
                            "retrying",
                            failure_class,
                            format!("upstream_stage={}", upstream_stage_id).as_str(),
                        ),
                        msg
                    );
                    sleep(Duration::from_millis(EXCHANGE_IO_RETRY_DELAY_MS)).await;
                    attempt += 1;
                    continue;
                }

                log::warn!(
                    "{} reason={}",
                    format_exchange_retry_decision_event(
                        query_run_id,
                        upstream_stage_id,
                        "list",
                        attempt,
                        "terminal",
                        failure_class,
                        format!("upstream_stage={}", upstream_stage_id).as_str(),
                    ),
                    msg
                );

                return Err(format!(
                    "failed to list exchange artifacts for upstream stage {} after {} attempt(s): {}",
                    upstream_stage_id, attempt, msg
                ));
            }
        }
    }
}

/// What: Read one exchange artifact with bounded retry semantics.
///
/// Inputs:
/// - `provider`: Configured storage provider.
/// - `key`: Artifact object key.
///
/// Output:
/// - Artifact bytes when found, `None` when missing, or explicit terminal error.
#[cfg(test)]
pub(crate) async fn get_exchange_artifact_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    key: &str,
) -> Result<Option<Vec<u8>>, String> {
    let mut attempt = 1usize;
    loop {
        match provider.get_object(key).await {
            Ok(bytes) => return Ok(bytes),
            Err(error) => {
                let msg = error.to_string();
                let class = classify_storage_error(&msg);
                let should_retry =
                    class == StorageErrorClass::Retriable && attempt < EXCHANGE_IO_MAX_ATTEMPTS;
                let failure_class = match class {
                    StorageErrorClass::Retriable => "retriable",
                    StorageErrorClass::NonRetriable => "non_retriable",
                };

                if should_retry {
                    log::warn!(
                        "{} reason={}",
                        format_exchange_retry_decision_event(
                            "unknown",
                            0,
                            "get",
                            attempt,
                            "retrying",
                            failure_class,
                            key,
                        ),
                        msg
                    );
                    sleep(Duration::from_millis(EXCHANGE_IO_RETRY_DELAY_MS)).await;
                    attempt += 1;
                    continue;
                }

                log::warn!(
                    "{} reason={}",
                    format_exchange_retry_decision_event(
                        "unknown",
                        0,
                        "get",
                        attempt,
                        "terminal",
                        failure_class,
                        key,
                    ),
                    msg
                );

                return Err(format!(
                    "failed to read exchange artifact {} after {} attempt(s): {}",
                    key, attempt, msg
                ));
            }
        }
    }
}

/// What: Parse expected artifact size/checksum from exchange metadata sidecar.
///
/// Inputs:
/// - `metadata_bytes`: Metadata JSON bytes for one exchange partition.
/// - `data_key`: Expected parquet artifact key.
///
/// Output:
/// - `(size_bytes, checksum_fnv64)` expected for the parquet artifact.
#[cfg(test)]
pub(crate) fn expected_exchange_artifact_from_metadata(
    metadata_bytes: &[u8],
    data_key: &str,
) -> Result<(u64, String), String> {
    let metadata: serde_json::Value = serde_json::from_slice(metadata_bytes)
        .map_err(|e| format!("failed to parse exchange metadata JSON: {}", e))?;

    let artifacts = metadata
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "exchange metadata missing artifacts array".to_string())?;

    let artifact = artifacts
        .iter()
        .find(|entry| {
            entry
                .get("key")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|value| value == data_key)
        })
        .ok_or_else(|| {
            format!(
                "exchange metadata missing artifact entry for object {}",
                data_key
            )
        })?;

    let size_bytes = artifact
        .get("size_bytes")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            format!(
                "exchange metadata artifact missing size_bytes for object {}",
                data_key
            )
        })?;

    let checksum_fnv64 = artifact
        .get("checksum_fnv64")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            format!(
                "exchange metadata artifact missing checksum_fnv64 for object {}",
                data_key
            )
        })?
        .to_string();

    Ok((size_bytes, checksum_fnv64))
}

/// What: Compute deterministic FNV-1a 64-bit checksum as lowercase hex.
///
/// Inputs:
/// - `bytes`: Raw artifact bytes.
///
/// Output:
/// - 16-char lowercase hex checksum.
#[cfg(test)]
fn checksum_fnv64_hex(bytes: &[u8]) -> String {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }

    format!("{hash:016x}")
}

/// What: Load one exchange parquet artifact and validate it against metadata sidecar.
///
/// Inputs:
/// - `provider`: Configured storage provider.
/// - `query_run_id`: Query run identifier for exchange scope.
/// - `session_id`: Session identifier for exchange scope.
/// - `upstream_stage_id`: Upstream stage identifier.
/// - `data_key`: Parquet artifact key.
///
/// Output:
/// - Raw parquet bytes when size/checksum validation succeeds.
#[cfg(test)]
pub(crate) async fn load_validated_exchange_artifact_with_retry(
    provider: &std::sync::Arc<dyn StorageProvider + Send + Sync>,
    query_run_id: &str,
    session_id: &str,
    upstream_stage_id: u32,
    data_key: &str,
) -> Result<Vec<u8>, String> {
    let partition_index = parse_partition_index_from_exchange_key(data_key).ok_or_else(|| {
        format!(
            "exchange artifact has invalid partition key format: {}",
            data_key
        )
    })?;

    let metadata_key =
        stage_exchange_metadata_key(query_run_id, session_id, upstream_stage_id, partition_index);

    let metadata_bytes = get_exchange_artifact_with_retry(provider, &metadata_key)
        .await?
        .ok_or_else(|| format!("exchange metadata not found: {}", metadata_key))?;

    let (expected_size_bytes, expected_checksum) =
        expected_exchange_artifact_from_metadata(&metadata_bytes, data_key)?;

    let bytes = get_exchange_artifact_with_retry(provider, data_key)
        .await?
        .ok_or_else(|| format!("exchange artifact not found: {}", data_key))?;

    if expected_size_bytes != bytes.len() as u64 {
        return Err(format!(
            "exchange artifact size mismatch for {}: expected={} actual={}",
            data_key,
            expected_size_bytes,
            bytes.len()
        ));
    }

    let checksum = checksum_fnv64_hex(&bytes);
    if expected_checksum != checksum {
        return Err(format!(
            "exchange artifact checksum mismatch for {}",
            data_key
        ));
    }

    Ok(bytes)
}
