use super::StorageProvider;
use std::sync::Arc;

/// What: Build deterministic prefix for stage exchange artifacts.
///
/// Inputs:
/// - `query_run_id`: Server-generated run identifier shared by all stages in a query dispatch.
/// - `session_id`: Session identifier associated with the query.
/// - `stage_id`: Distributed stage identifier.
///
/// Output:
/// - Stable object-store prefix under `distributed_exchange/` for stage artifacts.
pub fn stage_exchange_prefix(query_run_id: &str, session_id: &str, stage_id: u32) -> String {
    format!(
        "distributed_exchange/{}/{}/stage-{}/",
        sanitize_component(query_run_id),
        sanitize_component(session_id),
        stage_id
    )
}

/// What: Build parquet data key for one stage partition artifact.
///
/// Inputs:
/// - `query_run_id`: Server-generated run identifier.
/// - `session_id`: Session identifier associated with the query.
/// - `stage_id`: Distributed stage identifier.
/// - `partition_index`: Zero-based partition index.
///
/// Output:
/// - Stable parquet object key for exchange artifact bytes.
pub fn stage_exchange_data_key(
    query_run_id: &str,
    session_id: &str,
    stage_id: u32,
    partition_index: u32,
) -> String {
    format!(
        "{}part-{partition_index:05}.parquet",
        stage_exchange_prefix(query_run_id, session_id, stage_id)
    )
}

/// What: Build metadata key for one stage partition exchange artifact.
///
/// Inputs:
/// - `query_run_id`: Server-generated run identifier.
/// - `session_id`: Session identifier associated with the query.
/// - `stage_id`: Distributed stage identifier.
/// - `partition_index`: Zero-based partition index.
///
/// Output:
/// - Stable metadata object key for exchange artifact sidecar JSON.
pub fn stage_exchange_metadata_key(
    query_run_id: &str,
    session_id: &str,
    stage_id: u32,
    partition_index: u32,
) -> String {
    format!(
        "{}part-{partition_index:05}.metadata.json",
        stage_exchange_prefix(query_run_id, session_id, stage_id)
    )
}

/// What: List parquet exchange artifacts produced by one upstream stage.
///
/// Inputs:
/// - `provider`: Configured storage provider.
/// - `query_run_id`: Server-generated run identifier.
/// - `session_id`: Session identifier associated with the query.
/// - `stage_id`: Upstream distributed stage identifier.
///
/// Output:
/// - Sorted parquet keys for all exchange partition artifacts produced by the stage.
pub async fn list_stage_exchange_data_keys(
    provider: &Arc<dyn StorageProvider + Send + Sync>,
    query_run_id: &str,
    session_id: &str,
    stage_id: u32,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let prefix = stage_exchange_prefix(query_run_id, session_id, stage_id);
    let mut keys = provider.list_objects(&prefix).await?;
    keys.retain(|key| key.ends_with(".parquet"));
    keys.sort();
    Ok(keys)
}

/// What: Normalize object key path components used for exchange storage.
///
/// Inputs:
/// - `raw`: Raw identifier string.
///
/// Output:
/// - Lowercase ASCII-safe component retaining only `[a-z0-9_-]`.
fn sanitize_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() || lower == '_' || lower == '-' {
            out.push(lower);
        } else {
            out.push('_');
        }
    }

    if out.is_empty() {
        "unknown".to_string()
    } else {
        out
    }
}

#[cfg(test)]
#[path = "../tests/storage_exchange_tests.rs"]
mod tests;
