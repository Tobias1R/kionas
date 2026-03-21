use chrono::Utc;
use log::{error, info};
use serde_json::Value;
use std::sync::Arc;

use super::StorageProvider;
use crate::txn::manifest::{
    final_manifest_key, final_prefix_key, staging_manifest_key, staging_prefix_key,
};

/// Stage a transaction: write each task JSON under `staging/{staging_prefix}/{tx_id}/...`
/// and write a staging manifest. Returns the list of keys written.
pub async fn stage_tx(
    provider: Arc<dyn StorageProvider>,
    tx_id: &str,
    staging_prefix: &str,
    tasks: &[Value],
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let base_key_prefix = staging_prefix_key(staging_prefix, tx_id);
    let mut written_keys = Vec::with_capacity(tasks.len());

    for t in tasks.iter() {
        let task_id = t
            .get("task_id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "task missing task_id".to_string())?;
        let key = format!("{}{}.json", base_key_prefix, task_id);
        let bytes = serde_json::to_vec_pretty(t)?;
        provider.put_object(&key, bytes).await.map_err(|e| {
            error!("failed to put staged object {}: {}", key, e);
            e
        })?;
        written_keys.push(key);
    }

    // Write manifest
    let manifest = serde_json::json!({
        "tx_id": tx_id,
        "staging_prefix": staging_prefix,
        "tasks": tasks.iter().filter_map(|t| t.get("task_id").and_then(|v| v.as_str()).map(|s| s.to_string())).collect::<Vec<_>>(),
        "created_at": Utc::now().to_rfc3339(),
    });
    let manifest_key = staging_manifest_key(staging_prefix, tx_id);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    provider
        .put_object(&manifest_key, manifest_bytes)
        .await
        .map_err(|e| {
            error!("failed to put staging manifest {}: {}", manifest_key, e);
            e
        })?;

    info!("staged tx {} at prefix {}", tx_id, base_key_prefix);
    Ok(written_keys)
}

/// Promote a staged tx: copy all staged objects from staging -> final and write final manifest.
pub async fn promote_tx(
    provider: Arc<dyn StorageProvider>,
    tx_id: &str,
    staging_prefix: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let staging_prefix_full = staging_prefix_key(staging_prefix, tx_id);
    let final_prefix_full = final_prefix_key(staging_prefix, tx_id);

    let keys = provider.list_objects(&staging_prefix_full).await?;
    for key in keys.iter() {
        if let Some(bytes) = provider.get_object(key).await? {
            let dest_key = key.replacen(&staging_prefix_full, &final_prefix_full, 1);
            provider.put_object(&dest_key, bytes).await?;
            if let Err(e) = provider.delete_object(key).await {
                log::warn!("failed to delete staging object {}: {}", key, e);
            }
        } else {
            log::warn!("staged object {} disappeared during commit", key);
        }
    }

    // write final manifest
    let manifest = serde_json::json!({
        "tx_id": tx_id,
        "final_prefix": final_prefix_full,
        "committed_at": Utc::now().to_rfc3339(),
    });
    let manifest_key = final_manifest_key(staging_prefix, tx_id);
    let manifest_bytes = serde_json::to_vec_pretty(&manifest)?;
    provider.put_object(&manifest_key, manifest_bytes).await?;
    Ok(())
}

/// Abort a staged tx: delete all objects under the staging prefix.
pub async fn abort_tx(
    provider: Arc<dyn StorageProvider>,
    tx_id: &str,
    staging_prefix: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let staging_prefix_full = staging_prefix_key(staging_prefix, tx_id);
    let keys = provider.list_objects(&staging_prefix_full).await?;
    for k in keys.iter() {
        if let Err(e) = provider.delete_object(k).await {
            log::warn!("failed to delete staged object {}: {}", k, e);
        }
    }
    Ok(())
}
