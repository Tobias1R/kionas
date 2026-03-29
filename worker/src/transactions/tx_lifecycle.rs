use crate::state::SharedData;
use serde_json::Value as JsonValue;

pub(crate) async fn prepare_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
    tasks: &[JsonValue],
) -> Result<(), String> {
    if let Some(provider) = shared.storage_provider.clone() {
        match crate::storage::staging::stage_tx(provider, tx_id, staging_prefix, tasks).await {
            Ok(_keys) => Ok(()),
            Err(e) => Err(format!("{}", e)),
        }
    } else {
        let staging_dir = format!("worker_storage/staging/{}/{}", staging_prefix, tx_id);
        if let Err(e) = tokio::fs::create_dir_all(&staging_dir).await {
            return Err(format!(
                "failed to create staging dir {}: {}",
                staging_dir, e
            ));
        }
        for t in tasks {
            if let Some(task_id) = t.get("task_id").and_then(|v| v.as_str()) {
                let task_file = format!("{}/{}.json", staging_dir, task_id);
                let payload =
                    serde_json::to_vec_pretty(t).unwrap_or_else(|_| t.to_string().into_bytes());
                if let Err(e) = tokio::fs::write(&task_file, payload).await {
                    return Err(format!(
                        "failed to write staged task file {}: {}",
                        task_file, e
                    ));
                }
            }
        }
        let manifest = serde_json::json!({
            "tx_id": tx_id,
            "staging_prefix": staging_prefix,
            "tasks": tasks
                .iter()
                .filter_map(|t| t.get("task_id").and_then(|v| v.as_str().map(|s| s.to_string())))
                .collect::<Vec<String>>(),
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let manifest_path = format!("{}/manifest_{}.json", staging_dir, tx_id);
        if let Err(e) = tokio::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest)
                .unwrap_or_else(|_| manifest.to_string().into_bytes()),
        )
        .await
        {
            return Err(format!("failed to write manifest {}: {}", manifest_path, e));
        }
        Ok(())
    }
}

pub(crate) async fn commit_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
) -> Result<(), String> {
    if let Some(provider) = shared.storage_provider.clone() {
        crate::storage::staging::promote_tx(provider, tx_id, staging_prefix)
            .await
            .map_err(|e| format!("{}", e))
    } else {
        let staging_dir = format!("worker_storage/staging/{}/{}", staging_prefix, tx_id);
        let final_parent = format!("worker_storage/final/{}", staging_prefix);
        let final_dir = format!("{}/{}", final_parent, tx_id);
        if let Err(e) = tokio::fs::create_dir_all(&final_parent).await {
            return Err(format!(
                "failed to create final parent {}: {}",
                final_parent, e
            ));
        }
        if let Err(e) = tokio::fs::rename(&staging_dir, &final_dir).await {
            return Err(format!(
                "failed to promote staging {} -> {}: {}",
                staging_dir, final_dir, e
            ));
        }
        let manifest = serde_json::json!({
            "tx_id": tx_id,
            "final_path": final_dir.clone(),
            "committed_at": chrono::Utc::now().to_rfc3339(),
        });
        let manifest_path = format!("{}/manifest_{}.json", final_parent, tx_id);
        if let Err(e) = tokio::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest)
                .unwrap_or_else(|_| manifest.to_string().into_bytes()),
        )
        .await
        {
            return Err(format!(
                "failed to write final manifest {}: {}",
                manifest_path, e
            ));
        }
        Ok(())
    }
}

pub(crate) async fn abort_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
) -> Result<(), String> {
    if let Some(provider) = shared.storage_provider.clone() {
        crate::storage::staging::abort_tx(provider, tx_id, staging_prefix)
            .await
            .map_err(|e| format!("{}", e))
    } else {
        let staging_dir = format!("worker_storage/staging/{}/{}", staging_prefix, tx_id);
        match tokio::fs::remove_dir_all(&staging_dir).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(format!(
                        "failed to remove staging dir {}: {}",
                        staging_dir, e
                    ))
                }
            }
        }
    }
}
