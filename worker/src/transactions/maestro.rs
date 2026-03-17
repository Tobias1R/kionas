use crate::state::SharedData;
use serde_json::Value;

/// Ensure or lazily initialize the interops pool and return a cloned Arc to it.
async fn ensure_pool(
    shared: SharedData,
) -> Option<std::sync::Arc<deadpool::managed::Pool<crate::interops::InteropsManager>>> {
    let mut guard = shared.master_pool.lock().await;
    if guard.is_none() {
        let info = shared.worker_info.clone();
        let manager = crate::interops::InteropsManager {
            addr: info.server_url.clone(),
            ca_cert_path: Some(info.ca_cert_path.clone()),
            tls_cert_path: Some(info.tls_cert_path.clone()),
            tls_key_path: Some(info.tls_key_path.clone()),
        };
        let pool_size: usize = std::env::var("MASTER_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        match deadpool::managed::Pool::builder(manager)
            .max_size(pool_size)
            .build()
        {
            Ok(p) => {
                let arcp = std::sync::Arc::new(p);
                *guard = Some(arcp.clone());
                Some(arcp)
            }
            Err(e) => {
                log::error!(
                    "Failed to build interops pool in transactions::ensure_pool: {}",
                    e
                );
                None
            }
        }
    } else {
        guard.as_ref().map(|p| p.clone())
    }
}

pub async fn prepare_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
    tasks: &[Value],
) -> Result<(), String> {
    if let Some(provider) = shared.storage_provider.clone() {
        // stage_tx returns a Vec<String> of staged keys; ignore result and map errors
        match crate::storage::staging::stage_tx(provider, tx_id, staging_prefix, tasks).await {
            Ok(_keys) => Ok(()),
            Err(e) => Err(format!("{}", e)),
        }
    } else {
        // fallback to local filesystem behavior
        let staging_dir = format!("worker_storage/staging/{}/{}", staging_prefix, tx_id);
        if let Err(e) = tokio::fs::create_dir_all(&staging_dir).await {
            return Err(format!(
                "failed to create staging dir {}: {}",
                staging_dir, e
            ));
        }
        for t in tasks.iter() {
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
            "tasks": tasks.iter().filter_map(|t| t.get("task_id").and_then(|v| v.as_str().map(|s| s.to_string()))).collect::<Vec<String>>(),
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

pub async fn commit_tx(
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

pub async fn abort_tx(shared: SharedData, tx_id: &str, staging_prefix: &str) -> Result<(), String> {
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

pub fn handle_execute_task(
    shared: SharedData,
    req: crate::services::worker_service_server::worker_service::TaskRequest,
) -> crate::services::worker_service_server::worker_service::TaskResponse {
    let task_id = req
        .tasks
        .get(0)
        .map(|t| t.task_id.clone())
        .unwrap_or_else(|| "".to_string());
    let result_location = "arrow-flight-endpoint".to_string();
    let result_location_for_spawn = result_location.clone();
    let shared_clone = shared.clone();
    tokio::spawn(async move {
        // simulate work
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        // ensure pool and send update
        if let Some(pool_arc) = ensure_pool(shared_clone.clone()).await {
            match pool_arc.get().await {
                Ok(mut pooled_client) => {
                    let update = crate::interops_service::TaskUpdateRequest {
                        task_id: task_id.clone(),
                        status: "succeeded".to_string(),
                        result_location: result_location_for_spawn.clone(),
                        error: String::new(),
                    };
                    if let Err(e) = pooled_client.task_update(tonic::Request::new(update)).await {
                        log::error!(
                            "Failed to send TaskUpdate for {} using pool: {}",
                            task_id,
                            e
                        );
                    }
                }
                Err(e) => log::error!("Failed to acquire pooled master client: {}", e),
            }
        } else {
            log::error!(
                "No interops pool available; dropping TaskUpdate for {}",
                task_id
            );
        }
    });

    crate::services::worker_service_server::worker_service::TaskResponse {
        status: "ok".to_string(),
        error: String::new(),
        result_location,
    }
}
