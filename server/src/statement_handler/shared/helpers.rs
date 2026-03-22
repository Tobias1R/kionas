use crate::warehouse::state::SharedData;
use std::collections::{HashMap, HashSet};
use std::error::Error;

use crate::workers::PooledConn;
use uuid::Uuid;

/// What: Auth metadata propagated from warehouse query dispatch to worker execution.
///
/// Inputs:
/// - `rbac_user`: Authenticated RBAC principal.
/// - `rbac_role`: Effective RBAC role.
/// - `scope`: Server-computed trusted scope for worker authorization.
/// - `query_id`: Correlation id for tracing.
///
/// Output:
/// - Metadata carrier used by task dispatch helpers.
///
/// Details:
/// - Scope is evaluated on the server and enforced on the worker.
#[derive(Clone, Debug)]
pub struct DispatchAuthContext {
    pub rbac_user: String,
    pub rbac_role: String,
    pub scope: String,
    pub query_id: String,
}

/// Resolve session and worker key for a session id
pub async fn resolve_session_and_key(
    shared_data: &SharedData,
    session_id: &str,
) -> Result<(crate::session::Session, String), Box<dyn Error + Send + Sync>> {
    // Get session
    let session_opt = {
        let state = shared_data.lock().await;
        state
            .session_manager
            .get_session(session_id.to_string())
            .await
    };
    let session = match session_opt {
        Some(s) => s,
        None => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "session not found",
            )));
        }
    };

    // Resolve worker key
    let key_opt = {
        let state = shared_data.lock().await;
        state.resolve_worker_key(session_id).await
    };
    let key = match key_opt {
        Some(k) => k,
        None => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no matching worker for session",
            )));
        }
    };

    Ok((session, key))
}

/// Create a Task in TaskManager and mark it Scheduled.
pub async fn build_task_and_schedule(
    shared_data: &SharedData,
    query_id: String,
    session_id: &str,
    operation: &str,
    payload: String,
    params: std::collections::HashMap<String, String>,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    let task_manager = {
        let state = shared_data.lock().await;
        state.task_manager.clone()
    };
    let task_id = task_manager
        .create_task(
            query_id,
            session_id.to_string(),
            operation.to_string(),
            payload,
            params,
        )
        .await;
    task_manager
        .set_state(&task_id, crate::tasks::TaskState::Scheduled)
        .await;
    Ok(task_id)
}

/// Acquire a pooled connection for a worker key and validate with heartbeat
pub async fn acquire_pooled_conn(
    shared_data: &SharedData,
    key: &str,
    warehouse_name: &str,
    timeout_secs: u64,
) -> Result<PooledConn, Box<dyn Error + Send + Sync>> {
    // Get or create pool
    let pool = {
        let state = shared_data.lock().await;
        state
            .get_or_create_pool_for_key(key)
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?
    };
    // Acquire pooled connection with heartbeat
    let conn =
        crate::workers::acquire_channel_with_heartbeat(&pool, warehouse_name, timeout_secs).await?;
    Ok(conn)
}

/// Dispatch the TaskRequest to worker and record the result back to TaskManager
pub async fn dispatch_task_and_record(
    shared_data: &SharedData,
    conn: PooledConn,
    req: crate::services::worker_service_client::worker_service::TaskRequest,
    auth_ctx: Option<&DispatchAuthContext>,
    task_id: &str,
    timeout_secs: u64,
) -> Result<
    crate::services::worker_service_client::worker_service::TaskResponse,
    Box<dyn Error + Send + Sync>,
> {
    let resp = crate::workers::send_task_to_worker(conn, req, auth_ctx, timeout_secs).await?;

    let task_manager = {
        let state = shared_data.lock().await;
        state.task_manager.clone()
    };

    if let Some(task_arc) = task_manager.get_task(task_id).await {
        let mut t = task_arc.lock().await;
        if resp.status == "ok" {
            t.result_location = Some(resp.result_location.clone());
            t.state = crate::tasks::TaskState::Succeeded;
        } else {
            t.error = Some(resp.error.clone());
            t.state = crate::tasks::TaskState::Failed;
        }
    }

    Ok(resp)
}

/// High-level helper: resolve session/key, create task, acquire connection, dispatch and record.
pub async fn run_task_for_input(
    shared_data: &SharedData,
    session_id: &str,
    operation: &str,
    payload: String,
    timeout_secs: u64,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    run_task_for_input_with_params(
        shared_data,
        session_id,
        operation,
        payload,
        std::collections::HashMap::new(),
        None,
        timeout_secs,
    )
    .await
}

/// Same as `run_task_for_input`, but allows passing operation-specific task params.
pub async fn run_task_for_input_with_params(
    shared_data: &SharedData,
    session_id: &str,
    operation: &str,
    payload: String,
    params: std::collections::HashMap<String, String>,
    auth_ctx: Option<&DispatchAuthContext>,
    timeout_secs: u64,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    // Resolve session and worker key
    let (session, key) = resolve_session_and_key(shared_data, session_id).await?;

    // Create a query id and schedule task
    let query_id = Uuid::new_v4().to_string();
    let task_id = build_task_and_schedule(
        shared_data,
        query_id,
        session_id,
        operation,
        payload,
        params,
    )
    .await?;

    // Acquire pooled connection (validates via heartbeat)
    let conn =
        acquire_pooled_conn(shared_data, &key, &session.get_warehouse(), timeout_secs).await?;

    // Fetch task to convert into TaskRequest
    let task_arc_opt = {
        let state = shared_data.lock().await;
        state.task_manager.get_task(&task_id).await
    };
    let task_arc = match task_arc_opt {
        Some(a) => a,
        None => {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "task not found after creation",
            )));
        }
    };
    let task_obj = {
        let t = task_arc.lock().await;
        t.clone()
    };
    let req = crate::tasks::task_to_request(&task_obj);

    // Mark running and dispatch
    {
        let state = shared_data.lock().await;
        state
            .task_manager
            .set_state(&task_id, crate::tasks::TaskState::Running)
            .await;
    }
    let resp =
        dispatch_task_and_record(shared_data, conn, req, auth_ctx, &task_id, timeout_secs).await?;

    if resp.status == "ok" {
        Ok(resp.result_location)
    } else {
        Err(Box::new(std::io::Error::other(resp.error)))
    }
}

/// What: Execute stage task groups in dependency order for distributed query scheduling.
///
/// Inputs:
/// - `shared_data`: Shared server state used for session and worker resolution.
/// - `session_id`: Active session id.
/// - `stage_groups`: Compiled stage groups with dependency metadata.
/// - `timeout_secs`: Per-stage dispatch timeout.
///
/// Output:
/// - Final stage result location when stage execution succeeds.
/// - Error when no schedulable stage remains or a stage dispatch fails.
///
/// Details:
/// - This scheduler performs a deterministic topological walk over stage dependencies.
/// - Current implementation dispatches all partitions of a ready stage in parallel and marks the stage complete only when every partition succeeds.
pub async fn run_stage_groups_for_input(
    shared_data: &SharedData,
    session_id: &str,
    stage_groups: &[crate::statement_handler::shared::distributed_dag::StageTaskGroup],
    auth_ctx: Option<&DispatchAuthContext>,
    timeout_secs: u64,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    if stage_groups.is_empty() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "stage scheduler received no stage groups",
        )));
    }

    let mut groups_by_stage = HashMap::<
        u32,
        Vec<crate::statement_handler::shared::distributed_dag::StageTaskGroup>,
    >::new();
    let mut deps_by_stage = HashMap::<u32, Vec<u32>>::new();
    for group in stage_groups {
        groups_by_stage
            .entry(group.stage_id)
            .or_default()
            .push(group.clone());
        deps_by_stage
            .entry(group.stage_id)
            .or_insert_with(|| group.upstream_stage_ids.clone());
    }

    for groups in groups_by_stage.values_mut() {
        groups.sort_by_key(|group| group.partition_index);
    }

    let mut completed = HashSet::<u32>::new();
    let mut result_by_stage = HashMap::<u32, String>::new();

    while completed.len() < groups_by_stage.len() {
        let next_stage_id = groups_by_stage
            .keys()
            .filter(|stage_id| !completed.contains(stage_id))
            .filter(|stage_id| {
                deps_by_stage
                    .get(stage_id)
                    .map(|deps| deps.iter().all(|upstream| completed.contains(upstream)))
                    .unwrap_or(true)
            })
            .copied()
            .min();

        let stage_id = match next_stage_id {
            Some(value) => value,
            None => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no schedulable stage found; dependency graph may be cyclic",
                )));
            }
        };

        let stage_partitions = groups_by_stage.get(&stage_id).cloned().ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("missing stage partition groups for stage {}", stage_id),
            )) as Box<dyn Error + Send + Sync>
        })?;

        let expected_partition_count = stage_partitions
            .first()
            .map(|group| group.partition_count)
            .unwrap_or(0);
        if expected_partition_count == 0
            || stage_partitions.len() != expected_partition_count as usize
        {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "stage {} partition fan-out mismatch: expected {} tasks, found {}",
                    stage_id,
                    expected_partition_count,
                    stage_partitions.len()
                ),
            )));
        }

        log::info!(
            "distributed scheduler dispatching stage_id={} partitions={} upstream={:?}",
            stage_id,
            expected_partition_count,
            stage_partitions[0].upstream_stage_ids
        );

        let mut join_set = tokio::task::JoinSet::new();
        for group in stage_partitions {
            let shared_clone = shared_data.clone();
            let session_id_owned = session_id.to_string();
            let auth_ctx_cloned = auth_ctx.cloned();
            join_set.spawn(async move {
                let partition_location = run_task_for_input_with_params(
                    &shared_clone,
                    &session_id_owned,
                    &group.operation,
                    group.payload.clone(),
                    group.params.clone(),
                    auth_ctx_cloned.as_ref(),
                    timeout_secs,
                )
                .await;

                (group.partition_index, partition_location)
            });
        }

        let mut stage_locations = Vec::<(u32, String)>::new();
        while let Some(joined) = join_set.join_next().await {
            let (partition_index, partition_result) = joined.map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "stage {} partition task panicked: {}",
                    stage_id, e
                ))) as Box<dyn Error + Send + Sync>
            })?;

            let location = partition_result.map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "stage {} partition {} dispatch failed: {}",
                    stage_id, partition_index, e
                ))) as Box<dyn Error + Send + Sync>
            })?;
            stage_locations.push((partition_index, location));
        }

        stage_locations.sort_by_key(|(partition_index, _)| *partition_index);
        let stage_result_location = stage_locations
            .first()
            .map(|(_, location)| location.clone())
            .ok_or_else(|| {
                Box::new(std::io::Error::other(format!(
                    "stage {} completed without partition results",
                    stage_id
                ))) as Box<dyn Error + Send + Sync>
            })?;

        completed.insert(stage_id);
        result_by_stage.insert(stage_id, stage_result_location);
    }

    let final_stage = stage_groups
        .iter()
        .max_by_key(|group| group.stage_id)
        .ok_or_else(|| {
            Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "failed to determine final stage",
            )) as Box<dyn Error + Send + Sync>
        })?;

    result_by_stage
        .remove(&final_stage.stage_id)
        .ok_or_else(|| {
            Box::new(std::io::Error::other(format!(
                "final stage {} has no result location",
                final_stage.stage_id
            ))) as Box<dyn Error + Send + Sync>
        })
}
