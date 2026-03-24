use crate::warehouse::state::SharedData;
use std::collections::HashMap;
use std::error::Error;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::workers::PooledConn;
use uuid::Uuid;

const OUTCOME_PREFIX: &str = "RESULT";

fn format_outcome(category: &str, code: &str, message: &str) -> String {
    format!("{}|{}|{}|{}", OUTCOME_PREFIX, category, code, message)
}

fn parse_u32_param(params: &HashMap<String, String>, key: &str) -> Option<u32> {
    params.get(key).and_then(|v| v.parse::<u32>().ok())
}

/// What: Format the stage-dispatch diagnostics event for EP-1 runtime observability.
///
/// Inputs:
/// - `query_id`: Query correlation id.
/// - `stage_id`: Distributed stage id.
/// - `task_id`: Concrete scheduled task id.
/// - `partition_count`: Stage partition fan-out.
///
/// Output:
/// - Stable event line suitable for structured log parsing.
///
/// Details:
/// - Event name is fixed to `execution.stage_dispatch_boundary`.
pub(crate) fn format_stage_dispatch_boundary_event(
    query_id: &str,
    stage_id: u32,
    task_id: &str,
    partition_count: u32,
) -> String {
    format!(
        "event=execution.stage_dispatch_boundary query_id={} stage_id={} task_id={} category=execution origin=server_dispatch partition_count={}",
        query_id, stage_id, task_id, partition_count
    )
}

fn validate_query_dispatch_context(
    operation: &str,
    params: &HashMap<String, String>,
    stage_metadata: Option<&crate::tasks::StageTaskMetadata>,
    auth_ctx: Option<&DispatchAuthContext>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if !operation.eq_ignore_ascii_case("query") {
        return Ok(());
    }

    let query_id = auth_ctx
        .map(|ctx| ctx.query_id.trim())
        .filter(|v| !v.is_empty());

    if query_id.is_none() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format_outcome(
                "INFRA",
                "INFRA_WORKER_EXECUTION_CONTEXT_MISSING",
                "dispatch checkpoint failed: query_id is missing for query task",
            ),
        )));
    }

    if let Some(stage_metadata) = stage_metadata {
        if stage_metadata.partition_count == 0
            || stage_metadata.partition_id >= stage_metadata.partition_count
        {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format_outcome(
                    "EXECUTION",
                    "EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING",
                    "dispatch checkpoint failed: partition_index is missing or invalid",
                ),
            )));
        }
    } else if parse_u32_param(params, "stage_id").is_some()
        && parse_u32_param(params, "partition_index").is_none()
    {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format_outcome(
                "EXECUTION",
                "EXECUTION_EXCHANGE_PARTITION_CONTEXT_MISSING",
                "dispatch checkpoint failed: partition_index is missing or invalid",
            ),
        )));
    }

    Ok(())
}

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
    stage_metadata: Option<crate::tasks::StageTaskMetadata>,
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
    if let Some(metadata) = stage_metadata
        && let Some(task_arc) = task_manager.get_task(&task_id).await
    {
        let mut task = task_arc.lock().await;
        task.stage_metadata = Some(metadata);
    }
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
        None,
        timeout_secs,
    )
    .await
}

/// Same as `run_task_for_input`, but allows passing operation-specific task params.
#[allow(clippy::too_many_arguments)]
pub async fn run_task_for_input_with_params(
    shared_data: &SharedData,
    session_id: &str,
    operation: &str,
    payload: String,
    params: std::collections::HashMap<String, String>,
    stage_metadata: Option<crate::tasks::StageTaskMetadata>,
    auth_ctx: Option<&DispatchAuthContext>,
    timeout_secs: u64,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    validate_query_dispatch_context(operation, &params, stage_metadata.as_ref(), auth_ctx)?;

    // Resolve session and worker key
    let (session, key) = resolve_session_and_key(shared_data, session_id).await?;

    // Create a query id and schedule task
    let query_id = auth_ctx
        .map(|ctx| ctx.query_id.clone())
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| Uuid::new_v4().to_string());
    let mut params = params;
    let diagnostics_stage_id = stage_metadata
        .as_ref()
        .map(|value| value.stage_id)
        .or_else(|| parse_u32_param(&params, "stage_id"));
    let diagnostics_partition_count = stage_metadata
        .as_ref()
        .map(|value| value.partition_count)
        .or_else(|| parse_u32_param(&params, "partition_count"))
        .filter(|v| *v > 0)
        .unwrap_or(1);
    if operation.eq_ignore_ascii_case("query")
        && let Some(ctx) = auth_ctx
    {
        params.insert("__query_id".to_string(), ctx.query_id.clone());
    }
    let query_id_for_event = query_id.clone();
    let task_id = build_task_and_schedule(
        shared_data,
        query_id,
        session_id,
        operation,
        payload,
        params,
        stage_metadata,
    )
    .await?;

    if operation.eq_ignore_ascii_case("query")
        && let Some(stage_id) = diagnostics_stage_id
    {
        log::info!(
            "{}",
            format_stage_dispatch_boundary_event(
                &query_id_for_event,
                stage_id,
                &task_id,
                diagnostics_partition_count,
            )
        );
    }

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
    let shared_clone = shared_data.clone();
    let session_id_owned = session_id.to_string();

    let execute_partition: StagePartitionExecutor = Arc::new(
        move |group, auth_ctx_cloned, timeout_secs| {
            let shared_clone = shared_clone.clone();
            let session_id_owned = session_id_owned.clone();
            Box::pin(async move {
                run_task_for_input_with_params(
                &shared_clone,
                &session_id_owned,
                &group.operation,
                group.payload.clone(),
                group.params.clone(),
                Some(crate::tasks::StageTaskMetadata {
                    stage_id: group.stage_id,
                    partition_id: group.partition_index,
                    partition_count: group.partition_count,
                    upstream_stage_ids: group.upstream_stage_ids.clone(),
                    upstream_partition_counts: group.upstream_partition_counts.clone(),
                    partition_spec: serde_json::to_string(&group.partition_spec)
                        .unwrap_or_else(|_| "\"Single\"".to_string()),
                    query_run_id: None,
                    execution_mode_hint: match group.execution_mode_hint {
                        crate::statement_handler::shared::distributed_dag::ExecutionModeHint::LocalOnly => {
                            crate::services::worker_service_client::worker_service::ExecutionModeHint::LocalOnly
                                as i32
                        }
                        crate::statement_handler::shared::distributed_dag::ExecutionModeHint::Distributed => {
                            crate::services::worker_service_client::worker_service::ExecutionModeHint::Distributed
                                as i32
                        }
                    },
                    output_destinations: group
                        .output_destinations
                        .iter()
                        .map(|destination| {
                            crate::services::worker_service_client::worker_service::OutputDestination {
                                downstream_stage_id: destination.downstream_stage_id,
                                worker_addresses: destination.worker_addresses.clone(),
                                partitioning: serde_json::to_string(&destination.partitioning)
                                    .unwrap_or_else(|_| "\"Single\"".to_string()),
                                downstream_partition_count: destination.downstream_partition_count,
                            }
                        })
                        .collect::<Vec<_>>(),
                }),
                auth_ctx_cloned.as_ref(),
                timeout_secs,
            )
            .await
            })
        },
    );

    run_stage_groups_with_partition_executor(
        stage_groups,
        auth_ctx,
        timeout_secs,
        execute_partition,
    )
    .await
}

type StagePartitionFuture =
    Pin<Box<dyn Future<Output = Result<String, Box<dyn Error + Send + Sync>>> + Send>>;

type StagePartitionExecutor = Arc<
    dyn Fn(
            crate::statement_handler::shared::distributed_dag::StageTaskGroup,
            Option<DispatchAuthContext>,
            u64,
        ) -> StagePartitionFuture
        + Send
        + Sync,
>;

/// What: Execute distributed stage groups through an injectable partition executor.
///
/// Inputs:
/// - `stage_groups`: Compiled stage groups with dependency metadata.
/// - `auth_ctx`: Optional dispatch auth context used by query-mode execution checks.
/// - `timeout_secs`: Per-partition execution timeout.
/// - `execute_partition`: Async executor for one stage partition task.
///
/// Output:
/// - Final stage result location when stage execution succeeds.
/// - Error when no schedulable stage remains or a partition execution fails.
///
/// Details:
/// - Used by production dispatch and in-memory tests to validate stage wave semantics.
async fn run_stage_groups_with_partition_executor(
    stage_groups: &[crate::statement_handler::shared::distributed_dag::StageTaskGroup],
    auth_ctx: Option<&DispatchAuthContext>,
    timeout_secs: u64,
    execute_partition: StagePartitionExecutor,
) -> Result<String, Box<dyn Error + Send + Sync>> {
    if stage_groups.is_empty() {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "stage scheduler received no stage groups",
        )));
    }

    if let Some(ctx) = auth_ctx
        && ctx.query_id.trim().is_empty()
    {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "dispatch checkpoint failed: query_id is missing for staged query dispatch",
        )));
    }

    let mut groups_by_stage = HashMap::<
        u32,
        Vec<crate::statement_handler::shared::distributed_dag::StageTaskGroup>,
    >::new();
    for group in stage_groups {
        groups_by_stage
            .entry(group.stage_id)
            .or_default()
            .push(group.clone());
    }

    for groups in groups_by_stage.values_mut() {
        groups.sort_by_key(|group| group.partition_index);
    }

    let mut result_by_stage = HashMap::<u32, String>::new();
    let stage_waves =
        crate::statement_handler::shared::distributed_dag::build_stage_execution_waves(
            stage_groups,
        )
        .map_err(|e| {
            Box::new(std::io::Error::new(std::io::ErrorKind::InvalidInput, e))
                as Box<dyn Error + Send + Sync>
        })?;

    for wave in stage_waves {
        let mut wave_join_set = tokio::task::JoinSet::new();

        for stage_id in wave {
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
            let auth_ctx_cloned = auth_ctx.cloned();
            let execute_partition = execute_partition.clone();

            wave_join_set.spawn(async move {
                let mut join_set = tokio::task::JoinSet::new();
                for group in stage_partitions {
                    let auth_ctx_cloned = auth_ctx_cloned.clone();
                    let execute_partition = execute_partition.clone();
                    join_set.spawn(async move {
                        let partition_location =
                            execute_partition(group.clone(), auth_ctx_cloned.clone(), timeout_secs)
                                .await;

                        (group.partition_index, partition_location)
                    });
                }

                let mut stage_locations = Vec::<(u32, String)>::new();
                while let Some(joined) = join_set.join_next().await {
                    let (partition_index, partition_result) = joined.map_err(|e| {
                        std::io::Error::other(format!(
                            "stage {} partition task panicked: {}",
                            stage_id, e
                        ))
                    })?;

                    let location = partition_result.map_err(|e| {
                        std::io::Error::other(format!(
                            "stage {} partition {} dispatch failed: {}",
                            stage_id, partition_index, e
                        ))
                    })?;
                    stage_locations.push((partition_index, location));
                }

                stage_locations.sort_by_key(|(partition_index, _)| *partition_index);
                let stage_result_location = stage_locations
                    .first()
                    .map(|(_, location)| location.clone())
                    .ok_or_else(|| {
                        std::io::Error::other(format!(
                            "stage {} completed without partition results",
                            stage_id
                        ))
                    })?;

                Ok::<(u32, String), std::io::Error>((stage_id, stage_result_location))
            });
        }

        while let Some(joined) = wave_join_set.join_next().await {
            let stage_result = joined.map_err(|e| {
                Box::new(std::io::Error::other(format!(
                    "stage wave task panicked: {}",
                    e
                ))) as Box<dyn Error + Send + Sync>
            })?;
            let (stage_id, stage_result_location) =
                stage_result.map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)?;
            result_by_stage.insert(stage_id, stage_result_location);
        }
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

#[cfg(test)]
#[path = "../../tests/statement_handler_shared_helpers_tests.rs"]
mod tests;
