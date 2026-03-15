use std::error::Error;
use crate::warehouse::state::SharedData;

use crate::workers::PooledConn;

/// Resolve session and worker key for a session id
pub async fn resolve_session_and_key(shared_data: &SharedData, session_id: &str) -> Result<(crate::session::Session, String), Box<dyn Error>> {
    // Get session
    let session_opt = {
        let state = shared_data.lock().await;
        state.session_manager.get_session(session_id.to_string()).await
    };
    let session = match session_opt {
        Some(s) => s,
        None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "session not found"))),
    };

    // Resolve worker key
    let key_opt = {
        let state = shared_data.lock().await;
        state.resolve_worker_key(session_id).await
    };
    let key = match key_opt {
        Some(k) => k,
        None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "no matching worker for session"))),
    };

    Ok((session, key))
}

/// Create a Task in TaskManager and mark it Scheduled.
pub async fn build_task_and_schedule(shared_data: &SharedData, query_id: String, session_id: &str, operation: &str, payload: String) -> Result<String, Box<dyn Error>> {
    let task_manager = {
        let state = shared_data.lock().await;
        state.task_manager.clone()
    };
    let task_id = task_manager.create_task(query_id, session_id.to_string(), operation.to_string(), payload).await;
    task_manager.set_state(&task_id, crate::tasks::TaskState::Scheduled).await;
    Ok(task_id)
}

/// Acquire a pooled connection for a worker key and validate with heartbeat
pub async fn acquire_pooled_conn(shared_data: &SharedData, key: &str, warehouse_name: &str, timeout_secs: u64) -> Result<PooledConn, Box<dyn Error>> {
    // Get or create pool
    let pool = {
        let state = shared_data.lock().await;
        state.get_or_create_pool_for_key(key).await.map_err(|e| Box::new(e) as Box<dyn Error>)?
    };
    // Acquire pooled connection with heartbeat
    let conn = crate::workers::acquire_channel_with_heartbeat(&pool, warehouse_name, timeout_secs).await?;
    Ok(conn)
}

/// Dispatch the TaskRequest to worker and record the result back to TaskManager
pub async fn dispatch_task_and_record(shared_data: &SharedData, conn: PooledConn, req: crate::services::worker_service_client::worker_service::TaskRequest, task_id: &str, timeout_secs: u64) -> Result<crate::services::worker_service_client::worker_service::TaskResponse, Box<dyn Error>> {
    let resp = crate::workers::send_task_to_worker(conn, req, timeout_secs).await?;

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
