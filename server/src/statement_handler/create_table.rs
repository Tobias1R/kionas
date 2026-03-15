use kionas::parser::datafusion_sql::sqlparser::ast::ObjectName;
use crate::services::worker_service_client::worker_service::{TaskRequest, Task};
use crate::workers::{acquire_channel_with_heartbeat, send_task_to_worker};
use crate::tasks::task_to_request;
use std::collections::HashMap;
use crate::warehouse::state::SharedData;
use std::error::Error;

use crate::statement_handler::helpers;

pub async fn handle(table_name: &ObjectName, session_id: &str, shared_data: &SharedData) -> Result<String, Box<dyn Error>> {
    log::info!("create_table::handle invoked for session {} table={:?}", session_id, table_name);

    // Resolve session and worker key
    let (session, key) = helpers::resolve_session_and_key(shared_data, session_id).await?;

    // Schedule task
    let task_id = helpers::build_task_and_schedule(shared_data, "".to_string(), session_id, "create_table", table_name.to_string()).await?;

    // Acquire pooled connection
    let conn = helpers::acquire_pooled_conn(shared_data, &key, &session.get_warehouse(), 10).await?;

    // Convert Task -> TaskRequest
    let task_arc = match shared_data.lock().await.task_manager.get_task(&task_id).await {
        Some(a) => a,
        None => return Err(Box::new(std::io::Error::new(std::io::ErrorKind::NotFound, "task not found after creation"))),
    };
    let task_obj = { let t = task_arc.lock().await; t.clone() };
    let req = task_to_request(&task_obj);

    // Mark running and dispatch
    shared_data.lock().await.task_manager.set_state(&task_id, crate::tasks::TaskState::Running).await;
    log::info!("Dispatching TaskRequest to worker for session {} (task_id={})", session_id, task_id);
    let resp = helpers::dispatch_task_and_record(shared_data, conn, req, &task_id, 30).await?;
    log::info!("Received response from worker for session {} (task_id={})", session_id, task_id);

    if resp.status == "ok" {
        Ok(format!("Table created successfully: {}", resp.result_location))
    } else {
        Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, resp.error)))
    }
}
