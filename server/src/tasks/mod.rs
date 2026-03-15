use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, Mutex, Notify};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub enum TaskState {
    Pending,
    Scheduled,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: String,
    pub query_id: String,
    pub session_id: String,
    pub operation: String,
    pub payload: String,
    pub state: TaskState,
    pub attempts: u32,
    pub max_retries: u32,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub result_location: Option<String>,
    pub error: Option<String>,
}

impl Task {
    pub fn new(query_id: String, session_id: String, operation: String, payload: String) -> Self {
        Task {
            id: Uuid::new_v4().to_string(),
            query_id,
            session_id,
            operation,
            payload,
            state: TaskState::Pending,
            attempts: 0,
            max_retries: 3,
            created_at: Utc::now(),
            started_at: None,
            finished_at: None,
            result_location: None,
            error: None,
        }
    }
}

pub type TaskMap = Arc<RwLock<HashMap<String, Arc<Mutex<Task>>>>>;
pub type TaskNotifiers = Arc<RwLock<HashMap<String, Arc<Notify>>>>;

#[derive(Clone, Debug)]
pub struct TaskManager {
    tasks: TaskMap,
    notifiers: TaskNotifiers,
}

impl TaskManager {
    pub fn new() -> Self {
        TaskManager { tasks: Arc::new(RwLock::new(HashMap::new())), notifiers: Arc::new(RwLock::new(HashMap::new())) }
    }

    pub async fn create_task(&self, query_id: String, session_id: String, operation: String, payload: String) -> String {
        let t = Task::new(query_id, session_id, operation, payload);
        let id = t.id.clone();
        let at = Arc::new(Mutex::new(t));
        let mut map = self.tasks.write().await;
        map.insert(id.clone(), at);
        // create and register a notifier for this task so waiters can be notified
        let notifier = Arc::new(Notify::new());
        let mut not_map = self.notifiers.write().await;
        not_map.insert(id.clone(), notifier);
        id
    }

    pub async fn get_task(&self, id: &str) -> Option<Arc<Mutex<Task>>> {
        let map = self.tasks.read().await;
        map.get(id).cloned()
    }

    /// Wait for task to reach a terminal state or timeout.
    pub async fn wait_for_completion(&self, id: &str, timeout_secs: u64) -> Option<Task> {
        use tokio::time::{timeout, Duration};
        let start = Utc::now();
        let deadline = Duration::from_secs(timeout_secs);

        // Fast-path: if task already terminal, return immediately
        if let Some(task_arc) = self.get_task(id).await {
            let t = task_arc.lock().await;
            match t.state {
                TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled => return Some(t.clone()),
                _ => {}
            }
        } else {
            return None;
        }

        // Get notifier for this task
        let notifier_opt = {
            let nmap = self.notifiers.read().await;
            nmap.get(id).cloned()
        };

        if notifier_opt.is_none() {
            // No notifier exists for this task — this should not happen because
            // notifiers are created when tasks are created. Return None to
            // indicate we cannot wait for completion.
            log::warn!("No notifier found for task {} in wait_for_completion", id);
            return None;
        }

        let notifier = notifier_opt.unwrap();
        // Wait until notifier signals or timeout
        loop {
            let elapsed = Utc::now().signed_duration_since(start).to_std().unwrap_or_default();
            if elapsed >= deadline { break; }
            let remaining = deadline - elapsed;
            // wait for notification with timeout = remaining
            let notified = timeout(remaining, notifier.notified()).await;
            match notified {
                Ok(_) => {
                    if let Some(task_arc) = self.get_task(id).await {
                        let t = task_arc.lock().await;
                        match t.state {
                            TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled => return Some(t.clone()),
                            _ => continue,
                        }
                    } else { return None; }
                }
                Err(_) => break, // timeout
            }
        }

        // final read
        if let Some(task_arc) = self.get_task(id).await {
            let t = task_arc.lock().await;
            Some(t.clone())
        } else {
            None
        }
    }

    pub async fn set_state(&self, id: &str, new_state: TaskState) {
        if let Some(task_arc) = self.get_task(id).await {
            let mut t = task_arc.lock().await;
            t.state = new_state.clone();
            if let TaskState::Running = t.state {
                t.started_at = Some(Utc::now());
            }
            if let TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled = t.state {
                t.finished_at = Some(Utc::now());
            }
        }
        // notify waiters if any
        if let Some(n) = { let nmap = self.notifiers.read().await; nmap.get(id).cloned() } {
            n.notify_waiters();
        }
    }

    /// Update task fields based on worker update and notify waiters.
    pub async fn update_from_worker(&self, id: &str, new_state: TaskState, result_location: Option<String>, error: Option<String>) {
        if let Some(task_arc) = self.get_task(id).await {
            let mut t = task_arc.lock().await;
            t.state = new_state.clone();
            if let Some(loc) = result_location {
                t.result_location = Some(loc);
            }
            if let Some(err) = error {
                t.error = Some(err);
            }
            if let TaskState::Running = t.state {
                t.started_at = Some(Utc::now());
            }
            if let TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled = t.state {
                t.finished_at = Some(Utc::now());
            }
        }
        if let Some(n) = { let nmap = self.notifiers.read().await; nmap.get(id).cloned() } {
            n.notify_waiters();
        }
    }
}

pub fn sample_task_request_from_task(_task: &Task) -> crate::services::worker_service_client::worker_service::TaskRequest {
    // Placeholder conversion; handlers should build proper TaskRequest
    crate::services::worker_service_client::worker_service::TaskRequest {
        session_id: "".to_string(),
        tasks: vec![],
    }
}

pub fn task_to_request(task: &Task) -> crate::services::worker_service_client::worker_service::TaskRequest {
    let mut params = std::collections::HashMap::new();
    crate::services::worker_service_client::worker_service::TaskRequest {
        session_id: task.session_id.clone(),
        tasks: vec![crate::services::worker_service_client::worker_service::Task {
            task_id: task.id.clone(),
            input: task.payload.clone(),
            operation: task.operation.clone(),
            output: String::new(),
            params,
        }],
    }
}
