use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify, RwLock};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq)]
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
    pub params: HashMap<String, String>,
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
    pub fn new(
        query_id: String,
        session_id: String,
        operation: String,
        payload: String,
        params: HashMap<String, String>,
    ) -> Self {
        Task {
            id: Uuid::new_v4().to_string(),
            query_id,
            session_id,
            operation,
            payload,
            params,
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
    fn is_terminal_state(state: &TaskState) -> bool {
        matches!(
            state,
            TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled
        )
    }

    fn parse_u32_param(params: &HashMap<String, String>, key: &str) -> Option<u32> {
        params.get(key).and_then(|value| value.parse::<u32>().ok())
    }

    pub fn new() -> Self {
        TaskManager {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            notifiers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn create_task(
        &self,
        query_id: String,
        session_id: String,
        operation: String,
        payload: String,
        params: HashMap<String, String>,
    ) -> String {
        let t = Task::new(query_id, session_id, operation, payload, params);
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
        use tokio::time::{Duration, timeout};
        let start = Utc::now();
        let deadline = Duration::from_secs(timeout_secs);

        // Fast-path: if task already terminal, return immediately
        if let Some(task_arc) = self.get_task(id).await {
            let t = task_arc.lock().await;
            match t.state {
                TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled => {
                    return Some(t.clone());
                }
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
            let elapsed = Utc::now()
                .signed_duration_since(start)
                .to_std()
                .unwrap_or_default();
            if elapsed >= deadline {
                break;
            }
            let remaining = deadline - elapsed;
            // wait for notification with timeout = remaining
            let notified = timeout(remaining, notifier.notified()).await;
            match notified {
                Ok(_) => {
                    if let Some(task_arc) = self.get_task(id).await {
                        let t = task_arc.lock().await;
                        match t.state {
                            TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled => {
                                return Some(t.clone());
                            }
                            _ => continue,
                        }
                    } else {
                        return None;
                    }
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
        if let Some(n) = {
            let nmap = self.notifiers.read().await;
            nmap.get(id).cloned()
        } {
            n.notify_waiters();
        }
    }

    /// Update task fields based on worker update and notify waiters.
    pub async fn update_from_worker(
        &self,
        id: &str,
        new_state: TaskState,
        result_location: Option<String>,
        error: Option<String>,
    ) {
        let _ = self
            .update_from_worker_with_stage_progress(
                id,
                new_state,
                result_location,
                error,
                None,
                None,
                None,
                HashMap::new(),
            )
            .await;
    }

    /// What: Update task fields from worker status while enforcing idempotent stage progress guards.
    ///
    /// Inputs:
    /// - `id`: Task id being updated.
    /// - `new_state`: Desired task state from worker.
    /// - `result_location`: Optional result location update.
    /// - `error`: Optional error message update.
    /// - `stage_id`: Optional stage identifier.
    /// - `partition_count`: Optional total partition count for stage.
    /// - `partition_completed`: Optional completed partitions count for stage.
    /// - `metadata`: Optional stage metadata key-value map.
    ///
    /// Output:
    /// - `true` when update is applied.
    /// - `false` when update is ignored by idempotency guards.
    ///
    /// Details:
    /// - Prevents terminal-state regressions.
    /// - Enforces monotonic `partition_completed` updates.
    /// - Rejects partition progress that exceeds partition_count.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_from_worker_with_stage_progress(
        &self,
        id: &str,
        new_state: TaskState,
        result_location: Option<String>,
        error: Option<String>,
        stage_id: Option<String>,
        partition_count: Option<u32>,
        partition_completed: Option<u32>,
        metadata: HashMap<String, String>,
    ) -> bool {
        let mut applied = false;

        if let Some(task_arc) = self.get_task(id).await {
            let mut t = task_arc.lock().await;

            if Self::is_terminal_state(&t.state) {
                // Ignore duplicated or conflicting post-terminal updates.
                return false;
            }

            let existing_partition_count = Self::parse_u32_param(&t.params, "partition_count");
            let existing_partition_completed =
                Self::parse_u32_param(&t.params, "partition_completed").unwrap_or(0);

            if let Some(new_completed) = partition_completed {
                if new_completed < existing_partition_completed {
                    log::warn!(
                        "Ignoring non-monotonic partition_completed for task {}: {} -> {}",
                        id,
                        existing_partition_completed,
                        new_completed
                    );
                    return false;
                }

                let effective_count = partition_count.or(existing_partition_count);
                if let Some(total) = effective_count {
                    if new_completed > total {
                        log::warn!(
                            "Ignoring invalid partition progress for task {}: completed={} exceeds total={}",
                            id,
                            new_completed,
                            total
                        );
                        return false;
                    }
                }
            }

            t.state = new_state.clone();
            if let Some(loc) = result_location {
                t.result_location = Some(loc);
            }
            if let Some(err) = error {
                t.error = Some(err);
            }

            if let Some(value) = stage_id {
                t.params.insert("stage_id".to_string(), value);
            }
            if let Some(value) = partition_count {
                t.params
                    .insert("partition_count".to_string(), value.to_string());
            }
            if let Some(value) = partition_completed {
                t.params
                    .insert("partition_completed".to_string(), value.to_string());
            }
            for (key, value) in metadata {
                t.params.insert(format!("stage_meta_{}", key), value);
            }

            if let TaskState::Running = t.state {
                t.started_at = Some(Utc::now());
            }
            if let TaskState::Succeeded | TaskState::Failed | TaskState::Cancelled = t.state {
                t.finished_at = Some(Utc::now());
            }

            applied = true;
        }

        if applied {
            if let Some(n) = {
                let nmap = self.notifiers.read().await;
                nmap.get(id).cloned()
            } {
                n.notify_waiters();
            }
        }

        applied
    }
}

#[cfg(test)]
mod tests {
    use super::{TaskManager, TaskState};
    use std::collections::HashMap;

    #[tokio::test]
    async fn ignores_non_monotonic_partition_progress() {
        let tm = TaskManager::new();
        let task_id = tm
            .create_task(
                "q1".to_string(),
                "s1".to_string(),
                "query".to_string(),
                "payload".to_string(),
                HashMap::new(),
            )
            .await;

        let applied = tm
            .update_from_worker_with_stage_progress(
                &task_id,
                TaskState::Running,
                None,
                None,
                Some("1".to_string()),
                Some(3),
                Some(2),
                HashMap::new(),
            )
            .await;
        assert!(applied);

        let ignored = tm
            .update_from_worker_with_stage_progress(
                &task_id,
                TaskState::Running,
                None,
                None,
                Some("1".to_string()),
                Some(3),
                Some(1),
                HashMap::new(),
            )
            .await;
        assert!(!ignored);

        let task = tm
            .get_task(&task_id)
            .await
            .expect("task must exist")
            .lock()
            .await
            .clone();
        assert_eq!(
            task.params.get("partition_completed"),
            Some(&"2".to_string())
        );
    }

    #[tokio::test]
    async fn ignores_updates_after_terminal_state() {
        let tm = TaskManager::new();
        let task_id = tm
            .create_task(
                "q2".to_string(),
                "s2".to_string(),
                "query".to_string(),
                "payload".to_string(),
                HashMap::new(),
            )
            .await;

        let first = tm
            .update_from_worker_with_stage_progress(
                &task_id,
                TaskState::Succeeded,
                Some("loc1".to_string()),
                None,
                None,
                None,
                None,
                HashMap::new(),
            )
            .await;
        assert!(first);

        let second = tm
            .update_from_worker_with_stage_progress(
                &task_id,
                TaskState::Failed,
                Some("loc2".to_string()),
                Some("late failure".to_string()),
                None,
                None,
                None,
                HashMap::new(),
            )
            .await;
        assert!(!second);

        let task = tm
            .get_task(&task_id)
            .await
            .expect("task must exist")
            .lock()
            .await
            .clone();
        assert_eq!(task.state, TaskState::Succeeded);
        assert_eq!(task.result_location, Some("loc1".to_string()));
    }
}

pub fn sample_task_request_from_task(
    _task: &Task,
) -> crate::services::worker_service_client::worker_service::TaskRequest {
    // Placeholder conversion; handlers should build proper TaskRequest
    crate::services::worker_service_client::worker_service::TaskRequest {
        session_id: "".to_string(),
        tasks: vec![],
    }
}

pub fn task_to_request(
    task: &Task,
) -> crate::services::worker_service_client::worker_service::TaskRequest {
    crate::services::worker_service_client::worker_service::TaskRequest {
        session_id: task.session_id.clone(),
        tasks: vec![
            crate::services::worker_service_client::worker_service::Task {
                task_id: task.id.clone(),
                input: task.payload.clone(),
                operation: task.operation.clone(),
                output: String::new(),
                params: task.params.clone(),
            },
        ],
    }
}
