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
