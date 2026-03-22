use super::{Task, TaskManager, TaskState, task_to_request};
use crate::services::worker_service_client::worker_service;
use kionas::planner::{
    PhysicalExpr, PhysicalOperator, PredicateComparisonOp, PredicateExpr, PredicateValue,
};
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

#[test]
fn task_to_request_emits_structured_filter_predicate_from_payload() {
    let predicate = PredicateExpr::Comparison {
        column: "id".to_string(),
        op: PredicateComparisonOp::Eq,
        value: PredicateValue::Int(42),
    };
    let operators = vec![PhysicalOperator::Filter {
        predicate: PhysicalExpr::Predicate {
            predicate: predicate.clone(),
        },
    }];
    let payload = serde_json::to_string(&operators).expect("operators payload must serialize");

    let task = Task::new(
        "query-1".to_string(),
        "session-1".to_string(),
        "query".to_string(),
        payload,
        HashMap::new(),
    );

    let req = task_to_request(&task);
    let proto = req.tasks[0]
        .filter_predicate
        .clone()
        .expect("filter predicate must be serialized");

    match proto.variant {
        Some(worker_service::filter_predicate::Variant::Comparison(node)) => {
            assert_eq!(node.column_name, "id");
            assert_eq!(
                node.operator,
                worker_service::ComparisonOperator::Equal as i32
            );
            let value = node.value.expect("comparison value must be present");
            match value.variant {
                Some(worker_service::filter_value::Variant::IntValue(v)) => assert_eq!(v, 42),
                _ => panic!("comparison literal must be int"),
            }
        }
        _ => panic!("expected comparison filter predicate variant"),
    }
}

#[test]
fn task_to_request_omits_filter_predicate_when_payload_has_no_filter() {
    let operators = vec![PhysicalOperator::Materialize];
    let payload = serde_json::to_string(&operators).expect("operators payload must serialize");

    let task = Task::new(
        "query-2".to_string(),
        "session-2".to_string(),
        "query".to_string(),
        payload,
        HashMap::new(),
    );

    let req = task_to_request(&task);
    assert!(req.tasks[0].filter_predicate.is_none());
}

#[test]
fn task_to_request_preserves_all_conjunction_clauses() {
    let predicate = PredicateExpr::Conjunction {
        clauses: vec![
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Ge,
                value: PredicateValue::Int(10),
            },
            PredicateExpr::InList {
                column: "name".to_string(),
                values: PredicateValue::StrList(vec!["alice".to_string(), "bob".to_string()]),
            },
        ],
    };
    let operators = vec![PhysicalOperator::Filter {
        predicate: PhysicalExpr::Predicate { predicate },
    }];
    let payload = serde_json::to_string(&operators).expect("operators payload must serialize");

    let task = Task::new(
        "query-3".to_string(),
        "session-3".to_string(),
        "query".to_string(),
        payload,
        HashMap::new(),
    );

    let req = task_to_request(&task);
    let proto = req.tasks[0]
        .filter_predicate
        .clone()
        .expect("filter predicate must be serialized");

    match proto.variant {
        Some(worker_service::filter_predicate::Variant::Conjunction(node)) => {
            assert_eq!(node.clauses.len(), 2);
        }
        _ => panic!("expected conjunction filter predicate variant"),
    }
}
