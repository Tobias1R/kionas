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
        Vec::new(),
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
        Vec::new(),
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
        Vec::new(),
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

#[test]
fn task_to_request_serializes_disjunction_predicate_variant() {
    let predicate = PredicateExpr::Or {
        clauses: vec![
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Int(1),
            },
            PredicateExpr::Comparison {
                column: "id".to_string(),
                op: PredicateComparisonOp::Eq,
                value: PredicateValue::Int(2),
            },
        ],
    };
    let operators = vec![PhysicalOperator::Filter {
        predicate: PhysicalExpr::Predicate { predicate },
    }];
    let payload = serde_json::to_string(&operators).expect("operators payload must serialize");

    let task = Task::new(
        "query-3b".to_string(),
        "session-3b".to_string(),
        "query".to_string(),
        payload,
        Vec::new(),
        HashMap::new(),
    );

    let req = task_to_request(&task);
    let proto = req.tasks[0]
        .filter_predicate
        .clone()
        .expect("filter predicate must be serialized");

    match proto.variant {
        Some(worker_service::filter_predicate::Variant::Disjunction(node)) => {
            assert_eq!(node.clauses.len(), 2);
        }
        _ => panic!("expected disjunction filter predicate variant"),
    }
}

#[test]
fn task_to_request_serializes_not_predicate_variant() {
    let predicate = PredicateExpr::Not {
        expr: Box::new(PredicateExpr::Comparison {
            column: "id".to_string(),
            op: PredicateComparisonOp::Eq,
            value: PredicateValue::Int(1),
        }),
    };
    let operators = vec![PhysicalOperator::Filter {
        predicate: PhysicalExpr::Predicate { predicate },
    }];
    let payload = serde_json::to_string(&operators).expect("operators payload must serialize");

    let task = Task::new(
        "query-3c".to_string(),
        "session-3c".to_string(),
        "query".to_string(),
        payload,
        Vec::new(),
        HashMap::new(),
    );

    let req = task_to_request(&task);
    let proto = req.tasks[0]
        .filter_predicate
        .clone()
        .expect("filter predicate must be serialized");

    match proto.variant {
        Some(worker_service::filter_predicate::Variant::Not(node)) => {
            let inner = node
                .predicate
                .as_ref()
                .expect("NOT variant must include inner predicate");
            assert!(matches!(
                inner.variant,
                Some(worker_service::filter_predicate::Variant::Comparison(_))
            ));
        }
        _ => panic!("expected NOT filter predicate variant"),
    }
}

#[test]
fn task_to_request_populates_stage_metadata_fields() {
    let mut params = HashMap::new();
    params.insert("database_name".to_string(), "analytics".to_string());

    let mut task = Task::new(
        "query-4".to_string(),
        "session-4".to_string(),
        "query".to_string(),
        "[]".to_string(),
        Vec::new(),
        params,
    );

    task.stage_metadata = Some(crate::tasks::StageTaskMetadata {
        stage_id: 4,
        partition_id: 2,
        partition_count: 8,
        upstream_stage_ids: vec![1, 3],
        upstream_partition_counts: [(1_u32, 4_u32), (3_u32, 2_u32)].into_iter().collect(),
        partition_spec: "\"Hash\"".to_string(),
        query_run_id: Some("run-123".to_string()),
        execution_mode_hint: worker_service::ExecutionModeHint::Distributed as i32,
        output_destinations: vec![],
    });

    let req = task_to_request(&task);
    let stage = &req.tasks[0];
    assert_eq!(stage.stage_id, 4);
    assert_eq!(stage.partition_id, 2);
    assert_eq!(stage.partition_count, 8);
    assert_eq!(stage.upstream_stage_ids, vec![1, 3]);
    assert_eq!(stage.upstream_partition_counts.get(&1), Some(&4));
    assert_eq!(stage.upstream_partition_counts.get(&3), Some(&2));
    assert_eq!(stage.partition_spec, "\"Hash\"");
    assert_eq!(stage.query_run_id, "run-123");
    assert_eq!(
        stage.execution_mode_hint,
        worker_service::ExecutionModeHint::Distributed as i32
    );
    assert_eq!(
        stage.params.get("database_name").map(String::as_str),
        Some("analytics")
    );
    assert!(!stage.params.contains_key("stage_id"));
    assert!(!stage.params.contains_key("partition_index"));
    assert!(!stage.params.contains_key("partition_count"));
    assert!(!stage.params.contains_key("upstream_stage_ids"));
    assert!(!stage.params.contains_key("upstream_partition_counts"));
    assert!(!stage.params.contains_key("partition_spec"));
    assert!(!stage.params.contains_key("query_run_id"));
    assert!(!stage.params.contains_key("execution_mode_hint"));
    assert!(!stage.params.contains_key("output_destinations_json"));
}

#[test]
fn task_to_request_defaults_query_run_id_to_query_id() {
    let task = Task::new(
        "query-stable-run-id".to_string(),
        "session-5".to_string(),
        "query".to_string(),
        "[]".to_string(),
        Vec::new(),
        HashMap::new(),
    );

    let req = task_to_request(&task);
    let stage = &req.tasks[0];
    assert_eq!(stage.query_run_id, "query-stable-run-id");
}

#[test]
fn task_to_request_prefers_typed_stage_metadata() {
    let mut task = Task::new(
        "query-typed-stage".to_string(),
        "session-typed-stage".to_string(),
        "query".to_string(),
        "[]".to_string(),
        Vec::new(),
        HashMap::new(),
    );

    task.stage_metadata = Some(crate::tasks::StageTaskMetadata {
        stage_id: 7,
        partition_id: 3,
        partition_count: 16,
        upstream_stage_ids: vec![2, 4],
        upstream_partition_counts: [(2_u32, 8_u32), (4_u32, 4_u32)].into_iter().collect(),
        partition_spec: "\"Hash\"".to_string(),
        query_run_id: Some("run-typed-1".to_string()),
        execution_mode_hint: worker_service::ExecutionModeHint::Distributed as i32,
        output_destinations: vec![worker_service::OutputDestination {
            downstream_stage_id: 8,
            worker_addresses: vec!["localhost".to_string()],
            partitioning: "\"Single\"".to_string(),
            downstream_partition_count: 1,
        }],
    });

    let req = task_to_request(&task);
    let stage = &req.tasks[0];
    assert_eq!(stage.stage_id, 7);
    assert_eq!(stage.partition_id, 3);
    assert_eq!(stage.partition_count, 16);
    assert_eq!(stage.upstream_stage_ids, vec![2, 4]);
    assert_eq!(stage.upstream_partition_counts.get(&2), Some(&8));
    assert_eq!(stage.upstream_partition_counts.get(&4), Some(&4));
    assert_eq!(stage.partition_spec, "\"Hash\"");
    assert_eq!(stage.query_run_id, "run-typed-1");
    assert_eq!(
        stage.execution_mode_hint,
        worker_service::ExecutionModeHint::Distributed as i32
    );
    assert_eq!(stage.output_destinations.len(), 1);
    assert_eq!(stage.output_destinations[0].downstream_stage_id, 8);
}
