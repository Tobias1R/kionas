use super::{apply_hash_join_pipeline, apply_nested_loop_join_pipeline};
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use kionas::planner::{
    JoinKeyPair, JoinType, LogicalRelation, PhysicalJoinPredicate, PhysicalJoinSpec,
};
use std::sync::Arc;

fn build_join_spec() -> PhysicalJoinSpec {
    PhysicalJoinSpec {
        join_type: JoinType::Inner,
        right_relation: LogicalRelation {
            database: "sales".to_string(),
            schema: "public".to_string(),
            table: "orders".to_string(),
        },
        predicates: Vec::new(),
        keys: vec![JoinKeyPair {
            left: "id".to_string(),
            right: "id".to_string(),
        }],
    }
}

fn left_batch() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 2, 1])) as _),
        (
            "name",
            Arc::new(StringArray::from(vec!["L1", "L2", "L3"])) as _,
        ),
    ])
    .expect("left batch must build")
}

fn right_batch() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 1, 3])) as _),
        (
            "value",
            Arc::new(StringArray::from(vec!["R1", "R2", "R3"])) as _,
        ),
    ])
    .expect("right batch must build")
}

#[test]
fn join_keeps_deterministic_match_order() {
    let spec = build_join_spec();
    let joined = apply_hash_join_pipeline(&[left_batch()], &[right_batch()], &spec)
        .expect("join must succeed");

    assert_eq!(joined.len(), 1);
    let batch = &joined[0];
    assert_eq!(batch.num_rows(), 4);

    let left_names = batch
        .column_by_name("name")
        .expect("left name column must exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column must be utf8");
    let right_values = batch
        .column_by_name("value")
        .expect("right value column must exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("value column must be utf8");

    let observed_pairs: Vec<(String, String)> = (0..batch.num_rows())
        .map(|idx| {
            (
                left_names.value(idx).to_string(),
                right_values.value(idx).to_string(),
            )
        })
        .collect();

    assert_eq!(
        observed_pairs,
        vec![
            ("L1".to_string(), "R1".to_string()),
            ("L1".to_string(), "R2".to_string()),
            ("L3".to_string(), "R1".to_string()),
            ("L3".to_string(), "R2".to_string()),
        ]
    );
}

#[test]
fn join_renames_duplicate_right_columns() {
    let spec = build_join_spec();
    let joined = apply_hash_join_pipeline(&[left_batch()], &[right_batch()], &spec)
        .expect("join must succeed");
    let schema = joined[0].schema();

    assert!(schema.column_with_name("id").is_some());
    assert!(schema.column_with_name("orders_id").is_some());
    assert!(schema.column_with_name("value").is_some());
}

#[test]
fn join_with_no_matches_returns_empty_batch_with_expected_schema() {
    let spec = build_join_spec();
    let left = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![10, 11])) as _),
        ("name", Arc::new(StringArray::from(vec!["X", "Y"])) as _),
    ])
    .expect("left batch must build");

    let joined =
        apply_hash_join_pipeline(&[left], &[right_batch()], &spec).expect("join must succeed");

    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0].num_rows(), 0);

    let schema = joined[0].schema();
    assert!(schema.column_with_name("id").is_some());
    assert!(schema.column_with_name("name").is_some());
    assert!(schema.column_with_name("orders_id").is_some());
    assert!(schema.column_with_name("value").is_some());
}

#[test]
fn join_rejects_spec_with_empty_keys() {
    let mut spec = build_join_spec();
    spec.keys.clear();

    let err = apply_hash_join_pipeline(&[left_batch()], &[right_batch()], &spec)
        .expect_err("join must reject empty key set");

    assert!(err.contains("requires at least one join key"));
}

#[test]
fn join_rejects_spec_with_empty_right_relation() {
    let mut spec = build_join_spec();
    spec.right_relation.table = "   ".to_string();

    let err = apply_hash_join_pipeline(&[left_batch()], &[right_batch()], &spec)
        .expect_err("join must reject empty right relation metadata");

    assert!(err.contains("right relation must be non-empty"));
}

#[test]
fn nested_loop_cross_join_without_predicates() {
    let mut spec = build_join_spec();
    spec.keys.clear();

    let joined = apply_nested_loop_join_pipeline(&[left_batch()], &[right_batch()], &spec)
        .expect("nested-loop cross join must succeed");

    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0].num_rows(), 9);
}

#[test]
fn nested_loop_applies_theta_predicate() {
    let mut spec = build_join_spec();
    spec.keys.clear();
    spec.predicates = vec![PhysicalJoinPredicate::Theta {
        left: "id".to_string(),
        op: "<".to_string(),
        right: "id".to_string(),
    }];

    let joined = apply_nested_loop_join_pipeline(&[left_batch()], &[right_batch()], &spec)
        .expect("nested-loop theta join must succeed");

    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0].num_rows(), 2);

    let left_names = joined[0]
        .column_by_name("name")
        .expect("left name column must exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("name column must be utf8");
    let right_values = joined[0]
        .column_by_name("value")
        .expect("right value column must exist")
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("value column must be utf8");

    let observed_pairs: Vec<(String, String)> = (0..joined[0].num_rows())
        .map(|idx| {
            (
                left_names.value(idx).to_string(),
                right_values.value(idx).to_string(),
            )
        })
        .collect();

    assert_eq!(
        observed_pairs,
        vec![
            ("L1".to_string(), "R3".to_string()),
            ("L3".to_string(), "R3".to_string()),
        ]
    );
}
