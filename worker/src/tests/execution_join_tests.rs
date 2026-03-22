use super::apply_hash_join_pipeline;
use arrow::array::{Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use kionas::planner::{JoinKeyPair, JoinType, LogicalRelation, PhysicalJoinSpec};
use std::sync::Arc;

fn build_join_spec() -> PhysicalJoinSpec {
    PhysicalJoinSpec {
        join_type: JoinType::Inner,
        right_relation: LogicalRelation {
            database: "sales".to_string(),
            schema: "public".to_string(),
            table: "orders".to_string(),
        },
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
