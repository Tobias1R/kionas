use crate::execution::router::{
    route_batch, route_batch_from_output_partitioning, route_batch_hash, route_batch_range,
    route_batch_roundrobin, route_batch_single,
};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use kionas::planner::PartitionSpec;
use serde_json::json;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn hash_input_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("city", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![
                1_i64,
                2_i64,
                3_i64,
                i64::MIN,
                i64::MAX,
                7_i64,
                8_i64,
                9_i64,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec![
                "oslo",
                "helsinki",
                "stockholm",
                "reykjavik",
                "copenhagen",
                "oslo",
                "oslo",
                "helsinki",
            ])) as ArrayRef,
        ],
    )
    .expect("hash input batch must build")
}

fn int_key_batch(values: Vec<i64>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
    RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values)) as ArrayRef])
        .expect("batch must build")
}

fn collect_ids(batch: &RecordBatch, column: &str) -> Vec<i64> {
    let index = batch
        .schema()
        .index_of(column)
        .expect("column should exist in test schema");
    let ids = batch
        .column(index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("column should be Int64");
    (0..ids.len()).map(|idx| ids.value(idx)).collect::<Vec<_>>()
}

#[test]
fn hash_routing_preserves_all_rows_without_duplicates() {
    let batch = hash_input_batch();
    let routed = route_batch_hash(&batch, &["id".to_string(), "city".to_string()], 4)
        .expect("hash routing should succeed");

    assert_eq!(routed.len(), 4);

    let total_rows = routed.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(total_rows, batch.num_rows());

    let mut seen = HashSet::<(i64, String)>::new();
    for part in &routed {
        let ids = collect_ids(part, "id");
        let city_idx = part.schema().index_of("city").expect("city should exist");
        let cities = part
            .column(city_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("city should be string");

        for (row, id) in ids.iter().enumerate() {
            let city = cities.value(row).to_string();
            assert!(seen.insert((*id, city)), "row should appear only once");
        }
    }
}

#[test]
fn hash_routing_empty_batch_returns_partition_shape() {
    let batch = int_key_batch(Vec::new());
    let routed = route_batch_hash(&batch, &["k".to_string()], 3)
        .expect("hash routing should support empty batches");

    assert_eq!(routed.len(), 3);
    assert!(routed.iter().all(|part| part.num_rows() == 0));
}

#[test]
fn roundrobin_routing_cycles_and_persists_state() {
    let counter = Arc::new(AtomicUsize::new(0));
    let first = route_batch_roundrobin(&int_key_batch((0..6).collect::<Vec<_>>()), 3, &counter)
        .expect("round-robin routing should succeed");
    let second = route_batch_roundrobin(&int_key_batch((6..10).collect::<Vec<_>>()), 3, &counter)
        .expect("round-robin routing should continue counter state");

    assert_eq!(collect_ids(&first[0], "k"), vec![0, 3]);
    assert_eq!(collect_ids(&first[1], "k"), vec![1, 4]);
    assert_eq!(collect_ids(&first[2], "k"), vec![2, 5]);

    assert_eq!(collect_ids(&second[0], "k"), vec![6, 9]);
    assert_eq!(collect_ids(&second[1], "k"), vec![7]);
    assert_eq!(collect_ids(&second[2], "k"), vec![8]);
    assert_eq!(counter.load(Ordering::SeqCst), 10);
}

#[test]
fn range_routing_clamps_edges_and_assigns_middle_bounds() {
    let batch = int_key_batch(vec![50, 150, 250, 350]);
    let bounds = vec![json!(100), json!(200), json!(300)];

    let routed = route_batch_range(&batch, "k", &bounds, 4).expect("range routing should succeed");

    assert_eq!(collect_ids(&routed[0], "k"), vec![50]);
    assert_eq!(collect_ids(&routed[1], "k"), vec![150]);
    assert_eq!(collect_ids(&routed[2], "k"), vec![250]);
    assert_eq!(collect_ids(&routed[3], "k"), vec![350]);
}

#[test]
fn single_partition_routes_all_rows_to_first_partition() {
    let batch = int_key_batch(vec![10, 11, 12]);
    let routed = route_batch_single(&batch, 3).expect("single routing should succeed");

    assert_eq!(collect_ids(&routed[0], "k"), vec![10, 11, 12]);
    assert_eq!(routed[1].num_rows(), 0);
    assert_eq!(routed[2].num_rows(), 0);
}

#[test]
fn dispatcher_routes_and_propagates_errors() {
    let batch = int_key_batch(vec![1, 2, 3]);
    let hash_spec = PartitionSpec::Hash {
        keys: vec!["k".to_string()],
    };
    let routed = route_batch(&batch, &hash_spec, 2, None).expect("dispatcher should route hash");
    assert_eq!(routed.len(), 2);

    let bad_hash = PartitionSpec::Hash {
        keys: vec!["missing".to_string()],
    };
    let error = route_batch(&batch, &bad_hash, 2, None)
        .expect_err("dispatcher should propagate invalid key errors");
    assert!(error.contains("is not present in output schema"));
}

#[test]
fn output_partitioning_parser_supports_round_robin_and_range_payloads() {
    let rr_counter = Arc::new(AtomicUsize::new(0));
    let rr_batch = int_key_batch((0..5).collect::<Vec<_>>());
    let rr_routed =
        route_batch_from_output_partitioning(&rr_batch, "round_robin_batch", 2, Some(&rr_counter))
            .expect("round-robin string strategy should route");
    assert_eq!(collect_ids(&rr_routed[0], "k"), vec![0, 2, 4]);
    assert_eq!(collect_ids(&rr_routed[1], "k"), vec![1, 3]);

    let range_batch = int_key_batch(vec![10, 110, 210]);
    let range_payload = serde_json::json!({
        "Range": {
            "column": "k",
            "bounds": [100, 200]
        }
    })
    .to_string();
    let range_routed =
        route_batch_from_output_partitioning(&range_batch, &range_payload, 3, Some(&rr_counter))
            .expect("range payload strategy should route");

    assert_eq!(collect_ids(&range_routed[0], "k"), vec![10]);
    assert_eq!(collect_ids(&range_routed[1], "k"), vec![110]);
    assert_eq!(collect_ids(&range_routed[2], "k"), vec![210]);
}
