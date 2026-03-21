use super::{
    apply_filter_pipeline, apply_limit_pipeline, apply_projection_pipeline,
    parse_projection_identifier, split_case_insensitive,
};
use crate::execution::artifacts::{QueryArtifactMetadata, encode_result_metadata};
use crate::execution::pipeline::{
    decode_parquet_batches, parse_partition_index_from_exchange_key, partition_input_batches,
    source_table_staging_prefix, validate_upstream_exchange_partition_set,
};
use crate::execution::planner::extract_runtime_plan;
use arrow::array::{ArrayRef, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::io::Cursor;
use std::sync::Arc;

fn batch_two_rows() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    )
    .expect("test batch must build")
}

fn batch_temporal_two_rows() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "occurred_at",
        DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
        false,
    )]));

    RecordBatch::try_new(
        schema,
        vec![Arc::new(TimestampMillisecondArray::from(vec![
            1_704_067_200_000_i64,
            1_704_153_600_000_i64,
        ])) as ArrayRef],
    )
    .expect("temporal test batch must build")
}

#[test]
fn decodes_written_parquet() {
    let batch = batch_two_rows();
    let props = WriterProperties::builder().build();
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let mut writer =
        ArrowWriter::try_new(&mut cursor, batch.schema(), Some(props)).expect("writer must build");
    writer.write(&batch).expect("write must succeed");
    writer.close().expect("close must succeed");

    let decoded = decode_parquet_batches(cursor.into_inner()).expect("decode must succeed");
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].num_rows(), 2);
}

#[test]
fn applies_simple_int_filter() {
    let filtered =
        apply_filter_pipeline(&[batch_two_rows()], "id > 1", None).expect("filter must run");
    assert_eq!(filtered[0].num_rows(), 1);
}

#[test]
fn applies_timestamp_filter_with_quoted_literal() {
    let filtered = apply_filter_pipeline(
        &[batch_temporal_two_rows()],
        "occurred_at >= '2024-01-02T00:00:00Z'",
        None,
    )
    .expect("timestamp filter must run");

    assert_eq!(filtered[0].num_rows(), 1);
}

#[test]
fn rejects_unquoted_temporal_filter_literal() {
    let err = apply_filter_pipeline(
        &[batch_temporal_two_rows()],
        "occurred_at >= 1704067200000",
        None,
    )
    .expect_err("unquoted temporal literal must fail");
    assert!(err.contains("unsupported temporal filter literal"));
}

#[test]
fn applies_projection_raw_identifier() {
    let projected = apply_projection_pipeline(
        &[batch_two_rows()],
        &[kionas::planner::PhysicalExpr::Raw {
            sql: "name".to_string(),
        }],
    )
    .expect("projection must run");

    assert_eq!(projected[0].num_columns(), 1);
    assert_eq!(projected[0].schema().field(0).name(), "name");
}

#[test]
fn builds_bucket_relative_source_prefix() {
    let ns = crate::execution::query::QueryNamespace {
        database: "db1".to_string(),
        schema: "s1".to_string(),
        table: "t1".to_string(),
    };

    let prefix = source_table_staging_prefix(&ns);
    assert_eq!(prefix, "databases/db1/schemas/s1/tables/t1/staging/");
}

#[test]
fn splits_case_insensitive_and_tokens() {
    let parts = split_case_insensitive("id > 1 AnD name = 'a'", "AND");
    assert_eq!(parts, vec!["id > 1", "name = 'a'"]);
}

#[test]
fn encodes_result_metadata_row_count_and_columns() {
    let artifact = QueryArtifactMetadata {
        key: "query/db1/s1/t1/w1/staging/s/t/part-00000.parquet".to_string(),
        size_bytes: 128,
        checksum_fnv64: "0011223344556677".to_string(),
    };
    let bytes =
        encode_result_metadata(&[batch_two_rows()], &[artifact]).expect("metadata must encode");
    let parsed: serde_json::Value =
        serde_json::from_slice(&bytes).expect("metadata json must decode");

    assert_eq!(
        parsed.get("row_count").and_then(serde_json::Value::as_u64),
        Some(2)
    );
    assert_eq!(
        parsed
            .get("source_batch_count")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
    assert_eq!(
        parsed
            .get("parquet_file_count")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
    let columns = parsed
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .expect("columns array must exist");
    assert_eq!(columns.len(), 2);
    assert_eq!(
        columns[0].get("name").and_then(serde_json::Value::as_str),
        Some("id")
    );
    assert_eq!(
        columns[1].get("name").and_then(serde_json::Value::as_str),
        Some("name")
    );
    let artifacts = parsed
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .expect("artifacts array must exist");
    assert_eq!(artifacts.len(), 1);
}

#[test]
fn rejects_unsupported_projection_expression() {
    let err = parse_projection_identifier("CAST(id AS STRING)")
        .expect_err("complex projection must be rejected");
    assert!(err.contains("unsupported projection expression"));
}

#[test]
fn rejects_unsupported_filter_column_expression() {
    let err = apply_filter_pipeline(&[batch_two_rows()], "lower(name) = 'a'", None)
        .expect_err("complex filter lhs must be rejected");
    assert!(err.contains("unsupported filter column expression"));
}

#[test]
fn partitions_source_rows_deterministically() {
    let part0 = partition_input_batches(&[batch_two_rows()], 2, 0)
        .expect("partition 0 slicing must succeed");
    let part1 = partition_input_batches(&[batch_two_rows()], 2, 1)
        .expect("partition 1 slicing must succeed");

    assert_eq!(part0[0].num_rows(), 1);
    assert_eq!(part1[0].num_rows(), 1);
}

#[test]
fn rejects_out_of_bounds_partition_index() {
    let err = partition_input_batches(&[batch_two_rows()], 2, 2)
        .expect_err("partition index out of range must be rejected");
    assert!(err.contains("invalid partition index"));
}

#[test]
fn parses_partition_index_from_exchange_key() {
    let key = "distributed_exchange/run/s1/stage-4/part-00003.parquet";
    let parsed = parse_partition_index_from_exchange_key(key)
        .expect("partition index must parse from exchange key");
    assert_eq!(parsed, 3);
}

#[test]
fn validates_complete_upstream_partition_set() {
    let keys = vec![
        "distributed_exchange/run/s1/stage-1/part-00000.parquet".to_string(),
        "distributed_exchange/run/s1/stage-1/part-00001.parquet".to_string(),
        "distributed_exchange/run/s1/stage-1/part-00002.parquet".to_string(),
    ];

    validate_upstream_exchange_partition_set(1, &keys, 3)
        .expect("complete partition set must validate");
}

#[test]
fn rejects_incomplete_upstream_partition_set() {
    let keys = vec![
        "distributed_exchange/run/s1/stage-1/part-00000.parquet".to_string(),
        "distributed_exchange/run/s1/stage-1/part-00002.parquet".to_string(),
    ];

    let err = validate_upstream_exchange_partition_set(1, &keys, 3)
        .expect_err("incomplete partition set must be rejected");
    assert!(err.contains("mismatch"));
}

#[test]
fn applies_limit_pipeline_with_offset_across_batches() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64, 3_i64])) as ArrayRef],
    )
    .expect("batch1 must build");
    let batch2 = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![4_i64, 5_i64, 6_i64])) as ArrayRef],
    )
    .expect("batch2 must build");

    let sliced = apply_limit_pipeline(
        &[batch1, batch2],
        &kionas::planner::PhysicalLimitSpec {
            count: 2,
            offset: 3,
        },
    )
    .expect("limit pipeline must succeed");

    assert_eq!(sliced.len(), 1);
    assert_eq!(sliced[0].num_rows(), 2);
    let ids = sliced[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("id column must be Int64");
    assert_eq!(ids.value(0), 4);
    assert_eq!(ids.value(1), 5);
}

#[test]
fn applies_zero_limit_as_empty_result() {
    let sliced = apply_limit_pipeline(
        &[batch_two_rows()],
        &kionas::planner::PhysicalLimitSpec {
            count: 0,
            offset: 0,
        },
    )
    .expect("limit pipeline must succeed");
    assert_eq!(sliced.len(), 1);
    assert_eq!(sliced[0].num_rows(), 0);
}

#[test]
fn applies_high_offset_as_empty_result() {
    let sliced = apply_limit_pipeline(
        &[batch_two_rows()],
        &kionas::planner::PhysicalLimitSpec {
            count: 5,
            offset: 999,
        },
    )
    .expect("limit pipeline must succeed");
    assert_eq!(sliced.len(), 1);
    assert_eq!(sliced[0].num_rows(), 0);
}

#[test]
fn extracts_runtime_limit_spec() {
    let task = crate::services::worker_service_server::worker_service::Task {
        task_id: "t-limit".to_string(),
        operation: "query".to_string(),
        input: serde_json::json!({
            "physical_plan": {
                "operators": [
                    {"TableScan": {"relation": {"database": "db1", "schema": "s1", "table": "t1"}}},
                    {"Projection": {"expressions": [{"Raw": {"sql": "id"}}]}},
                    {"Limit": {"spec": {"count": 5, "offset": 2}}},
                    "Materialize"
                ]
            }
        })
        .to_string(),
        output: String::new(),
        params: std::collections::HashMap::new(),
    };

    let plan = extract_runtime_plan(&task).expect("runtime plan must parse");
    let limit = plan.limit_spec.expect("limit must be extracted");
    assert_eq!(limit.count, 5);
    assert_eq!(limit.offset, 2);
}
