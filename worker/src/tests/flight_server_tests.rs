use super::{
    checksum_fnv64_hex, expected_artifacts_from_metadata, schema_from_metadata, to_staging_prefix,
    validate_metadata_alignment,
};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

fn sample_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef],
    )
    .expect("batch must build");
    vec![batch]
}

fn sample_two_column_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ],
    )
    .expect("batch must build");
    vec![batch]
}

#[test]
fn builds_task_scoped_staging_prefix() {
    let prefix = to_staging_prefix(
        "flight://worker:32001/query/db1/s1/t1/worker1?session_id=s1&task_id=t1",
        "s1",
        "t1",
    )
    .expect("prefix must build");

    assert_eq!(prefix, "query/db1/s1/t1/worker1/staging/s1/t1/");
}

#[test]
fn rejects_empty_result_path() {
    let err = to_staging_prefix("flight://worker:32001", "s1", "t1")
        .expect_err("empty path must be rejected");
    assert_eq!(err.code(), tonic14::Code::InvalidArgument);
}

#[test]
fn validates_metadata_alignment_success() {
    let batches = sample_batches();
    let metadata = serde_json::json!({
        "row_count": 2,
        "source_batch_count": 1,
        "parquet_file_count": 1,
        "columns": [{"name": "id", "data_type": "Int64", "nullable": false}],
        "artifacts": [{"key": "k1", "size_bytes": 16, "checksum_fnv64": "0011223344556677"}],
    });

    validate_metadata_alignment(&metadata, &batches, 1, 1).expect("metadata should align");
}

#[test]
fn rejects_metadata_alignment_mismatch() {
    let batches = sample_batches();
    let metadata = serde_json::json!({
        "row_count": 99,
        "source_batch_count": 1,
        "parquet_file_count": 1,
        "columns": [{"name": "id", "data_type": "Int64", "nullable": false}],
        "artifacts": [{"key": "k1", "size_bytes": 16, "checksum_fnv64": "0011223344556677"}],
    });

    let err = validate_metadata_alignment(&metadata, &batches, 1, 1)
        .expect_err("row_count mismatch must fail");
    assert_eq!(err.code(), tonic14::Code::FailedPrecondition);
}

#[test]
fn rejects_metadata_file_count_mismatch() {
    let batches = sample_batches();
    let metadata = serde_json::json!({
        "row_count": 2,
        "source_batch_count": 1,
        "parquet_file_count": 2,
        "columns": [{"name": "id", "data_type": "Int64", "nullable": false}],
        "artifacts": [{"key": "k1", "size_bytes": 16, "checksum_fnv64": "0011223344556677"}],
    });

    let err = validate_metadata_alignment(&metadata, &batches, 1, 1)
        .expect_err("file count mismatch must fail");
    assert_eq!(err.code(), tonic14::Code::FailedPrecondition);
}

#[test]
fn parses_expected_artifacts_map() {
    let metadata = serde_json::json!({
        "artifacts": [
            {"key": "k1", "size_bytes": 10, "checksum_fnv64": "aaaaaaaaaaaaaaaa"},
            {"key": "k2", "size_bytes": 20, "checksum_fnv64": "bbbbbbbbbbbbbbbb"}
        ]
    });

    let parsed = expected_artifacts_from_metadata(&metadata).expect("must parse artifacts");
    assert_eq!(parsed.len(), 2);
    assert_eq!(parsed.get("k1").map(|v| v.size_bytes), Some(10));
}

#[test]
fn computes_stable_fnv64_checksum() {
    let checksum = checksum_fnv64_hex(b"abc");
    assert_eq!(checksum, "e71fa2190541574b");
}

#[test]
fn rejects_metadata_column_name_mismatch() {
    let batches = sample_batches();
    let metadata = serde_json::json!({
        "row_count": 2,
        "source_batch_count": 1,
        "parquet_file_count": 1,
        "columns": [{"name": "wrong_id", "data_type": "Int64", "nullable": false}],
        "artifacts": [{"key": "k1", "size_bytes": 16, "checksum_fnv64": "0011223344556677"}],
    });

    let err = validate_metadata_alignment(&metadata, &batches, 1, 1)
        .expect_err("column name mismatch must fail");
    assert_eq!(err.code(), tonic14::Code::FailedPrecondition);
}

#[test]
fn rejects_metadata_column_type_mismatch() {
    let batches = sample_batches();
    let metadata = serde_json::json!({
        "row_count": 2,
        "source_batch_count": 1,
        "parquet_file_count": 1,
        "columns": [{"name": "id", "data_type": "Utf8", "nullable": false}],
        "artifacts": [{"key": "k1", "size_bytes": 16, "checksum_fnv64": "0011223344556677"}],
    });

    let err = validate_metadata_alignment(&metadata, &batches, 1, 1)
        .expect_err("column type mismatch must fail");
    assert_eq!(err.code(), tonic14::Code::FailedPrecondition);
}

#[test]
fn rejects_metadata_column_order_mismatch() {
    let batches = sample_two_column_batches();
    let metadata = serde_json::json!({
        "row_count": 2,
        "source_batch_count": 1,
        "parquet_file_count": 1,
        "columns": [
            {"name": "name", "data_type": "Utf8", "nullable": false},
            {"name": "id", "data_type": "Int64", "nullable": false}
        ],
        "artifacts": [{"key": "k1", "size_bytes": 16, "checksum_fnv64": "0011223344556677"}],
    });

    let err = validate_metadata_alignment(&metadata, &batches, 1, 1)
        .expect_err("column order mismatch must fail");
    assert_eq!(err.code(), tonic14::Code::FailedPrecondition);
}

#[test]
fn builds_schema_from_metadata_columns() {
    let metadata = serde_json::json!({
        "columns": [
            {"name": "id", "data_type": "Int64", "nullable": false},
            {"name": "name", "data_type": "Utf8", "nullable": true}
        ]
    });

    let schema = schema_from_metadata(&metadata).expect("schema should build");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(format!("{:?}", schema.field(1).data_type()), "Utf8");
}

#[test]
fn builds_schema_from_temporal_metadata_columns() {
    let metadata = serde_json::json!({
        "columns": [
            {"name": "event_day", "data_type": "Date32", "nullable": true},
            {"name": "event_at", "data_type": "Timestamp(Millisecond, None)", "nullable": false}
        ]
    });

    let schema = schema_from_metadata(&metadata).expect("temporal schema should build");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(format!("{:?}", schema.field(0).data_type()), "Date32");
    assert_eq!(
        format!("{:?}", schema.field(1).data_type()),
        "Timestamp(Millisecond, None)"
    );
}
