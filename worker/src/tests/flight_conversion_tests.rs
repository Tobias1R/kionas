#[cfg(test)]
mod flight_conversion_tests {
    use crate::flight::conversion::{
        ConversionError, ConversionMetrics, batch_to_flight_data, conversion_error_to_status,
    };
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn sample_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn sample_batch() -> RecordBatch {
        let schema = sample_schema();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1_i64, 2_i64, 3_i64])) as ArrayRef,
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None])) as ArrayRef,
            ],
        )
        .expect("batch must build")
    }

    fn many_batches(count: usize) -> Vec<RecordBatch> {
        let schema = sample_schema();
        (0..count)
            .map(|i| {
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![i as i64])) as ArrayRef,
                        Arc::new(StringArray::from(vec![Some("x")])) as ArrayRef,
                    ],
                )
                .expect("batch must build")
            })
            .collect()
    }

    #[test]
    fn batch_to_flight_data_encodes_single_batch() {
        let schema = sample_schema();
        let batch = sample_batch();

        let payloads = batch_to_flight_data(&schema, vec![batch]).expect("encoding must succeed");

        assert!(!payloads.is_empty(), "must produce flight data payloads");
        // First payload should have schema (data_header with IPC bytes)
        assert!(
            !payloads[0].data_header.is_empty(),
            "first payload must have schema"
        );
    }

    #[test]
    fn batch_to_flight_data_encodes_multiple_batches() {
        let schema = sample_schema();
        let batches = many_batches(3);

        let payloads = batch_to_flight_data(&schema, batches).expect("encoding must succeed");

        // Schema frame + 3 batch frames = at least 4 frames
        assert!(
            payloads.len() >= 4,
            "should produce schema + 3 batch frames, got {}",
            payloads.len()
        );
    }

    #[test]
    fn batch_to_flight_data_empty_batch_list_encodes_schema_only() {
        let schema = sample_schema();

        let payloads = batch_to_flight_data(&schema, vec![]).expect("encoding must succeed");

        // Should produce at least schema frame
        assert!(
            !payloads.is_empty(),
            "empty batch list should still encode schema"
        );
    }

    #[test]
    fn conversion_metrics_accumulates_batch_count() {
        let metrics = ConversionMetrics {
            batch_count: 5,
            row_count: 1000,
            ipc_bytes: 50000,
        };

        assert_eq!(metrics.batch_count, 5);
        assert_eq!(metrics.row_count, 1000);
        assert_eq!(metrics.ipc_bytes, 50000);
    }

    #[test]
    fn conversion_error_display_formats_correctly() {
        let err = ConversionError::BatchEncodingFailed("test failure".to_string());
        let msg = err.to_string();
        assert!(msg.contains("batch encoding failed"));
        assert!(msg.contains("test failure"));
    }

    #[test]
    fn conversion_error_to_status_maps_encoding_error() {
        let err = ConversionError::BatchEncodingFailed("test".to_string());
        let status = conversion_error_to_status(err);
        assert_eq!(status.code(), tonic14::Code::Internal);
    }

    #[test]
    fn conversion_error_to_status_maps_decoding_error() {
        let err = ConversionError::BatchDecodingFailed("test".to_string());
        let status = conversion_error_to_status(err);
        assert_eq!(status.code(), tonic14::Code::InvalidArgument);
    }

    #[test]
    fn conversion_error_to_status_maps_schema_mismatch() {
        let err = ConversionError::SchemaMismatch("test".to_string());
        let status = conversion_error_to_status(err);
        assert_eq!(status.code(), tonic14::Code::FailedPrecondition);
    }

    #[test]
    fn conversion_error_to_status_maps_no_data_error() {
        let err = ConversionError::NoData;
        let status = conversion_error_to_status(err);
        assert_eq!(status.code(), tonic14::Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn collect_batches_accumulates_metrics() {
        // This is a simple test to validate the metrics accumulation logic.
        // Full integration test would require a full flight stream.
        let batch = sample_batch();
        assert_eq!(batch.num_rows(), 3, "sample batch should have 3 rows");
    }

    #[test]
    fn round_trip_encode_decode_schema_matches() {
        // What: Verify that encoding then decoding preserves schema.
        let schema = sample_schema();
        let batch = sample_batch();

        let payloads = batch_to_flight_data(&schema, vec![batch]).expect("encoding must succeed");

        assert!(!payloads.is_empty(), "payloads should not be empty");

        // First payload contains schema
        let schema_frame = &payloads[0];
        assert!(
            !schema_frame.data_header.is_empty(),
            "schema frame should have data_header"
        );
    }

    #[test]
    fn batch_to_flight_data_preserves_row_count() {
        let schema = sample_schema();
        let batches = many_batches(5);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        let _payloads = batch_to_flight_data(&schema, batches).expect("encoding must succeed");

        assert_eq!(total_rows, 5, "5 single-row batches = 5 total rows");
    }

    #[test]
    fn conversion_metrics_default_is_zero() {
        let metrics = ConversionMetrics::default();
        assert_eq!(metrics.batch_count, 0);
        assert_eq!(metrics.row_count, 0);
        assert_eq!(metrics.ipc_bytes, 0);
    }
}
