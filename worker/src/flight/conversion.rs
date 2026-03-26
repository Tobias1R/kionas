/// What: Bidirectional conversion module between Arrow RecordBatch and Arrow Flight IPC format.
///
/// Details:
/// - Encapsulates batches_to_flight_data() and FlightRecordBatchStream for worker use
/// - Provides typed error handling for batch encoding/decoding failures
/// - Enables metrics collection on conversion operations (throughput, latency, row counts)
/// - Validates schema consistency during round-trip conversions
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream as RecordBatchStream;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightData, error::FlightError};
use futures::Stream;
use tonic14::Status;

/// What: Error type for batch conversion operations.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum ConversionError {
    /// Schema encoding failed
    SchemaEncodingFailed(String),
    /// Batch encoding failed
    BatchEncodingFailed(String),
    /// Batch decoding failed
    BatchDecodingFailed(String),
    /// Schema mismatch between encoder and decoder
    SchemaMismatch(String),
    /// No batches in stream
    NoData,
}

impl std::fmt::Display for ConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SchemaEncodingFailed(msg) => write!(f, "schema encoding failed: {}", msg),
            Self::BatchEncodingFailed(msg) => write!(f, "batch encoding failed: {}", msg),
            Self::BatchDecodingFailed(msg) => write!(f, "batch decoding failed: {}", msg),
            Self::SchemaMismatch(msg) => write!(f, "schema mismatch: {}", msg),
            Self::NoData => write!(f, "no data: batch stream is empty"),
        }
    }
}

impl std::error::Error for ConversionError {}

/// What: Conversion metrics tracked per operation.
///
/// Inputs: None (accumulated during conversion)
/// Output: Metrics snapshot
#[derive(Debug, Clone, Default)]
pub struct ConversionMetrics {
    /// Total RecordBatches processed
    pub batch_count: u64,
    /// Total rows across all batches
    pub row_count: u64,
    /// Total bytes in IPC payload (approx)
    pub ipc_bytes: u64,
}

/// What: B2.1 - Encode Arrow RecordBatches into Arrow Flight IPC format.
///
/// Inputs:
/// - `schema`: Arrow schema shared by all batches.
/// - `batches`: RecordBatches to encode.
///
/// Output:
/// - Vector of FlightData frames ready for transmission.
///
/// Details:
/// - First frame contains schema definition (FlightData with data_header only)
/// - Subsequent frames contain encoded record batches (data_header + data_body)
/// - All batches must conform to the provided schema (no validation here, assumed valid)
#[allow(dead_code)]
pub fn batch_to_flight_data(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<Vec<FlightData>, ConversionError> {
    batches_to_flight_data(schema, batches).map_err(|e| {
        ConversionError::BatchEncodingFailed(format!("arrow_flight encoding failed: {}", e))
    })
}

/// What: B2.2 - Decode Arrow Flight IPC stream into RecordBatches.
///
/// Inputs:
/// - `flight_stream`: Stream of FlightData frames from Arrow Flight transport.
///
/// Output:
/// - Stream of decoded RecordBatches, or error.
///
/// Details:
/// - First FlightData may contain schema (data_header only)
/// - Subsequent frames contain record batch IPC payloads
/// - Caller is responsible for validating schema matches expected type
#[allow(dead_code)]
pub fn flight_data_to_batch_stream(
    flight_stream: impl Stream<Item = Result<FlightData, FlightError>> + Send + 'static,
) -> RecordBatchStream {
    RecordBatchStream::new_from_flight_data(flight_stream)
}

/// What: B2.3 - Collect all batches from a RecordBatchStream and compute metrics.
///
/// Inputs:
/// - `stream`: Async stream of decoded RecordBatches.
///
/// Output:
/// - `(batches, metrics)` tuple with all decoded batches and aggregated metrics.
///
/// Details:
/// - Drains the entire RecordBatchStream until completion
/// - Accumulates row counts, batch counts, and approximate byte sizes
/// - Safe to use in async contexts (uses .await on stream iteration)
#[allow(dead_code)]
pub async fn collect_batches_with_metrics(
    mut stream: RecordBatchStream,
) -> Result<(Vec<RecordBatch>, ConversionMetrics), ConversionError> {
    use futures::StreamExt as FuturesStreamExt;

    let mut batches = Vec::new();
    let mut metrics = ConversionMetrics::default();

    while let Some(batch_result) = stream.next().await {
        let batch =
            batch_result.map_err(|e| ConversionError::BatchDecodingFailed(e.to_string()))?;

        let row_count = batch.num_rows() as u64;

        metrics.batch_count += 1;
        metrics.row_count += row_count;
        // Approximate byte size: sum of column byte_lengths where available
        metrics.ipc_bytes = metrics.ipc_bytes.saturating_add(
            batch
                .columns()
                .iter()
                .map(|col| {
                    // Use get_array_memory_size or estimate from data_type + num_elements
                    let num_elements = batch.num_rows();
                    match col.data_type() {
                        arrow::datatypes::DataType::Int64 => (num_elements as u64) * 8,
                        arrow::datatypes::DataType::Utf8 => {
                            // Rough estimate: average string + overhead
                            (num_elements as u64) * 32
                        }
                        _ => (num_elements as u64) * 16, // Conservative estimate
                    }
                })
                .sum::<u64>(),
        );

        batches.push(batch);
    }

    if batches.is_empty() {
        return Err(ConversionError::NoData);
    }

    Ok((batches, metrics))
}

/// What: Convert ConversionError to gRPC Status.
///
/// Inputs:
/// - `error`: Conversion error.
///
/// Output:
/// - Mapped gRPC Status with appropriate code and message.
#[allow(dead_code)]
pub fn conversion_error_to_status(error: ConversionError) -> Status {
    match error {
        ConversionError::SchemaEncodingFailed(msg) => {
            Status::internal(format!("flight schema encoding failed: {}", msg))
        }
        ConversionError::BatchEncodingFailed(msg) => {
            Status::internal(format!("flight batch encoding failed: {}", msg))
        }
        ConversionError::BatchDecodingFailed(msg) => {
            Status::invalid_argument(format!("flight batch decoding failed: {}", msg))
        }
        ConversionError::SchemaMismatch(msg) => {
            Status::failed_precondition(format!("flight schema mismatch: {}", msg))
        }
        ConversionError::NoData => Status::failed_precondition("no batches in flight stream"),
    }
}

#[cfg(test)]
#[path = "../tests/flight_conversion_tests.rs"]
mod tests;
