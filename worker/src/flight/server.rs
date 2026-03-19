use crate::state::SharedData;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint,
    FlightInfo, Location,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use base64::Engine;
use bytes::Bytes;
use futures::{Stream, stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use tonic14::{Request, Response, Status, Streaming};
use url::Url;

#[derive(Clone)]
pub(crate) struct WorkerFlightService {
    shared_data: SharedData,
}

impl WorkerFlightService {
    pub(crate) fn new(shared_data: SharedData) -> Self {
        Self { shared_data }
    }
}

/// What: Decode the internal opaque Flight ticket into session, task, and worker identifiers.
///
/// Inputs:
/// - `ticket_raw`: UTF-8 bytes from `Ticket.ticket`, expected as URL-safe base64 without padding.
///
/// Output:
/// - Tuple `(session_id, task_id, worker_id)` when ticket is valid.
///
/// Details:
/// - Ticket payload is encoded as `session_id:task_id:worker_id`.
fn decode_internal_ticket(ticket_raw: &[u8]) -> Result<(String, String, String), Status> {
    let encoded = std::str::from_utf8(ticket_raw)
        .map_err(|_| Status::invalid_argument("ticket is not valid UTF-8"))?;

    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| Status::invalid_argument("ticket is not valid base64"))?;

    let decoded_text = String::from_utf8(decoded)
        .map_err(|_| Status::invalid_argument("ticket payload is not valid UTF-8"))?;

    let mut parts = decoded_text.splitn(3, ':');
    let session_id = parts.next().unwrap_or_default().trim().to_string();
    let task_id = parts.next().unwrap_or_default().trim().to_string();
    let worker_id = parts.next().unwrap_or_default().trim().to_string();

    if session_id.is_empty() || task_id.is_empty() || worker_id.is_empty() {
        return Err(Status::invalid_argument(
            "ticket payload is invalid; expected session_id:task_id:worker_id",
        ));
    }

    Ok((session_id, task_id, worker_id))
}

/// What: Build internal worker ticket payload for a session task pair.
///
/// Inputs:
/// - `session_id`: Session identifier.
/// - `task_id`: Task identifier.
/// - `worker_id`: Worker identifier.
///
/// Output:
/// - URL-safe base64 ticket bytes as UTF-8 string.
///
/// Details:
/// - Encoding format is `session_id:task_id:worker_id`.
fn build_internal_ticket(session_id: &str, task_id: &str, worker_id: &str) -> String {
    let payload = format!("{}:{}:{}", session_id, task_id, worker_id);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
}

/// What: Resolve worker Flight port from environment or default worker port.
///
/// Inputs:
/// - `default_worker_port`: Worker interops port.
///
/// Output:
/// - Worker Flight port.
///
/// Details:
/// - Defaults to `default_worker_port + 1` when `WORKER_FLIGHT_PORT` is unset.
fn resolve_worker_flight_port(default_worker_port: u32) -> u32 {
    std::env::var("WORKER_FLIGHT_PORT")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|p| *p > 0)
        .unwrap_or(default_worker_port.saturating_add(1))
}

/// What: Build worker Flight endpoint URI advertised in FlightInfo.
///
/// Inputs:
/// - `host`: Worker host.
/// - `default_worker_port`: Worker interops port.
///
/// Output:
/// - HTTP endpoint URI for internal Flight transport.
fn resolve_flight_endpoint(host: &str, default_worker_port: u32) -> String {
    let flight_port = resolve_worker_flight_port(default_worker_port);
    format!("http://{}:{}", host, flight_port)
}

/// What: Parse session and task identifiers from a Flight descriptor.
///
/// Inputs:
/// - `descriptor`: Flight descriptor from GetFlightInfo/GetSchema request.
///
/// Output:
/// - `(session_id, task_id)` tuple.
///
/// Details:
/// - Supports either `descriptor.path = [session_id, task_id]` or
///   command payload `session_id:task_id`.
fn parse_descriptor_scope(descriptor: &FlightDescriptor) -> Result<(String, String), Status> {
    if descriptor.path.len() >= 2 {
        let session_id = descriptor.path[0].trim().to_string();
        let task_id = descriptor.path[1].trim().to_string();
        if !session_id.is_empty() && !task_id.is_empty() {
            return Ok((session_id, task_id));
        }
    }

    if !descriptor.cmd.is_empty() {
        let cmd = std::str::from_utf8(&descriptor.cmd)
            .map_err(|_| Status::invalid_argument("flight descriptor cmd is not valid UTF-8"))?;
        let mut parts = cmd.splitn(2, ':');
        let session_id = parts.next().unwrap_or_default().trim().to_string();
        let task_id = parts.next().unwrap_or_default().trim().to_string();
        if !session_id.is_empty() && !task_id.is_empty() {
            return Ok((session_id, task_id));
        }
    }

    Err(Status::invalid_argument(
        "flight descriptor must provide session and task scope",
    ))
}

/// What: Encode Arrow schema into Flight schema payload bytes.
///
/// Inputs:
/// - `schema`: Arrow schema to encode.
///
/// Output:
/// - `SchemaResult` containing Flight-compatible schema bytes.
fn schema_result_from_schema(schema: &Schema) -> Result<SchemaResult, Status> {
    let payloads = batches_to_flight_data(schema, Vec::<RecordBatch>::new())
        .map_err(|e| Status::internal(format!("failed to encode flight schema: {}", e)))?;
    let flight_schema = payloads
        .into_iter()
        .next()
        .ok_or_else(|| Status::internal("schema encoding produced no payloads"))?;

    Ok(SchemaResult {
        schema: flight_schema.data_header,
    })
}

/// What: Build the object-prefix used to find staged parquet files for a task result location.
///
/// Inputs:
/// - `result_location`: URI emitted by task execution.
/// - `session_id`: Session id extracted from ticket payload.
/// - `task_id`: Task id extracted from ticket payload.
///
/// Output:
/// - Object prefix path suitable for StorageProvider listing.
///
/// Details:
/// - Query artifacts are task-scoped under `<result_root>/staging/<session_id>/<task_id>/`.
fn to_staging_prefix(
    result_location: &str,
    session_id: &str,
    task_id: &str,
) -> Result<String, Status> {
    let parsed = Url::parse(result_location).map_err(|e| {
        Status::invalid_argument(format!("result_location is not a valid URI: {}", e))
    })?;

    let path = parsed.path().trim_start_matches('/').trim_end_matches('/');
    if path.is_empty() {
        return Err(Status::invalid_argument(
            "result_location path is empty; cannot resolve parquet objects",
        ));
    }

    Ok(format!("{}/staging/{}/{}/", path, session_id, task_id))
}

/// What: Decode parquet bytes into Arrow record batches.
///
/// Inputs:
/// - `parquet_bytes`: Full parquet file bytes.
///
/// Output:
/// - Decoded Arrow batches from the parquet file.
///
/// Details:
/// - Uses parquet arrow reader builder with a moderate batch size.
fn decode_parquet_batches(parquet_bytes: Vec<u8>) -> Result<Vec<RecordBatch>, Status> {
    let reader_source = Bytes::from(parquet_bytes);
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(reader_source)
        .map_err(|e| Status::internal(format!("failed to open parquet reader: {}", e)))?
        .with_batch_size(1024)
        .build()
        .map_err(|e| Status::internal(format!("failed to build parquet batch reader: {}", e)))?;

    let mut batches = Vec::new();
    for maybe_batch in &mut reader {
        let batch = maybe_batch
            .map_err(|e| Status::internal(format!("failed reading parquet batch: {}", e)))?;
        batches.push(batch);
    }

    Ok(batches)
}

/// What: Read and parse result metadata sidecar JSON for task-scoped query artifacts.
///
/// Inputs:
/// - `shared_data`: Worker shared state containing storage provider.
/// - `prefix`: Task-scoped staging prefix.
///
/// Output:
/// - Parsed metadata JSON value.
async fn load_result_metadata(
    shared_data: &SharedData,
    prefix: &str,
) -> Result<serde_json::Value, Status> {
    let provider = shared_data
        .storage_provider
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("storage provider is not configured"))?;

    let metadata_key = format!("{}result_metadata.json", prefix);
    let bytes = provider
        .get_object(&metadata_key)
        .await
        .map_err(|e| Status::internal(format!("failed to read object {}: {}", metadata_key, e)))?
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "query result metadata is missing for prefix {}",
                prefix
            ))
        })?;

    serde_json::from_slice::<serde_json::Value>(&bytes)
        .map_err(|e| Status::internal(format!("failed to parse result metadata JSON: {}", e)))
}

#[derive(Debug, Clone)]
struct ExpectedArtifact {
    size_bytes: u64,
    checksum_fnv64: String,
}

/// What: Parse expected parquet artifact metadata map from sidecar JSON.
///
/// Inputs:
/// - `metadata`: Parsed metadata JSON.
///
/// Output:
/// - Map keyed by object key with expected size and checksum.
fn expected_artifacts_from_metadata(
    metadata: &serde_json::Value,
) -> Result<HashMap<String, ExpectedArtifact>, Status> {
    let artifacts = metadata
        .get("artifacts")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Status::failed_precondition("result metadata missing artifacts"))?;

    let mut map = HashMap::with_capacity(artifacts.len());
    for artifact in artifacts {
        let key = artifact
            .get("key")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| Status::failed_precondition("result metadata artifact missing key"))?
            .to_string();
        let size_bytes = artifact
            .get("size_bytes")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| {
                Status::failed_precondition("result metadata artifact missing size_bytes")
            })?;
        let checksum_fnv64 = artifact
            .get("checksum_fnv64")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                Status::failed_precondition("result metadata artifact missing checksum_fnv64")
            })?
            .to_string();

        map.insert(
            key,
            ExpectedArtifact {
                size_bytes,
                checksum_fnv64,
            },
        );
    }

    Ok(map)
}

/// What: Compute deterministic FNV-1a 64-bit checksum as lowercase hex.
///
/// Inputs:
/// - `bytes`: Raw artifact bytes.
///
/// Output:
/// - 16-char lowercase hex checksum.
fn checksum_fnv64_hex(bytes: &[u8]) -> String {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }

    format!("{hash:016x}")
}

/// What: Validate that result metadata and decoded parquet batches are aligned.
///
/// Inputs:
/// - `metadata`: Parsed result metadata JSON.
/// - `batches`: Decoded parquet batches for the same task prefix.
/// - `parquet_file_count`: Number of parquet files discovered under task prefix.
///
/// Output:
/// - `Ok(())` when metadata matches row/batch/schema shape.
/// - `Err(Status)` when alignment fails.
fn validate_metadata_alignment(
    metadata: &serde_json::Value,
    batches: &[RecordBatch],
    parquet_file_count: usize,
    expected_artifact_count: usize,
) -> Result<(), Status> {
    let expected_row_count = metadata
        .get("row_count")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| Status::failed_precondition("result metadata missing row_count"))?;
    let expected_columns = metadata
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Status::failed_precondition("result metadata missing columns"))?;
    let expected_file_count = metadata
        .get("parquet_file_count")
        .and_then(serde_json::Value::as_u64);

    let actual_row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as u64;
    let actual_columns = batches
        .first()
        .map(|batch| batch.num_columns() as u64)
        .ok_or_else(|| Status::not_found("result parquet files contain no record batches"))?;
    let actual_schema = batches
        .first()
        .map(RecordBatch::schema)
        .ok_or_else(|| Status::not_found("result parquet files contain no record batches"))?;

    if expected_row_count != actual_row_count {
        return Err(Status::failed_precondition(format!(
            "result metadata row_count mismatch: expected={} actual={}",
            expected_row_count, actual_row_count
        )));
    }
    if expected_columns.len() as u64 != actual_columns {
        return Err(Status::failed_precondition(format!(
            "result metadata column count mismatch: expected={} actual={}",
            expected_columns.len(),
            actual_columns
        )));
    }

    for (index, expected_column) in expected_columns.iter().enumerate() {
        let expected_name = expected_column
            .get("name")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "result metadata column at index {} missing name",
                    index
                ))
            })?;
        let expected_data_type = expected_column
            .get("data_type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "result metadata column at index {} missing data_type",
                    index
                ))
            })?;
        let expected_nullable = expected_column
            .get("nullable")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "result metadata column at index {} missing nullable",
                    index
                ))
            })?;

        let actual_field = actual_schema.field(index);
        let actual_name = actual_field.name();
        let actual_data_type = format!("{:?}", actual_field.data_type());
        let actual_nullable = actual_field.is_nullable();

        if expected_name != actual_name {
            return Err(Status::failed_precondition(format!(
                "result metadata column name mismatch at index {}: expected='{}' actual='{}'",
                index, expected_name, actual_name
            )));
        }
        if expected_data_type != actual_data_type {
            return Err(Status::failed_precondition(format!(
                "result metadata column type mismatch at index {}: expected='{}' actual='{}'",
                index, expected_data_type, actual_data_type
            )));
        }
        if expected_nullable != actual_nullable {
            return Err(Status::failed_precondition(format!(
                "result metadata column nullability mismatch at index {}: expected={} actual={}",
                index, expected_nullable, actual_nullable
            )));
        }
    }

    if let Some(expected_file_count) = expected_file_count
        && expected_file_count != parquet_file_count as u64
    {
        return Err(Status::failed_precondition(format!(
            "result metadata parquet_file_count mismatch: expected={} actual={}",
            expected_file_count, parquet_file_count
        )));
    }
    if expected_artifact_count != parquet_file_count {
        return Err(Status::failed_precondition(format!(
            "result metadata artifacts mismatch: expected={} actual={}",
            expected_artifact_count, parquet_file_count
        )));
    }

    Ok(())
}

/// What: Load all staged parquet batches for a worker task result location.
///
/// Inputs:
/// - `shared_data`: Worker shared state containing storage provider.
/// - `result_location`: Resolved task result location URI.
/// - `session_id`: Session id extracted from ticket payload.
/// - `task_id`: Task id extracted from ticket payload.
///
/// Output:
/// - Concatenated Arrow batches read from parquet objects.
///
/// Details:
/// - Reads all `.parquet` objects found under task-scoped staging prefix.
async fn load_result_batches(
    shared_data: &SharedData,
    result_location: &str,
    session_id: &str,
    task_id: &str,
) -> Result<Vec<RecordBatch>, Status> {
    let provider = shared_data
        .storage_provider
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("storage provider is not configured"))?;

    let prefix = to_staging_prefix(result_location, session_id, task_id)?;
    let metadata = load_result_metadata(shared_data, &prefix).await?;
    let expected_artifacts = expected_artifacts_from_metadata(&metadata)?;
    let mut keys = provider
        .list_objects(&prefix)
        .await
        .map_err(|e| Status::internal(format!("failed to list objects for {}: {}", prefix, e)))?;

    keys.retain(|k| k.ends_with(".parquet"));
    keys.sort();
    let parquet_file_count = keys.len();

    if keys.is_empty() {
        return Err(Status::not_found(format!(
            "no parquet result files found for prefix {}",
            prefix
        )));
    }

    let mut all_batches = Vec::new();
    for key in keys {
        let expected = expected_artifacts.get(&key).ok_or_else(|| {
            Status::failed_precondition(format!(
                "result metadata missing artifact entry for object {}",
                key
            ))
        })?;

        let bytes = provider
            .get_object(&key)
            .await
            .map_err(|e| Status::internal(format!("failed to read object {}: {}", key, e)))?
            .ok_or_else(|| Status::not_found(format!("result object not found: {}", key)))?;

        if expected.size_bytes != bytes.len() as u64 {
            return Err(Status::failed_precondition(format!(
                "result artifact size mismatch for {}: expected={} actual={}",
                key,
                expected.size_bytes,
                bytes.len()
            )));
        }

        let checksum = checksum_fnv64_hex(&bytes);
        if expected.checksum_fnv64 != checksum {
            return Err(Status::failed_precondition(format!(
                "result artifact checksum mismatch for {}",
                key
            )));
        }

        let batches = decode_parquet_batches(bytes)?;
        all_batches.extend(batches);
    }

    if all_batches.is_empty() {
        return Err(Status::not_found(
            "result parquet files contain no record batches",
        ));
    }

    validate_metadata_alignment(
        &metadata,
        &all_batches,
        parquet_file_count,
        expected_artifacts.len(),
    )?;

    Ok(all_batches)
}

type FlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic14::async_trait]
impl FlightService for WorkerFlightService {
    type HandshakeStream = FlightStream<HandshakeResponse>;
    type ListFlightsStream = FlightStream<FlightInfo>;
    type DoGetStream = FlightStream<FlightData>;
    type DoPutStream = FlightStream<PutResult>;
    type DoExchangeStream = FlightStream<FlightData>;
    type DoActionStream = FlightStream<arrow_flight::Result>;
    type ListActionsStream = FlightStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented(
            "worker Flight handshake is not implemented yet",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let output = stream::empty::<Result<FlightInfo, Status>>();
        Ok(Response::new(Box::pin(output)))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let (session_id, task_id) = parse_descriptor_scope(&descriptor)?;

        let result_location = self
            .shared_data
            .get_task_result_location(&session_id, &task_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!("no result location found for task_id={}", task_id))
            })?;

        let batches =
            load_result_batches(&self.shared_data, &result_location, &session_id, &task_id)
                .await?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| Status::not_found("no record batches available for FlightInfo"))?;
        let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>() as i64;
        let schema_result = schema_result_from_schema(schema.as_ref())?;

        let endpoint_uri = resolve_flight_endpoint(
            &self.shared_data.worker_info.host,
            self.shared_data.worker_info.port,
        );
        let ticket = build_internal_ticket(
            &session_id,
            &task_id,
            &self.shared_data.worker_info.worker_id,
        );

        let flight_info = FlightInfo {
            schema: schema_result.schema,
            flight_descriptor: Some(descriptor),
            endpoint: vec![FlightEndpoint {
                ticket: Some(Ticket {
                    ticket: ticket.into_bytes().into(),
                }),
                location: vec![Location { uri: endpoint_uri }],
                expiration_time: None,
                app_metadata: Default::default(),
            }],
            total_records: row_count,
            total_bytes: -1,
            ordered: false,
            app_metadata: Default::default(),
        };

        Ok(Response::new(flight_info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented(
            "worker Flight poll_flight_info is not implemented yet",
        ))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let (session_id, task_id) = parse_descriptor_scope(&descriptor)?;

        let result_location = self
            .shared_data
            .get_task_result_location(&session_id, &task_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!("no result location found for task_id={}", task_id))
            })?;

        let batches =
            load_result_batches(&self.shared_data, &result_location, &session_id, &task_id)
                .await?;
        let schema = batches
            .first()
            .map(RecordBatch::schema)
            .ok_or_else(|| Status::not_found("no record batches available for schema"))?;

        Ok(Response::new(schema_result_from_schema(schema.as_ref())?))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let (session_id, task_id, ticket_worker_id) = decode_internal_ticket(&ticket.ticket)?;

        if ticket_worker_id != self.shared_data.worker_info.worker_id {
            return Err(Status::permission_denied(
                "ticket worker_id does not match this worker",
            ));
        }

        let result_location = self
            .shared_data
            .get_task_result_location(&session_id, &task_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!("no result location found for task_id={}", task_id))
            })?;

        let batches =
            load_result_batches(&self.shared_data, &result_location, &session_id, &task_id).await?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| Status::not_found("no record batches available for DoGet"))?;
        let schema_owned = Schema::from(schema.as_ref().clone());

        let payloads = batches_to_flight_data(&schema_owned, batches)
            .map_err(|e| Status::internal(format!("failed to encode FlightData: {}", e)))?
            .into_iter()
            .map(Ok)
            .collect::<Vec<Result<FlightData, Status>>>();

        let output = stream::iter(payloads);
        Ok(Response::new(Box::pin(output)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "worker Flight do_put is not implemented yet",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "worker Flight do_exchange is not implemented yet",
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let output = stream::iter(vec![Err(Status::unimplemented(
            "worker Flight do_action is not implemented yet",
        ))]);
        Ok(Response::new(Box::pin(output)))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let output = stream::empty::<Result<ActionType, Status>>();
        Ok(Response::new(Box::pin(output)))
    }
}

pub(crate) async fn serve_flight(
    shared_data: SharedData,
    addr: SocketAddr,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    let service = WorkerFlightService::new(shared_data);

    tonic14::transport::Server::builder()
        .add_service(FlightServiceServer::new(service))
        .serve(addr)
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}

#[cfg(test)]
mod tests {
    use super::{
        checksum_fnv64_hex, expected_artifacts_from_metadata, to_staging_prefix,
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
}
