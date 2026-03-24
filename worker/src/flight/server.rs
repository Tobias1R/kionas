#![allow(dead_code)]

use crate::execution::artifacts::persist_query_artifacts;
use crate::flight::client_pool::FlightClientPool;
use crate::state::{SharedData, WorkerInformation};
use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, Location, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tonic14::{Request, Response, Status, Streaming};
use url::Url;

fn map_tonic_status(status: tonic::Status) -> Status {
    let code = match status.code() {
        tonic::Code::Ok => tonic14::Code::Ok,
        tonic::Code::Cancelled => tonic14::Code::Cancelled,
        tonic::Code::Unknown => tonic14::Code::Unknown,
        tonic::Code::InvalidArgument => tonic14::Code::InvalidArgument,
        tonic::Code::DeadlineExceeded => tonic14::Code::DeadlineExceeded,
        tonic::Code::NotFound => tonic14::Code::NotFound,
        tonic::Code::AlreadyExists => tonic14::Code::AlreadyExists,
        tonic::Code::PermissionDenied => tonic14::Code::PermissionDenied,
        tonic::Code::ResourceExhausted => tonic14::Code::ResourceExhausted,
        tonic::Code::FailedPrecondition => tonic14::Code::FailedPrecondition,
        tonic::Code::Aborted => tonic14::Code::Aborted,
        tonic::Code::OutOfRange => tonic14::Code::OutOfRange,
        tonic::Code::Unimplemented => tonic14::Code::Unimplemented,
        tonic::Code::Internal => tonic14::Code::Internal,
        tonic::Code::Unavailable => tonic14::Code::Unavailable,
        tonic::Code::DataLoss => tonic14::Code::DataLoss,
        tonic::Code::Unauthenticated => tonic14::Code::Unauthenticated,
    };
    Status::new(code, status.message().to_string())
}

fn dispatch_context_from_flight_metadata(
    metadata: &tonic14::metadata::MetadataMap,
    fallback_session_id: &str,
) -> Result<crate::authz::DispatchAuthContext, Status> {
    let session_id = metadata
        .get("session_id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .unwrap_or(fallback_session_id)
        .to_string();
    if session_id.is_empty() {
        return Err(Status::unauthenticated("missing session_id metadata"));
    }

    let rbac_user = metadata
        .get("rbac_user")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Status::permission_denied("missing rbac_user metadata"))?
        .to_string();

    let rbac_role = metadata
        .get("rbac_role")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Status::permission_denied("missing rbac_role metadata"))?
        .to_string();

    let auth_scope = metadata
        .get("auth_scope")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Status::permission_denied("missing auth_scope metadata"))?
        .to_string();

    let query_id = metadata
        .get("query_id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| Status::permission_denied("missing query_id metadata"))?
        .to_string();

    Ok(crate::authz::DispatchAuthContext {
        session_id,
        rbac_user,
        rbac_role,
        auth_scope,
        query_id,
    })
}

#[derive(Clone)]
pub(crate) struct WorkerFlightService {
    shared_data: SharedData,
    /// Pooled Flight client connections to downstream workers
    client_pool: Arc<FlightClientPool>,
}

impl WorkerFlightService {
    /// What: Create new worker Flight service with client pool.
    ///
    /// Inputs:
    /// - `shared_data`: Worker shared state.
    ///
    /// Output:
    /// - Initialized service with lazy-load client pool.
    pub(crate) fn new(shared_data: SharedData) -> Self {
        Self {
            shared_data,
            // Initialize pool: max 8 connections per endpoint, 5s connection timeout
            client_pool: Arc::new(FlightClientPool::new(8, Duration::from_secs(5))),
        }
    }

    /// What: Get a Flight client channel to a downstream worker.
    ///
    /// Inputs:
    /// - `endpoint_uri`: Destination worker Flight endpoint.
    ///
    /// Output:
    /// - Pooled gRPC channel or error if connection fails.
    ///
    /// Details:
    /// - Reuses channels per endpoint to reduce TLS overhead
    /// - Returns error if endpoint is unreachable (upstream death scenario)
    pub(crate) async fn get_downstream_channel(
        &self,
        endpoint_uri: &str,
    ) -> Result<tonic14::transport::Channel, Status> {
        self.client_pool
            .get_channel(endpoint_uri)
            .await
            .map_err(|e| {
                let code = match e {
                    crate::flight::client_pool::FlightClientPoolError::ConnectionTimeout => {
                        tonic14::Code::DeadlineExceeded
                    }
                    crate::flight::client_pool::FlightClientPoolError::PoolExhausted => {
                        tonic14::Code::ResourceExhausted
                    }
                    crate::flight::client_pool::FlightClientPoolError::InvalidEndpoint => {
                        tonic14::Code::InvalidArgument
                    }
                    crate::flight::client_pool::FlightClientPoolError::TlsError(_) => {
                        tonic14::Code::Unauthenticated
                    }
                    crate::flight::client_pool::FlightClientPoolError::ChannelError(_) => {
                        tonic14::Code::Unavailable
                    }
                };
                Status::new(code, e.to_string())
            })
    }
}

/// What: Decode and verify the signed internal Flight ticket.
///
/// Inputs:
/// - `ticket_raw`: UTF-8 bytes from `Ticket.ticket`, expected as URL-safe base64 without padding.
///
/// Output:
/// - Verified signed ticket claims.
///
/// Details:
/// - Ticket payload is a compact JWT signed by worker auth policy.
fn decode_internal_ticket(
    ticket_raw: &[u8],
    authorizer: &crate::authz::WorkerAuthorizer,
) -> Result<crate::authz::FlightTicketClaims, Status> {
    let token = std::str::from_utf8(ticket_raw)
        .map_err(|_| Status::invalid_argument("ticket is not valid UTF-8"))?;
    authorizer
        .verify_signed_flight_ticket(token)
        .map_err(map_tonic_status)
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

/// What: Backpressure limits applied to one ingest request.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Limit bundle used by ingest stream validation.
///
/// Details:
/// - Caps decoded batches and rows so a single client stream cannot exhaust worker memory.
#[derive(Debug, Clone, Copy)]
struct IngestBackpressureLimits {
    max_batches: usize,
    max_rows: usize,
    max_wire_bytes: usize,
}

/// What: Resolve ingest backpressure limits for worker Flight streams.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Static limits used by `do_put` and `do_exchange` ingest validation.
///
/// Details:
/// - Limits are intentionally conservative for now and can be made configurable later.
fn ingest_backpressure_limits() -> IngestBackpressureLimits {
    IngestBackpressureLimits {
        max_batches: 128,
        max_rows: 1_000_000,
        max_wire_bytes: 32 * 1024 * 1024,
    }
}

/// What: Estimate one FlightData frame wire footprint in bytes.
///
/// Inputs:
/// - `frame`: Flight frame to size.
///
/// Output:
/// - Approximate on-wire byte size used for request guardrails.
///
/// Details:
/// - Includes descriptor cmd/path strings and body/header/app metadata payloads.
fn flight_data_wire_size_bytes(frame: &FlightData) -> usize {
    let mut total = 0usize;
    total = total.saturating_add(frame.data_header.len());
    total = total.saturating_add(frame.data_body.len());
    total = total.saturating_add(frame.app_metadata.len());

    if let Some(descriptor) = frame.flight_descriptor.as_ref() {
        total = total.saturating_add(descriptor.cmd.len());
        for segment in &descriptor.path {
            total = total.saturating_add(segment.len());
        }
    }

    total
}

/// What: Build a deterministic result location for Flight do_put ingestion.
///
/// Inputs:
/// - `worker_info`: Worker metadata used for URI composition.
/// - `session_id`: Ingest session identifier.
/// - `task_id`: Ingest task identifier.
///
/// Output:
/// - Result location URI compatible with task-scoped staging prefixes.
fn build_ingest_result_location(
    worker_info: &WorkerInformation,
    session_id: &str,
    task_id: &str,
) -> String {
    format!(
        "flight://{}:{}/query/flight/{}/{}/{}",
        worker_info.host, worker_info.port, session_id, task_id, worker_info.worker_id
    )
}

/// What: Resolve existing task result location or create one for Flight ingestion.
///
/// Inputs:
/// - `shared_data`: Worker shared state.
/// - `session_id`: Session identifier.
/// - `task_id`: Task identifier.
///
/// Output:
/// - Existing or newly provisioned result location URI.
async fn resolve_or_create_ingest_result_location(
    shared_data: &SharedData,
    session_id: &str,
    task_id: &str,
) -> String {
    if let Some(existing) = shared_data
        .get_task_result_location(session_id, task_id)
        .await
    {
        return existing;
    }

    let created = build_ingest_result_location(&shared_data.worker_info, session_id, task_id);
    shared_data
        .set_task_result_location(session_id, task_id, &created)
        .await;
    created
}

/// What: Validate, decode, and persist one Flight ingest stream.
///
/// Inputs:
/// - `shared_data`: Worker shared state with optional storage provider.
/// - `request`: Streaming FlightData request.
/// - `stream_name`: Stream label used for user-facing error messages.
///
/// Output:
/// - `Ok(())` when at least one IPC record batch is decoded and persisted.
/// - `Err(Status)` for auth, scope, decode, or persistence failures.
async fn ingest_flight_stream(
    shared_data: &SharedData,
    request: Request<Streaming<FlightData>>,
    stream_name: &str,
) -> Result<(), Status> {
    let stream_name_owned = stream_name.to_string();
    let authz = crate::authz::WorkerAuthorizer::new();
    let metadata = request.metadata().clone();
    let mut input = request.into_inner();

    let first_message = input
        .message()
        .await
        .map_err(|e| {
            Status::internal(format!(
                "failed to read {} stream: {}",
                stream_name_owned, e
            ))
        })?
        .ok_or_else(|| {
            Status::invalid_argument(format!("{} stream is empty", stream_name_owned))
        })?;

    let descriptor = first_message.flight_descriptor.as_ref().ok_or_else(|| {
        Status::invalid_argument(format!(
            "first {} FlightData must include flight descriptor",
            stream_name_owned
        ))
    })?;
    let (session_id, task_id) = parse_descriptor_scope(descriptor)?;
    let dispatch_ctx = dispatch_context_from_flight_metadata(&metadata, &session_id)?;
    authz
        .validate_dispatch_context(&dispatch_ctx)
        .await
        .map_err(map_tonic_status)?;

    if dispatch_ctx.session_id != session_id {
        return Err(Status::permission_denied(
            "flight descriptor session_id does not match auth context",
        ));
    }

    let limits = ingest_backpressure_limits();
    let initial_wire_bytes = flight_data_wire_size_bytes(&first_message);
    if initial_wire_bytes > limits.max_wire_bytes {
        return Err(Status::resource_exhausted(format!(
            "{} stream exceeded wire byte limit: max_wire_bytes={}",
            stream_name_owned, limits.max_wire_bytes
        )));
    }

    let stream_name_for_remaining = stream_name_owned.clone();
    let remaining = stream::try_unfold((input, initial_wire_bytes), move |state| {
        let (mut stream_input, accumulated_wire_bytes) = state;
        let stream_name_for_remaining = stream_name_for_remaining.clone();
        async move {
            match stream_input.message().await {
                Ok(Some(data)) => {
                    let next_wire_bytes =
                        accumulated_wire_bytes.saturating_add(flight_data_wire_size_bytes(&data));
                    if next_wire_bytes > limits.max_wire_bytes {
                        return Err(Status::resource_exhausted(format!(
                            "{} stream exceeded wire byte limit: max_wire_bytes={}",
                            stream_name_for_remaining, limits.max_wire_bytes
                        )));
                    }
                    Ok(Some((data, (stream_input, next_wire_bytes))))
                }
                Ok(None) => Ok(None),
                Err(e) => Err(Status::internal(format!(
                    "failed to read {} stream: {}",
                    stream_name_for_remaining, e
                ))),
            }
        }
    });

    let flight_data_stream = stream::once(async move { Ok::<FlightData, Status>(first_message) })
        .chain(remaining)
        .map_err(Into::into);

    let mut decoder = FlightRecordBatchStream::new_from_flight_data(flight_data_stream);
    let mut decoded_batches = Vec::<RecordBatch>::new();
    let mut decoded_rows = 0usize;

    while let Some(batch) = decoder.next().await {
        let batch = batch.map_err(|e| {
            Status::invalid_argument(format!(
                "failed to decode {} FlightData stream: {}",
                stream_name_owned, e
            ))
        })?;

        if decoded_batches.len() >= limits.max_batches {
            return Err(Status::resource_exhausted(format!(
                "{} stream exceeded batch limit: max_batches={}",
                stream_name_owned, limits.max_batches
            )));
        }

        decoded_rows = decoded_rows.saturating_add(batch.num_rows());
        if decoded_rows > limits.max_rows {
            return Err(Status::resource_exhausted(format!(
                "{} stream exceeded row limit: max_rows={}",
                stream_name_owned, limits.max_rows
            )));
        }

        decoded_batches.push(batch);
    }

    if decoded_batches.is_empty() {
        return Err(Status::invalid_argument(format!(
            "{} stream must include at least one IPC record batch",
            stream_name_owned
        )));
    }

    let result_location =
        resolve_or_create_ingest_result_location(shared_data, &session_id, &task_id).await;

    persist_query_artifacts(
        shared_data,
        &result_location,
        &session_id,
        &task_id,
        &decoded_batches,
    )
    .await
    .map_err(|e| {
        if e.contains("storage provider is not configured") {
            Status::failed_precondition(e)
        } else {
            Status::internal(e)
        }
    })?;

    Ok(())
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

/// What: Parse Arrow data type from metadata-encoded debug string.
///
/// Inputs:
/// - `raw`: Data type string from metadata JSON.
///
/// Output:
/// - Arrow data type supported by current query result metadata contract.
fn parse_metadata_data_type(raw: &str) -> Result<DataType, Status> {
    match raw.trim() {
        "Int16" => Ok(DataType::Int16),
        "Int32" => Ok(DataType::Int32),
        "Int64" => Ok(DataType::Int64),
        "Boolean" => Ok(DataType::Boolean),
        "Utf8" => Ok(DataType::Utf8),
        "Date32" => Ok(DataType::Date32),
        "Date64" => Ok(DataType::Date64),
        "Null" => Ok(DataType::Null),
        other => parse_timestamp_metadata_data_type(other).ok_or_else(|| {
            Status::failed_precondition(format!(
                "unsupported metadata column data_type '{}'",
                other
            ))
        }),
    }
}

fn parse_timestamp_metadata_data_type(raw: &str) -> Option<DataType> {
    let trimmed = raw.trim();
    let inner = trimmed
        .strip_prefix("Timestamp(")?
        .strip_suffix(')')?
        .trim();
    let (unit_raw, timezone_raw) = inner.split_once(',')?;

    let unit = match unit_raw.trim() {
        "Second" => TimeUnit::Second,
        "Millisecond" => TimeUnit::Millisecond,
        "Microsecond" => TimeUnit::Microsecond,
        "Nanosecond" => TimeUnit::Nanosecond,
        _ => return None,
    };

    let timezone = parse_timestamp_timezone(timezone_raw.trim());
    Some(DataType::Timestamp(unit, timezone))
}

fn parse_timestamp_timezone(raw: &str) -> Option<Arc<str>> {
    if raw == "None" {
        return None;
    }

    let value = raw
        .strip_prefix("Some(\"")
        .and_then(|v| v.strip_suffix("\")"))?;
    Some(value.to_string().into())
}

/// What: Build Arrow schema from result metadata columns.
///
/// Inputs:
/// - `metadata`: Parsed query result metadata JSON.
///
/// Output:
/// - Arrow schema reconstructed from metadata column entries.
fn schema_from_metadata(metadata: &serde_json::Value) -> Result<Schema, Status> {
    let columns = metadata
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Status::failed_precondition("result metadata missing columns"))?;

    let mut fields = Vec::with_capacity(columns.len());
    for (idx, column) in columns.iter().enumerate() {
        let name = column
            .get("name")
            .and_then(serde_json::Value::as_str)
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "result metadata column at index {} missing name",
                    idx
                ))
            })?;

        let data_type_raw = column
            .get("data_type")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "result metadata column at index {} missing data_type",
                    idx
                ))
            })?;
        let data_type = parse_metadata_data_type(data_type_raw)?;

        let nullable = column
            .get("nullable")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "result metadata column at index {} missing nullable",
                    idx
                ))
            })?;

        fields.push(Field::new(name, data_type, nullable));
    }

    Ok(Schema::new(fields))
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
        let expected_row_count = metadata
            .get("row_count")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| Status::failed_precondition("result metadata missing row_count"))?;
        if expected_row_count == 0 {
            let schema = schema_from_metadata(&metadata)?;
            all_batches.push(RecordBatch::new_empty(std::sync::Arc::new(schema)));
        } else {
            return Err(Status::not_found(
                "result parquet files contain no record batches",
            ));
        }
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
        let authz = crate::authz::WorkerAuthorizer::new();
        let descriptor = request.get_ref().clone();
        let (session_id, task_id) = parse_descriptor_scope(&descriptor)?;
        let dispatch_ctx = dispatch_context_from_flight_metadata(request.metadata(), &session_id)?;
        authz
            .validate_dispatch_context(&dispatch_ctx)
            .await
            .map_err(map_tonic_status)?;

        if dispatch_ctx.session_id != session_id {
            return Err(Status::permission_denied(
                "flight descriptor session_id does not match auth context",
            ));
        }

        let result_location = self
            .shared_data
            .get_task_result_location(&session_id, &task_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "result location missing or expired for task_id={}",
                    task_id
                ))
            })?;

        let batches =
            load_result_batches(&self.shared_data, &result_location, &session_id, &task_id).await?;
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
        let ticket = authz
            .issue_signed_flight_ticket(
                &dispatch_ctx,
                &task_id,
                &self.shared_data.worker_info.worker_id,
            )
            .map_err(map_tonic_status)?;

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
        let authz = crate::authz::WorkerAuthorizer::new();
        let descriptor = request.get_ref().clone();
        let (session_id, task_id) = parse_descriptor_scope(&descriptor)?;
        let dispatch_ctx = dispatch_context_from_flight_metadata(request.metadata(), &session_id)?;
        authz
            .validate_dispatch_context(&dispatch_ctx)
            .await
            .map_err(map_tonic_status)?;

        if dispatch_ctx.session_id != session_id {
            return Err(Status::permission_denied(
                "flight descriptor session_id does not match auth context",
            ));
        }

        let result_location = self
            .shared_data
            .get_task_result_location(&session_id, &task_id)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "result location missing or expired for task_id={}",
                    task_id
                ))
            })?;

        let batches =
            load_result_batches(&self.shared_data, &result_location, &session_id, &task_id).await?;
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
        let authz = crate::authz::WorkerAuthorizer::new();
        let metadata = request.metadata().clone();
        let ticket = request.into_inner();
        let claims = decode_internal_ticket(&ticket.ticket, &authz)?;

        if metadata.get("rbac_user").is_some() {
            let dispatch_ctx = dispatch_context_from_flight_metadata(&metadata, &claims.sid)?;
            authz
                .validate_dispatch_context(&dispatch_ctx)
                .await
                .map_err(map_tonic_status)?;

            if dispatch_ctx.session_id != claims.sid {
                return Err(Status::permission_denied(
                    "flight auth session does not match ticket session",
                ));
            }
            if dispatch_ctx.rbac_user != claims.sub || dispatch_ctx.rbac_role != claims.role {
                return Err(Status::permission_denied(
                    "flight auth context does not match ticket principal",
                ));
            }
        }

        if claims.wid != self.shared_data.worker_info.worker_id {
            return Err(Status::permission_denied(
                "ticket worker_id does not match this worker",
            ));
        }

        let result_location = self
            .shared_data
            .get_task_result_location(&claims.sid, &claims.tid)
            .await
            .ok_or_else(|| {
                Status::not_found(format!(
                    "result location missing or expired for task_id={}",
                    claims.tid
                ))
            })?;

        let batches = load_result_batches(
            &self.shared_data,
            &result_location,
            &claims.sid,
            &claims.tid,
        )
        .await?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or_else(|| Status::not_found("no record batches available for DoGet"))?;
        let schema_owned = schema.as_ref().clone();

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
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        ingest_flight_stream(&self.shared_data, request, "do_put").await?;

        let output = stream::iter(vec![Ok(PutResult {
            app_metadata: bytes::Bytes::new(),
        })]);
        Ok(Response::new(Box::pin(output)))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        ingest_flight_stream(&self.shared_data, request, "do_exchange").await?;

        let output = stream::empty::<Result<FlightData, Status>>();
        Ok(Response::new(Box::pin(output)))
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
#[path = "../tests/flight_server_tests.rs"]
mod tests;
