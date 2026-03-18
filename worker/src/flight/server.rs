use crate::state::SharedData;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use base64::Engine;
use bytes::Bytes;
use futures::{Stream, stream};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
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

/// What: Build the object-prefix used to find staged parquet files for a task result location.
///
/// Inputs:
/// - `result_location`: URI emitted by task execution.
///
/// Output:
/// - Object prefix path suitable for StorageProvider listing.
///
/// Details:
/// - For current worker writes, parquet payloads are staged under `<table_root>/staging/`.
fn to_staging_prefix(result_location: &str) -> Result<String, Status> {
    let parsed = Url::parse(result_location).map_err(|e| {
        Status::invalid_argument(format!("result_location is not a valid URI: {}", e))
    })?;

    let path = parsed.path().trim_start_matches('/').trim_end_matches('/');
    if path.is_empty() {
        return Err(Status::invalid_argument(
            "result_location path is empty; cannot resolve parquet objects",
        ));
    }

    Ok(format!("{}/staging/", path))
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

/// What: Load all staged parquet batches for a worker task result location.
///
/// Inputs:
/// - `shared_data`: Worker shared state containing storage provider.
/// - `result_location`: Resolved task result location URI.
///
/// Output:
/// - Concatenated Arrow batches read from parquet objects.
///
/// Details:
/// - Reads all `.parquet` objects found under `<result_location>/staging/`.
async fn load_result_batches(
    shared_data: &SharedData,
    result_location: &str,
) -> Result<Vec<RecordBatch>, Status> {
    let provider = shared_data
        .storage_provider
        .as_ref()
        .ok_or_else(|| Status::failed_precondition("storage provider is not configured"))?;

    let prefix = to_staging_prefix(result_location)?;
    let mut keys = provider
        .list_objects(&prefix)
        .await
        .map_err(|e| Status::internal(format!("failed to list objects for {}: {}", prefix, e)))?;

    keys.retain(|k| k.ends_with(".parquet"));
    keys.sort();

    if keys.is_empty() {
        return Err(Status::not_found(format!(
            "no parquet result files found for prefix {}",
            prefix
        )));
    }

    let mut all_batches = Vec::new();
    for key in keys {
        let bytes = provider
            .get_object(&key)
            .await
            .map_err(|e| Status::internal(format!("failed to read object {}: {}", key, e)))?
            .ok_or_else(|| Status::not_found(format!("result object not found: {}", key)))?;

        let batches = decode_parquet_batches(bytes)?;
        all_batches.extend(batches);
    }

    if all_batches.is_empty() {
        return Err(Status::not_found(
            "result parquet files contain no record batches",
        ));
    }

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
        Err(Status::unimplemented(
            "worker Flight list_flights is not implemented yet",
        ))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "worker Flight get_flight_info is not implemented yet",
        ))
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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented(
            "worker Flight get_schema is not implemented yet",
        ))
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

        let batches = load_result_batches(&self.shared_data, &result_location).await?;
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
