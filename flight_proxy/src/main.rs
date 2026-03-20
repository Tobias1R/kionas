use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};
use base64::Engine;
use futures::{Stream, stream};
use std::error::Error;
use std::net::SocketAddr;
use std::pin::Pin;
use tonic14::{Request, Response, Status, Streaming};

#[derive(Clone, Default)]
struct ProxyFlightService;

type FlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

/// What: Resolve the bind port for the flight proxy service.
///
/// Inputs:
/// - Environment variable `FLIGHT_PROXY_PORT` (optional).
///
/// Output:
/// - Valid non-zero TCP port.
///
/// Details:
/// - Falls back to 32000 when not set or invalid.
fn resolve_flight_proxy_port() -> u16 {
    std::env::var("FLIGHT_PROXY_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|p| *p > 0)
        .unwrap_or(32000)
}

/// What: Resolve the worker Flight service port used by DoGet forwarding.
///
/// Inputs:
/// - Environment variable `WORKER_FLIGHT_PORT` (optional).
///
/// Output:
/// - Valid non-zero worker Flight port.
///
/// Details:
/// - Defaults to 32001 to match current worker conventions.
fn resolve_worker_flight_port() -> u16 {
    std::env::var("WORKER_FLIGHT_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .filter(|p| *p > 0)
        .unwrap_or(32001)
}

/// What: Decode signed ticket payload into scoped identifiers.
///
/// Inputs:
/// - `ticket_raw`: Ticket bytes from Flight `DoGet` request.
///
/// Output:
/// - `(session_id, task_id, worker_id)` when payload is valid.
///
/// Details:
/// - Expected payload format is signed compact JWT with `sid`, `tid`, and `wid` claims.
fn decode_internal_ticket(ticket_raw: &[u8]) -> Result<(String, String, String), Status> {
    let token = std::str::from_utf8(ticket_raw)
        .map_err(|_| Status::invalid_argument("ticket is not valid UTF-8"))?;

    let mut parts = token.split('.');
    let _header = parts
        .next()
        .ok_or_else(|| Status::invalid_argument("invalid signed ticket format"))?;
    let claims = parts
        .next()
        .ok_or_else(|| Status::invalid_argument("invalid signed ticket format"))?;

    let claims_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(claims)
        .map_err(|_| Status::invalid_argument("ticket claims are not valid base64url"))?;
    let claims_json: serde_json::Value = serde_json::from_slice(&claims_bytes)
        .map_err(|_| Status::invalid_argument("ticket claims are not valid JSON"))?;

    let session_id = claims_json
        .get("sid")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    let task_id = claims_json
        .get("tid")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string();
    let worker_id = claims_json
        .get("wid")
        .and_then(serde_json::Value::as_str)
        .map(str::trim)
        .unwrap_or_default()
        .to_string();

    if session_id.is_empty() || task_id.is_empty() || worker_id.is_empty() {
        return Err(Status::invalid_argument(
            "ticket payload is invalid; expected session_id:task_id:worker_id",
        ));
    }

    Ok((session_id, task_id, worker_id))
}

/// What: Build worker Flight endpoint URI for stream forwarding.
///
/// Inputs:
/// - `worker_id`: Worker identifier from ticket scope.
///
/// Output:
/// - URI consumable by tonic endpoint.
///
/// Details:
/// - Uses container DNS host = worker id and configured worker Flight port.
fn resolve_worker_flight_endpoint(worker_id: &str) -> String {
    let port = resolve_worker_flight_port();
    format!("http://{}:{}", worker_id, port)
}

/// What: Parse worker scope from Flight descriptor.
///
/// Inputs:
/// - `descriptor`: Descriptor from Flight metadata calls.
///
/// Output:
/// - `worker_id` extracted from descriptor.
///
/// Details:
/// - Accepts `path` with at least 3 segments where index 2 is worker id,
///   or `cmd` payload in `session_id:task_id:worker_id` format.
fn parse_descriptor_worker_scope(descriptor: &FlightDescriptor) -> Result<String, Status> {
    if descriptor.path.len() >= 3 {
        let worker_id = descriptor.path[2].trim().to_string();
        if !worker_id.is_empty() {
            return Ok(worker_id);
        }
    }

    if !descriptor.cmd.is_empty() {
        let cmd = std::str::from_utf8(&descriptor.cmd)
            .map_err(|_| Status::invalid_argument("descriptor cmd is not valid UTF-8"))?;
        let mut parts = cmd.splitn(3, ':');
        let _session_id = parts.next();
        let _task_id = parts.next();
        let worker_id = parts.next().unwrap_or_default().trim().to_string();
        if !worker_id.is_empty() {
            return Ok(worker_id);
        }
    }

    Err(Status::invalid_argument(
        "descriptor must provide worker scope in path[2] or cmd as session:task:worker",
    ))
}

/// What: Map an upstream Flight error into a proxy status while preserving status code.
///
/// Inputs:
/// - `status`: Upstream tonic status.
/// - `context`: Short operation context for diagnostics.
///
/// Output:
/// - Proxy status with same code and contextualized message.
fn map_upstream_status(status: Status, context: &str) -> Status {
    Status::new(status.code(), format!("{}: {}", context, status.message()))
}

fn copy_auth_metadata<T, U>(from: &Request<T>, to: &mut Request<U>) {
    for key in [
        "authorization",
        "session_id",
        "rbac_user",
        "rbac_role",
        "auth_scope",
        "query_id",
    ] {
        if let Some(value) = from.metadata().get(key) {
            to.metadata_mut().insert(key, value.clone());
        }
    }
}

#[tonic14::async_trait]
impl FlightService for ProxyFlightService {
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
            "flight proxy handshake is not implemented yet",
        ))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.get_ref().clone();
        let worker_id = parse_descriptor_worker_scope(&descriptor)?;

        let worker_endpoint = resolve_worker_flight_endpoint(&worker_id);
        let channel = tonic14::transport::Endpoint::from_shared(worker_endpoint.clone())
            .map_err(|e| Status::invalid_argument(format!("invalid worker endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "failed to connect to worker Flight endpoint {}: {}",
                    worker_endpoint, e
                ))
            })?;

        let mut client = FlightServiceClient::new(channel);
        let mut upstream_request = Request::new(descriptor);
        copy_auth_metadata(&request, &mut upstream_request);
        client
            .get_flight_info(upstream_request)
            .await
            .map_err(|e| map_upstream_status(e, "worker GetFlightInfo failed"))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented(
            "flight proxy poll_flight_info is not implemented yet",
        ))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.get_ref().clone();
        let worker_id = parse_descriptor_worker_scope(&descriptor)?;

        let worker_endpoint = resolve_worker_flight_endpoint(&worker_id);
        let channel = tonic14::transport::Endpoint::from_shared(worker_endpoint.clone())
            .map_err(|e| Status::invalid_argument(format!("invalid worker endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "failed to connect to worker Flight endpoint {}: {}",
                    worker_endpoint, e
                ))
            })?;

        let mut client = FlightServiceClient::new(channel);
        let mut upstream_request = Request::new(descriptor);
        copy_auth_metadata(&request, &mut upstream_request);
        client
            .get_schema(upstream_request)
            .await
            .map_err(|e| map_upstream_status(e, "worker GetSchema failed"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.get_ref().clone();
        let (_, _, worker_id) = decode_internal_ticket(&ticket.ticket)?;

        let worker_endpoint = resolve_worker_flight_endpoint(&worker_id);
        let channel = tonic14::transport::Endpoint::from_shared(worker_endpoint.clone())
            .map_err(|e| Status::invalid_argument(format!("invalid worker endpoint: {}", e)))?
            .connect()
            .await
            .map_err(|e| {
                Status::unavailable(format!(
                    "failed to connect to worker Flight endpoint {}: {}",
                    worker_endpoint, e
                ))
            })?;

        let mut client = FlightServiceClient::new(channel);
        let mut upstream_request = Request::new(ticket);
        copy_auth_metadata(&request, &mut upstream_request);
        let upstream = client
            .do_get(upstream_request)
            .await
            .map_err(|e| map_upstream_status(e, "worker DoGet failed"))?
            .into_inner();

        let output = futures::stream::try_unfold(upstream, |mut stream| async move {
            match stream.message().await {
                Ok(Some(data)) => Ok(Some((data, stream))),
                Ok(None) => Ok(None),
                Err(status) => Err(status),
            }
        });

        Ok(Response::new(Box::pin(output)))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "flight proxy do_put is not implemented yet",
        ))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented(
            "flight proxy do_exchange is not implemented yet",
        ))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let output = stream::iter(vec![Err(Status::unimplemented(
            "flight proxy do_action is not implemented yet",
        ))]);
        Ok(Response::new(Box::pin(output)))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Ok(Response::new(Box::pin(stream::empty())))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    env_logger::init();

    let port = resolve_flight_proxy_port();
    let addr: SocketAddr = format!("0.0.0.0:{}", port)
        .parse()
        .map_err(|e| format!("invalid flight proxy listen addr: {}", e))?;

    log::info!("flight_proxy starting on {}", addr);

    tonic14::transport::Server::builder()
        .add_service(FlightServiceServer::new(ProxyFlightService))
        .serve(addr)
        .await
        .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
}
