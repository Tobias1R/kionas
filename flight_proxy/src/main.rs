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

const OUTCOME_PREFIX: &str = "RESULT";

fn format_outcome(category: &str, code: &str, message: &str) -> String {
    format!("{}|{}|{}|{}", OUTCOME_PREFIX, category, code, message)
}

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
    let token = std::str::from_utf8(ticket_raw).map_err(|_| {
        Status::invalid_argument(format_outcome(
            "VALIDATION",
            "VALIDATION_FLIGHT_PROXY_TICKET_INVALID_UTF8",
            "ticket is not valid UTF-8",
        ))
    })?;

    let mut parts = token.split('.');
    let _header = parts.next().ok_or_else(|| {
        Status::invalid_argument(format_outcome(
            "VALIDATION",
            "VALIDATION_FLIGHT_PROXY_TICKET_INVALID_FORMAT",
            "invalid signed ticket format",
        ))
    })?;
    let claims = parts.next().ok_or_else(|| {
        Status::invalid_argument(format_outcome(
            "VALIDATION",
            "VALIDATION_FLIGHT_PROXY_TICKET_INVALID_FORMAT",
            "invalid signed ticket format",
        ))
    })?;

    let claims_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(claims)
        .map_err(|_| {
            Status::invalid_argument(format_outcome(
                "VALIDATION",
                "VALIDATION_FLIGHT_PROXY_TICKET_INVALID_BASE64URL",
                "ticket claims are not valid base64url",
            ))
        })?;
    let claims_json: serde_json::Value = serde_json::from_slice(&claims_bytes).map_err(|_| {
        Status::invalid_argument(format_outcome(
            "VALIDATION",
            "VALIDATION_FLIGHT_PROXY_TICKET_INVALID_JSON",
            "ticket claims are not valid JSON",
        ))
    })?;

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
        return Err(Status::invalid_argument(format_outcome(
            "VALIDATION",
            "VALIDATION_FLIGHT_PROXY_TICKET_INVALID_PAYLOAD",
            "ticket payload is invalid; expected session_id:task_id:worker_id",
        )));
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
/// - If worker scope already contains `host:port`, it is treated as an explicit endpoint host.
fn resolve_worker_flight_endpoint(worker_id: &str) -> String {
    if worker_id.contains(':') {
        return format!("http://{}", worker_id);
    }

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
        let cmd = std::str::from_utf8(&descriptor.cmd).map_err(|_| {
            Status::invalid_argument(format_outcome(
                "VALIDATION",
                "VALIDATION_FLIGHT_PROXY_DESCRIPTOR_INVALID_UTF8",
                "descriptor cmd is not valid UTF-8",
            ))
        })?;
        let mut parts = cmd.splitn(3, ':');
        let _session_id = parts.next();
        let _task_id = parts.next();
        let worker_id = parts.next().unwrap_or_default().trim().to_string();
        if !worker_id.is_empty() {
            return Ok(worker_id);
        }
    }

    Err(Status::invalid_argument(format_outcome(
        "VALIDATION",
        "VALIDATION_FLIGHT_PROXY_DESCRIPTOR_SCOPE_MISSING",
        "descriptor must provide worker scope in path[2] or cmd as session:task:worker",
    )))
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
    Status::new(
        status.code(),
        format_outcome(
            "INFRA",
            "INFRA_FLIGHT_PROXY_UPSTREAM_FAILED",
            format!("{}: {}", context, status.message()).as_str(),
        ),
    )
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
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();

        let first = inbound
            .message()
            .await
            .map_err(|e| {
                Status::internal(format_outcome(
                    "INFRA",
                    "INFRA_FLIGHT_PROXY_STREAM_READ_FAILED",
                    format!("failed to read do_put stream: {}", e).as_str(),
                ))
            })?
            .ok_or_else(|| {
                Status::invalid_argument(format_outcome(
                    "VALIDATION",
                    "VALIDATION_FLIGHT_PROXY_STREAM_EMPTY",
                    "do_put stream is empty",
                ))
            })?;

        let descriptor = first.flight_descriptor.as_ref().ok_or_else(|| {
            Status::invalid_argument(format_outcome(
                "VALIDATION",
                "VALIDATION_FLIGHT_PROXY_DESCRIPTOR_SCOPE_MISSING",
                "first do_put FlightData must include descriptor with worker scope",
            ))
        })?;
        let worker_id = parse_descriptor_worker_scope(descriptor)?;

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

        let mut payloads = vec![first];
        while let Some(data) = inbound.message().await.map_err(|e| {
            Status::internal(format_outcome(
                "INFRA",
                "INFRA_FLIGHT_PROXY_STREAM_READ_FAILED",
                format!("failed to read do_put stream: {}", e).as_str(),
            ))
        })? {
            payloads.push(data);
        }

        let mut client = FlightServiceClient::new(channel);
        let mut upstream_request = Request::new(stream::iter(payloads));
        for key in [
            "authorization",
            "session_id",
            "rbac_user",
            "rbac_role",
            "auth_scope",
            "query_id",
        ] {
            if let Some(value) = metadata.get(key) {
                upstream_request.metadata_mut().insert(key, value.clone());
            }
        }

        let upstream = client
            .do_put(upstream_request)
            .await
            .map_err(|e| map_upstream_status(e, "worker DoPut failed"))?
            .into_inner();

        let output = futures::stream::try_unfold(upstream, |mut stream| async move {
            match stream.message().await {
                Ok(Some(result)) => Ok(Some((result, stream))),
                Ok(None) => Ok(None),
                Err(status) => Err(status),
            }
        });

        Ok(Response::new(Box::pin(output)))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        let metadata = request.metadata().clone();
        let mut inbound = request.into_inner();

        let first = inbound
            .message()
            .await
            .map_err(|e| {
                Status::internal(format_outcome(
                    "INFRA",
                    "INFRA_FLIGHT_PROXY_STREAM_READ_FAILED",
                    format!("failed to read do_exchange stream: {}", e).as_str(),
                ))
            })?
            .ok_or_else(|| {
                Status::invalid_argument(format_outcome(
                    "VALIDATION",
                    "VALIDATION_FLIGHT_PROXY_STREAM_EMPTY",
                    "do_exchange stream is empty",
                ))
            })?;

        let descriptor = first.flight_descriptor.as_ref().ok_or_else(|| {
            Status::invalid_argument(format_outcome(
                "VALIDATION",
                "VALIDATION_FLIGHT_PROXY_DESCRIPTOR_SCOPE_MISSING",
                "first do_exchange FlightData must include descriptor with worker scope",
            ))
        })?;
        let worker_id = parse_descriptor_worker_scope(descriptor)?;

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

        let mut payloads = vec![first];
        while let Some(data) = inbound.message().await.map_err(|e| {
            Status::internal(format_outcome(
                "INFRA",
                "INFRA_FLIGHT_PROXY_STREAM_READ_FAILED",
                format!("failed to read do_exchange stream: {}", e).as_str(),
            ))
        })? {
            payloads.push(data);
        }

        let mut client = FlightServiceClient::new(channel);
        let mut upstream_request = Request::new(stream::iter(payloads));
        for key in [
            "authorization",
            "session_id",
            "rbac_user",
            "rbac_role",
            "auth_scope",
            "query_id",
        ] {
            if let Some(value) = metadata.get(key) {
                upstream_request.metadata_mut().insert(key, value.clone());
            }
        }

        let upstream = client
            .do_exchange(upstream_request)
            .await
            .map_err(|e| map_upstream_status(e, "worker DoExchange failed"))?
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

#[cfg(test)]
mod tests {
    use super::{map_upstream_status, parse_descriptor_worker_scope};
    use arrow_flight::flight_service_client::FlightServiceClient;
    use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
    use arrow_flight::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
        HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
    };
    use futures::{Stream, stream};
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tonic14::{Code, Status};
    use tonic14::{Request, Response, Streaming, transport::Channel};

    type TestFlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + 'static>>;

    #[derive(Clone)]
    struct StubWorkerFlightService {
        do_put_calls: Arc<AtomicUsize>,
        do_exchange_calls: Arc<AtomicUsize>,
    }

    #[tonic14::async_trait]
    impl FlightService for StubWorkerFlightService {
        type HandshakeStream = TestFlightStream<HandshakeResponse>;
        type ListFlightsStream = TestFlightStream<FlightInfo>;
        type DoGetStream = TestFlightStream<FlightData>;
        type DoPutStream = TestFlightStream<PutResult>;
        type DoExchangeStream = TestFlightStream<FlightData>;
        type DoActionStream = TestFlightStream<arrow_flight::Result>;
        type ListActionsStream = TestFlightStream<ActionType>;

        async fn handshake(
            &self,
            _request: Request<Streaming<HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented("stub handshake not implemented"))
        }

        async fn list_flights(
            &self,
            _request: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Ok(Response::new(Box::pin(stream::empty())))
        }

        async fn get_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            Err(Status::unimplemented(
                "stub get_flight_info not implemented",
            ))
        }

        async fn poll_flight_info(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<arrow_flight::PollInfo>, Status> {
            Err(Status::unimplemented(
                "stub poll_flight_info not implemented",
            ))
        }

        async fn get_schema(
            &self,
            _request: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented("stub get_schema not implemented"))
        }

        async fn do_get(
            &self,
            _request: Request<Ticket>,
        ) -> Result<Response<Self::DoGetStream>, Status> {
            Err(Status::unimplemented("stub do_get not implemented"))
        }

        async fn do_put(
            &self,
            request: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            self.do_put_calls.fetch_add(1, Ordering::SeqCst);
            let mut inbound = request.into_inner();
            let mut frame_count = 0usize;
            while inbound
                .message()
                .await
                .map_err(|e| Status::internal(format!("stub do_put read failed: {}", e)))?
                .is_some()
            {
                frame_count += 1;
            }

            if frame_count == 0 {
                return Err(Status::invalid_argument(
                    "stub worker received empty do_put",
                ));
            }

            let output = stream::iter(vec![Ok(PutResult {
                app_metadata: Default::default(),
            })]);
            Ok(Response::new(Box::pin(output)))
        }

        async fn do_exchange(
            &self,
            request: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            self.do_exchange_calls.fetch_add(1, Ordering::SeqCst);
            let mut inbound = request.into_inner();
            let mut frame_count = 0usize;
            while inbound
                .message()
                .await
                .map_err(|e| Status::internal(format!("stub do_exchange read failed: {}", e)))?
                .is_some()
            {
                frame_count += 1;
            }

            if frame_count == 0 {
                return Err(Status::invalid_argument(
                    "stub worker received empty do_exchange",
                ));
            }

            let output = stream::iter(vec![Ok(FlightData {
                app_metadata: Default::default(),
                ..Default::default()
            })]);
            Ok(Response::new(Box::pin(output)))
        }

        async fn do_action(
            &self,
            _request: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Ok(Response::new(Box::pin(stream::empty())))
        }

        async fn list_actions(
            &self,
            _request: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Ok(Response::new(Box::pin(stream::empty())))
        }
    }

    async fn start_stub_worker(
        port: u16,
    ) -> (
        Arc<AtomicUsize>,
        Arc<AtomicUsize>,
        tokio::task::JoinHandle<Result<(), tonic14::transport::Error>>,
    ) {
        let do_put_calls = Arc::new(AtomicUsize::new(0));
        let do_exchange_calls = Arc::new(AtomicUsize::new(0));
        let service = StubWorkerFlightService {
            do_put_calls: do_put_calls.clone(),
            do_exchange_calls: do_exchange_calls.clone(),
        };
        let addr: std::net::SocketAddr = format!("127.0.0.1:{}", port)
            .parse()
            .expect("stub worker addr must parse");

        let handle = tokio::spawn(async move {
            tonic14::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve(addr)
                .await
        });

        (do_put_calls, do_exchange_calls, handle)
    }

    async fn connect_proxy_test_client() -> FlightServiceClient<Channel> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("must bind test port");
        let addr = listener.local_addr().expect("local addr must resolve");
        drop(listener);

        tokio::spawn(async move {
            tonic14::transport::Server::builder()
                .add_service(FlightServiceServer::new(super::ProxyFlightService))
                .serve(addr)
                .await
        });

        let endpoint = format!("http://{}", addr);
        let channel = tonic14::transport::Endpoint::from_shared(endpoint)
            .expect("endpoint must parse")
            .connect()
            .await
            .expect("client must connect to proxy");

        FlightServiceClient::new(channel)
    }

    #[test]
    fn wraps_upstream_status_with_canonical_envelope() {
        let upstream = Status::new(Code::Unavailable, "worker connection failed");
        let wrapped = map_upstream_status(upstream, "worker DoGet failed");
        assert_eq!(wrapped.code(), Code::Unavailable);
        assert!(
            wrapped
                .message()
                .starts_with("RESULT|INFRA|INFRA_FLIGHT_PROXY_UPSTREAM_FAILED|")
        );
    }

    #[test]
    fn descriptor_scope_missing_returns_structured_validation_error() {
        let descriptor = FlightDescriptor::new_cmd(Vec::new());
        let err = parse_descriptor_worker_scope(&descriptor)
            .expect_err("missing scope should fail as invalid argument");
        assert_eq!(err.code(), Code::InvalidArgument);
        assert!(
            err.message()
                .starts_with("RESULT|VALIDATION|VALIDATION_FLIGHT_PROXY_DESCRIPTOR_SCOPE_MISSING|")
        );
    }

    #[tokio::test]
    async fn do_put_rejects_empty_stream_with_structured_validation_error() {
        let mut client = connect_proxy_test_client().await;

        let request = Request::new(stream::iter(Vec::<FlightData>::new()));
        let error = client
            .do_put(request)
            .await
            .expect_err("empty do_put stream must be rejected");

        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            error
                .message()
                .starts_with("RESULT|VALIDATION|VALIDATION_FLIGHT_PROXY_STREAM_EMPTY|")
        );
    }

    #[tokio::test]
    async fn do_put_rejects_first_frame_without_descriptor_scope() {
        let mut client = connect_proxy_test_client().await;

        let request = Request::new(stream::iter(vec![FlightData::default()]));
        let error = client
            .do_put(request)
            .await
            .expect_err("do_put without descriptor on first frame must fail");

        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            error
                .message()
                .starts_with("RESULT|VALIDATION|VALIDATION_FLIGHT_PROXY_DESCRIPTOR_SCOPE_MISSING|")
        );
    }

    #[tokio::test]
    async fn do_exchange_rejects_empty_stream_with_structured_validation_error() {
        let mut client = connect_proxy_test_client().await;

        let request = Request::new(stream::iter(Vec::<FlightData>::new()));
        let error = client
            .do_exchange(request)
            .await
            .expect_err("empty do_exchange stream must be rejected");

        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            error
                .message()
                .starts_with("RESULT|VALIDATION|VALIDATION_FLIGHT_PROXY_STREAM_EMPTY|")
        );
    }

    #[tokio::test]
    async fn do_exchange_rejects_first_frame_without_descriptor_scope() {
        let mut client = connect_proxy_test_client().await;

        let request = Request::new(stream::iter(vec![FlightData::default()]));
        let error = client
            .do_exchange(request)
            .await
            .expect_err("do_exchange without descriptor on first frame must fail");

        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(
            error
                .message()
                .starts_with("RESULT|VALIDATION|VALIDATION_FLIGHT_PROXY_DESCRIPTOR_SCOPE_MISSING|")
        );
    }

    #[tokio::test]
    async fn do_put_forwards_to_worker_and_returns_upstream_ack() {
        let worker_port = 32111_u16;
        let (do_put_calls, _do_exchange_calls, worker_handle) =
            start_stub_worker(worker_port).await;

        let mut client = connect_proxy_test_client().await;
        let first = FlightData {
            flight_descriptor: Some(FlightDescriptor::new_path(vec![
                "s1".to_string(),
                "t1".to_string(),
                format!("127.0.0.1:{}", worker_port),
            ])),
            ..Default::default()
        };
        let request = Request::new(stream::iter(vec![first]));
        let response = client
            .do_put(request)
            .await
            .expect("proxy do_put should forward to worker stub");
        let mut stream = response.into_inner();
        let ack = stream
            .message()
            .await
            .expect("ack stream should decode")
            .expect("ack stream should include one result");

        assert!(ack.app_metadata.is_empty());
        assert_eq!(do_put_calls.load(Ordering::SeqCst), 1);

        worker_handle.abort();
    }

    #[tokio::test]
    async fn do_exchange_forwards_to_worker_and_returns_upstream_stream() {
        let worker_port = 32112_u16;
        let (_do_put_calls, do_exchange_calls, worker_handle) =
            start_stub_worker(worker_port).await;

        let mut client = connect_proxy_test_client().await;
        let first = FlightData {
            flight_descriptor: Some(FlightDescriptor::new_path(vec![
                "s1".to_string(),
                "t2".to_string(),
                format!("127.0.0.1:{}", worker_port),
            ])),
            ..Default::default()
        };
        let request = Request::new(stream::iter(vec![first]));
        let response = client
            .do_exchange(request)
            .await
            .expect("proxy do_exchange should forward to worker stub");
        let mut stream = response.into_inner();
        let frame = stream
            .message()
            .await
            .expect("exchange stream should decode")
            .expect("exchange stream should include one frame");

        assert!(frame.app_metadata.is_empty());
        assert_eq!(do_exchange_calls.load(Ordering::SeqCst), 1);

        worker_handle.abort();
    }
}
