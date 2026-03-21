use clap::Parser;
// note: serde_yaml not needed at runtime here
use arrow::record_batch::RecordBatch;
use arrow_cast::pretty::pretty_format_batches;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use base64::Engine;
use futures::{StreamExt, TryStreamExt};
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::str::FromStr;
use tonic::Request;
use tonic::transport::{Certificate, Channel, ClientTlsConfig};

use tonic::metadata::MetadataValue;

pub mod warehouse_service {
    tonic::include_proto!("warehouse_service");
}

pub mod warehouse_auth_service {
    tonic::include_proto!("warehouse_auth_service");
}
use warehouse_auth_service::warehouse_auth_service_client::WarehouseAuthServiceClient;
use warehouse_auth_service::{AuthResponse, UserPassAuthRequest};
use warehouse_service::QueryResponse;

/// What: Parsed components from a structured Flight query handle.
///
/// Inputs:
/// - `endpoint`: Flight endpoint in HTTP scheme for tonic transport.
/// - `session_id`: Session identifier from query handle.
/// - `task_id`: Task identifier from query handle.
/// - `worker_id`: Worker identifier encoded in the query path.
///
/// Output:
/// - Runtime handle details used to build and execute DoGet requests.
///
/// Details:
/// - This struct normalizes parsed handle state so validation and DoGet helpers share one input model.
#[derive(Debug, Clone)]
struct ParsedHandle {
    endpoint: String,
    session_id: String,
    task_id: String,
    worker_id: String,
    signed_ticket: Option<String>,
}

/// What: Decode a query handle from structured response bytes.
///
/// Inputs:
/// - `response`: QueryResponse from warehouse service.
///
/// Output:
/// - Optional UTF-8 query handle when `response.data` is present and non-empty.
///
/// Details:
/// - Returns `None` for empty payloads and for invalid UTF-8 payloads.
fn query_handle_from_response(response: &QueryResponse) -> Option<String> {
    if response.data.is_empty() {
        return None;
    }
    String::from_utf8(response.data.clone())
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// What: Parse structured query handle into endpoint and ticket components.
///
/// Inputs:
/// - `handle`: Structured query handle returned in QueryResponse.data.
///
/// Output:
/// - Parsed handle fields required to call Flight DoGet.
///
/// Details:
/// - Validates scheme, authority, worker path segment, and required query parameters.
fn parse_structured_query_handle(handle: &str) -> Result<ParsedHandle, String> {
    let without_scheme = handle
        .strip_prefix("flight://")
        .ok_or_else(|| "query handle must start with flight://".to_string())?;

    let (authority, path_and_query) = without_scheme
        .split_once('/')
        .ok_or_else(|| "query handle missing path segment".to_string())?;

    if authority.trim().is_empty() {
        return Err("query handle missing authority host:port".to_string());
    }

    let (path, query) = path_and_query
        .split_once('?')
        .ok_or_else(|| "query handle missing query parameters".to_string())?;

    let worker_id = path
        .split('/')
        .next_back()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "query handle missing worker id in path".to_string())?
        .to_string();

    let mut session_id = String::new();
    let mut task_id = String::new();
    let mut signed_ticket = None;
    for pair in query.split('&') {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| format!("invalid query pair in handle: {}", pair))?;
        if k == "session_id" {
            session_id = v.to_string();
        } else if k == "task_id" {
            task_id = v.to_string();
        } else if k == "ticket" {
            signed_ticket = Some(v.to_string());
        }
    }

    if session_id.trim().is_empty() {
        return Err("query handle missing session_id".to_string());
    }
    if task_id.trim().is_empty() {
        return Err("query handle missing task_id".to_string());
    }

    Ok(ParsedHandle {
        endpoint: format!("http://{}", authority),
        session_id,
        task_id,
        worker_id,
        signed_ticket,
    })
}

/// What: Build worker-internal ticket payload from handle components.
///
/// Inputs:
/// - `parsed`: Parsed handle components.
///
/// Output:
/// - URL-safe base64 ticket string expected by worker Flight service.
///
/// Details:
/// - Encodes payload using `session_id:task_id:worker_id` order required by worker ticket validation.
fn build_internal_ticket(parsed: &ParsedHandle) -> String {
    if let Some(ticket) = &parsed.signed_ticket {
        return ticket.clone();
    }

    let payload = format!(
        "{}:{}:{}",
        parsed.session_id, parsed.task_id, parsed.worker_id
    );
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
}

/// What: Execute one Flight DoGet request and fingerprint received payloads.
///
/// Inputs:
/// - `parsed`: Parsed handle details with endpoint and ticket fields.
///
/// Output:
/// - Tuple `(payload_count, fingerprint)` for deterministic comparison.
///
/// Details:
/// - Hashes all payload sections in order to detect non-deterministic stream content changes.
async fn fetch_doget_fingerprint(parsed: &ParsedHandle) -> Result<(usize, u64), String> {
    let endpoint = tonic14::transport::Endpoint::from_shared(parsed.endpoint.clone())
        .map_err(|e| format!("invalid flight endpoint: {}", e))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("failed to connect to flight endpoint: {}", e))?;

    let mut client = FlightServiceClient::new(channel);

    let ticket = Ticket {
        ticket: build_internal_ticket(parsed).into_bytes().into(),
    };

    let mut stream = client
        .do_get(ticket)
        .await
        .map_err(|e| format!("DoGet failed: {}", e))?
        .into_inner();

    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut payload_count = 0usize;

    while let Some(data) = stream
        .message()
        .await
        .map_err(|e| format!("DoGet stream read failed: {}", e))?
    {
        payload_count += 1;
        data.data_header.hash(&mut hasher);
        data.data_body.hash(&mut hasher);
        data.app_metadata.hash(&mut hasher);
    }

    Ok((payload_count, hasher.finish()))
}

/// What: Execute one Flight DoGet request with a custom raw ticket payload.
///
/// Inputs:
/// - `parsed`: Parsed handle fields used to resolve endpoint.
/// - `ticket_bytes`: Raw bytes sent as Ticket.ticket without mutation.
///
/// Output:
/// - `Ok(())` when call succeeds and stream drains.
/// - `Err(message)` when server rejects or transport fails.
///
/// Details:
/// - Used by negative-path tests to send intentionally malformed/tampered ticket bytes.
async fn run_doget_with_custom_ticket(
    parsed: &ParsedHandle,
    ticket_bytes: Vec<u8>,
) -> Result<(), String> {
    let endpoint = tonic14::transport::Endpoint::from_shared(parsed.endpoint.clone())
        .map_err(|e| format!("invalid flight endpoint: {}", e))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("failed to connect to flight endpoint: {}", e))?;

    let mut client = FlightServiceClient::new(channel);
    let ticket = Ticket {
        ticket: ticket_bytes.into(),
    };

    let mut stream = client
        .do_get(ticket)
        .await
        .map_err(|e| format!("DoGet failed: {}", e))?
        .into_inner();

    while stream
        .message()
        .await
        .map_err(|e| format!("DoGet stream read failed: {}", e))?
        .is_some()
    {}

    Ok(())
}

/// What: Validate malformed-ticket rejection behavior on the active Flight endpoint.
///
/// Inputs:
/// - `handle`: Structured handle used only for endpoint resolution.
///
/// Output:
/// - `Ok(())` when malformed ticket is rejected.
/// - `Err(message)` when malformed ticket is unexpectedly accepted.
///
/// Details:
/// - Uses a fixed invalid base64 payload to verify argument validation behavior at the Flight edge.
async fn run_malformed_ticket_check(handle: &str) -> Result<(), String> {
    let parsed = parse_structured_query_handle(handle)?;
    let malformed = b"%%%not_base64%%%".to_vec();

    match run_doget_with_custom_ticket(&parsed, malformed).await {
        Ok(()) => Err("malformed ticket was unexpectedly accepted".to_string()),
        Err(e) => {
            println!("Malformed ticket rejected as expected: {}", e);
            Ok(())
        }
    }
}

/// What: Validate wrong-worker ticket rejection behavior on the active Flight endpoint.
///
/// Inputs:
/// - `handle`: Structured handle used to build a tampered worker scope.
///
/// Output:
/// - `Ok(())` when tampered worker scope is rejected.
/// - `Err(message)` when tampered worker scope is unexpectedly accepted.
///
/// Details:
/// - Appends a suffix to the worker id in ticket payload while preserving session/task to isolate worker-scope rejection.
async fn run_wrong_worker_ticket_check(handle: &str) -> Result<(), String> {
    let parsed = parse_structured_query_handle(handle)?;
    let tampered_payload = format!(
        "{}:{}:{}-tampered",
        parsed.session_id, parsed.task_id, parsed.worker_id
    );
    let tampered_ticket = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .encode(tampered_payload)
        .into_bytes();

    match run_doget_with_custom_ticket(&parsed, tampered_ticket).await {
        Ok(()) => Err("wrong-worker ticket was unexpectedly accepted".to_string()),
        Err(e) => {
            println!("Wrong-worker ticket rejected as expected: {}", e);
            Ok(())
        }
    }
}

/// What: Run deterministic retrieval check by comparing two DoGet attempts.
///
/// Inputs:
/// - `handle`: Structured query handle returned from query dispatch.
///
/// Output:
/// - `Ok(())` when both attempts produce identical payload count and fingerprint.
/// - `Err(message)` when retrieval fails or is non-deterministic.
///
/// Details:
/// - Executes two sequential DoGet calls and compares both count and hash fingerprint.
async fn run_retrieval_determinism_check(handle: &str) -> Result<(), String> {
    let parsed = parse_structured_query_handle(handle)?;
    let first = fetch_doget_fingerprint(&parsed).await?;
    let second = fetch_doget_fingerprint(&parsed).await?;

    if first != second {
        return Err(format!(
            "retrieval is not deterministic: first={:?} second={:?}",
            first, second
        ));
    }

    println!(
        "Deterministic retrieval confirmed: payload_count={} fingerprint={}",
        first.0, first.1
    );
    Ok(())
}

type WarehouseClient = warehouse_service::warehouse_service_client::WarehouseServiceClient<Channel>;

/// What: Create an authenticated warehouse channel using configured server URL.
///
/// Inputs:
/// - `server_url`: Warehouse endpoint URL.
///
/// Output:
/// - Connected tonic channel.
///
/// Details:
/// - Loads root CA from workspace cert path and configures TLS before connecting.
async fn build_warehouse_channel(server_url: &str) -> Result<Channel, String> {
    let ca_cert = fs::read_to_string("/workspace/certs/Kionas-RootCA/Kionas-RootCA.crt")
        .map_err(|e| format!("failed to read CA certificate: {}", e))?;
    let ca_certificate = Certificate::from_pem(ca_cert);
    let tls_config = ClientTlsConfig::new().ca_certificate(ca_certificate);

    tonic::transport::Endpoint::from_shared(server_url.to_string())
        .map_err(|e| format!("invalid server_url '{}': {}", server_url, e))?
        .tls_config(tls_config)
        .map_err(|e| format!("failed to configure TLS: {}", e))?
        .connect()
        .await
        .map_err(|e| format!("failed to connect to warehouse server: {}", e))
}

/// What: Authenticate user and return auth/session payload.
///
/// Inputs:
/// - `channel`: Connected warehouse channel.
/// - `username`: Login username.
/// - `password`: Login password.
///
/// Output:
/// - Auth response containing JWT token and session id.
///
/// Details:
/// - Uses user/password auth RPC and returns token/session for subsequent authenticated queries.
async fn authenticate_user(
    channel: Channel,
    username: &str,
    password: &str,
) -> Result<AuthResponse, String> {
    let mut auth_client = WarehouseAuthServiceClient::new(channel);
    let login_request = Request::new(UserPassAuthRequest {
        username: username.to_string(),
        password: password.to_string(),
    });

    auth_client
        .user_pass_auth(login_request)
        .await
        .map(|resp| resp.into_inner())
        .map_err(|e| format!("authentication failed: {}", e))
}

/// What: Attach auth/session metadata to a warehouse query request.
///
/// Inputs:
/// - `request`: Query request to decorate.
/// - `session_id`: Active session id.
/// - `token`: JWT token.
///
/// Output:
/// - `Ok(())` when metadata is applied.
/// - `Err(message)` when metadata values are invalid.
///
/// Details:
/// - Injects both `session_id` and `authorization` headers required by warehouse query RPC.
fn apply_auth_metadata(
    request: &mut Request<warehouse_service::QueryRequest>,
    session_id: &str,
    token: &str,
) -> Result<(), String> {
    request.metadata_mut().insert(
        "session_id",
        MetadataValue::from_str(session_id)
            .map_err(|e| format!("invalid session metadata value: {}", e))?,
    );

    request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token))
            .map_err(|e| format!("invalid authorization metadata value: {}", e))?,
    );

    Ok(())
}

/// What: Fetch and decode DoGet response batches for a structured query handle.
///
/// Inputs:
/// - `handle`: Structured query handle.
///
/// Output:
/// - Decoded Arrow record batches.
///
/// Details:
/// - Decodes Flight stream frames into Arrow batches using Flight decoder utilities.
async fn fetch_doget_batches(handle: &str) -> Result<Vec<RecordBatch>, String> {
    let parsed = parse_structured_query_handle(handle)?;
    let endpoint = tonic14::transport::Endpoint::from_shared(parsed.endpoint.clone())
        .map_err(|e| format!("invalid flight endpoint: {}", e))?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("failed to connect to flight endpoint: {}", e))?;

    let mut client = FlightServiceClient::new(channel);
    let ticket = Ticket {
        ticket: build_internal_ticket(&parsed).into_bytes().into(),
    };

    let flight_stream = client
        .do_get(ticket)
        .await
        .map_err(|e| format!("DoGet failed: {}", e))?
        .into_inner()
        .map_err(Into::into);

    let mut decoder = FlightRecordBatchStream::new_from_flight_data(flight_stream);
    let mut batches = Vec::new();
    while let Some(batch) = decoder.next().await {
        let batch = batch.map_err(|e| format!("failed to decode Flight batch: {}", e))?;
        batches.push(batch);
    }

    Ok(batches)
}

/// What: Print decoded record batches in a human-readable table format.
///
/// Inputs:
/// - `batches`: Decoded query result batches.
///
/// Output:
/// - `Ok(())` when table is printed.
/// - `Err(message)` when pretty formatting fails.
///
/// Details:
/// - Prints row/column/batch counts after table rendering for quick result inspection.
fn print_batches_as_table(batches: &[RecordBatch]) -> Result<(), String> {
    if batches.is_empty() {
        println!("Result set is empty");
        return Ok(());
    }

    let formatted = pretty_format_batches(batches)
        .map_err(|e| format!("failed to format result batches: {}", e))?;
    println!("{}", formatted);

    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let column_count = batches.first().map(RecordBatch::num_columns).unwrap_or(0);
    println!(
        "rows={} columns={} batches={}",
        row_count,
        column_count,
        batches.len()
    );

    Ok(())
}

/// What: Execute one SQL query and render server + data-plane outputs.
///
/// Inputs:
/// - `warehouse_client`: Warehouse gRPC client.
/// - `session_id`: Active session id.
/// - `token`: JWT token.
/// - `query`: SQL string to execute.
///
/// Output:
/// - `Ok(true)` when query succeeds from the control plane perspective.
/// - `Ok(false)` when query is accepted but returns non-success status/error code.
/// - `Err(message)` on RPC dispatch, metadata, or Flight decode/render failures.
///
/// Details:
/// - Prints control-plane status first, then performs data-plane retrieval when a structured handle is present.
async fn execute_and_render_query(
    warehouse_client: &mut WarehouseClient,
    session_id: &str,
    token: &str,
    query: &str,
) -> Result<bool, String> {
    let trimmed = query.trim();
    if trimmed.is_empty() {
        return Ok(true);
    }

    let mut request = Request::new(warehouse_service::QueryRequest {
        query: trimmed.to_string(),
    });
    apply_auth_metadata(&mut request, session_id, token)?;

    let response = warehouse_client
        .query(request)
        .await
        .map_err(|e| format!("query dispatch failed: {}", e))?
        .into_inner();

    // if status is not OK, we expect error_code and message to be populated for debugging. If status is OK, error_code should be 0 and message may or may not be populated but should not contain critical error info.
    // print the error code and message in either case for visibility, but the client should primarily rely on the status field for control flow decisions.

    if response.error_code != "0".to_string() {
        println!(
            "Query failed with status={} error_code={} message={}",
            response.status, response.error_code, response.message
        );
        return Ok(false);
    } else {
        if let Some(handle) = query_handle_from_response(&response) {
            // println!("Structured query handle: {}", handle);
            let batches = fetch_doget_batches(&handle).await?;
            print_batches_as_table(&batches)?;
        }
        if response.status == "OK".to_string() {
            println!("Query executed successfully");
            return Ok(true);
        } else {
            println!(
                "Query completed with status={} error_code={} message={}",
                response.status, response.error_code, response.message
            );
            return Ok(false);
        }
    }
}

/// What: Split SQL script text into statements while respecting quotes and comments.
///
/// Inputs:
/// - `script`: Raw SQL script text that may contain multiple statements.
///
/// Output:
/// - Ordered SQL statements without trailing semicolons.
///
/// Details:
/// - Ignores semicolons inside single quotes, double quotes, line comments, and block comments.
fn split_sql_statements(script: &str) -> Vec<String> {
    let chars = script.chars().collect::<Vec<char>>();
    let mut out = Vec::<String>::new();
    let mut current = String::new();

    let mut idx = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while idx < chars.len() {
        let ch = chars[idx];
        let next = if idx + 1 < chars.len() {
            Some(chars[idx + 1])
        } else {
            None
        };

        if in_line_comment {
            if ch == '\n' {
                in_line_comment = false;
                current.push(ch);
            }
            idx += 1;
            continue;
        }

        if in_block_comment {
            if ch == '*' && next == Some('/') {
                in_block_comment = false;
                idx += 2;
                continue;
            }
            idx += 1;
            continue;
        }

        if !in_single_quote && !in_double_quote {
            if ch == '-' && next == Some('-') {
                in_line_comment = true;
                idx += 2;
                continue;
            }
            if ch == '/' && next == Some('*') {
                in_block_comment = true;
                idx += 2;
                continue;
            }
        }

        if ch == '\'' && !in_double_quote {
            current.push(ch);
            if in_single_quote {
                if next == Some('\'') {
                    current.push('\'');
                    idx += 2;
                    continue;
                }
                in_single_quote = false;
            } else {
                in_single_quote = true;
            }
            idx += 1;
            continue;
        }

        if ch == '"' && !in_single_quote {
            in_double_quote = !in_double_quote;
            current.push(ch);
            idx += 1;
            continue;
        }

        if ch == ';' && !in_single_quote && !in_double_quote {
            let stmt = current.trim();
            if !stmt.is_empty() {
                out.push(stmt.to_string());
            }
            current.clear();
            idx += 1;
            continue;
        }

        current.push(ch);
        idx += 1;
    }

    let tail = current.trim();
    if !tail.is_empty() {
        out.push(tail.to_string());
    }

    out
}

/// What: Execute a SQL file as an ordered multi-statement script.
///
/// Inputs:
/// - `warehouse_client`: Warehouse gRPC client.
/// - `session_id`: Active session id.
/// - `token`: JWT token.
/// - `query_file`: File path containing SQL statements.
///
/// Output:
/// - `Ok(())` when all statements execute successfully.
/// - `Err(message)` when one or more statements fail or cannot be executed.
///
/// Details:
/// - Continues executing remaining statements after failures and returns a summary error at the end.
async fn run_query_file(
    warehouse_client: &mut WarehouseClient,
    session_id: &str,
    token: &str,
    query_file: &str,
) -> Result<(), String> {
    let script = fs::read_to_string(query_file)
        .map_err(|e| format!("failed to read query file '{}': {}", query_file, e))?;
    let statements = split_sql_statements(&script);

    if statements.is_empty() {
        return Err(format!(
            "query file '{}' contains no executable statements",
            query_file
        ));
    }

    println!(
        "Running {} SQL statements from file: {}",
        statements.len(),
        query_file
    );

    let mut success_count = 0usize;
    let mut failed_indices = Vec::<usize>::new();

    for (index, statement) in statements.iter().enumerate() {
        let ordinal = index + 1;
        println!("\\n--- Statement {}/{} ---", ordinal, statements.len());
        println!("{}", statement);

        match execute_and_render_query(warehouse_client, session_id, token, statement).await {
            Ok(true) => {
                success_count += 1;
                println!("statement {}: SUCCESS", ordinal);
            }
            Ok(false) => {
                failed_indices.push(ordinal);
                println!("statement {}: FAILED (non-success status)", ordinal);
            }
            Err(e) => {
                failed_indices.push(ordinal);
                println!("statement {}: ERROR ({})", ordinal, e);
            }
        }
    }

    println!(
        "\\nExecution summary: total={} success={} failed={}",
        statements.len(),
        success_count,
        failed_indices.len()
    );

    if failed_indices.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "failed statements at indexes: {:?}",
            failed_indices
        ))
    }
}

/// What: Run interactive query loop.
///
/// Inputs:
/// - `warehouse_client`: Warehouse gRPC client.
/// - `session_id`: Active session id.
/// - `token`: JWT token.
///
/// Output:
/// - `Ok(())` when loop exits cleanly.
/// - `Err(message)` on prompt I/O failures.
///
/// Details:
/// - Supports `exit` and `quit` commands and keeps processing subsequent queries after per-query errors.
async fn run_repl(
    warehouse_client: &mut WarehouseClient,
    session_id: &str,
    token: &str,
) -> Result<(), String> {
    println!("Interactive mode ready. Enter SQL and press Enter. Type 'exit' or 'quit' to leave.");

    let stdin = io::stdin();
    loop {
        print!("kionas> ");
        io::stdout()
            .flush()
            .map_err(|e| format!("failed to flush prompt: {}", e))?;

        let mut line = String::new();
        let bytes = stdin
            .read_line(&mut line)
            .map_err(|e| format!("failed to read input: {}", e))?;
        if bytes == 0 {
            println!();
            break;
        }

        let query = line.trim();
        if query.eq_ignore_ascii_case("exit") || query.eq_ignore_ascii_case("quit") {
            break;
        }

        if let Err(e) = execute_and_render_query(warehouse_client, session_id, token, query).await {
            println!("query error: {}", e);
        }
    }

    Ok(())
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "https://kionas-warehouse:443")]
    server_url: String,
    #[arg(short, long)]
    username: Option<String>,
    #[arg(short, long)]
    password: Option<String>,
    #[arg(long, conflicts_with = "query_file")]
    query: Option<String>,
    #[arg(long, conflicts_with = "query")]
    query_file: Option<String>,
    #[arg(long)]
    check_handle: Option<String>,
    #[arg(long)]
    check_malformed_ticket: Option<String>,
    #[arg(long)]
    check_wrong_worker_ticket: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if let Some(handle) = args.check_malformed_ticket.as_deref() {
        match run_malformed_ticket_check(handle).await {
            Ok(()) => println!("Malformed-ticket check passed"),
            Err(e) => println!("Malformed-ticket check failed: {}", e),
        }
        return Ok(());
    }

    if let Some(handle) = args.check_wrong_worker_ticket.as_deref() {
        match run_wrong_worker_ticket_check(handle).await {
            Ok(()) => println!("Wrong-worker-ticket check passed"),
            Err(e) => println!("Wrong-worker-ticket check failed: {}", e),
        }
        return Ok(());
    }

    if let Some(handle) = args.check_handle.as_deref() {
        match run_retrieval_determinism_check(handle).await {
            Ok(()) => println!("Handle retrieval check passed"),
            Err(e) => println!("Handle retrieval check failed: {}", e),
        }
        return Ok(());
    }

    let username = args
        .username
        .as_deref()
        .ok_or("--username is required for query execution modes")?;
    let password = args
        .password
        .as_deref()
        .ok_or("--password is required for query execution modes")?;

    let channel = build_warehouse_channel(&args.server_url).await?;
    let login_response = authenticate_user(channel.clone(), username, password).await?;
    println!("Login Response: {:?}", login_response);

    let token = login_response.token;
    let session_id = login_response.session_id;
    let mut warehouse_client = WarehouseClient::new(channel);

    if let Some(query) = args.query.as_deref() {
        if let Err(e) = execute_and_render_query(&mut warehouse_client, &session_id, &token, query)
            .await
            .map(|_| ())
        {
            println!("query error: {}", e);
        }
        return Ok(());
    }

    if let Some(query_file) = args.query_file.as_deref() {
        if let Err(e) = run_query_file(&mut warehouse_client, &session_id, &token, query_file).await
        {
            println!("query-file error: {}", e);
        }
        return Ok(());
    }

    if let Err(e) = run_repl(&mut warehouse_client, &session_id, &token).await {
        println!("repl error: {}", e);
    }

    Ok(())
}
