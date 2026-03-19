use clap::Parser;
// note: serde_yaml not needed at runtime here
use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use base64::Engine;
use std::fs;
use std::hash::{Hash, Hasher};
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
#[derive(Debug, Clone)]
struct ParsedHandle {
    endpoint: String,
    session_id: String,
    task_id: String,
    worker_id: String,
}

/// What: Decode a query handle from structured response bytes.
///
/// Inputs:
/// - `response`: QueryResponse from warehouse service.
///
/// Output:
/// - Optional UTF-8 query handle when `response.data` is present and non-empty.
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
    for pair in query.split('&') {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| format!("invalid query pair in handle: {}", pair))?;
        if k == "session_id" {
            session_id = v.to_string();
        } else if k == "task_id" {
            task_id = v.to_string();
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
    })
}

/// What: Build worker-internal ticket payload from handle components.
///
/// Inputs:
/// - `parsed`: Parsed handle components.
///
/// Output:
/// - URL-safe base64 ticket string expected by worker Flight service.
fn build_internal_ticket(parsed: &ParsedHandle) -> String {
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

/// What: Run deterministic retrieval check by comparing two DoGet attempts.
///
/// Inputs:
/// - `handle`: Structured query handle returned from query dispatch.
///
/// Output:
/// - `Ok(())` when both attempts produce identical payload count and fingerprint.
/// - `Err(message)` when retrieval fails or is non-deterministic.
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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "http://localhost:443")]
    server_url: String,
    #[arg(short, long, required_unless_present = "check_handle")]
    username: Option<String>,
    #[arg(short, long, required_unless_present = "check_handle")]
    password: Option<String>,
    #[arg(long)]
    check_handle: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if let Some(handle) = args.check_handle.as_deref() {
        match run_retrieval_determinism_check(handle).await {
            Ok(()) => println!("Handle retrieval check passed"),
            Err(e) => println!("Handle retrieval check failed: {}", e),
        }
        return Ok(());
    }

    // Load the CA certificate
    let ca_cert = fs::read_to_string("/workspace/certs/Kionas-RootCA/Kionas-RootCA.crt")?;
    let ca_certificate = Certificate::from_pem(ca_cert);

    // generate a random database name for testing
    let random_db_name = format!(
        "testdb_{}",
        7 // rand::random::<u16>()
    );
    // generate a random schema name for testing
    let random_schema_name = format!(
        "testschema_{}",
        2 // rand::random::<u16>()
    );
    // random table name
    let random_table_name = format!(
        "testtable_{}",
        2 //rand::random::<u16>()
    );

    // Configure TLS settings with the CA certificate
    let tls_config = ClientTlsConfig::new().ca_certificate(ca_certificate);

    // Create a channel to the gRPC server with TLS
    let channel = Channel::from_static("https://kionas-warehouse:443")
        .tls_config(tls_config)?
        .connect()
        .await?;

    // Create a client for the authentication service
    let mut auth_client = WarehouseAuthServiceClient::new(channel.clone());

    let username = args
        .username
        .as_deref()
        .ok_or("--username is required unless --check-handle is provided")?;
    let password = args
        .password
        .as_deref()
        .ok_or("--password is required unless --check-handle is provided")?;

    // Perform login to get the JWT token
    let login_request = Request::new(UserPassAuthRequest {
        username: username.to_string(),
        password: password.to_string(),
    });

    let login_response: AuthResponse = auth_client
        .user_pass_auth(login_request)
        .await?
        .into_inner();
    println!("Login Response: {:?}", login_response);

    let token = login_response.token;
    let session_id = login_response.session_id;
    println!("Received JWT Token: {}", token);

    // println!("Enter your query:");
    // let mut query = String::new();
    // io::stdin().read_line(&mut query).expect("Failed to read query");
    // let query = query.trim();
    // println!("Query read: {}", query);

    // if query.starts_with("create schema ") {
    //     let schema_name = query.strip_prefix("create schema ").unwrap().trim_end_matches(';').trim();
    //     let request = warehouse_service::QueryRequest {
    //         query: query.to_string()
    //     };
    //     let mut grpc_request = Request::new(request);
    //     grpc_request.metadata_mut().insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    //     grpc_request.metadata_mut().insert("authorization", MetadataValue::from_str(&format!("Bearer {}", token)).unwrap());
    //     let mut warehouse_client = warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    //     let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    //     println!("Server response: {:?}", response);
    // } else {
    //     println!("Unsupported query type.");
    // }

    let query_use_warehouse = "use warehouse kionas-worker1;";
    let request = warehouse_service::QueryRequest {
        query: query_use_warehouse.to_string(),
    };
    let mut grpc_request = Request::new(request);
    grpc_request
        .metadata_mut()
        .insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    grpc_request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    let mut warehouse_client =
        warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    println!("Server response: {:?}", response);

    // create database
    let querydb = format!("create database {};", random_db_name);

    let reqdb = warehouse_service::QueryRequest {
        query: querydb.to_string(),
    };
    let mut grpc_request = Request::new(reqdb);
    grpc_request
        .metadata_mut()
        .insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    grpc_request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    let mut warehouse_client =
        warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    println!("Server response: {:?}", response);

    let query = format!("create schema {}.{};", random_db_name, random_schema_name);

    let _schema_name = query
        .strip_prefix("create schema ")
        .unwrap()
        .trim_end_matches(';')
        .trim();
    let request = warehouse_service::QueryRequest {
        query: query.to_string(),
    };
    let mut grpc_request = Request::new(request);
    grpc_request
        .metadata_mut()
        .insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    grpc_request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    let mut warehouse_client =
        warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    println!("Server response: {:?}", response);

    // create table
    let query = format!(
        "create table {}.{}.{} (id int, name string);",
        random_db_name, random_schema_name, random_table_name
    );

    let _table_name = query
        .strip_prefix("create table ")
        .unwrap()
        .trim_end_matches(';')
        .trim();
    let request = warehouse_service::QueryRequest {
        query: query.to_string(),
    };
    let mut grpc_request = Request::new(request);
    grpc_request
        .metadata_mut()
        .insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    grpc_request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    let mut warehouse_client =
        warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    println!("Server response: {:?}", response);

    // the so long waited: INSERT command
    let query_insert = format!(
        "insert into {}.{}.{} values (1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David');",
        random_db_name, random_schema_name, random_table_name
    );
    let req_insert = warehouse_service::QueryRequest {
        query: query_insert.to_string(),
    };
    let mut grpc_request = Request::new(req_insert);
    grpc_request
        .metadata_mut()
        .insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    grpc_request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    let mut warehouse_client =
        warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    println!("Server response: {:?}", response);

    // query the inserted data
    let query_select = format!(
        "select * from {}.{}.{};",
        random_db_name, random_schema_name, random_table_name
    );
    let req_select = warehouse_service::QueryRequest {
        query: query_select.to_string(),
    };
    let mut grpc_request = Request::new(req_select);
    grpc_request
        .metadata_mut()
        .insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    grpc_request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    let mut warehouse_client =
        warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    println!("Server response: {:?}", response);
    if let Some(handle) = query_handle_from_response(&response) {
        println!("Structured query handle: {}", handle);
        if let Err(e) = run_retrieval_determinism_check(&handle).await {
            println!("Retrieval determinism check failed: {}", e);
        }
    }

    // an invalid query to test error handling using join(not accepted by the server) to trigger an error response
    let query_invalid = format!(
        "select * from {}.{}.{} t1 join {}.{}.{} t2 on t1.id = t2.id;",
        random_db_name,
        random_schema_name,
        random_table_name,
        random_db_name,
        random_schema_name,
        random_table_name
    );
    let req_invalid = warehouse_service::QueryRequest {
        query: query_invalid.to_string(),
    };
    let mut grpc_request = Request::new(req_invalid);
    grpc_request
        .metadata_mut()
        .insert("session_id", MetadataValue::from_str(&session_id).unwrap());
    grpc_request.metadata_mut().insert(
        "authorization",
        MetadataValue::from_str(&format!("Bearer {}", token)).unwrap(),
    );
    let mut warehouse_client =
        warehouse_service::warehouse_service_client::WarehouseServiceClient::new(channel.clone());
    let response: QueryResponse = warehouse_client.query(grpc_request).await?.into_inner();
    println!("Server response for invalid query: {:?}", response);

    Ok(())
}
