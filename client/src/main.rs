use clap::Parser;
// note: serde_yaml not needed at runtime here
use std::fs;
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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "http://localhost:443")]
    server_url: String,
    #[arg(short, long)]
    username: String,
    #[arg(short, long)]
    password: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    // Load the CA certificate
    let ca_cert = fs::read_to_string("/workspace/certs/Kionas-RootCA/Kionas-RootCA.crt")?;
    let ca_certificate = Certificate::from_pem(ca_cert);

    // generate a random database name for testing
    let random_db_name = format!(
        "testdb_{}",
        6 // rand::random::<u16>()
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

    // Perform login to get the JWT token
    let login_request = Request::new(UserPassAuthRequest {
        username: args.username,
        password: args.password,
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
    }

    // an invalid query to test error handling using join(not accepted by the server) to trigger an error response
    let query_invalid = format!(
        "select * from {}.{}.{} t1 join {}.{}.{} t2 on t1.id = t2.id;",
        random_db_name, random_schema_name, random_table_name, random_db_name, random_schema_name, random_table_name
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
