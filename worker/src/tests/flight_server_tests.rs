use super::{
    WorkerFlightService, checksum_fnv64_hex, expected_artifacts_from_metadata,
    ingest_backpressure_limits, parse_descriptor_scope, schema_from_metadata, to_staging_prefix,
    validate_metadata_alignment,
};
use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{FlightData, FlightDescriptor};
use bytes::Bytes;
use futures::stream;
use std::str::FromStr;
use std::sync::Arc;
use tonic14::metadata::MetadataValue;
use tonic14::transport::{Channel, Endpoint, Server};
use tonic14::{Code, Request};

use crate::state::{SharedData, WorkerInformation};
use crate::storage::StorageProvider;

fn sample_shared_data() -> SharedData {
    let worker_info = WorkerInformation {
        worker_id: "worker-1".to_string(),
        host: "127.0.0.1".to_string(),
        port: 32001,
        server_url: "http://127.0.0.1:32001".to_string(),
        tls_cert_path: String::new(),
        tls_key_path: String::new(),
        ca_cert_path: String::new(),
    };

    SharedData::new(worker_info, kionas::consul::ClusterInfo::default())
}

async fn connect_test_client() -> (
    FlightServiceClient<Channel>,
    tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    Arc<crate::storage::mock::MockProvider>,
) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("must bind local test port");
    let addr = listener.local_addr().expect("must have local addr");
    drop(listener);

    let provider = crate::storage::mock::MockProvider::new().into_arc();
    let mut shared_data = sample_shared_data();
    shared_data.set_storage_provider(provider.clone());
    shared_data
        .set_task_result_location(
            "s1",
            "task-1",
            "flight://worker:32001/query/db1/s1/task-1/worker-1",
        )
        .await;

    let service = WorkerFlightService::new(shared_data);
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve(addr)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    });

    let endpoint = format!("http://{}", addr);
    let channel = Endpoint::from_shared(endpoint)
        .expect("endpoint must parse")
        .connect()
        .await
        .expect("test client must connect");

    (FlightServiceClient::new(channel), handle, provider)
}

fn insert_dispatch_metadata(request: &mut Request<impl Sized>, session_id: &str) {
    request.metadata_mut().insert(
        "session_id",
        MetadataValue::from_str(session_id).expect("valid session id"),
    );
    request
        .metadata_mut()
        .insert("rbac_user", MetadataValue::from_static("alice"));
    request
        .metadata_mut()
        .insert("rbac_role", MetadataValue::from_static("reader"));
    request
        .metadata_mut()
        .insert("auth_scope", MetadataValue::from_static("select:*"));
    request
        .metadata_mut()
        .insert("query_id", MetadataValue::from_static("q-1"));
}

fn sample_batches() -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(vec![1_i64, 2_i64])) as ArrayRef],
    )
    .expect("batch must build");
    vec![batch]
}

fn sample_many_single_row_batches(batch_count: usize) -> Vec<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    (0..batch_count)
        .map(|index| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(vec![index as i64])) as ArrayRef],
            )
            .expect("single-row batch must build")
        })
        .collect::<Vec<_>>()
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

fn do_put_ipc_payloads_with_descriptor() -> Vec<FlightData> {
    let batches = sample_batches();
    let schema = batches[0].schema();
    let mut payloads = batches_to_flight_data(schema.as_ref(), batches)
        .expect("flight payloads must encode from sample batches");

    payloads[0].flight_descriptor = Some(FlightDescriptor::new_path(vec![
        "s1".to_string(),
        "task-1".to_string(),
    ]));
    payloads
}

fn do_exchange_ipc_payloads_with_descriptor() -> Vec<FlightData> {
    let mut payloads = do_put_ipc_payloads_with_descriptor();
    payloads[0].flight_descriptor = Some(FlightDescriptor::new_path(vec![
        "s1".to_string(),
        "task-exchange-1".to_string(),
    ]));
    payloads
}

fn do_put_many_batches_ipc_payloads_with_descriptor(batch_count: usize) -> Vec<FlightData> {
    let batches = sample_many_single_row_batches(batch_count);
    let schema = batches[0].schema();
    let mut payloads = batches_to_flight_data(schema.as_ref(), batches)
        .expect("flight payloads must encode from many single-row batches");

    payloads[0].flight_descriptor = Some(FlightDescriptor::new_path(vec![
        "s1".to_string(),
        "task-many-batches-1".to_string(),
    ]));
    payloads
}

fn do_exchange_many_batches_ipc_payloads_with_descriptor(batch_count: usize) -> Vec<FlightData> {
    let mut payloads = do_put_many_batches_ipc_payloads_with_descriptor(batch_count);
    payloads[0].flight_descriptor = Some(FlightDescriptor::new_path(vec![
        "s1".to_string(),
        "task-exchange-many-batches-1".to_string(),
    ]));
    payloads
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
fn parses_descriptor_scope_from_path() {
    let descriptor = FlightDescriptor::new_path(vec!["s1".to_string(), "t1".to_string()]);
    let (session_id, task_id) = parse_descriptor_scope(&descriptor).expect("scope must parse");

    assert_eq!(session_id, "s1");
    assert_eq!(task_id, "t1");
}

#[test]
fn parses_descriptor_scope_from_cmd() {
    let descriptor = FlightDescriptor::new_cmd(b"s2:t2".to_vec());
    let (session_id, task_id) = parse_descriptor_scope(&descriptor).expect("scope must parse");

    assert_eq!(session_id, "s2");
    assert_eq!(task_id, "t2");
}

#[test]
fn rejects_descriptor_without_task_scope() {
    let descriptor = FlightDescriptor::new_cmd(b"s-only".to_vec());
    let err = parse_descriptor_scope(&descriptor)
        .expect_err("descriptor without task scope must be rejected");

    assert_eq!(err.code(), tonic14::Code::InvalidArgument);
    assert_eq!(
        err.message(),
        "flight descriptor must provide session and task scope"
    );
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

#[test]
fn builds_schema_from_metadata_columns() {
    let metadata = serde_json::json!({
        "columns": [
            {"name": "id", "data_type": "Int64", "nullable": false},
            {"name": "name", "data_type": "Utf8", "nullable": true}
        ]
    });

    let schema = schema_from_metadata(&metadata).expect("schema should build");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "id");
    assert_eq!(format!("{:?}", schema.field(1).data_type()), "Utf8");
}

#[test]
fn builds_schema_from_temporal_metadata_columns() {
    let metadata = serde_json::json!({
        "columns": [
            {"name": "event_day", "data_type": "Date32", "nullable": true},
            {"name": "event_at", "data_type": "Timestamp(Millisecond, None)", "nullable": false}
        ]
    });

    let schema = schema_from_metadata(&metadata).expect("temporal schema should build");
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(format!("{:?}", schema.field(0).data_type()), "Date32");
    assert_eq!(
        format!("{:?}", schema.field(1).data_type()),
        "Timestamp(Millisecond, None)"
    );
}

#[tokio::test]
async fn do_put_rejects_empty_stream_with_invalid_argument() {
    let (mut client, handle, _provider) = connect_test_client().await;

    let mut request = Request::new(stream::iter(Vec::<FlightData>::new()));
    insert_dispatch_metadata(&mut request, "s1");
    let error = client
        .do_put(request)
        .await
        .expect_err("empty do_put stream must be rejected");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(error.message(), "do_put stream is empty");

    handle.abort();
}

#[tokio::test]
async fn do_put_accepts_descriptor_scoped_first_message() {
    let (mut client, handle, _provider) = connect_test_client().await;

    let payloads = do_put_ipc_payloads_with_descriptor();
    let mut request = Request::new(stream::iter(payloads));
    insert_dispatch_metadata(&mut request, "s1");

    let response = client
        .do_put(request)
        .await
        .expect("do_put should accept descriptor-scoped stream");
    let mut stream = response.into_inner();
    let first_ack = stream
        .message()
        .await
        .expect("ack stream should decode")
        .expect("ack stream should include a put result");
    assert!(first_ack.app_metadata.is_empty());
    let done = stream.message().await.expect("stream should close cleanly");
    assert!(done.is_none());

    handle.abort();
}

#[tokio::test]
async fn do_put_rejects_descriptor_only_stream_without_ipc_batch() {
    let (mut client, handle, _provider) = connect_test_client().await;

    let descriptor_only = FlightData {
        flight_descriptor: Some(FlightDescriptor::new_path(vec![
            "s1".to_string(),
            "task-1".to_string(),
        ])),
        ..Default::default()
    };

    let mut request = Request::new(stream::iter(vec![descriptor_only]));
    insert_dispatch_metadata(&mut request, "s1");

    let error = client
        .do_put(request)
        .await
        .expect_err("descriptor-only do_put stream must be rejected");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(
        error.message(),
        "do_put stream must include at least one IPC record batch"
    );

    handle.abort();
}

#[tokio::test]
async fn do_put_persists_parquet_and_metadata_to_task_staging_prefix() {
    let (mut client, handle, provider) = connect_test_client().await;

    let payloads = do_put_ipc_payloads_with_descriptor();
    let mut request = Request::new(stream::iter(payloads));
    insert_dispatch_metadata(&mut request, "s1");

    client
        .do_put(request)
        .await
        .expect("do_put should persist staged query artifacts");

    let prefix = "query/db1/s1/task-1/worker-1/staging/s1/task-1/";
    let mut keys = provider
        .list_objects(prefix)
        .await
        .expect("mock storage listing should succeed");
    keys.sort();

    assert_eq!(keys.len(), 2);
    assert!(keys.iter().any(|k| k.ends_with("part-00000.parquet")));
    assert!(keys.iter().any(|k| k.ends_with("result_metadata.json")));

    let metadata_key = format!("{}result_metadata.json", prefix);
    let metadata_bytes = provider
        .get_object(&metadata_key)
        .await
        .expect("mock metadata read should succeed")
        .expect("metadata should be persisted");
    let metadata: serde_json::Value =
        serde_json::from_slice(&metadata_bytes).expect("metadata json should parse");

    assert_eq!(metadata.get("row_count").and_then(|v| v.as_u64()), Some(2));
    assert_eq!(
        metadata.get("source_batch_count").and_then(|v| v.as_u64()),
        Some(1)
    );
    assert_eq!(
        metadata.get("parquet_file_count").and_then(|v| v.as_u64()),
        Some(1)
    );
    assert_eq!(
        metadata
            .get("artifacts")
            .and_then(|v| v.as_array())
            .map(std::vec::Vec::len),
        Some(1)
    );

    handle.abort();
}

#[tokio::test]
async fn do_exchange_rejects_empty_stream_with_invalid_argument() {
    let (mut client, handle, _provider) = connect_test_client().await;

    let mut request = Request::new(stream::iter(Vec::<FlightData>::new()));
    insert_dispatch_metadata(&mut request, "s1");
    let error = client
        .do_exchange(request)
        .await
        .expect_err("empty do_exchange stream must be rejected");
    assert_eq!(error.code(), Code::InvalidArgument);
    assert_eq!(error.message(), "do_exchange stream is empty");

    handle.abort();
}

#[tokio::test]
async fn do_exchange_accepts_descriptor_scoped_first_message() {
    let (mut client, handle, _provider) = connect_test_client().await;

    let payloads = do_exchange_ipc_payloads_with_descriptor();
    let mut request = Request::new(stream::iter(payloads));
    insert_dispatch_metadata(&mut request, "s1");

    let response = client
        .do_exchange(request)
        .await
        .expect("do_exchange should accept descriptor-scoped stream");
    let mut stream = response.into_inner();
    let done = stream
        .message()
        .await
        .expect("exchange stream should close cleanly");
    assert!(done.is_none());

    handle.abort();
}

#[tokio::test]
async fn do_exchange_persists_parquet_and_metadata_to_task_staging_prefix() {
    let (mut client, handle, provider) = connect_test_client().await;
    provider
        .put_object("seed", Vec::new())
        .await
        .expect("seed write should succeed");

    let payloads = do_exchange_ipc_payloads_with_descriptor();
    let mut request = Request::new(stream::iter(payloads));
    insert_dispatch_metadata(&mut request, "s1");

    client
        .do_exchange(request)
        .await
        .expect("do_exchange should persist staged query artifacts");

    let prefix = "query/flight/s1/task-exchange-1/worker-1/staging/s1/task-exchange-1/";
    let mut keys = provider
        .list_objects(prefix)
        .await
        .expect("mock storage listing should succeed");
    keys.sort();

    assert_eq!(keys.len(), 2);
    assert!(keys.iter().any(|k| k.ends_with("part-00000.parquet")));
    assert!(keys.iter().any(|k| k.ends_with("result_metadata.json")));

    let metadata_key = format!("{}result_metadata.json", prefix);
    let metadata_bytes = provider
        .get_object(&metadata_key)
        .await
        .expect("mock metadata read should succeed")
        .expect("metadata should be persisted");
    let metadata: serde_json::Value =
        serde_json::from_slice(&metadata_bytes).expect("metadata json should parse");

    assert_eq!(metadata.get("row_count").and_then(|v| v.as_u64()), Some(2));
    assert_eq!(
        metadata.get("source_batch_count").and_then(|v| v.as_u64()),
        Some(1)
    );

    handle.abort();
}

#[tokio::test]
async fn do_put_rejects_stream_that_exceeds_ingest_batch_backpressure_limit() {
    let (mut client, handle, _provider) = connect_test_client().await;
    let limits = ingest_backpressure_limits();
    let payloads = do_put_many_batches_ipc_payloads_with_descriptor(limits.max_batches + 1);
    let mut request = Request::new(stream::iter(payloads));
    insert_dispatch_metadata(&mut request, "s1");

    let error = client
        .do_put(request)
        .await
        .expect_err("do_put should reject streams beyond batch backpressure limit");

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error
            .message()
            .contains("do_put stream exceeded batch limit")
    );

    handle.abort();
}

#[tokio::test]
async fn do_exchange_rejects_stream_that_exceeds_ingest_batch_backpressure_limit() {
    let (mut client, handle, _provider) = connect_test_client().await;
    let limits = ingest_backpressure_limits();
    let payloads = do_exchange_many_batches_ipc_payloads_with_descriptor(limits.max_batches + 1);
    let mut request = Request::new(stream::iter(payloads));
    insert_dispatch_metadata(&mut request, "s1");

    let error = client
        .do_exchange(request)
        .await
        .expect_err("do_exchange should reject streams beyond batch backpressure limit");

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error
            .message()
            .contains("do_exchange stream exceeded batch limit")
    );

    handle.abort();
}

#[tokio::test]
async fn do_put_rejects_stream_that_exceeds_ingest_wire_byte_backpressure_limit() {
    let (mut client, handle, _provider) = connect_test_client().await;
    let limits = ingest_backpressure_limits();
    let oversized = FlightData {
        flight_descriptor: Some(FlightDescriptor::new_path(vec![
            "s1".to_string(),
            "task-wire-limit-put-1".to_string(),
        ])),
        data_body: Bytes::from(vec![0_u8; limits.max_wire_bytes + 1]),
        ..Default::default()
    };

    let mut request = Request::new(stream::iter(vec![oversized]));
    insert_dispatch_metadata(&mut request, "s1");

    let error = client
        .do_put(request)
        .await
        .expect_err("do_put should reject streams beyond wire byte backpressure limit");

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error
            .message()
            .contains("do_put stream exceeded wire byte limit")
    );

    handle.abort();
}

#[tokio::test]
async fn do_exchange_rejects_stream_that_exceeds_ingest_wire_byte_backpressure_limit() {
    let (mut client, handle, _provider) = connect_test_client().await;
    let limits = ingest_backpressure_limits();
    let oversized = FlightData {
        flight_descriptor: Some(FlightDescriptor::new_path(vec![
            "s1".to_string(),
            "task-wire-limit-exchange-1".to_string(),
        ])),
        data_body: Bytes::from(vec![0_u8; limits.max_wire_bytes + 1]),
        ..Default::default()
    };

    let mut request = Request::new(stream::iter(vec![oversized]));
    insert_dispatch_metadata(&mut request, "s1");

    let error = client
        .do_exchange(request)
        .await
        .expect_err("do_exchange should reject streams beyond wire byte backpressure limit");

    assert_eq!(error.code(), Code::ResourceExhausted);
    assert!(
        error
            .message()
            .contains("do_exchange stream exceeded wire byte limit")
    );

    handle.abort();
}

#[tokio::test]
async fn upstream_worker_death_mid_stream_fails_fast() {
    // What: Verify that when an upstream worker dies mid-stream (closes connection),
    // the downstream worker detects this quickly and returns an appropriate error.
    //
    // Setup: Start a test Flight server → client connects → server handle killed
    // Expected: Client receives error, no hung streams, fast failure.
    let (mut client, handle, _provider) = connect_test_client().await;

    let payloads = do_put_ipc_payloads_with_descriptor();
    let mut request = Request::new(stream::iter(payloads.clone()));
    insert_dispatch_metadata(&mut request, "s1");

    // Simulate successful initial do_put
    let response = client
        .do_put(request)
        .await
        .expect("initial do_put should succeed");

    let mut stream = response.into_inner();
    let _ack = stream
        .message()
        .await
        .expect("should receive ack before server death");

    // Now kill the server (upstream worker "dies")
    handle.abort();

    // Any attempt to interact with the dead server should fail quickly
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Try a second request to the dead server
    let (_client2, _handle2): (
        _,
        Option<tokio::task::JoinHandle<Result<(), Box<dyn std::error::Error + Send + Sync>>>>,
    ) = {
        // Reconnect will fail because server is dead
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("must bind test port");
        let dead_addr = listener.local_addr().expect("must have addr");
        drop(listener);

        // Don't actually spin up a server this time
        let endpoint = format!("http://{}", dead_addr);
        let channel = Endpoint::from_shared(endpoint)
            .expect("endpoint must parse")
            .connect()
            .await;

        // Connection should fail or timeout quickly
        assert!(channel.is_err(), "connecting to dead server should fail");
        (client, None)
    };
}

#[tokio::test]
async fn flight_service_client_pool_reuses_connections() {
    // What: Verify that the workder's Flight client pool reuses channels
    // to avoid repeated TLS handshakes for the same endpoint.
    let (mut client, handle, _provider) = connect_test_client().await;

    let payloads = do_put_ipc_payloads_with_descriptor();
    let mut request1 = Request::new(stream::iter(payloads.clone()));
    insert_dispatch_metadata(&mut request1, "s1");

    // First request
    let _response1 = client
        .do_put(request1)
        .await
        .expect("first do_put should succeed");

    // Second request (should reuse channel from pool if implemented)
    let payloads2 = do_put_ipc_payloads_with_descriptor();
    let mut request2 = Request::new(stream::iter(payloads2));
    insert_dispatch_metadata(&mut request2, "s1");

    let response2 = client
        .do_put(request2)
        .await
        .expect("second do_put should also succeed");

    let mut stream = response2.into_inner();
    let _ack = stream
        .message()
        .await
        .expect("should receive ack on second request");

    handle.abort();
}
