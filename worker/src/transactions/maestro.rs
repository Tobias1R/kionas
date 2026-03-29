#[cfg(test)]
type Task = crate::services::worker_service_server::worker_service::StagePartitionExecution;
use crate::state::SharedData;
#[cfg(test)]
use arrow::record_batch::RecordBatch;
use serde_json::Value as JsonValue;
#[cfg(test)]
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) enum InsertScalar {
    Int(i64),
    Bool(bool),
    Str(String),
    Null,
}

#[derive(Clone, Debug)]
pub(crate) struct ParsedInsertPayload {
    pub(crate) table_name: String,
    pub(crate) columns: Vec<String>,
    pub(crate) rows: Vec<Vec<InsertScalar>>,
}

/// What: Build a Delta table URI from storage config and a SQL table name.
///
/// Inputs:
/// - `storage`: Cluster storage JSON config
/// - `table_name`: SQL table identifier
///
/// Output:
/// - Optional `s3://` URI that points to the table root
///
/// Details:
/// - Uses configured bucket and maps canonical `database.schema.table` to
///   `databases/<db>/schemas/<schema>/tables/<table>`.
/// - Preserves a legacy fallback path mapping for non-3-part names.
#[cfg(test)]
fn derive_table_uri_from_storage(storage: &JsonValue, table_name: &str) -> Option<String> {
    crate::transactions::task_executor::derive_table_uri_from_storage(storage, table_name)
}

#[cfg(test)]
#[path = "../tests/transactions_maestro_uri_tests.rs"]
mod uri_tests;

/// What: Parse strict protobuf INSERT payload from task input into typed row values.
///
/// Inputs:
/// - `task`: Insert task containing base64 protobuf payload in `input`.
///
/// Output:
/// - Parsed table name, columns, and rows for Arrow conversion.
///
/// Details:
/// - Rejects missing or malformed payloads with actionable messages.
#[cfg(test)]
fn parse_insert_payload(task: &Task) -> Result<ParsedInsertPayload, String> {
    crate::transactions::insert_contract::parse_insert_payload(task)
}

/// What: Convert parsed INSERT values to an Arrow RecordBatch.
///
/// Inputs:
/// - `parsed`: Parsed insert payload
///
/// Output:
/// - One RecordBatch that can be written to Delta
///
/// Details:
/// - Type inference is per-column: int, bool, otherwise utf8.
#[cfg(test)]
fn parse_insert_column_type_hints(
    task: &crate::services::worker_service_server::worker_service::StagePartitionExecution,
) -> Result<HashMap<String, String>, String> {
    crate::transactions::insert_contract::parse_insert_column_type_hints(task)
}

#[cfg(test)]
fn parse_datetime_literal(raw: &str) -> Result<chrono::NaiveDateTime, String> {
    crate::transactions::insert_coercion::parse_datetime_literal(raw)
}

#[cfg(test)]
fn normalize_decimal_literal(
    raw: &str,
    precision_scale: Option<(u16, u16)>,
) -> Result<String, String> {
    crate::transactions::insert_coercion::normalize_decimal_literal(raw, precision_scale)
}

#[cfg(test)]
#[allow(dead_code)]
fn build_record_batch_from_insert(
    parsed: &ParsedInsertPayload,
    column_type_hints: &HashMap<String, String>,
) -> Result<RecordBatch, String> {
    crate::transactions::insert_record_batch::build_record_batch_from_insert(
        parsed,
        column_type_hints,
    )
}

pub async fn prepare_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
    tasks: &[JsonValue],
) -> Result<(), String> {
    crate::transactions::tx_lifecycle::prepare_tx(shared, tx_id, staging_prefix, tasks).await
}

pub async fn commit_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
) -> Result<(), String> {
    crate::transactions::tx_lifecycle::commit_tx(shared, tx_id, staging_prefix).await
}

pub async fn abort_tx(shared: SharedData, tx_id: &str, staging_prefix: &str) -> Result<(), String> {
    crate::transactions::tx_lifecycle::abort_tx(shared, tx_id, staging_prefix).await
}

pub async fn handle_execute_task(
    shared: SharedData,
    req: crate::services::worker_service_server::worker_service::TaskRequest,
) -> crate::services::worker_service_server::worker_service::TaskResponse {
    crate::transactions::task_executor::handle_execute_task(shared, req).await
}

#[cfg(test)]
#[path = "../tests/transactions_maestro_insert_tests.rs"]
mod insert_tests;
