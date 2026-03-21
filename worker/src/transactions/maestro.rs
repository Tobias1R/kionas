use crate::state::SharedData;
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use kionas::parser::datafusion_sql::sqlparser::ast::{Expr, SetExpr, Statement, Value as SqlValue};
use kionas::parser::sql::parse_query;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::sync::Arc;

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

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
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
fn derive_table_uri_from_storage(storage: &JsonValue, table_name: &str) -> Option<String> {
    fn normalize_segment(raw: &str) -> String {
        raw.trim()
            .trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']')
            .to_ascii_lowercase()
    }

    let bucket = storage.get("bucket").and_then(|v| v.as_str())?.trim();
    if bucket.is_empty() {
        return None;
    }

    let clean_table = table_name.trim();
    let parts = clean_table
        .split('.')
        .map(normalize_segment)
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>();

    if parts.len() == 3 {
        return Some(format!(
            "s3://{}/databases/{}/schemas/{}/tables/{}",
            bucket, parts[0], parts[1], parts[2]
        ));
    }

    let clean_table = clean_table
        .trim_matches('"')
        .trim_matches('`')
        .replace('.', "/");
    if clean_table.is_empty() {
        return None;
    }
    Some(format!("s3://{}/{}", bucket, clean_table))
}

#[cfg(test)]
mod tests {
    use super::derive_table_uri_from_storage;
    use serde_json::json;

    #[test]
    fn derive_insert_uri_uses_canonical_three_part_layout() {
        let storage = json!({ "bucket": "warehouse" });
        let uri = derive_table_uri_from_storage(&storage, "testdb_4.testschema_2.testtable_2")
            .expect("uri must be derived");
        assert_eq!(
            uri,
            "s3://warehouse/databases/testdb_4/schemas/testschema_2/tables/testtable_2"
        );
    }

    #[test]
    fn derive_insert_uri_normalizes_quoted_three_part_layout() {
        let storage = json!({ "bucket": "warehouse" });
        let uri = derive_table_uri_from_storage(&storage, "\"TestDB\".\"Schema\".\"Table\"")
            .expect("uri must be derived");
        assert_eq!(
            uri,
            "s3://warehouse/databases/testdb/schemas/schema/tables/table"
        );
    }
}

/// What: Parse an INSERT SQL payload into table metadata and row values.
///
/// Inputs:
/// - `sql`: Raw SQL payload from task input
///
/// Output:
/// - Parsed table name, optional column list and VALUES rows
///
/// Details:
/// - Supports INSERT statements where source is a VALUES clause.
fn parse_insert_payload(sql: &str) -> Result<ParsedInsertPayload, String> {
    let statements = parse_query(sql).map_err(|e| format!("insert parse error: {}", e))?;
    let stmt = statements
        .first()
        .ok_or_else(|| "insert parse error: empty statement list".to_string())?;

    let insert = match stmt {
        Statement::Insert(insert_stmt) => insert_stmt,
        _ => return Err("task payload is not an INSERT statement".to_string()),
    };

    let source = insert
        .source
        .as_ref()
        .ok_or_else(|| "INSERT source is missing".to_string())?;

    let values = match source.body.as_ref() {
        SetExpr::Values(values) => values,
        _ => {
            return Err(
                "INSERT source is not VALUES; only VALUES inserts are supported in worker"
                    .to_string(),
            );
        }
    };

    if values.rows.is_empty() {
        return Err("INSERT VALUES has no rows".to_string());
    }

    let mut rows = Vec::with_capacity(values.rows.len());
    for row in &values.rows {
        let mut parsed_row = Vec::with_capacity(row.len());
        for expr in row {
            let scalar = match expr {
                Expr::Value(vws) => match &vws.value {
                    SqlValue::Number(n, _) => n
                        .parse::<i64>()
                        .map(InsertScalar::Int)
                        .unwrap_or_else(|_| InsertScalar::Str(n.clone())),
                    SqlValue::Boolean(b) => InsertScalar::Bool(*b),
                    SqlValue::Null => InsertScalar::Null,
                    other => InsertScalar::Str(other.to_string()),
                },
                other => InsertScalar::Str(other.to_string()),
            };
            parsed_row.push(scalar);
        }
        rows.push(parsed_row);
    }

    let column_count = rows.first().map(|r| r.len()).unwrap_or(0);
    if column_count == 0 {
        return Err("INSERT VALUES produced zero columns".to_string());
    }
    if rows.iter().any(|r| r.len() != column_count) {
        return Err("INSERT VALUES row width mismatch".to_string());
    }

    let columns = if insert.columns.is_empty() {
        (1..=column_count).map(|i| format!("c{}", i)).collect()
    } else {
        insert.columns.iter().map(ToString::to_string).collect()
    };

    Ok(ParsedInsertPayload {
        table_name: insert.table.to_string(),
        columns,
        rows,
    })
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
fn parse_insert_column_type_hints(
    task: &crate::services::worker_service_server::worker_service::Task,
) -> Result<HashMap<String, String>, String> {
    let strict_contract = task
        .params
        .get("datatype_contract_version")
        .map(|value| !value.trim().is_empty() && value.trim() != "0")
        .unwrap_or(false);

    let Some(raw) = task.params.get("column_type_hints_json") else {
        if strict_contract {
            return Err(
                "INSERT_TYPE_HINTS_MALFORMED: missing column_type_hints_json for datatype contract"
                    .to_string(),
            );
        }
        return Ok(HashMap::new());
    };

    let hints = serde_json::from_str::<HashMap<String, String>>(raw)
        .map(|value| {
            value
                .into_iter()
                .map(|(k, v)| (normalize_identifier(&k), v))
                .filter(|(k, _)| !k.is_empty())
                .collect::<HashMap<_, _>>()
        })
        .map_err(|e| {
            format!(
                "INSERT_TYPE_HINTS_MALFORMED: invalid column_type_hints_json: {}",
                e
            )
        })?;

    if strict_contract && hints.is_empty() {
        return Err(
            "INSERT_TYPE_HINTS_MALFORMED: column_type_hints_json cannot be empty when datatype contract is enabled"
                .to_string(),
        );
    }

    Ok(hints)
}

fn strip_sql_literal_quotes(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0] as char;
        let last = bytes[trimmed.len() - 1] as char;
        if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

fn parse_timestamp_millis_literal(raw: &str) -> Option<i64> {
    let unquoted = strip_sql_literal_quotes(raw);
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&unquoted) {
        return Some(parsed.timestamp_millis());
    }
    if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(&unquoted, "%Y-%m-%d %H:%M:%S") {
        return Some(parsed.and_utc().timestamp_millis());
    }
    if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(&unquoted, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(parsed.and_utc().timestamp_millis());
    }
    None
}

fn parse_datetime_literal(raw: &str) -> Result<chrono::NaiveDateTime, String> {
    let unquoted = strip_sql_literal_quotes(raw);
    let lower = unquoted.to_ascii_lowercase();
    if lower.ends_with('z')
        || lower.contains('+')
        || (lower.matches('-').count() > 2 && lower.contains('t'))
    {
        return Err(
            "DATETIME_TIMEZONE_NOT_ALLOWED: DATETIME literals must not include timezone offsets"
                .to_string(),
        );
    }
    if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(&unquoted, "%Y-%m-%d %H:%M:%S") {
        return Ok(parsed);
    }
    if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(&unquoted, "%Y-%m-%d %H:%M:%S%.f") {
        return Ok(parsed);
    }
    Err(format!(
        "TEMPORAL_LITERAL_INVALID: invalid DATETIME literal '{}'",
        unquoted
    ))
}

fn format_datetime_literal(value: chrono::NaiveDateTime) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn parse_decimal_precision_scale(declared: &str) -> Option<(u16, u16)> {
    let lower = declared.to_ascii_lowercase();
    let start = lower.find('(')?;
    let end = lower.rfind(')')?;
    if end <= start + 1 {
        return None;
    }

    let inner = &lower[start + 1..end];
    let parts = inner
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 2 {
        return None;
    }

    let precision = parts[0].parse::<u16>().ok()?;
    let scale = parts[1].parse::<u16>().ok()?;
    Some((precision, scale))
}

fn normalize_decimal_literal(
    raw: &str,
    precision_scale: Option<(u16, u16)>,
) -> Result<String, String> {
    let mut value = strip_sql_literal_quotes(raw);
    if value.is_empty() {
        return Err("DECIMAL_COERCION_FAILED: empty decimal literal".to_string());
    }

    if value.contains('e') || value.contains('E') {
        return Err(format!(
            "DECIMAL_COERCION_FAILED: scientific notation is not supported for decimal literal '{}'",
            value
        ));
    }

    let sign = if value.starts_with('-') {
        value = value.trim_start_matches('-').to_string();
        "-"
    } else if value.starts_with('+') {
        value = value.trim_start_matches('+').to_string();
        ""
    } else {
        ""
    };

    let parts = value.split('.').collect::<Vec<_>>();
    if parts.len() > 2 || parts.is_empty() {
        return Err(format!(
            "DECIMAL_COERCION_FAILED: invalid decimal literal '{}'",
            value
        ));
    }

    let integer_part = parts[0];
    let fraction_part = if parts.len() == 2 { parts[1] } else { "" };
    if integer_part.is_empty() && fraction_part.is_empty() {
        return Err("DECIMAL_COERCION_FAILED: invalid decimal literal".to_string());
    }
    if !integer_part.chars().all(|ch| ch.is_ascii_digit())
        || !fraction_part.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(format!(
            "DECIMAL_COERCION_FAILED: invalid decimal literal '{}'",
            value
        ));
    }

    let mut normalized_integer = integer_part.trim_start_matches('0').to_string();
    if normalized_integer.is_empty() {
        normalized_integer = "0".to_string();
    }

    let mut normalized_fraction = fraction_part.to_string();
    if let Some((precision, scale)) = precision_scale {
        if normalized_fraction.len() > usize::from(scale) {
            return Err(format!(
                "DECIMAL_COERCION_FAILED: literal '{}' exceeds scale {}",
                value, scale
            ));
        }
        while normalized_fraction.len() < usize::from(scale) {
            normalized_fraction.push('0');
        }

        let digits_total = normalized_integer
            .chars()
            .filter(|ch| ch.is_ascii_digit())
            .count()
            + normalized_fraction
                .chars()
                .filter(|ch| ch.is_ascii_digit())
                .count();
        if digits_total > usize::from(precision) {
            return Err(format!(
                "DECIMAL_COERCION_FAILED: literal '{}' exceeds precision {}",
                value, precision
            ));
        }
    }

    if normalized_fraction.is_empty() {
        Ok(format!("{}{}", sign, normalized_integer))
    } else {
        Ok(format!(
            "{}{}.{}",
            sign, normalized_integer, normalized_fraction
        ))
    }
}

fn build_record_batch_from_insert(
    parsed: &ParsedInsertPayload,
    column_type_hints: &HashMap<String, String>,
) -> Result<RecordBatch, String> {
    let row_count = parsed.rows.len();
    let col_count = parsed.columns.len();
    if row_count == 0 || col_count == 0 {
        return Err("insert payload has no rows or columns".to_string());
    }

    let mut fields = Vec::with_capacity(col_count);
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(col_count);
    let strict_hints = !column_type_hints.is_empty();

    for col_idx in 0..col_count {
        let mut only_int = true;
        let mut only_bool = true;
        let normalized_column = normalize_identifier(&parsed.columns[col_idx]);
        let hinted_type = column_type_hints
            .get(&normalized_column)
            .map(|v| v.to_ascii_lowercase());

        if strict_hints && hinted_type.is_none() {
            return Err(format!(
                "INSERT_TYPE_HINTS_MALFORMED: missing type hint for column '{}' while datatype contract is enabled",
                parsed.columns[col_idx]
            ));
        }

        if let Some(hint) = hinted_type.as_deref() {
            if hint.contains("timestamp") {
                let mut values: Vec<Option<i64>> = Vec::with_capacity(row_count);
                for (row_idx, row) in parsed.rows.iter().enumerate() {
                    match &row[col_idx] {
                        InsertScalar::Null => values.push(None),
                        InsertScalar::Int(v) => values.push(Some(*v)),
                        InsertScalar::Str(v) => {
                            if let Some(ts_ms) = parse_timestamp_millis_literal(v) {
                                values.push(Some(ts_ms));
                            } else {
                                return Err(format!(
                                    "TEMPORAL_LITERAL_INVALID: invalid TIMESTAMP literal '{}' for column '{}' at row {}",
                                    strip_sql_literal_quotes(v),
                                    parsed.columns[col_idx],
                                    row_idx + 1
                                ));
                            }
                        }
                        other => {
                            return Err(format!(
                                "TEMPORAL_LITERAL_INVALID: unsupported TIMESTAMP value '{:?}' for column '{}' at row {}",
                                other,
                                parsed.columns[col_idx],
                                row_idx + 1
                            ));
                        }
                    }
                }
                fields.push(Field::new(
                    &parsed.columns[col_idx],
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    true,
                ));
                arrays.push(Arc::new(TimestampMillisecondArray::from(values)) as ArrayRef);
                continue;
            }

            if hint.contains("datetime") {
                let mut values: Vec<Option<String>> = Vec::with_capacity(row_count);
                for (row_idx, row) in parsed.rows.iter().enumerate() {
                    match &row[col_idx] {
                        InsertScalar::Null => values.push(None),
                        InsertScalar::Str(v) => {
                            let parsed_dt = parse_datetime_literal(v)?;
                            values.push(Some(format_datetime_literal(parsed_dt)));
                        }
                        InsertScalar::Int(v) => {
                            let parsed_dt = chrono::DateTime::from_timestamp_millis(*v)
                                .ok_or_else(|| {
                                    format!(
                                        "TEMPORAL_LITERAL_INVALID: invalid DATETIME epoch '{}' for column '{}' at row {}",
                                        v,
                                        parsed.columns[col_idx],
                                        row_idx + 1
                                    )
                                })?
                                .naive_utc();
                            values.push(Some(format_datetime_literal(parsed_dt)));
                        }
                        other => {
                            return Err(format!(
                                "TEMPORAL_LITERAL_INVALID: unsupported DATETIME value '{:?}' for column '{}' at row {}",
                                other,
                                parsed.columns[col_idx],
                                row_idx + 1
                            ));
                        }
                    }
                }
                let refs: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
                fields.push(Field::new(&parsed.columns[col_idx], DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(refs)) as ArrayRef);
                continue;
            }

            if hint.contains("decimal") || hint.contains("numeric") {
                let precision_scale = parse_decimal_precision_scale(hint);
                let mut values: Vec<Option<String>> = Vec::with_capacity(row_count);
                for (row_idx, row) in parsed.rows.iter().enumerate() {
                    match &row[col_idx] {
                        InsertScalar::Null => values.push(None),
                        InsertScalar::Int(v) => {
                            let normalized =
                                normalize_decimal_literal(&v.to_string(), precision_scale)?;
                            values.push(Some(normalized));
                        }
                        InsertScalar::Str(v) => {
                            let normalized = normalize_decimal_literal(v, precision_scale)
                                .map_err(|e| {
                                    format!(
                                        "{} for column '{}' at row {}",
                                        e,
                                        parsed.columns[col_idx],
                                        row_idx + 1
                                    )
                                })?;
                            values.push(Some(normalized));
                        }
                        other => {
                            return Err(format!(
                                "DECIMAL_COERCION_FAILED: unsupported decimal value '{:?}' for column '{}' at row {}",
                                other,
                                parsed.columns[col_idx],
                                row_idx + 1
                            ));
                        }
                    }
                }
                let refs: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
                fields.push(Field::new(&parsed.columns[col_idx], DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(refs)) as ArrayRef);
                continue;
            }
        }

        for row in &parsed.rows {
            match &row[col_idx] {
                InsertScalar::Int(_) | InsertScalar::Null => {}
                _ => only_int = false,
            }
            match &row[col_idx] {
                InsertScalar::Bool(_) | InsertScalar::Null => {}
                _ => only_bool = false,
            }
        }

        if only_int {
            let mut values: Vec<Option<i64>> = Vec::with_capacity(row_count);
            for row in &parsed.rows {
                match &row[col_idx] {
                    InsertScalar::Int(v) => values.push(Some(*v)),
                    InsertScalar::Null => values.push(None),
                    _ => values.push(None),
                }
            }
            fields.push(Field::new(&parsed.columns[col_idx], DataType::Int64, true));
            arrays.push(Arc::new(Int64Array::from(values)) as ArrayRef);
        } else if only_bool {
            let mut values: Vec<Option<bool>> = Vec::with_capacity(row_count);
            for row in &parsed.rows {
                match &row[col_idx] {
                    InsertScalar::Bool(v) => values.push(Some(*v)),
                    InsertScalar::Null => values.push(None),
                    _ => values.push(None),
                }
            }
            fields.push(Field::new(
                &parsed.columns[col_idx],
                DataType::Boolean,
                true,
            ));
            arrays.push(Arc::new(BooleanArray::from(values)) as ArrayRef);
        } else {
            let mut values: Vec<Option<String>> = Vec::with_capacity(row_count);
            for row in &parsed.rows {
                match &row[col_idx] {
                    InsertScalar::Str(v) => values.push(Some(v.clone())),
                    InsertScalar::Int(v) => values.push(Some(v.to_string())),
                    InsertScalar::Bool(v) => values.push(Some(v.to_string())),
                    InsertScalar::Null => values.push(None),
                }
            }
            let refs: Vec<Option<&str>> = values.iter().map(|v| v.as_deref()).collect();
            fields.push(Field::new(&parsed.columns[col_idx], DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from(refs)) as ArrayRef);
        }
    }

    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("failed to build insert record batch: {}", e))
}

/// Ensure or lazily initialize the interops pool and return a cloned Arc to it.
async fn ensure_pool(
    shared: SharedData,
) -> Option<std::sync::Arc<deadpool::managed::Pool<crate::interops::InteropsManager>>> {
    let mut guard = shared.master_pool.lock().await;
    if guard.is_none() {
        let info = shared.worker_info.clone();
        let manager = crate::interops::InteropsManager {
            addr: info.server_url.clone(),
            ca_cert_path: Some(info.ca_cert_path.clone()),
            tls_cert_path: Some(info.tls_cert_path.clone()),
            tls_key_path: Some(info.tls_key_path.clone()),
        };
        let pool_size: usize = std::env::var("MASTER_POOL_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(5);
        match deadpool::managed::Pool::builder(manager)
            .max_size(pool_size)
            .build()
        {
            Ok(p) => {
                let arcp = std::sync::Arc::new(p);
                *guard = Some(arcp.clone());
                Some(arcp)
            }
            Err(e) => {
                log::error!(
                    "Failed to build interops pool in transactions::ensure_pool: {}",
                    e
                );
                None
            }
        }
    } else {
        guard.as_ref().map(|p| p.clone())
    }
}

pub async fn prepare_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
    tasks: &[JsonValue],
) -> Result<(), String> {
    if let Some(provider) = shared.storage_provider.clone() {
        // stage_tx returns a Vec<String> of staged keys; ignore result and map errors
        match crate::storage::staging::stage_tx(provider, tx_id, staging_prefix, tasks).await {
            Ok(_keys) => Ok(()),
            Err(e) => Err(format!("{}", e)),
        }
    } else {
        // fallback to local filesystem behavior
        let staging_dir = format!("worker_storage/staging/{}/{}", staging_prefix, tx_id);
        if let Err(e) = tokio::fs::create_dir_all(&staging_dir).await {
            return Err(format!(
                "failed to create staging dir {}: {}",
                staging_dir, e
            ));
        }
        for t in tasks.iter() {
            if let Some(task_id) = t.get("task_id").and_then(|v| v.as_str()) {
                let task_file = format!("{}/{}.json", staging_dir, task_id);
                let payload =
                    serde_json::to_vec_pretty(t).unwrap_or_else(|_| t.to_string().into_bytes());
                if let Err(e) = tokio::fs::write(&task_file, payload).await {
                    return Err(format!(
                        "failed to write staged task file {}: {}",
                        task_file, e
                    ));
                }
            }
        }
        let manifest = serde_json::json!({
            "tx_id": tx_id,
            "staging_prefix": staging_prefix,
            "tasks": tasks.iter().filter_map(|t| t.get("task_id").and_then(|v| v.as_str().map(|s| s.to_string()))).collect::<Vec<String>>(),
            "created_at": chrono::Utc::now().to_rfc3339(),
        });
        let manifest_path = format!("{}/manifest_{}.json", staging_dir, tx_id);
        if let Err(e) = tokio::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest)
                .unwrap_or_else(|_| manifest.to_string().into_bytes()),
        )
        .await
        {
            return Err(format!("failed to write manifest {}: {}", manifest_path, e));
        }
        Ok(())
    }
}

pub async fn commit_tx(
    shared: SharedData,
    tx_id: &str,
    staging_prefix: &str,
) -> Result<(), String> {
    if let Some(provider) = shared.storage_provider.clone() {
        crate::storage::staging::promote_tx(provider, tx_id, staging_prefix)
            .await
            .map_err(|e| format!("{}", e))
    } else {
        let staging_dir = format!("worker_storage/staging/{}/{}", staging_prefix, tx_id);
        let final_parent = format!("worker_storage/final/{}", staging_prefix);
        let final_dir = format!("{}/{}", final_parent, tx_id);
        if let Err(e) = tokio::fs::create_dir_all(&final_parent).await {
            return Err(format!(
                "failed to create final parent {}: {}",
                final_parent, e
            ));
        }
        if let Err(e) = tokio::fs::rename(&staging_dir, &final_dir).await {
            return Err(format!(
                "failed to promote staging {} -> {}: {}",
                staging_dir, final_dir, e
            ));
        }
        let manifest = serde_json::json!({
            "tx_id": tx_id,
            "final_path": final_dir.clone(),
            "committed_at": chrono::Utc::now().to_rfc3339(),
        });
        let manifest_path = format!("{}/manifest_{}.json", final_parent, tx_id);
        if let Err(e) = tokio::fs::write(
            &manifest_path,
            serde_json::to_vec_pretty(&manifest)
                .unwrap_or_else(|_| manifest.to_string().into_bytes()),
        )
        .await
        {
            return Err(format!(
                "failed to write final manifest {}: {}",
                manifest_path, e
            ));
        }
        Ok(())
    }
}

pub async fn abort_tx(shared: SharedData, tx_id: &str, staging_prefix: &str) -> Result<(), String> {
    if let Some(provider) = shared.storage_provider.clone() {
        crate::storage::staging::abort_tx(provider, tx_id, staging_prefix)
            .await
            .map_err(|e| format!("{}", e))
    } else {
        let staging_dir = format!("worker_storage/staging/{}/{}", staging_prefix, tx_id);
        match tokio::fs::remove_dir_all(&staging_dir).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Ok(())
                } else {
                    Err(format!(
                        "failed to remove staging dir {}: {}",
                        staging_dir, e
                    ))
                }
            }
        }
    }
}

pub async fn handle_execute_task(
    shared: SharedData,
    req: crate::services::worker_service_server::worker_service::TaskRequest,
) -> crate::services::worker_service_server::worker_service::TaskResponse {
    let first_task = req.tasks.first().cloned();
    let operation = first_task
        .as_ref()
        .map(|t| t.operation.to_lowercase())
        .unwrap_or_default();
    let task_input = first_task
        .as_ref()
        .map(|t| t.input.clone())
        .unwrap_or_default();
    let task_id = first_task
        .as_ref()
        .map(|t| t.task_id.clone())
        .unwrap_or_default();
    let session_id = req.session_id.clone();

    if operation == "create_database" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "create_database task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::transactions::ddl::create_database::execute_create_database_task(&shared, task)
            .await
        {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    if operation == "create_schema" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "create_schema task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::transactions::ddl::create_schema::execute_create_schema_task(&shared, task)
            .await
        {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    if operation == "create_table" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "create_table task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::transactions::ddl::create_table::execute_create_table_task(&shared, task).await
        {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    if operation == "query" {
        let task = match first_task.as_ref() {
            Some(t) => t,
            None => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: "query task payload is missing".to_string(),
                    result_location: String::new(),
                };
            }
        };

        match crate::services::query::execute_query_task_stub(&shared, task, &session_id).await {
            Ok(location) => {
                shared
                    .set_task_result_location(&session_id, &task.task_id, &location)
                    .await;
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "ok".to_string(),
                    error: String::new(),
                    result_location: location,
                };
            }
            Err(e) => {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: e,
                    result_location: String::new(),
                };
            }
        }
    }

    let explicit_delta_table_uri = first_task.as_ref().and_then(|task| {
        task.params
            .get("table_uri")
            .filter(|uri| !uri.trim().is_empty())
            .cloned()
            .or_else(|| {
                if task.output.trim().is_empty() {
                    None
                } else {
                    Some(task.output.clone())
                }
            })
    });
    let parsed_insert = if operation == "insert" && !task_input.trim().is_empty() {
        parse_insert_payload(&task_input).ok()
    } else {
        None
    };
    let insert_column_type_hints = if operation == "insert" {
        match first_task.as_ref() {
            Some(task) => match parse_insert_column_type_hints(task) {
                Ok(hints) => hints,
                Err(message) => {
                    return crate::services::worker_service_server::worker_service::TaskResponse {
                        status: "error".to_string(),
                        error: message,
                        result_location: String::new(),
                    };
                }
            },
            None => HashMap::new(),
        }
    } else {
        HashMap::new()
    };
    if operation == "insert" {
        if let (Some(task), Some(parsed)) = (first_task.as_ref(), parsed_insert.as_ref()) {
            let required_columns =
                crate::transactions::constraints::insert_not_null::parse_required_not_null_columns(
                    task,
                );
            if let Err(message) =
                crate::transactions::constraints::insert_not_null::enforce_not_null_columns(
                    task,
                    parsed,
                    &required_columns,
                )
            {
                return crate::services::worker_service_server::worker_service::TaskResponse {
                    status: "error".to_string(),
                    error: message,
                    result_location: String::new(),
                };
            }
        }
    }
    let table_name_override = first_task
        .as_ref()
        .and_then(|task| task.params.get("table_name").cloned())
        .filter(|s| !s.trim().is_empty());
    let derived_insert_uri = if explicit_delta_table_uri.is_none() {
        let table_name =
            table_name_override.or_else(|| parsed_insert.as_ref().map(|p| p.table_name.clone()));
        table_name
            .as_deref()
            .and_then(|name| derive_table_uri_from_storage(&shared.cluster_info.storage, name))
    } else {
        None
    };
    let delta_table_uri = explicit_delta_table_uri.or(derived_insert_uri);
    let result_location = delta_table_uri
        .clone()
        .unwrap_or_else(|| "arrow-flight-endpoint".to_string());

    let stage_id = first_task
        .as_ref()
        .and_then(|task| task.params.get("stage_id").cloned())
        .filter(|s| !s.trim().is_empty());
    let partition_count = first_task
        .as_ref()
        .and_then(|task| task.params.get("partition_count"))
        .and_then(|value| value.parse::<u32>().ok());
    let upstream_stage_ids = first_task
        .as_ref()
        .and_then(|task| task.params.get("upstream_stage_ids").cloned())
        .unwrap_or_else(|| "[]".to_string());
    let partition_spec = first_task
        .as_ref()
        .and_then(|task| task.params.get("partition_spec").cloned())
        .unwrap_or_else(|| "\"Single\"".to_string());

    shared
        .set_task_result_location(&session_id, &task_id, &result_location)
        .await;
    let result_location_for_spawn = result_location.clone();
    let shared_clone = shared.clone();
    let stage_id_for_spawn = stage_id.clone();
    let partition_count_for_spawn = partition_count;
    let upstream_stage_ids_for_spawn = upstream_stage_ids.clone();
    let partition_spec_for_spawn = partition_spec.clone();
    tokio::spawn(async move {
        let mut status = "succeeded".to_string();
        let mut error_message = String::new();

        // Exercise Delta write path when a table URI is provided or derived.
        if let Some(table_uri) = delta_table_uri {
            let batch_res = if operation == "insert" {
                if let Some(parsed) = parsed_insert.as_ref() {
                    build_record_batch_from_insert(parsed, &insert_column_type_hints)
                } else {
                    Err("failed to parse INSERT payload".to_string())
                }
            } else {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("session_id", DataType::Utf8, false),
                    Field::new("task_id", DataType::Utf8, false),
                    Field::new("result_location", DataType::Utf8, false),
                    Field::new(
                        "committed_at_ms",
                        DataType::Timestamp(TimeUnit::Millisecond, None),
                        false,
                    ),
                ]));

                let committed_at = chrono::Utc::now().timestamp_millis();
                RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(StringArray::from(vec![session_id.clone()])),
                        Arc::new(StringArray::from(vec![task_id.clone()])),
                        Arc::new(StringArray::from(vec![result_location_for_spawn.clone()])),
                        Arc::new(TimestampMillisecondArray::from(vec![committed_at])),
                    ],
                )
                .map_err(|e| format!("failed to build record batch: {}", e))
            };

            match batch_res {
                Ok(batch) => {
                    if let Err(e) = crate::storage::deltalake::write_parquet_and_commit(
                        shared_clone.clone(),
                        &table_uri,
                        vec![batch],
                    )
                    .await
                    {
                        status = "failed".to_string();
                        error_message = format!("delta write/commit failed: {}", e);
                        log::error!(
                            "Failed delta write for task {} table {}: {}",
                            task_id,
                            table_uri,
                            e
                        );
                    }
                }
                Err(e) => {
                    status = "failed".to_string();
                    error_message = format!("failed to build record batch: {}", e);
                    log::error!(
                        "Failed to build Arrow record batch for task {}: {}",
                        task_id,
                        e
                    );
                }
            }
        } else {
            // Keep previous behavior when no Delta destination was provided.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        // ensure pool and send update
        if let Some(pool_arc) = ensure_pool(shared_clone.clone()).await {
            match pool_arc.get().await {
                Ok(mut pooled_client) => {
                    let mut metadata = std::collections::HashMap::new();
                    metadata.insert(
                        "upstream_stage_ids".to_string(),
                        upstream_stage_ids_for_spawn.clone(),
                    );
                    metadata.insert(
                        "partition_spec".to_string(),
                        partition_spec_for_spawn.clone(),
                    );

                    let partition_completed = partition_count_for_spawn
                        .map(|count| if status == "succeeded" { count } else { 0 });

                    let update = crate::interops_service::TaskUpdateRequest {
                        task_id: task_id.clone(),
                        status,
                        result_location: result_location_for_spawn.clone(),
                        error: error_message,
                        stage_id: stage_id_for_spawn.clone(),
                        partition_count: partition_count_for_spawn,
                        partition_completed,
                        metadata,
                    };
                    if let Err(e) = pooled_client.task_update(tonic::Request::new(update)).await {
                        log::error!(
                            "Failed to send TaskUpdate for {} using pool: {}",
                            task_id,
                            e
                        );
                    }
                }
                Err(e) => log::error!("Failed to acquire pooled master client: {}", e),
            }
        } else {
            log::error!(
                "No interops pool available; dropping TaskUpdate for {}",
                task_id
            );
        }
    });

    crate::services::worker_service_server::worker_service::TaskResponse {
        status: "ok".to_string(),
        error: String::new(),
        result_location,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_decimal_literal, parse_datetime_literal, parse_insert_column_type_hints,
    };
    use crate::services::worker_service_server::worker_service;
    use std::collections::HashMap;

    fn task_with_params(params: HashMap<String, String>) -> worker_service::Task {
        worker_service::Task {
            task_id: "t1".to_string(),
            operation: "insert".to_string(),
            input: String::new(),
            output: String::new(),
            params,
        }
    }

    #[test]
    fn rejects_datetime_literal_with_timezone_offset() {
        let err = parse_datetime_literal("'2024-01-01T00:00:00+02:00'")
            .expect_err("datetime timezone offsets must be rejected");
        assert!(err.contains("DATETIME_TIMEZONE_NOT_ALLOWED"));
    }

    #[test]
    fn normalizes_decimal_literal_with_scale_padding() {
        let normalized = normalize_decimal_literal("'10.5'", Some((6, 3)))
            .expect("decimal coercion should normalize value");
        assert_eq!(normalized, "10.500");
    }

    #[test]
    fn rejects_decimal_literal_exceeding_precision() {
        let err = normalize_decimal_literal("'1234.56'", Some((5, 2)))
            .expect_err("precision overflow should fail");
        assert!(err.contains("DECIMAL_COERCION_FAILED"));
        assert!(err.contains("exceeds precision"));
    }

    #[test]
    fn rejects_missing_type_hints_in_contract_mode() {
        let mut params = HashMap::new();
        params.insert("datatype_contract_version".to_string(), "1".to_string());
        let task = task_with_params(params);

        let err = parse_insert_column_type_hints(&task)
            .expect_err("contract mode should require type hints payload");
        assert!(err.contains("INSERT_TYPE_HINTS_MALFORMED"));
    }
}
