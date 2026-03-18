use crate::state::SharedData;
use crate::storage::object_store_pool::ObjectStoreManager;
use arrow::datatypes::DataType as ArrowDataType;
use bytes::Bytes;
use chrono::Utc;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, Add, DataType as DeltaDataType, PrimitiveType, StructField};
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::{DeltaTable, open_table_with_storage_options};
use object_store::ObjectStoreExt;
use object_store::path::Path as ObjPath;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use url::Url;
use uuid::Uuid;

type DynError = Box<dyn std::error::Error + Send + Sync>;

static OBJECT_STORE_POOL_INIT_COUNT: AtomicU64 = AtomicU64::new(0);
static OBJECT_STORE_POOL_CHECKOUT_COUNT: AtomicU64 = AtomicU64::new(0);

fn storage_options_from_cluster(storage: &serde_json::Value) -> HashMap<String, String> {
    let mut options = HashMap::new();

    let endpoint = storage
        .get("endpoint")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let region = storage
        .get("region")
        .and_then(|v| v.as_str())
        .unwrap_or("us-east-1")
        .trim();
    let access_key = storage
        .get("access_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();
    let secret_key = storage
        .get("secret_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim();

    if !region.is_empty() {
        options.insert("aws_region".to_string(), region.to_string());
    }
    if !access_key.is_empty() {
        options.insert("aws_access_key_id".to_string(), access_key.to_string());
    }
    if !secret_key.is_empty() {
        options.insert("aws_secret_access_key".to_string(), secret_key.to_string());
    }
    if !endpoint.is_empty() {
        options.insert("aws_endpoint".to_string(), endpoint.to_string());
        options.insert("aws_endpoint_url".to_string(), endpoint.to_string());
        if endpoint.starts_with("http://") {
            options.insert("aws_allow_http".to_string(), "true".to_string());
            options.insert("allow_http".to_string(), "true".to_string());
        }
    }

    options
}

async fn ensure_object_store_pool(
    shared: &SharedData,
) -> Result<Arc<deadpool::managed::Pool<ObjectStoreManager>>, DynError> {
    let mut guard = shared.object_store_pool.lock().await;
    if let Some(pool) = guard.as_ref() {
        return Ok(pool.clone());
    }

    let manager = ObjectStoreManager::new(&shared.cluster_info.storage);
    let pool_size: usize = std::env::var("OBJECT_STORE_POOL_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    let pool = deadpool::managed::Pool::builder(manager)
        .max_size(pool_size)
        .build()
        .map_err(|e| format!("failed to build object-store pool: {}", e))?;
    let pool = Arc::new(pool);
    let init_count = OBJECT_STORE_POOL_INIT_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    log::info!(
        "Initialized object-store pool for delta writes with max_size={} init_count={}",
        pool_size,
        init_count
    );
    *guard = Some(pool.clone());
    Ok(pool)
}

/// Open a Delta table by URI. Returns an error if the table cannot be opened.
pub async fn open_table(shared: SharedData, table_uri: &str) -> Result<DeltaTable, DynError> {
    let url = Url::parse(table_uri)?;
    let storage_options = storage_options_from_cluster(&shared.cluster_info.storage);
    let tbl = open_table_with_storage_options(url, storage_options).await?;
    Ok(tbl)
}

/// Create a new Delta table at `table_uri`.
pub async fn create_table(
    shared: SharedData,
    table_uri: &str,
    schema: &arrow::datatypes::Schema,
) -> Result<DeltaTable, DynError> {
    let url = Url::parse(table_uri)?;
    let storage_options = storage_options_from_cluster(&shared.cluster_info.storage);

    // Idempotent behavior: if a table already exists, reuse it.
    if let Ok(existing) =
        open_table_with_storage_options(url.clone(), storage_options.clone()).await
    {
        return Ok(existing);
    }

    let columns: Vec<StructField> = schema
        .fields()
        .iter()
        .map(|field| {
            let dtype = match field.data_type() {
                ArrowDataType::Boolean => DeltaDataType::BOOLEAN,
                ArrowDataType::Int8 => DeltaDataType::BYTE,
                ArrowDataType::Int16 => DeltaDataType::SHORT,
                ArrowDataType::Int32 => DeltaDataType::INTEGER,
                ArrowDataType::Int64 => DeltaDataType::LONG,
                ArrowDataType::Float32 => DeltaDataType::FLOAT,
                ArrowDataType::Float64 => DeltaDataType::DOUBLE,
                ArrowDataType::Date32 | ArrowDataType::Date64 => DeltaDataType::DATE,
                ArrowDataType::Timestamp(_, _) => DeltaDataType::TIMESTAMP,
                ArrowDataType::Decimal128(precision, scale)
                | ArrowDataType::Decimal256(precision, scale) => {
                    PrimitiveType::decimal(*precision as u8, *scale as u8)
                        .map(DeltaDataType::from)
                        .unwrap_or(DeltaDataType::STRING)
                }
                ArrowDataType::Binary | ArrowDataType::LargeBinary => DeltaDataType::BINARY,
                _ => DeltaDataType::STRING,
            };
            StructField::new(field.name(), dtype, field.is_nullable())
        })
        .collect();

    if columns.is_empty() {
        return Err("cannot create Delta table with empty schema".into());
    }

    let table = DeltaTable::try_from_url_with_storage_options(url, storage_options.clone()).await?;
    let table_name = table_uri
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .unwrap_or("table");
    let created = table
        .create()
        .with_table_name(table_name)
        .with_storage_options(storage_options)
        .with_columns(columns)
        .with_save_mode(SaveMode::ErrorIfExists)
        .await?;
    Ok(created)
}

/// Write given record batches as Parquet files and commit to the Delta log.
pub async fn write_parquet_and_commit(
    shared: SharedData,
    table_uri: &str,
    record_batches: Vec<arrow::record_batch::RecordBatch>,
) -> Result<(), DynError> {
    // Serialize RecordBatches to Parquet in-memory and upload to the configured
    // object store. Committing the file to the Delta log (AddFile + commit)
    // is left as a TODO because it requires constructing proper Delta actions.

    if record_batches.is_empty() {
        return Ok(());
    }

    // build in-memory parquet
    let schema_ref = record_batches[0].schema();
    let props = WriterProperties::builder().build();
    let mut cursor = Cursor::new(Vec::new());
    let mut writer = ArrowWriter::try_new(&mut cursor, schema_ref, Some(props))?;
    for batch in &record_batches {
        writer.write(batch)?;
    }
    writer.close()?;
    let bytes = cursor.into_inner();
    let bytes_len = bytes.len();

    // determine object store and upload path from cluster config and table URI
    let pool = ensure_object_store_pool(&shared).await?;
    let pooled_store = pool
        .get()
        .await
        .map_err(|e| format!("failed to acquire object-store client from pool: {}", e))?;
    let checkout_count = OBJECT_STORE_POOL_CHECKOUT_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
    log::info!(
        "Checked out object-store client from delta pool checkout_count={}",
        checkout_count
    );
    let store = pooled_store.as_ref();
    let url = Url::parse(table_uri)?;
    let table_path = url.path().trim_start_matches('/');
    let fname = format!("{}.parquet", Uuid::new_v4());
    let relative_data_path = format!("staging/{}", fname);
    let object_key = if table_path.is_empty() {
        relative_data_path.clone()
    } else {
        format!("{}/{}", table_path, relative_data_path)
    };

    let obj_path = ObjPath::from(object_key);
    store.put(&obj_path, Bytes::from(bytes).into()).await?;

    // TODO: create a Delta AddFile action for `key` (relative to table root)
    // and call the DeltaTable commit API so the new file is visible to the table.
    // For now we simply upload the data file to the table's storage.

    // Build typed `Add` action and commit using the deltalake crate's API.
    let partition_values: HashMap<String, Option<String>> = HashMap::new();
    let add = Add {
        path: relative_data_path,
        size: bytes_len as i64,
        partition_values,
        modification_time: Utc::now().timestamp_millis(),
        data_change: true,
        stats: None,
        tags: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        clustering_provider: None,
    };

    let action = Action::Add(add);

    // Commit through deltalake_core transaction API. DeltaTable itself does not
    // expose a direct `commit` method in this version.
    let storage_options = storage_options_from_cluster(&shared.cluster_info.storage);
    let mut tbl = open_table_with_storage_options(url, storage_options).await?;
    let table_state = tbl.snapshot()?;
    let operation = DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    };
    CommitBuilder::default()
        .with_actions(vec![action])
        .build(Some(table_state), tbl.log_store(), operation)
        .await?;
    tbl.update_state().await?;

    Ok(())
}
