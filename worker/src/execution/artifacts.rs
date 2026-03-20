use crate::state::SharedData;
use crate::storage::exchange::{stage_exchange_data_key, stage_exchange_metadata_key};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::io::Cursor;
use url::Url;

/// What: Metadata describing one persisted query artifact.
///
/// Inputs:
/// - `key`: Object-store key of the artifact.
/// - `size_bytes`: Artifact size in bytes.
/// - `checksum_fnv64`: Deterministic checksum for integrity checks.
///
/// Output:
/// - Serializable artifact descriptor for metadata sidecars.
#[derive(Debug, Clone, serde::Serialize)]
pub(crate) struct QueryArtifactMetadata {
    pub(crate) key: String,
    pub(crate) size_bytes: u64,
    pub(crate) checksum_fnv64: String,
}

/// What: Persist stage output batches as deterministic storage-mediated exchange artifacts.
///
/// Inputs:
/// - `shared`: Worker state with storage provider.
/// - `session_id`: Session identifier used for exchange scoping.
/// - `query_run_id`: Query run identifier from stage context.
/// - `stage_id`: Stage identifier.
/// - `partition_index`: Current partition index.
/// - `partition_count`: Total partitions for this stage.
/// - `upstream_stage_ids`: Upstream dependency stage ids.
/// - `batches`: Stage output batches.
///
/// Output:
/// - `Ok(())` when parquet artifact and metadata sidecar are written.
pub(crate) async fn persist_stage_exchange_artifacts(
    shared: &SharedData,
    session_id: &str,
    query_run_id: &str,
    stage_id: u32,
    partition_index: u32,
    partition_count: u32,
    upstream_stage_ids: &[u32],
    batches: &[RecordBatch],
) -> Result<(), String> {
    let provider = shared.storage_provider.as_ref().ok_or_else(|| {
        "storage provider is not configured for stage exchange persistence".to_string()
    })?;

    let data_key = stage_exchange_data_key(query_run_id, session_id, stage_id, partition_index);
    let metadata_key =
        stage_exchange_metadata_key(query_run_id, session_id, stage_id, partition_index);

    let parquet_bytes = encode_batches_to_parquet(batches)?;
    let artifacts = vec![QueryArtifactMetadata {
        key: data_key.clone(),
        size_bytes: parquet_bytes.len() as u64,
        checksum_fnv64: checksum_fnv64_hex(&parquet_bytes),
    }];

    let metadata_bytes = serde_json::to_vec_pretty(&serde_json::json!({
        "stage_id": stage_id,
        "partition_index": partition_index,
        "partition_count": partition_count,
        "upstream_stage_ids": upstream_stage_ids,
        "query_run_id": query_run_id,
        "row_count": batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        "batch_count": batches.len(),
        "artifacts": artifacts,
    }))
    .map_err(|e| format!("failed to encode stage exchange metadata json: {}", e))?;

    provider
        .put_object(&data_key, parquet_bytes)
        .await
        .map_err(|e| {
            format!(
                "failed to persist stage exchange artifact {}: {}",
                data_key, e
            )
        })?;

    provider
        .put_object(&metadata_key, metadata_bytes)
        .await
        .map_err(|e| {
            format!(
                "failed to persist stage exchange metadata {}: {}",
                metadata_key, e
            )
        })?;

    Ok(())
}

/// What: Persist result batches to deterministic task-scoped artifact location.
///
/// Inputs:
/// - `shared`: Worker state with storage provider.
/// - `result_location`: Flight URI used as retrieval root.
/// - `session_id`: Session id component for task scoping.
/// - `task_id`: Task id component for task scoping.
/// - `batches`: Executed result batches.
///
/// Output:
/// - `Ok(())` when result parquet file is stored.
pub(crate) async fn persist_query_artifacts(
    shared: &SharedData,
    result_location: &str,
    session_id: &str,
    task_id: &str,
    batches: &[RecordBatch],
) -> Result<(), String> {
    let provider = shared.storage_provider.as_ref().ok_or_else(|| {
        "storage provider is not configured for query result persistence".to_string()
    })?;

    let path = Url::parse(result_location)
        .map_err(|e| format!("result_location is not a valid URI: {}", e))?
        .path()
        .trim_start_matches('/')
        .trim_end_matches('/')
        .to_string();

    if path.is_empty() {
        return Err("result_location path is empty".to_string());
    }

    let prefix = format!("{}/staging/{}/{}/", path, session_id, task_id);
    let key = format!("{}part-00000.parquet", prefix);
    let parquet_bytes = encode_batches_to_parquet(batches)?;
    let metadata_key = format!("{}result_metadata.json", prefix);
    let artifacts = vec![QueryArtifactMetadata {
        key: key.clone(),
        size_bytes: parquet_bytes.len() as u64,
        checksum_fnv64: checksum_fnv64_hex(&parquet_bytes),
    }];
    let metadata_bytes = encode_result_metadata(batches, &artifacts)?;

    provider
        .put_object(&key, parquet_bytes)
        .await
        .map_err(|e| format!("failed to persist query artifact {}: {}", key, e))?;

    provider
        .put_object(&metadata_key, metadata_bytes)
        .await
        .map_err(|e| format!("failed to persist query metadata {}: {}", metadata_key, e))
}

/// What: Encode record batches into a single parquet payload.
///
/// Inputs:
/// - `batches`: Ordered Arrow batches with compatible schema.
///
/// Output:
/// - Encoded parquet bytes.
fn encode_batches_to_parquet(batches: &[RecordBatch]) -> Result<Vec<u8>, String> {
    let first = batches
        .first()
        .ok_or_else(|| "cannot encode empty batch list".to_string())?;

    let schema_ref = first.schema();
    let props = WriterProperties::builder().build();
    let mut cursor = Cursor::new(Vec::<u8>::new());
    let mut writer = ArrowWriter::try_new(&mut cursor, schema_ref, Some(props))
        .map_err(|e| format!("failed to create parquet writer: {}", e))?;

    for batch in batches {
        writer
            .write(batch)
            .map_err(|e| format!("failed writing parquet batch: {}", e))?;
    }

    writer
        .close()
        .map_err(|e| format!("failed closing parquet writer: {}", e))?;
    Ok(cursor.into_inner())
}

/// What: Encode query result metadata sidecar for deterministic task artifacts.
///
/// Inputs:
/// - `batches`: Executed result batches.
/// - `artifacts`: Persisted query result artifact metadata.
///
/// Output:
/// - JSON bytes containing row count and schema summary.
pub(crate) fn encode_result_metadata(
    batches: &[RecordBatch],
    artifacts: &[QueryArtifactMetadata],
) -> Result<Vec<u8>, String> {
    let first = batches
        .first()
        .ok_or_else(|| "cannot encode metadata for empty batch list".to_string())?;

    let row_count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    let fields = first
        .schema()
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "data_type": format!("{:?}", f.data_type()),
                "nullable": f.is_nullable(),
            })
        })
        .collect::<Vec<_>>();

    let artifact_values = artifacts
        .iter()
        .map(|artifact| {
            serde_json::json!({
                "key": artifact.key,
                "size_bytes": artifact.size_bytes,
                "checksum_fnv64": artifact.checksum_fnv64,
            })
        })
        .collect::<Vec<_>>();

    serde_json::to_vec_pretty(&serde_json::json!({
        "row_count": row_count,
        "source_batch_count": batches.len(),
        "parquet_file_count": artifacts.len(),
        "columns": fields,
        "artifacts": artifact_values,
    }))
    .map_err(|e| format!("failed to encode query metadata json: {}", e))
}

/// What: Compute deterministic FNV-1a 64-bit checksum as lowercase hex.
///
/// Inputs:
/// - `bytes`: Raw artifact bytes.
///
/// Output:
/// - 16-char lowercase hex checksum.
fn checksum_fnv64_hex(bytes: &[u8]) -> String {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }

    format!("{hash:016x}")
}
