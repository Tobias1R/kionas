use crate::execution::query::QueryNamespace;
use crate::services::worker_service_server::worker_service;
use arrow::array::StringArray;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;

/// What: Build a normalized relation key from canonical namespace parts.
///
/// Inputs:
/// - `namespace`: Canonical database/schema/table namespace.
///
/// Output:
/// - Lower-cased `<database>.<schema>.<table>` relation key.
///
/// Details:
/// - Used to match relation metadata maps carried in task parameters.
pub(crate) fn relation_key(namespace: &QueryNamespace) -> String {
    format!(
        "{}.{}.{}",
        namespace.database.to_ascii_lowercase(),
        namespace.schema.to_ascii_lowercase(),
        namespace.table.to_ascii_lowercase()
    )
}

/// What: Parse relation column-name mapping from task parameters.
///
/// Inputs:
/// - `task`: Current stage task that may include `relation_columns_json`.
///
/// Output:
/// - Mapping from normalized relation key to ordered column names.
///
/// Details:
/// - Returns an empty map on missing parameter or malformed JSON.
pub(crate) fn parse_relation_columns_map(
    task: &worker_service::StagePartitionExecution,
) -> HashMap<String, Vec<String>> {
    task.params
        .get("relation_columns_json")
        .and_then(|value| serde_json::from_str::<HashMap<String, Vec<String>>>(value).ok())
        .unwrap_or_default()
}

/// What: Apply relation-defined column names to a batch collection.
///
/// Inputs:
/// - `batches`: Input batches to rename.
/// - `relation_columns`: Target ordered column names.
///
/// Output:
/// - Renamed batches with original values and updated field names.
///
/// Details:
/// - Fails when batch column count does not match metadata column count.
pub(crate) fn apply_relation_column_names(
    batches: &[RecordBatch],
    relation_columns: &[String],
) -> Result<Vec<RecordBatch>, String> {
    if relation_columns.is_empty() {
        return Ok(batches.to_vec());
    }

    let mut renamed = Vec::with_capacity(batches.len());
    for batch in batches {
        if batch.num_columns() != relation_columns.len() {
            return Err(format!(
                "relation column mapping mismatch: expected {} columns from metastore, batch has {}",
                relation_columns.len(),
                batch.num_columns()
            ));
        }

        let fields = batch
            .schema()
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                field
                    .as_ref()
                    .clone()
                    .with_name(relation_columns[index].clone())
            })
            .collect::<Vec<_>>();

        let renamed_batch = RecordBatch::try_new(
            std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
            batch.columns().to_vec(),
        )
        .map_err(|e| format!("failed to apply relation column mapping: {}", e))?;

        renamed.push(renamed_batch);
    }

    Ok(renamed)
}

/// What: Build an empty Utf8 batch using metastore relation column names.
///
/// Inputs:
/// - `relation_columns`: Ordered relation columns from metastore mapping.
///
/// Output:
/// - Empty record batch with normalized Utf8 fields for each non-empty column name.
///
/// Details:
/// - Lower-cases and trims column names.
/// - Skips empty/whitespace-only names.
pub(crate) fn build_empty_batch_from_relation_columns(
    relation_columns: &[String],
) -> Result<RecordBatch, String> {
    let mut fields = Vec::<Field>::with_capacity(relation_columns.len());
    let mut arrays =
        Vec::<std::sync::Arc<dyn arrow::array::Array>>::with_capacity(relation_columns.len());

    for column in relation_columns {
        let normalized = column.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            continue;
        }

        fields.push(Field::new(normalized, DataType::Utf8, true));
        arrays.push(std::sync::Arc::new(StringArray::from(
            Vec::<Option<String>>::new(),
        )));
    }

    let schema = std::sync::Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| format!("failed to build empty fallback record batch: {}", e))
}
