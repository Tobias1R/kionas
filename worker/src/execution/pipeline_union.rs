use crate::services::query_execution::resolve_schema_column_index;
use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use std::collections::HashSet;

/// What: Summarize representative values for UNION operand diagnostics.
///
/// Inputs:
/// - `batches`: Operand batches after optional filtering.
///
/// Output:
/// - Optional summary string with selected column name, row count, unique count, and sample.
///
/// Details:
/// - Prefers `id` column, then falls back to `c1`.
/// - Returns `None` when neither column exists in the provided batches.
pub(crate) fn summarize_union_operand_values(batches: &[RecordBatch]) -> Option<String> {
    let mut unique_values = HashSet::<String>::new();
    let mut sample_values = Vec::<String>::new();
    let mut total_rows = 0usize;
    let mut selected_column_name: Option<String> = None;

    for batch in batches {
        let Some(column_idx) = resolve_schema_column_index(batch.schema().as_ref(), "id")
            .or_else(|| resolve_schema_column_index(batch.schema().as_ref(), "c1"))
        else {
            continue;
        };

        if selected_column_name.is_none() {
            selected_column_name = Some(batch.schema().field(column_idx).name().to_string());
        }

        let column = batch.column(column_idx);
        for row_idx in 0..batch.num_rows() {
            total_rows += 1;
            let rendered = array_value_to_string(column.as_ref(), row_idx)
                .unwrap_or_else(|_| "<render-error>".to_string());
            if unique_values.insert(rendered.clone()) && sample_values.len() < 8 {
                sample_values.push(rendered);
            }
        }
    }

    selected_column_name.map(|name| {
        format!(
            "column={} total_rows={} unique_values={} sample={:?}",
            name,
            total_rows,
            unique_values.len(),
            sample_values
        )
    })
}

/// What: Deduplicate UNION DISTINCT output batches while preserving first-seen row order.
///
/// Inputs:
/// - `batches`: Input UNION operand batches concatenated in evaluation order.
///
/// Output:
/// - Filtered batches where duplicate rows have been removed.
///
/// Details:
/// - Row identity is computed by rendering each column value to a stable text key.
/// - Keeps first occurrence and drops subsequent duplicates.
pub(crate) fn deduplicate_union_batches(
    batches: &[RecordBatch],
) -> Result<Vec<RecordBatch>, String> {
    let mut seen = HashSet::<String>::new();
    let mut output = Vec::<RecordBatch>::new();

    for batch in batches {
        let mut keep_mask = Vec::<bool>::with_capacity(batch.num_rows());
        for row_index in 0..batch.num_rows() {
            let mut key = String::new();
            for (col_index, column) in batch.columns().iter().enumerate() {
                if col_index > 0 {
                    key.push('|');
                }
                let rendered = array_value_to_string(column.as_ref(), row_index)
                    .map_err(|e| format!("failed to stringify union row value: {}", e))?;
                key.push_str(rendered.as_str());
            }
            let is_new = seen.insert(key);
            keep_mask.push(is_new);
        }

        if keep_mask.iter().any(|keep| *keep) {
            let mask = BooleanArray::from(keep_mask);
            let filtered = filter_record_batch(batch, &mask)
                .map_err(|e| format!("failed to filter deduplicated union rows: {}", e))?;
            output.push(filtered);
        }
    }

    Ok(output)
}
