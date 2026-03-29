use crate::transactions::insert_coercion::{
    format_datetime_literal, normalize_decimal_literal, parse_datetime_literal,
    parse_decimal_precision_scale, parse_timestamp_millis_literal, strip_sql_literal_quotes,
};
use crate::transactions::maestro::{InsertScalar, ParsedInsertPayload};
use arrow::array::{ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::sync::Arc;

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

#[tracing::instrument(skip(parsed, column_type_hints), fields(table_name = %parsed.table_name, row_count = parsed.rows.len()))]
pub(crate) fn build_record_batch_from_insert(
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
