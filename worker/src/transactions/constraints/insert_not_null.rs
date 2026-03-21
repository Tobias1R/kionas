use crate::services::worker_service_server::worker_service;
use crate::transactions::maestro::{InsertScalar, ParsedInsertPayload};
use std::collections::HashMap;

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

fn is_synthetic_insert_column(name: &str, expected_index: usize) -> bool {
    let normalized = normalize_identifier(name);
    normalized == format!("c{}", expected_index + 1)
}

fn parse_table_columns(task: &worker_service::Task) -> Vec<String> {
    let raw = match task.params.get("table_columns_json") {
        Some(value) => value,
        None => return Vec::new(),
    };

    let parsed = match serde_json::from_str::<Vec<String>>(raw) {
        Ok(values) => values,
        Err(_) => return Vec::new(),
    };

    parsed
        .into_iter()
        .map(|column| normalize_identifier(&column))
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>()
}

fn resolved_insert_columns(
    parsed_insert: &ParsedInsertPayload,
    table_columns: &[String],
) -> Vec<String> {
    let parsed_columns = parsed_insert
        .columns
        .iter()
        .map(|column| normalize_identifier(column))
        .collect::<Vec<_>>();

    let looks_synthetic = parsed_columns
        .iter()
        .enumerate()
        .all(|(index, name)| is_synthetic_insert_column(name, index));

    if looks_synthetic
        && !table_columns.is_empty()
        && parsed_columns.len() <= table_columns.len()
        && parsed_insert
            .rows
            .first()
            .map(|row| row.len())
            .unwrap_or_default()
            == parsed_columns.len()
    {
        return table_columns
            .iter()
            .take(parsed_columns.len())
            .cloned()
            .collect();
    }

    parsed_columns
}

/// What: Parse NOT NULL column requirements from worker task params.
///
/// Inputs:
/// - `task`: Worker task with optional `not_null_columns_json` param.
///
/// Output:
/// - Canonical NOT NULL column names to enforce.
///
/// Details:
/// - Missing or malformed params degrade to an empty requirement set.
pub(crate) fn parse_required_not_null_columns(task: &worker_service::Task) -> Vec<String> {
    let raw = match task.params.get("not_null_columns_json") {
        Some(value) => value,
        None => return Vec::new(),
    };

    let parsed = match serde_json::from_str::<Vec<String>>(raw) {
        Ok(values) => values,
        Err(_) => return Vec::new(),
    };

    let mut required = parsed
        .into_iter()
        .map(|column| normalize_identifier(&column))
        .filter(|column| !column.is_empty())
        .collect::<Vec<_>>();
    required.sort();
    required.dedup();
    required
}

/// What: Enforce NOT NULL column requirements against one parsed INSERT payload.
///
/// Inputs:
/// - `parsed_insert`: Parsed INSERT rows and columns.
/// - `required_columns`: Canonical column names marked NOT NULL.
///
/// Output:
/// - `Ok(())` when no violations are found.
/// - `Err(message)` describing first violation or payload mismatch.
///
/// Details:
/// - Empty `required_columns` means no enforcement is required.
pub(crate) fn enforce_not_null_columns(
    task: &worker_service::Task,
    parsed_insert: &ParsedInsertPayload,
    required_columns: &[String],
) -> Result<(), String> {
    if required_columns.is_empty() {
        return Ok(());
    }

    let table_columns = parse_table_columns(task);
    let effective_columns = resolved_insert_columns(parsed_insert, &table_columns);

    let column_index = effective_columns
        .iter()
        .enumerate()
        .map(|(idx, name)| (name.clone(), idx))
        .collect::<HashMap<_, _>>();

    let mut missing = Vec::new();
    let mut required_positions = Vec::new();
    for required in required_columns {
        if let Some(position) = column_index.get(required) {
            required_positions.push((required.clone(), *position));
        } else {
            missing.push(required.clone());
        }
    }

    if !missing.is_empty() {
        missing.sort();
        return Err(format!(
            "insert is missing NOT NULL column(s): {}",
            missing.join(", ")
        ));
    }

    for (row_idx, row) in parsed_insert.rows.iter().enumerate() {
        for (column_name, column_idx) in &required_positions {
            if *column_idx >= row.len() {
                return Err(format!(
                    "insert row {} does not include value for NOT NULL column '{}'",
                    row_idx + 1,
                    column_name
                ));
            }

            if matches!(row[*column_idx], InsertScalar::Null) {
                return Err(format!(
                    "NOT NULL constraint violated for column '{}' at row {}",
                    column_name,
                    row_idx + 1
                ));
            }
        }
    }

    Ok(())
}
