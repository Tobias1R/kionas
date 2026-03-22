use crate::execution::query::QueryNamespace;
use crate::services::worker_service_server::worker_service;
use crate::state::SharedData;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Int16Array, Int32Array, Int64Array,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray,
};
use arrow::compute::filter_record_batch;
use arrow::compute::kernels::cast::cast;
use arrow::compute::{SortColumn, SortOptions, concat_batches, lexsort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use kionas::planner::{
    PhysicalExpr, PhysicalLimitSpec, PhysicalSortExpr, PredicateComparisonOp, PredicateExpr,
    PredicateValue, parse_predicate_sql,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum FilterValue {
    Int(i64),
    Bool(bool),
    Str(String),
    IntList(Vec<i64>),    // Phase 9c: IN with int list
    StrList(Vec<String>), // Phase 9c: IN with string list
    BoolList(Vec<bool>),  // Phase 9c: IN with bool list
}

#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Between,    // Phase 9c: col BETWEEN lower AND upper
    NotBetween, // Phase 9c: col NOT BETWEEN lower AND upper
    In,         // Phase 9c: col IN (val1, val2, ...)
    IsNull,     // Phase 9a: col IS NULL
    IsNotNull,  // Phase 9a: col IS NOT NULL
}

#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,
    // Phase 9c: Additional operand for BETWEEN (stores upper bound)
    value2: Option<FilterValue>,
}

/// What: Execute validated query payload on the worker and materialize deterministic artifacts.
///
/// Inputs:
/// - `shared`: Worker state with storage provider and cluster storage config.
/// - `task`: Worker task carrying canonical payload with physical plan.
/// - `session_id`: Session id used for deterministic artifact partitioning.
/// - `namespace`: Canonical namespace resolved by payload validation.
/// - `result_location`: Flight URI returned to server/client.
///
/// Output:
/// - `Ok(())` when execution succeeds and parquet artifacts are persisted.
/// - `Err(message)` when runtime execution or storage operations fail.
#[allow(dead_code)]
pub(crate) async fn execute_query_task(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
    namespace: &QueryNamespace,
    result_location: &str,
) -> Result<(), String> {
    crate::execution::pipeline::execute_query_task(
        shared,
        task,
        session_id,
        namespace,
        result_location,
    )
    .await
}

/// What: Pre-validate filter predicate column types against schema before filter execution.
///
/// Inputs:
/// - `filter_sql`: Raw SQL predicate string.
/// - `schema_metadata`: Column type contract from physical plan.
///
/// Output:
/// - Ok() if all predicates satisfy strict type coercion policy.
/// - Err(...) if predicate references non-existent columns or violates type contract.
///
/// Details:
/// - Phase 9b Step 6: Worker-side runtime type validation complements planner validation.
/// - Phase 9b Step 8: Enhanced error messages with remediation suggestions and taxonomy
/// - Enforces DecimalCoercionPolicy::Strict: operand type must match column type exactly.
/// - NULL semantics: NULL operands are rejected here (NULL is not a type); IS NULL handled separately.
/// - Supported patterns: IS NULL, IS NOT NULL, NOT (IS NULL), comparison operators (=, !=, <, >, <=, >=).
/// - Error taxonomy:
///   1. MissingColumn: Column not found in schema
///   2. TypeMismatch: Operand type incompatible with column canonical_type
///   3. UnsupportedLiteral: Literal value format not recognized
#[cfg(test)]
#[allow(dead_code)]
fn validate_filter_predicates_types(
    filter_sql: &str,
    schema: &std::collections::HashMap<String, kionas::sql::datatypes::ColumnDatatypeSpec>,
) -> Result<(), String> {
    // Split by AND to get individual clauses
    let clauses = split_case_insensitive(filter_sql, "AND");

    for clause in clauses {
        let trimmed = clause.trim();
        if trimmed.is_empty() {
            continue;
        }

        // Check for IS NULL / IS NOT NULL / NOT (IS NULL) patterns (no type validation needed)
        let lower = trimmed.to_ascii_lowercase();
        if lower.ends_with("is null") || lower.ends_with("is not null") {
            // Extract column name before IS
            let parts: Vec<&str> = lower.split_whitespace().collect();
            if !parts.is_empty() {
                let col_name = parts[0];
                if !schema.contains_key(col_name) {
                    return Err(format_type_error(
                        "MissingColumn",
                        col_name,
                        "(any type)",
                        "",
                        "",
                    ));
                }
            }
            continue; // IS NULL patterns don't require type validation
        }

        if lower.starts_with("not") && lower.contains("is null") {
            // Pattern: NOT (column IS NULL)
            let inner = trimmed
                .trim_start_matches("NOT")
                .trim_start_matches("(")
                .trim_end_matches(")")
                .trim();
            let parts: Vec<&str> = inner.split_whitespace().collect();
            if !parts.is_empty() {
                let col_name = parts[0];
                if !schema.contains_key(col_name) {
                    return Err(format_type_error(
                        "MissingColumn",
                        col_name,
                        "(any type)",
                        "",
                        "",
                    ));
                }
            }
            continue;
        }

        // Comparison operators: validate type compatibility
        // Try to parse as a filter clause (column op literal)
        match parse_single_clause(trimmed) {
            Ok((column, _op, literal)) => {
                // Column must exist in schema
                let col_spec = schema.get(&column).ok_or_else(|| {
                    format_type_error("MissingColumn", &column, "(any type)", "", "")
                })?;

                // Get operand type from literal
                let operand_type = infer_filter_literal_type(literal)
                    .map_err(|_| format_type_error("UnsupportedLiteral", "", "", literal, ""))?;

                // Check type compatibility with column canonical type
                validate_type_compatibility(
                    &column,
                    &col_spec.canonical_type,
                    &operand_type,
                    literal,
                )?;
            }
            Err(_) => {
                // If parse fails, schema column existence validation is best-effort
                let first_token = trimmed.split_whitespace().next().unwrap_or("");
                if !first_token.starts_with('(')
                    && !first_token.is_empty()
                    && !schema.contains_key(first_token)
                {
                    return Err(format_type_error(
                        "MissingColumn",
                        first_token,
                        "(any type)",
                        "",
                        "",
                    ));
                }
            }
        }
    }

    Ok(())
}

fn validate_filter_clause_types(
    clauses: &[FilterClause],
    schema: &std::collections::HashMap<String, kionas::sql::datatypes::ColumnDatatypeSpec>,
) -> Result<(), String> {
    for clause in clauses {
        let col_spec = schema.get(&clause.column).ok_or_else(|| {
            format_type_error("MissingColumn", &clause.column, "(any type)", "", "")
        })?;

        let operand_type = match &clause.value {
            FilterValue::Int(_) | FilterValue::IntList(_) => "int",
            FilterValue::Bool(_) | FilterValue::BoolList(_) => "bool",
            FilterValue::Str(_) | FilterValue::StrList(_) => "string",
        };

        validate_type_compatibility(
            &clause.column,
            &col_spec.canonical_type,
            operand_type,
            "(structured)",
        )?;

        if let Some(upper) = &clause.value2 {
            let upper_type = match upper {
                FilterValue::Int(_) | FilterValue::IntList(_) => "int",
                FilterValue::Bool(_) | FilterValue::BoolList(_) => "bool",
                FilterValue::Str(_) | FilterValue::StrList(_) => "string",
            };
            if upper_type != operand_type {
                return Err(format!(
                    "type coercion violation: BETWEEN bounds type mismatch for column '{}'",
                    clause.column
                ));
            }
        }
    }

    Ok(())
}

/// What: Infer the type of a filter literal value.
///
/// Inputs:
/// - `literal`: Raw literal string from filter predicate.
///
/// Output:
/// - Type identifier: "bool", "int", "string"
#[cfg(test)]
#[allow(dead_code)]
fn infer_filter_literal_type(literal: &str) -> Result<String, String> {
    let trimmed = literal.trim();

    if trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("false") {
        return Ok("bool".to_string());
    }

    if trimmed.parse::<i64>().is_ok() {
        return Ok("int".to_string());
    }

    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        return Ok("string".to_string());
    }

    Err(format!(
        "unsupported filter literal '{}': inferred type unknown",
        trimmed
    ))
}

/// What: Generate descriptive error message for type coercion violations with remediation suggestions.
///
/// Inputs:
/// - `scenario`: Type of error (MissingColumn, TypeMismatch, UnsupportedLiteral)
/// - `column`: Column name (may be empty for unsupported literal errors)
/// - `column_type`: Expected column type specification
/// - `operand_value`: Actual value in the filter predicate
/// - `operand_type`: Actual inferred type from operand
///
/// Output:
/// - Formatted error message with context and remediation guidance
///
/// Details:
/// - Phase 9b Step 8 error taxonomy implementation
/// - Error taxonomy scenarios:
///   1. MissingColumn: Column not found in schema (remediation: check column name spelling)
///   2. TypeMismatch: Operand type incompatible with column type (remediation: use CAST or adjust schema)
///   3. UnsupportedLiteral: Literal value format not recognized (remediation: use quoted string or valid boolean/number)
fn format_type_error(
    scenario: &str,
    column: &str,
    column_type: &str,
    operand_value: &str,
    operand_type: &str,
) -> String {
    match scenario {
        "MissingColumn" => {
            format!(
                "type coercion violation: column '{}' not found in schema. \
                remediations: [verify column name spelling, check schema definition]",
                column
            )
        }
        "TypeMismatch" => {
            format!(
                "type coercion violation: column '{}' expects type '{}' but got value '{}' of type '{}'. \
                remediations: [use CAST('{}' AS {}), update schema definition, adjust query predicate]",
                column, column_type, operand_value, operand_type, operand_value, column_type
            )
        }
        "UnsupportedLiteral" => {
            format!(
                "type coercion violation: unsupported filter literal '{}' (inferred type unknown). \
                remediations: [quote string literals with single quotes, use 'true'/'false' for booleans, use unquoted numbers for integers]",
                operand_value
            )
        }
        _ => {
            format!(
                "type coercion violation: type mismatch for column '{}' (expected '{}', got '{}' of type '{}')",
                column, column_type, operand_value, operand_type
            )
        }
    }
}

/// What: Validate that operand type is compatible with column type under strict coercion policy.
///
/// Inputs:
/// - `column`: Column name for error messages.
/// - `column_type`: Canonical column type from schema.
/// - `operand_type`: Inferred type from filter literal.
/// - `operand_value`: Original value string for error messages.
///
/// Output:
/// - Ok() if types compatible, Err(...) if violation with remediation suggestions.
///
/// Details:
/// - Strict policy: operand type must match column type exactly (no implicit coercions).
/// - Phase 9b Step 8: Enhanced error messages with remediation suggestions
/// - Coercion compatibility:
///   - int → matches int/int32/int16 columns
///   - bool → matches bool columns
///   - string → matches string/varchar columns
///   - Others: strict mismatch
fn validate_type_compatibility(
    column: &str,
    column_type: &str,
    operand_type: &str,
    operand_value: &str,
) -> Result<(), String> {
    let col_lower = column_type.to_ascii_lowercase();
    let op_lower = operand_type.to_ascii_lowercase();

    // Check compatibility matrix
    let compatible = match op_lower.as_str() {
        "int" => col_lower.contains("int"), // Matches int, int64, int32, int16
        "bool" => col_lower == "bool",
        "string" => col_lower.contains("string") || col_lower.contains("varchar"),
        _ => false,
    };

    if !compatible {
        return Err(format_type_error(
            "TypeMismatch",
            column,
            column_type,
            operand_value,
            operand_type,
        ));
    }

    Ok(())
}

/// What: Apply a simple conjunction filter pipeline to all input batches.
///
/// Inputs:
/// - `input`: Source batches.
/// - `filter_sql`: Raw SQL predicate containing simple `AND` clauses.
/// - `schema_metadata`: Optional column type contract for Phase 9b type validation.
///
/// Output:
/// - Filtered batches preserving input order.
///
/// Details:
/// - If schema_metadata provided, pre-validates types before filter evaluation.
/// - If schema_metadata absent, skips type checking (Phase 9b interim behavior).
#[allow(dead_code)]
pub fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
    schema_metadata: Option<
        &std::collections::HashMap<String, kionas::sql::datatypes::ColumnDatatypeSpec>,
    >,
) -> Result<Vec<RecordBatch>, String> {
    let predicate = parse_predicate_sql(filter_sql)?;
    apply_filter_predicate_pipeline(input, &predicate, schema_metadata)
}

/// What: Apply structured predicate filters to input batches without SQL string parsing.
///
/// Inputs:
/// - `input`: Source batches.
/// - `predicate`: Structured predicate from physical plan.
/// - `schema`: Optional schema metadata contract used by legacy type-check pathways.
///
/// Output:
/// - Filtered batches preserving original order.
pub fn apply_filter_predicate_pipeline(
    input: &[RecordBatch],
    predicate: &PredicateExpr,
    schema: Option<&std::collections::HashMap<String, kionas::sql::datatypes::ColumnDatatypeSpec>>,
) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let clauses = predicate_to_clauses(predicate)?;
    let mut out = Vec::with_capacity(input.len());

    for batch in input {
        if let Some(schema_map) = schema {
            validate_filter_clause_types(&clauses, schema_map)?;
        }

        let mask = build_filter_mask(batch, &clauses)?;
        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| format!("failed to apply filter mask: {}", e))?;
        out.push(filtered);
    }

    Ok(out)
}

fn predicate_to_clauses(predicate: &PredicateExpr) -> Result<Vec<FilterClause>, String> {
    match predicate {
        PredicateExpr::Conjunction { clauses } => {
            let mut out = Vec::new();
            for clause in clauses {
                out.extend(predicate_to_clauses(clause)?);
            }
            Ok(out)
        }
        PredicateExpr::Comparison { column, op, value } => Ok(vec![FilterClause {
            column: normalize_projection_identifier(column),
            op: match op {
                PredicateComparisonOp::Eq => FilterOp::Eq,
                PredicateComparisonOp::Ne => FilterOp::Ne,
                PredicateComparisonOp::Gt => FilterOp::Gt,
                PredicateComparisonOp::Ge => FilterOp::Ge,
                PredicateComparisonOp::Lt => FilterOp::Lt,
                PredicateComparisonOp::Le => FilterOp::Le,
            },
            value: predicate_value_to_filter_value(value)?,
            value2: None,
        }]),
        PredicateExpr::Between {
            column,
            lower,
            upper,
            negated,
        } => Ok(vec![FilterClause {
            column: normalize_projection_identifier(column),
            op: if *negated {
                FilterOp::NotBetween
            } else {
                FilterOp::Between
            },
            value: predicate_value_to_filter_value(lower)?,
            value2: Some(predicate_value_to_filter_value(upper)?),
        }]),
        PredicateExpr::InList { column, values } => Ok(vec![FilterClause {
            column: normalize_projection_identifier(column),
            op: FilterOp::In,
            value: predicate_value_to_filter_value(values)?,
            value2: None,
        }]),
        PredicateExpr::IsNull { column } => Ok(vec![FilterClause {
            column: normalize_projection_identifier(column),
            op: FilterOp::IsNull,
            value: FilterValue::Bool(false),
            value2: None,
        }]),
        PredicateExpr::IsNotNull { column } => Ok(vec![FilterClause {
            column: normalize_projection_identifier(column),
            op: FilterOp::IsNotNull,
            value: FilterValue::Bool(false),
            value2: None,
        }]),
    }
}

fn predicate_value_to_filter_value(value: &PredicateValue) -> Result<FilterValue, String> {
    match value {
        PredicateValue::Int(v) => Ok(FilterValue::Int(*v)),
        PredicateValue::Bool(v) => Ok(FilterValue::Bool(*v)),
        PredicateValue::Str(v) => Ok(FilterValue::Str(v.clone())),
        PredicateValue::IntList(values) => Ok(FilterValue::IntList(values.clone())),
        PredicateValue::BoolList(values) => Ok(FilterValue::BoolList(values.clone())),
        PredicateValue::StrList(values) => Ok(FilterValue::StrList(values.clone())),
    }
}

/// What: Split filter SQL by AND, respecting BETWEEN...AND and NOT BETWEEN...AND boundaries.
#[cfg(test)]
#[allow(dead_code)]
fn split_and_respecting_between(filter_sql: &str) -> Vec<String> {
    let mut clauses = Vec::new();
    let mut current_pos = 0;
    let lower = filter_sql.to_ascii_lowercase();

    while current_pos < filter_sql.len() {
        // Check for NOT BETWEEN or BETWEEN pattern
        let remaining_lower = &lower[current_pos..];

        // Try to find BETWEEN keyword
        let between_pos = remaining_lower
            .find(" between ")
            .map(|pos| current_pos + pos);

        // Check if there's a NOT before BETWEEN
        let mut clause_start = current_pos;
        if let Some(b_pos) = between_pos {
            // Check backwards from BETWEEN to find column start
            let mut col_end = b_pos;
            while col_end > clause_start && lower.as_bytes()[col_end - 1].is_ascii_whitespace() {
                col_end -= 1;
            }

            // Scan back for "NOT"
            if col_end >= 4 && lower[col_end - 3..col_end].eq_ignore_ascii_case("not") {
                // Could be NOT BETWEEN, find actual column start
                let mut not_start = col_end - 3;
                while not_start > clause_start
                    && lower.as_bytes()[not_start - 1].is_ascii_whitespace()
                {
                    not_start -= 1;
                }
                // Scan back to find column name or AND boundary
                while not_start > clause_start
                    && (lower.as_bytes()[not_start - 1].is_ascii_alphanumeric()
                        || lower.as_bytes()[not_start - 1] == b'_')
                {
                    not_start -= 1;
                }

                // Check if there's an AND before this position
                let prefix = &lower[clause_start..not_start];
                if let Some(and_pos) = prefix.rfind(" and ") {
                    let clause = filter_sql[clause_start..clause_start + and_pos]
                        .trim()
                        .to_string();
                    if !clause.is_empty() {
                        clauses.push(clause);
                    }
                    clause_start = clause_start + and_pos + " and ".len();
                }
            }
        }

        // Now find the next AND that is NOT inside BETWEEN...AND
        // First, identify which ANDs close BETWEEN clauses (so we can skip them)
        // For each BETWEEN in the current clause, find its matching AND
        let mut between_closing_ands = std::collections::HashSet::new();

        let mut between_search_pos = clause_start;
        while between_search_pos < filter_sql.len() {
            let remaining_lower = &lower[between_search_pos..];
            if let Some(between_offset) = remaining_lower.find(" between ") {
                let between_pos = between_search_pos + between_offset;

                // Now find the first AND after this BETWEEN
                let after_between = &lower[between_pos + " between ".len()..];
                if let Some(and_offset) = after_between.find(" and ") {
                    let closing_and_pos = between_pos + " between ".len() + and_offset;
                    between_closing_ands.insert(closing_and_pos);

                    // Continue searching from after this AND to find nested BETWEENs
                    between_search_pos = closing_and_pos + " and ".len();
                } else {
                    // BETWEEN without closing AND in this clause (shouldn't happen in valid SQL)
                    break;
                }
            } else {
                break;
            }
        }

        // Now find the next AND that is NOT a BETWEEN-closing AND
        let mut next_and_pos = None;
        let mut search_pos = clause_start;

        while search_pos < filter_sql.len() {
            let remaining = &lower[search_pos..];

            // Find next AND
            if let Some(and_idx) = remaining.find(" and ") {
                let and_pos = search_pos + and_idx;

                // Check if this AND closes a BETWEEN clause
                if !between_closing_ands.contains(&and_pos) {
                    // This AND is a clause separator, not BETWEEN-closing
                    next_and_pos = Some(and_pos);
                    break;
                } else {
                    // This AND closes a BETWEEN, skip it and continue
                    search_pos = and_pos + " and ".len();
                }
            } else {
                // No more AND found
                break;
            }
        }

        if let Some(and_pos) = next_and_pos {
            let clause = filter_sql[clause_start..and_pos].trim().to_string();
            if !clause.is_empty() {
                clauses.push(clause);
            }
            current_pos = and_pos + " and ".len();
        } else {
            // No more ANDs, take the rest
            let clause = filter_sql[clause_start..].trim().to_string();
            if !clause.is_empty() {
                clauses.push(clause);
            }
            break;
        }
    }

    clauses
}

/// What: Parse a restricted SQL predicate subset into executable clauses.
///
/// Inputs:
/// - `filter_sql`: Raw filter SQL.
///
/// Output:
/// - Ordered filter clauses combined with logical `AND`.
/// - Phase 9c: Supports BETWEEN and IN predicates in addition to comparison operators
#[cfg(test)]
#[allow(dead_code)]
fn parse_filter_clauses(filter_sql: &str) -> Result<Vec<FilterClause>, String> {
    if filter_sql.trim().is_empty() {
        return Err("filter predicate is empty".to_string());
    }
    if !filter_sql.is_ascii() {
        return Err("filter predicate must use ASCII characters in this phase".to_string());
    }

    let normalized = format!(" {} ", filter_sql.to_ascii_lowercase());
    if normalized.contains(" or ") {
        return Err("filter predicate with OR is not supported in this phase".to_string());
    }

    // Phase 9c: Use BETWEEN-aware AND split to preserve BETWEEN...AND boundaries
    let raw_clauses = split_and_respecting_between(filter_sql);

    let mut clauses = Vec::new();
    for clause in raw_clauses {
        let clause = clause.trim();
        if clause.is_empty() {
            continue;
        }

        let lower_clause = clause.to_ascii_lowercase();

        // Phase 9a: Check for IS NULL / IS NOT NULL (must check before BETWEEN/IN)
        if lower_clause.ends_with(" is not null") {
            let column = extract_column_from_is_null_clause(clause, "IS NOT NULL")?;
            clauses.push(FilterClause {
                column,
                op: FilterOp::IsNotNull,
                value: FilterValue::Str("".to_string()),
                value2: None,
            });
            continue;
        }

        if lower_clause.ends_with(" is null") {
            let column = extract_column_from_is_null_clause(clause, "IS NULL")?;
            clauses.push(FilterClause {
                column,
                op: FilterOp::IsNull,
                value: FilterValue::Str("".to_string()),
                value2: None,
            });
            continue;
        }

        // Phase 9c: Check for NOT BETWEEN pattern (must check before BETWEEN)
        if lower_clause.contains(" not between ") {
            clauses.push(parse_not_between_clause(clause)?);
            continue;
        }

        // Phase 9c: Check for BETWEEN pattern
        if lower_clause.contains(" between ") {
            clauses.push(parse_between_clause(clause)?);
            continue;
        }

        // Phase 9c: Check for IN pattern
        if lower_clause.contains(" in (") {
            clauses.push(parse_in_clause(clause)?);
            continue;
        }

        // Standard comparison operators (Phase 9a/9b)
        let (column, op, literal) = parse_single_clause(clause)?;
        clauses.push(FilterClause {
            column,
            op,
            value: parse_filter_value(literal)?,
            value2: None,
        });
    }

    if clauses.is_empty() {
        return Err("filter predicate produced no executable clauses".to_string());
    }

    Ok(clauses)
}

/// What: Build a row mask for a batch given executable filter clauses.
///
/// Inputs:
/// - `batch`: Batch to evaluate.
/// - `clauses`: Parsed filter clauses.
///
/// Output:
/// - Boolean mask that marks rows passing all clauses.
fn build_filter_mask(
    batch: &RecordBatch,
    clauses: &[FilterClause],
) -> Result<BooleanArray, String> {
    let mut rows = vec![true; batch.num_rows()];

    for clause in clauses {
        let idx = resolve_schema_column_index(batch.schema().as_ref(), &clause.column)
            .ok_or_else(|| format!("filter column '{}' not found", clause.column))?;
        let array = batch.column(idx);

        for (row_idx, row_flag) in rows.iter_mut().enumerate() {
            if !*row_flag {
                continue;
            }

            let pass = evaluate_clause_at_row(array, row_idx, clause)?;
            if !pass {
                *row_flag = false;
            }
        }
    }

    Ok(BooleanArray::from(rows))
}

/// What: Evaluate one clause at one row index.
///
/// Inputs:
/// - `array`: Input column array.
/// - `row_idx`: Row index.
/// - `clause`: Clause metadata and literal.
///
/// Output:
/// - `true` when the row satisfies the clause.
/// - Phase 9c: Handles BETWEEN and IN operators with type dispatch
fn evaluate_clause_at_row(
    array: &ArrayRef,
    row_idx: usize,
    clause: &FilterClause,
) -> Result<bool, String> {
    // Phase 9a: Handle IS NULL / IS NOT NULL before checking for null values
    match clause.op {
        FilterOp::IsNull => {
            return Ok(array.is_null(row_idx));
        }
        FilterOp::IsNotNull => {
            return Ok(!array.is_null(row_idx));
        }
        _ => {}
    }

    // For other operators, null values fail the predicate
    if array.is_null(row_idx) {
        return Ok(false);
    }

    // Phase 9c: BETWEEN, NOT BETWEEN, and IN operators require special handling
    match clause.op {
        FilterOp::Between => {
            if let Some(upper_value) = &clause.value2 {
                return evaluate_between_at_row(
                    array,
                    row_idx,
                    &clause.column,
                    &clause.value,
                    upper_value,
                );
            }
            return Err("invalid BETWEEN: missing upper bound".to_string());
        }
        FilterOp::NotBetween => {
            if let Some(upper_value) = &clause.value2 {
                let result = evaluate_between_at_row(
                    array,
                    row_idx,
                    &clause.column,
                    &clause.value,
                    upper_value,
                )?;
                return Ok(!result); // Negate BETWEEN result
            }
            return Err("invalid NOT BETWEEN: missing upper bound".to_string());
        }
        FilterOp::In => {
            return evaluate_in_at_row(array, row_idx, &clause.column, &clause.value);
        }
        _ => {}
    }

    // Standard comparison operators (Phase 9a/9b)
    match (&clause.value, array.data_type()) {
        (FilterValue::Int(rhs), DataType::Int64) => {
            let lhs = downcast_required::<Int64Array>(array, "Int64")?.value(row_idx);
            Ok(compare_i64(lhs, *rhs, clause.op))
        }
        (FilterValue::Int(rhs), DataType::Int32) => {
            let lhs = i64::from(downcast_required::<Int32Array>(array, "Int32")?.value(row_idx));
            Ok(compare_i64(lhs, *rhs, clause.op))
        }
        (FilterValue::Int(rhs), DataType::Int16) => {
            let lhs = i64::from(downcast_required::<Int16Array>(array, "Int16")?.value(row_idx));
            Ok(compare_i64(lhs, *rhs, clause.op))
        }
        (FilterValue::Bool(rhs), DataType::Boolean) => {
            let lhs = downcast_required::<BooleanArray>(array, "Boolean")?.value(row_idx);
            Ok(compare_bool(lhs, *rhs, clause.op))
        }
        (FilterValue::Str(rhs), DataType::Utf8) => {
            let lhs = downcast_required::<StringArray>(array, "Utf8")?.value(row_idx);
            Ok(compare_str(lhs, rhs.as_str(), clause.op))
        }
        (FilterValue::Str(rhs), DataType::Date32) => {
            let parsed = parse_date32_filter_literal(rhs).ok_or_else(|| {
                format!(
                    "unsupported Date32 filter literal '{}' for column '{}': expected quoted date/datetime",
                    rhs, clause.column
                )
            })?;
            let lhs = i64::from(downcast_required::<Date32Array>(array, "Date32")?.value(row_idx));
            Ok(compare_i64(lhs, i64::from(parsed), clause.op))
        }
        (FilterValue::Str(rhs), DataType::Date64) => {
            let parsed = parse_date64_filter_literal(rhs).ok_or_else(|| {
                format!(
                    "unsupported Date64 filter literal '{}' for column '{}': expected quoted date/datetime",
                    rhs, clause.column
                )
            })?;
            let lhs = downcast_required::<Date64Array>(array, "Date64")?.value(row_idx);
            Ok(compare_i64(lhs, parsed, clause.op))
        }
        (FilterValue::Str(rhs), DataType::Timestamp(unit, timezone)) => {
            if timezone.is_some() {
                return Err(format!(
                    "unsupported timestamp filter for column '{}': timezone-bearing timestamps are not supported in this phase",
                    clause.column
                ));
            }

            let parsed = parse_timestamp_filter_literal_for_unit(rhs, unit).ok_or_else(|| {
                format!(
                    "unsupported timestamp filter literal '{}' for column '{}': expected quoted ISO-8601 or 'YYYY-MM-DD HH:MM:SS[.fff]'",
                    rhs, clause.column
                )
            })?;
            let lhs = read_timestamp_value(array, row_idx, unit)?;
            Ok(compare_i64(lhs, parsed, clause.op))
        }
        (FilterValue::Int(_), DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)) => {
            Err(format!(
                "unsupported temporal filter literal for column '{}': use quoted date/time literal",
                clause.column
            ))
        }
        (_, data_type) => Err(format!(
            "filter type mismatch for column '{}': unsupported type {:?}",
            clause.column, data_type
        )),
    }
}

/// What: Apply ordered projection expressions to all input batches.
///
/// Inputs:
/// - `input`: Source batches.
/// - `projection_exprs`: Ordered projection expressions.
///
/// Output:
/// - Projected batches preserving order and output column ordering.
pub(crate) fn apply_projection_pipeline(
    input: &[RecordBatch],
    projection_exprs: &[PhysicalExpr],
) -> Result<Vec<RecordBatch>, String> {
    let mut out = Vec::with_capacity(input.len());

    for batch in input {
        let mut projected_arrays = Vec::<ArrayRef>::new();
        let mut projected_fields = Vec::<Field>::new();

        for expr in projection_exprs {
            match expr {
                PhysicalExpr::ColumnRef { name } => {
                    let output_name = normalize_projection_identifier(name);
                    push_projected_column(
                        batch,
                        name,
                        Some(output_name.as_str()),
                        &mut projected_arrays,
                        &mut projected_fields,
                    )?;
                }
                PhysicalExpr::Raw { sql } => {
                    let raw = sql.trim();
                    if raw == "*" {
                        for (idx, field) in batch.schema().fields().iter().enumerate() {
                            projected_arrays.push(batch.column(idx).clone());
                            projected_fields.push(field.as_ref().clone());
                        }
                        continue;
                    }

                    let (name, output_name) = parse_projection_binding(raw)?;
                    push_projected_column(
                        batch,
                        &name,
                        Some(output_name.as_str()),
                        &mut projected_arrays,
                        &mut projected_fields,
                    )?;
                }
                PhysicalExpr::Predicate { .. } => {
                    return Err(
                        "projection expression cannot be a structured predicate".to_string()
                    );
                }
            }
        }

        let projected_schema = Arc::new(Schema::new(projected_fields));
        let projected = RecordBatch::try_new(projected_schema, projected_arrays)
            .map_err(|e| format!("failed to build projected record batch: {}", e))?;
        out.push(projected);
    }

    Ok(out)
}

/// What: Apply ORDER BY sorting to input batches.
///
/// Inputs:
/// - `input`: Source batches.
/// - `sort_exprs`: Ordered sort directives.
///
/// Output:
/// - Sorted batches with deterministic row order.
pub(crate) fn apply_sort_pipeline(
    input: &[RecordBatch],
    sort_exprs: &[PhysicalSortExpr],
) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let normalized_batches = normalize_batches_for_sort(input)?;
    let schema = normalized_batches[0].schema();
    let merged = concat_batches(&schema, &normalized_batches)
        .map_err(|e| format!("failed to merge batches for sorting: {}", e))?;

    if merged.num_rows() <= 1 {
        return Ok(vec![merged]);
    }

    let mut sort_columns = Vec::with_capacity(sort_exprs.len());
    for sort_expr in sort_exprs {
        let raw_name = match &sort_expr.expression {
            PhysicalExpr::ColumnRef { name } => name.trim().to_string(),
            PhysicalExpr::Raw { sql } => parse_projection_identifier(sql)?,
            PhysicalExpr::Predicate { .. } => {
                return Err("sort expression cannot be a structured predicate".to_string());
            }
        };

        let idx = resolve_schema_column_index(merged.schema().as_ref(), raw_name.as_str())
            .ok_or_else(|| format!("sort column '{}' not found", raw_name))?;

        sort_columns.push(SortColumn {
            values: merged.column(idx).clone(),
            options: Some(SortOptions {
                descending: !sort_expr.ascending,
                nulls_first: false,
            }),
        });
    }

    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| format!("failed to compute sort indices: {}", e))?;

    let sorted_columns = merged
        .columns()
        .iter()
        .map(|column| {
            take(column.as_ref(), &indices, None)
                .map_err(|e| format!("failed to reorder sorted column: {}", e))
        })
        .collect::<Result<Vec<ArrayRef>, String>>()?;

    let sorted = RecordBatch::try_new(merged.schema(), sorted_columns)
        .map_err(|e| format!("failed to build sorted record batch: {}", e))?;

    Ok(vec![sorted])
}

/// What: Apply LIMIT/OFFSET slicing to input batches while preserving row order.
///
/// Inputs:
/// - `input`: Ordered source batches.
/// - `limit_spec`: Count and offset values.
///
/// Output:
/// - Sliced batches that satisfy OFFSET then LIMIT semantics.
pub(crate) fn apply_limit_pipeline(
    input: &[RecordBatch],
    limit_spec: &PhysicalLimitSpec,
) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    if limit_spec.count == 0 {
        return Ok(vec![RecordBatch::new_empty(input[0].schema())]);
    }

    let mut remaining_offset = usize::try_from(limit_spec.offset)
        .map_err(|_| "OFFSET value is too large for this platform".to_string())?;
    let mut remaining_count = usize::try_from(limit_spec.count)
        .map_err(|_| "LIMIT value is too large for this platform".to_string())?;

    let mut out = Vec::new();
    for batch in input {
        if remaining_count == 0 {
            break;
        }

        let row_count = batch.num_rows();
        if remaining_offset >= row_count {
            remaining_offset -= row_count;
            continue;
        }

        let start = remaining_offset;
        remaining_offset = 0;
        let available = row_count - start;
        let take_count = std::cmp::min(available, remaining_count);
        if take_count == 0 {
            continue;
        }

        out.push(batch.slice(start, take_count));
        remaining_count -= take_count;
    }

    if out.is_empty() {
        return Ok(vec![RecordBatch::new_empty(input[0].schema())]);
    }

    Ok(out)
}

/// What: Normalize input batches into a compatible schema for global sorting.
///
/// Inputs:
/// - `input`: Source batches that may carry type variations across files.
///
/// Output:
/// - New batch list with per-column types aligned for concat and sort.
pub(crate) fn normalize_batches_for_sort(
    input: &[RecordBatch],
) -> Result<Vec<RecordBatch>, String> {
    if input.is_empty() {
        return Ok(Vec::new());
    }

    let column_count = input[0].num_columns();
    for batch in input {
        if batch.num_columns() != column_count {
            return Err("cannot sort batches with different column counts".to_string());
        }
    }

    let mut fields = Vec::with_capacity(column_count);
    for col_idx in 0..column_count {
        let base_field = input[0].schema().field(col_idx).as_ref().clone();
        let mut target_type: Option<DataType> = None;

        for batch in input {
            let data_type = batch.column(col_idx).data_type();
            target_type = Some(unify_sort_column_type(target_type.as_ref(), data_type));
        }

        let resolved_type = target_type.unwrap_or(DataType::Null);
        fields.push(Field::new(
            base_field.name(),
            resolved_type,
            base_field.is_nullable(),
        ));
    }

    let target_schema = Arc::new(Schema::new(fields));
    let mut normalized = Vec::with_capacity(input.len());
    for batch in input {
        let mut arrays = Vec::with_capacity(column_count);
        for col_idx in 0..column_count {
            let source = batch.column(col_idx);
            let target_type = target_schema.field(col_idx).data_type();
            if source.data_type() == target_type {
                arrays.push(source.clone());
                continue;
            }

            let casted = cast(source.as_ref(), target_type).map_err(|e| {
                format!(
                    "failed to cast column '{}' from {:?} to {:?} for sorting: {}",
                    target_schema.field(col_idx).name(),
                    source.data_type(),
                    target_type,
                    e
                )
            })?;
            arrays.push(casted);
        }

        let rebuilt = RecordBatch::try_new(target_schema.clone(), arrays)
            .map_err(|e| format!("failed to rebuild normalized batch for sorting: {}", e))?;
        normalized.push(rebuilt);
    }

    Ok(normalized)
}

fn unify_sort_column_type(existing: Option<&DataType>, incoming: &DataType) -> DataType {
    match existing {
        None => incoming.clone(),
        Some(current) if current == incoming => current.clone(),
        Some(DataType::Null) => incoming.clone(),
        Some(current) if matches!(incoming, DataType::Null) => current.clone(),
        Some(current) if is_int_type(current) && is_int_type(incoming) => DataType::Int64,
        Some(current)
            if matches!(current, DataType::Utf8) || matches!(incoming, DataType::Utf8) =>
        {
            DataType::Utf8
        }
        Some(current) => current.clone(),
    }
}

fn is_int_type(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int16 | DataType::Int32 | DataType::Int64
    )
}

fn push_projected_column(
    batch: &RecordBatch,
    name: &str,
    output_name: Option<&str>,
    arrays: &mut Vec<ArrayRef>,
    fields: &mut Vec<Field>,
) -> Result<(), String> {
    let idx = resolve_schema_column_index(batch.schema().as_ref(), name)
        .ok_or_else(|| format!("projection column '{}' not found", name))?;

    arrays.push(batch.column(idx).clone());
    let base_field = batch.schema().field(idx).as_ref().clone();
    let field = output_name
        .filter(|value| !value.trim().is_empty())
        .map(|value| base_field.clone().with_name(value.trim()))
        .unwrap_or(base_field);
    fields.push(field);
    Ok(())
}

fn normalize_projection_identifier(raw: &str) -> String {
    let trimmed = raw.trim();
    let no_prefix = if let Some((_, rhs)) = trimmed.rsplit_once('.') {
        rhs
    } else {
        trimmed
    };

    no_prefix
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_string()
}

fn parse_projection_binding(raw_sql: &str) -> Result<(String, String), String> {
    let trimmed = raw_sql.trim();

    if let Some((lhs, rhs)) = split_projection_alias(trimmed) {
        if !is_simple_identifier_reference(lhs) {
            return Err(format!(
                "unsupported projection expression '{}': only simple column references are supported in this phase",
                trimmed
            ));
        }

        let alias = normalize_alias_identifier(rhs).ok_or_else(|| {
            format!(
                "unsupported projection alias '{}': alias must be a simple identifier",
                rhs.trim()
            )
        })?;
        return Ok((normalize_projection_identifier(lhs), alias));
    }

    let source = parse_projection_identifier(trimmed)?;
    let output = normalize_projection_identifier(trimmed);
    Ok((source, output))
}

fn split_projection_alias(raw: &str) -> Option<(&str, &str)> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    let lower = trimmed.to_ascii_lowercase();
    if let Some(idx) = lower.rfind(" as ") {
        let lhs = trimmed[..idx].trim();
        let rhs = trimmed[idx + 4..].trim();
        if lhs.is_empty() || rhs.is_empty() {
            return None;
        }
        return Some((lhs, rhs));
    }

    let mut parts = trimmed.split_whitespace();
    let first = parts.next()?;
    let second = parts.next()?;
    if parts.next().is_some() {
        return None;
    }

    if !is_simple_identifier_reference(first) {
        return None;
    }

    Some((first, second))
}

fn normalize_alias_identifier(raw: &str) -> Option<String> {
    if !is_identifier_segment(raw) {
        return None;
    }

    Some(
        raw.trim()
            .trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']')
            .to_string(),
    )
}

pub(crate) fn resolve_schema_column_index(schema: &Schema, requested: &str) -> Option<usize> {
    let requested = requested.trim();
    if requested.is_empty() {
        return None;
    }

    if let Ok(idx) = schema.index_of(requested) {
        return Some(idx);
    }

    let normalized = normalize_projection_identifier(requested);
    if normalized != requested
        && let Ok(idx) = schema.index_of(normalized.as_str())
    {
        return Some(idx);
    }

    if let Some(idx) = fallback_semantic_to_physical_column_index(schema, normalized.as_str()) {
        return Some(idx);
    }

    None
}

fn fallback_semantic_to_physical_column_index(schema: &Schema, normalized: &str) -> Option<usize> {
    let requested = normalized.to_ascii_lowercase();

    if requested == "id" {
        if let Ok(idx) = schema.index_of("c1") {
            return Some(idx);
        }

        if let Some((idx, _)) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name().ends_with("_c1"))
        {
            return Some(idx);
        }
    }

    if (requested == "name" || requested == "value")
        && let Ok(idx) = schema.index_of("c2")
    {
        return Some(idx);
    }

    if requested == "document" {
        if let Some((idx, _)) = schema
            .fields()
            .iter()
            .enumerate()
            .find(|(_, field)| field.name().ends_with("_c2"))
        {
            return Some(idx);
        }

        if let Ok(idx) = schema.index_of("c2") {
            return Some(idx);
        }
    }

    None
}

/// What: Validate and parse a projection raw expression into a column identifier.
///
/// Inputs:
/// - `raw_sql`: Raw projection SQL expression.
///
/// Output:
/// - Parsed column identifier for projection lookup.
fn parse_projection_identifier(raw_sql: &str) -> Result<String, String> {
    let trimmed = raw_sql.trim();
    if let Some((lhs, _)) = split_projection_alias(trimmed) {
        if !is_simple_identifier_reference(lhs) {
            return Err(format!(
                "unsupported projection expression '{}': only simple column references are supported in this phase",
                raw_sql.trim()
            ));
        }
        return Ok(normalize_projection_identifier(lhs));
    }

    if !is_simple_identifier_reference(trimmed) {
        return Err(format!(
            "unsupported projection expression '{}': only '*' or simple column references are supported in this phase",
            raw_sql.trim()
        ));
    }
    Ok(normalize_projection_identifier(trimmed))
}

/// What: Validate whether an expression is a simple identifier reference.
///
/// Inputs:
/// - `expr`: Raw expression text.
///
/// Output:
/// - `true` when expression is `col` or `qualifier.col` with common quotes.
fn is_simple_identifier_reference(expr: &str) -> bool {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.contains('(')
        || trimmed.contains(')')
        || trimmed.contains('+')
        || trimmed.contains('-')
        || trimmed.contains('*')
        || trimmed.contains('/')
        || trimmed.contains(',')
        || trimmed.contains(':')
        || trimmed.contains(' ')
    {
        return false;
    }

    let parts = trimmed.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 2 {
        return false;
    }

    parts.into_iter().all(is_identifier_segment)
}

fn is_identifier_segment(segment: &str) -> bool {
    let raw = segment
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']');

    !raw.is_empty()
        && raw
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

#[cfg(test)]
#[allow(dead_code)]
fn parse_single_clause(input: &str) -> Result<(String, FilterOp, &str), String> {
    const OPS: [(&str, FilterOp); 6] = [
        ("!=", FilterOp::Ne), // Check compound operators FIRST before single-char
        (">=", FilterOp::Ge),
        ("<=", FilterOp::Le),
        ("=", FilterOp::Eq),
        (">", FilterOp::Gt),
        ("<", FilterOp::Lt),
    ];

    for (token, op) in OPS {
        if let Some((lhs, rhs)) = input.split_once(token) {
            if !is_simple_identifier_reference(lhs) {
                return Err(format!(
                    "unsupported filter column expression '{}': only simple column references are supported",
                    lhs.trim()
                ));
            }
            let column = normalize_projection_identifier(lhs);
            let literal = rhs.trim();
            if column.is_empty() || literal.is_empty() {
                return Err(format!("invalid filter clause '{}'", input));
            }
            return Ok((column, op, literal));
        }
    }

    Err(format!(
        "unsupported filter clause '{}': expected one of =, !=, >, >=, <, <=",
        input
    ))
}

/// What: Extract column name from IS NULL / IS NOT NULL clause.
///
/// Inputs:
/// - `clause`: Raw clause string (e.g., "column_name IS NULL").
/// - `keyword`: The keyword to strip ("IS NULL" or "IS NOT NULL").
///
/// Output:
/// - Column name, normalized.
#[cfg(test)]
#[allow(dead_code)]
fn extract_column_from_is_null_clause(clause: &str, keyword: &str) -> Result<String, String> {
    let lower_clause = clause.to_ascii_lowercase();
    let lower_keyword = keyword.to_ascii_lowercase();

    if let Some(pos) = lower_clause.rfind(&lower_keyword) {
        let column_part = clause[..pos].trim();
        if !column_part.is_empty() && is_simple_identifier_reference(column_part) {
            return Ok(normalize_projection_identifier(column_part));
        }
    }

    Err(format!(
        "invalid {} predicate '{}': expected 'column_name {}'",
        keyword, clause, keyword
    ))
}

/// What: Parse BETWEEN predicate: "column BETWEEN lower AND upper"
///
/// Inputs:
/// - `clause`: Raw BETWEEN predicate string.
///
/// Output:
/// - FilterClause with op=Between, value=lower bound, value2=upper bound
#[cfg(test)]
#[allow(dead_code)]
fn parse_between_clause(clause: &str) -> Result<FilterClause, String> {
    let lower_case = clause.to_ascii_lowercase();

    // Locate BETWEEN keyword (UTF-8 safe: find the substring then extract positions)
    if let Some(between_idx) = lower_case.find(" between ") {
        let column_part = clause[..between_idx].trim();

        // Everything after "BETWEEN " - use string operations instead of hardcoded byte offsets
        let after_between_idx = between_idx + " between ".len();
        let after_between = &clause[after_between_idx..];
        let after_between_lower = after_between.to_ascii_lowercase();

        // Find AND keyword that separates lower and upper bounds
        if let Some(and_idx) = after_between_lower.find(" and ") {
            let lower_part = after_between[..and_idx].trim();
            let upper_part = after_between[and_idx + " and ".len()..].trim();

            let column = normalize_projection_identifier(column_part);
            let lower_value = parse_filter_value(lower_part)?;
            let upper_value = parse_filter_value(upper_part)?;

            return Ok(FilterClause {
                column,
                op: FilterOp::Between,
                value: lower_value,
                value2: Some(upper_value),
            });
        }
    }

    Err(format!(
        "invalid BETWEEN predicate '{}': expected format 'column BETWEEN lower AND upper'",
        clause
    ))
}

/// What: Parse NOT BETWEEN predicate: "column NOT BETWEEN lower AND upper"
///
/// Inputs:
/// - `clause`: Raw NOT BETWEEN predicate string.
///
/// Output:
/// - FilterClause with op=NotBetween, value=lower bound, value2=upper bound
#[cfg(test)]
#[allow(dead_code)]
fn parse_not_between_clause(clause: &str) -> Result<FilterClause, String> {
    let lower_case = clause.to_ascii_lowercase();

    // Locate NOT BETWEEN keyword
    if let Some(not_between_idx) = lower_case.find(" not between ") {
        let column_part = clause[..not_between_idx].trim();

        // Everything after "NOT BETWEEN "
        let after_not_between_idx = not_between_idx + " not between ".len();
        let after_not_between = &clause[after_not_between_idx..];
        let after_not_between_lower = after_not_between.to_ascii_lowercase();

        // Find AND keyword that separates lower and upper bounds
        if let Some(and_idx) = after_not_between_lower.find(" and ") {
            let lower_part = after_not_between[..and_idx].trim();
            let upper_part = after_not_between[and_idx + " and ".len()..].trim();

            let column = normalize_projection_identifier(column_part);
            let lower_value = parse_filter_value(lower_part)?;
            let upper_value = parse_filter_value(upper_part)?;

            return Ok(FilterClause {
                column,
                op: FilterOp::NotBetween,
                value: lower_value,
                value2: Some(upper_value),
            });
        }
    }

    Err(format!(
        "invalid NOT BETWEEN predicate '{}': expected format 'column NOT BETWEEN lower AND upper'",
        clause
    ))
}

/// What: Parse IN predicate: "column IN (val1, val2, ...)"
///
/// Inputs:
/// - `clause`: Raw IN predicate string.
///
/// Output:
/// - FilterClause with op=In, value=homogeneous list (IntList, StrList, or BoolList)
#[cfg(test)]
#[allow(dead_code)]
fn parse_in_clause(clause: &str) -> Result<FilterClause, String> {
    let lower_case = clause.to_ascii_lowercase();

    // Pattern: "column IN (val1, val2, ...)"
    if let Some(in_pos) = lower_case.find(" in (") {
        let column_part = clause[..in_pos].trim();
        let list_start = in_pos + 5; // " in (" is 5 chars
        let after_in = &clause[list_start..];

        // Find closing parenthesis
        if let Some(close_paren) = after_in.rfind(')') {
            let values_str = &after_in[..close_paren];
            let column = normalize_projection_identifier(column_part);

            // Parse value list, inferring type from first value
            let values: Vec<&str> = values_str.split(',').map(|v| v.trim()).collect();

            if values.is_empty() {
                return Err(format!(
                    "invalid IN predicate '{}': value list is empty",
                    clause
                ));
            }

            // Infer type from first value
            let first_value = parse_filter_value(values[0])?;

            // Parse all values and ensure homogeneity
            match &first_value {
                FilterValue::Int(_) => {
                    let mut int_list = Vec::new();
                    for val_str in &values {
                        let parsed = parse_filter_value(val_str)?;
                        match parsed {
                            FilterValue::Int(i) => int_list.push(i),
                            _ => {
                                return Err(format!(
                                    "invalid IN predicate '{}': mixed types in list",
                                    clause
                                ));
                            }
                        }
                    }
                    return Ok(FilterClause {
                        column,
                        op: FilterOp::In,
                        value: FilterValue::IntList(int_list),
                        value2: None,
                    });
                }
                FilterValue::Str(_) => {
                    let mut str_list = Vec::new();
                    for val_str in &values {
                        let parsed = parse_filter_value(val_str)?;
                        match parsed {
                            FilterValue::Str(s) => str_list.push(s),
                            _ => {
                                return Err(format!(
                                    "invalid IN predicate '{}': mixed types in list",
                                    clause
                                ));
                            }
                        }
                    }
                    return Ok(FilterClause {
                        column,
                        op: FilterOp::In,
                        value: FilterValue::StrList(str_list),
                        value2: None,
                    });
                }
                FilterValue::Bool(_) => {
                    let mut bool_list = Vec::new();
                    for val_str in &values {
                        let parsed = parse_filter_value(val_str)?;
                        match parsed {
                            FilterValue::Bool(b) => bool_list.push(b),
                            _ => {
                                return Err(format!(
                                    "invalid IN predicate '{}': mixed types in list",
                                    clause
                                ));
                            }
                        }
                    }
                    return Ok(FilterClause {
                        column,
                        op: FilterOp::In,
                        value: FilterValue::BoolList(bool_list),
                        value2: None,
                    });
                }
                _ => {
                    return Err(format!(
                        "invalid IN predicate '{}': unsupported list type",
                        clause
                    ));
                }
            }
        }
    }

    Err(format!(
        "invalid IN predicate '{}': expected format 'column IN (val1, val2, ...)'",
        clause
    ))
}

#[cfg(test)]
#[allow(dead_code)]
fn parse_filter_value(raw: &str) -> Result<FilterValue, String> {
    let trimmed = raw.trim();
    if trimmed.eq_ignore_ascii_case("true") {
        return Ok(FilterValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Ok(FilterValue::Bool(false));
    }
    if let Ok(value) = trimmed.parse::<i64>() {
        return Ok(FilterValue::Int(value));
    }

    if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
        let inner = trimmed[1..trimmed.len() - 1].trim().to_string();
        return Ok(FilterValue::Str(inner));
    }

    Err(format!(
        "unsupported filter literal '{}': expected int, bool, or quoted string",
        raw
    ))
}

#[cfg(test)]
#[allow(dead_code)]
pub(crate) fn split_case_insensitive<'a>(text: &'a str, token: &str) -> Vec<&'a str> {
    let splitter = format!(" {} ", token.to_ascii_lowercase());
    let lower = text.to_ascii_lowercase();

    let mut out = Vec::new();
    let mut start = 0usize;
    for (idx, _) in lower.match_indices(&splitter) {
        out.push(text[start..idx].trim());
        start = idx + splitter.len();
    }

    out.push(text[start..].trim());
    out
}

fn downcast_required<'a, T: 'static>(array: &'a ArrayRef, expected: &str) -> Result<&'a T, String> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| format!("failed to downcast array to {}", expected))
}

fn read_timestamp_value(array: &ArrayRef, row_idx: usize, unit: &TimeUnit) -> Result<i64, String> {
    match unit {
        TimeUnit::Second => {
            Ok(downcast_required::<TimestampSecondArray>(array, "TimestampSecond")?.value(row_idx))
        }
        TimeUnit::Millisecond => Ok(downcast_required::<TimestampMillisecondArray>(
            array,
            "TimestampMillisecond",
        )?
        .value(row_idx)),
        TimeUnit::Microsecond => Ok(downcast_required::<TimestampMicrosecondArray>(
            array,
            "TimestampMicrosecond",
        )?
        .value(row_idx)),
        TimeUnit::Nanosecond => Ok(downcast_required::<TimestampNanosecondArray>(
            array,
            "TimestampNanosecond",
        )?
        .value(row_idx)),
    }
}

fn parse_timestamp_filter_literal_for_unit(raw: &str, unit: &TimeUnit) -> Option<i64> {
    let nanos = parse_timestamp_literal_to_utc_nanos(raw)?;
    let unit_value = match unit {
        TimeUnit::Second => nanos / 1_000_000_000,
        TimeUnit::Millisecond => nanos / 1_000_000,
        TimeUnit::Microsecond => nanos / 1_000,
        TimeUnit::Nanosecond => nanos,
    };
    i64::try_from(unit_value).ok()
}

fn parse_timestamp_literal_to_utc_nanos(raw: &str) -> Option<i128> {
    if let Ok(parsed) = DateTime::parse_from_rfc3339(raw) {
        return parsed.timestamp_nanos_opt().map(i128::from);
    }
    if let Ok(parsed) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f") {
        return parsed.and_utc().timestamp_nanos_opt().map(i128::from);
    }
    if let Ok(parsed) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        return parsed
            .and_hms_opt(0, 0, 0)
            .and_then(|value| value.and_utc().timestamp_nanos_opt())
            .map(i128::from);
    }
    None
}

fn parse_date32_filter_literal(raw: &str) -> Option<i32> {
    let parsed_date = if let Ok(date) = NaiveDate::parse_from_str(raw, "%Y-%m-%d") {
        date
    } else if let Ok(datetime) = NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f") {
        datetime.date()
    } else if let Ok(datetime) = DateTime::parse_from_rfc3339(raw) {
        datetime.date_naive()
    } else {
        return None;
    };

    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)?;
    Some(parsed_date.signed_duration_since(epoch).num_days() as i32)
}

fn parse_date64_filter_literal(raw: &str) -> Option<i64> {
    let millis = if let Some(nanos) = parse_timestamp_literal_to_utc_nanos(raw) {
        nanos / 1_000_000
    } else {
        return None;
    };
    i64::try_from(millis).ok()
}

/// What: Evaluate BETWEEN predicate for a single row.
///
/// Inputs:
/// - `array`: Column array to check
/// - `row_idx`: Row index
/// - `column`: Column name (for error messages)
/// - `lower`: Lower bound FilterValue
/// - `upper`: Upper bound FilterValue
///
/// Output:
/// - `true` if lower <= value <= upper (both inclusive)
fn evaluate_between_at_row(
    array: &ArrayRef,
    row_idx: usize,
    column: &str,
    lower: &FilterValue,
    upper: &FilterValue,
) -> Result<bool, String> {
    match (lower, upper, array.data_type()) {
        (FilterValue::Int(lower_val), FilterValue::Int(upper_val), DataType::Int64) => {
            let lhs = downcast_required::<Int64Array>(array, "Int64")?.value(row_idx);
            Ok(lhs >= *lower_val && lhs <= *upper_val)
        }
        (FilterValue::Int(lower_val), FilterValue::Int(upper_val), DataType::Int32) => {
            let lhs = i64::from(downcast_required::<Int32Array>(array, "Int32")?.value(row_idx));
            Ok(lhs >= *lower_val && lhs <= *upper_val)
        }
        (FilterValue::Int(lower_val), FilterValue::Int(upper_val), DataType::Int16) => {
            let lhs = i64::from(downcast_required::<Int16Array>(array, "Int16")?.value(row_idx));
            Ok(lhs >= *lower_val && lhs <= *upper_val)
        }
        (FilterValue::Str(lower_str), FilterValue::Str(upper_str), DataType::Utf8) => {
            let lhs = downcast_required::<StringArray>(array, "Utf8")?.value(row_idx);
            Ok(lhs >= lower_str.as_str() && lhs <= upper_str.as_str())
        }
        (FilterValue::Str(lower_str), FilterValue::Str(upper_str), DataType::Date32) => {
            let lower_parsed = parse_date32_filter_literal(lower_str).ok_or_else(|| {
                format!(
                    "invalid Date32 lower bound '{}' for BETWEEN in column '{}'",
                    lower_str, column
                )
            })?;
            let upper_parsed = parse_date32_filter_literal(upper_str).ok_or_else(|| {
                format!(
                    "invalid Date32 upper bound '{}' for BETWEEN in column '{}'",
                    upper_str, column
                )
            })?;
            let lhs = i64::from(downcast_required::<Date32Array>(array, "Date32")?.value(row_idx));
            Ok(lhs >= i64::from(lower_parsed) && lhs <= i64::from(upper_parsed))
        }
        _ => Err(format!(
            "unsupported type combination for BETWEEN in column '{}': {:?}",
            column,
            array.data_type()
        )),
    }
}

/// What: Evaluate IN predicate for a single row.
///
/// Inputs:
/// - `array`: Column array to check
/// - `row_idx`: Row index
/// - `column`: Column name (for error messages)
/// - `value`: FilterValue containing the list (IntList, StrList, or BoolList)
///
/// Output:
/// - `true` if column value is in the list
fn evaluate_in_at_row(
    array: &ArrayRef,
    row_idx: usize,
    column: &str,
    value: &FilterValue,
) -> Result<bool, String> {
    match (value, array.data_type()) {
        (FilterValue::IntList(list), DataType::Int64) => {
            let lhs = downcast_required::<Int64Array>(array, "Int64")?.value(row_idx);
            Ok(list.contains(&lhs))
        }
        (FilterValue::IntList(list), DataType::Int32) => {
            let lhs = i64::from(downcast_required::<Int32Array>(array, "Int32")?.value(row_idx));
            Ok(list.contains(&lhs))
        }
        (FilterValue::IntList(list), DataType::Int16) => {
            let lhs = i64::from(downcast_required::<Int16Array>(array, "Int16")?.value(row_idx));
            Ok(list.contains(&lhs))
        }
        (FilterValue::StrList(list), DataType::Utf8) => {
            let lhs = downcast_required::<StringArray>(array, "Utf8")?.value(row_idx);
            Ok(list.iter().any(|s| s.as_str() == lhs))
        }
        (FilterValue::BoolList(list), DataType::Boolean) => {
            let lhs = downcast_required::<BooleanArray>(array, "Boolean")?.value(row_idx);
            Ok(list.contains(&lhs))
        }
        _ => Err(format!(
            "unsupported type for IN in column '{}': {:?}",
            column,
            array.data_type()
        )),
    }
}

fn compare_i64(lhs: i64, rhs: i64, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Ge => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Le => lhs <= rhs,
        FilterOp::Between
        | FilterOp::NotBetween
        | FilterOp::In
        | FilterOp::IsNull
        | FilterOp::IsNotNull => false, // Should not reach here
    }
}

fn compare_bool(lhs: bool, rhs: bool, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs && !rhs,
        FilterOp::Ge => lhs == rhs || (lhs && !rhs),
        FilterOp::Lt => !lhs && rhs,
        FilterOp::Le => lhs == rhs || (!lhs && rhs),
        FilterOp::Between
        | FilterOp::NotBetween
        | FilterOp::In
        | FilterOp::IsNull
        | FilterOp::IsNotNull => false, // Should not reach here
    }
}

fn compare_str(lhs: &str, rhs: &str, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Ge => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Le => lhs <= rhs,
        FilterOp::Between
        | FilterOp::NotBetween
        | FilterOp::In
        | FilterOp::IsNull
        | FilterOp::IsNotNull => false, // Should not reach here
    }
}

#[cfg(test)]
#[path = "../tests/services_query_execution_tests.rs"]
mod tests;
