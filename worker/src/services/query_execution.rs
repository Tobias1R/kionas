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
use kionas::planner::{PhysicalExpr, PhysicalLimitSpec, PhysicalSortExpr};
use std::sync::Arc;

#[derive(Debug, Clone)]
enum FilterValue {
    Int(i64),
    Bool(bool),
    Str(String),
}

#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,
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

/// What: Apply a simple conjunction filter pipeline to all input batches.
///
/// Inputs:
/// - `input`: Source batches.
/// - `filter_sql`: Raw SQL predicate containing simple `AND` clauses.
///
/// Output:
/// - Filtered batches preserving input order.
pub(crate) fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
) -> Result<Vec<RecordBatch>, String> {
    let clauses = parse_filter_clauses(filter_sql)?;
    let mut out = Vec::with_capacity(input.len());

    for batch in input {
        let mask = build_filter_mask(batch, &clauses)?;
        let filtered = filter_record_batch(batch, &mask)
            .map_err(|e| format!("failed to apply filter: {}", e))?;
        out.push(filtered);
    }

    Ok(out)
}

/// What: Parse a restricted SQL predicate subset into executable clauses.
///
/// Inputs:
/// - `filter_sql`: Raw filter SQL.
///
/// Output:
/// - Ordered filter clauses combined with logical `AND`.
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

    let mut clauses = Vec::new();
    for raw_clause in split_case_insensitive(filter_sql, "AND") {
        let clause = raw_clause.trim();
        if clause.is_empty() {
            continue;
        }

        let (column, op, literal) = parse_single_clause(clause)?;
        clauses.push(FilterClause {
            column,
            op,
            value: parse_filter_value(literal)?,
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
fn evaluate_clause_at_row(
    array: &ArrayRef,
    row_idx: usize,
    clause: &FilterClause,
) -> Result<bool, String> {
    if array.is_null(row_idx) {
        return Ok(false);
    }

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

                    let name = parse_projection_identifier(raw)?;
                    let output_name = normalize_projection_identifier(raw);
                    push_projected_column(
                        batch,
                        &name,
                        Some(output_name.as_str()),
                        &mut projected_arrays,
                        &mut projected_fields,
                    )?;
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
    if !is_simple_identifier_reference(raw_sql) {
        return Err(format!(
            "unsupported projection expression '{}': only '*' or simple column references are supported in this phase",
            raw_sql.trim()
        ));
    }
    Ok(normalize_projection_identifier(raw_sql))
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

fn parse_single_clause(input: &str) -> Result<(String, FilterOp, &str), String> {
    const OPS: [(&str, FilterOp); 6] = [
        (">=", FilterOp::Ge),
        ("<=", FilterOp::Le),
        ("!=", FilterOp::Ne),
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
        return Ok(FilterValue::Str(trimmed[1..trimmed.len() - 1].to_string()));
    }

    Err(format!(
        "unsupported filter literal '{}': expected int, bool, or quoted string",
        raw
    ))
}

fn split_case_insensitive<'a>(text: &'a str, token: &str) -> Vec<&'a str> {
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

fn compare_i64(lhs: i64, rhs: i64, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Ge => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Le => lhs <= rhs,
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
    }
}

#[cfg(test)]
#[path = "../tests/services_query_execution_tests.rs"]
mod tests;
