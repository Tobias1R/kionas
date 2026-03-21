# Worker Filter Execution: Detailed Code Reference

## Complete Function Signatures and Implementations

### 1. Filter Pipeline Entry Points

#### 1.1 apply_filter_pipeline (Public API)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L72-L90)

```rust
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
```

---

### 2. SQL Parsing Pipeline

#### 2.1 parse_filter_clauses (Private)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L127-L169)

**Signature**:
```rust
fn parse_filter_clauses(filter_sql: &str) -> Result<Vec<FilterClause>, String>
```

**Key Validation Steps**:
1. Empty string check → `"filter predicate is empty"`
2. ASCII-only validation → `"filter predicate must use ASCII characters in this phase"`
3. OR detection → `"filter predicate with OR is not supported in this phase"`
4. Clause parsing loop (by "AND" delimiter)
5. Empty clauses check → `"filter predicate produced no executable clauses"`

**Returns**: `Vec<FilterClause>` containing parsed filter metadata

---

#### 2.2 parse_single_clause (Private)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L703-L730)

**Signature**:
```rust
fn parse_single_clause(input: &str) -> Result<(String, FilterOp, &str), String>
```

**Processing**:
- Operators tested in order: `>=`, `<=`, `!=`, `=`, `>`, `<`
- Uses `split_once()` on first matching operator
- Validates LHS is simple identifier: `is_simple_identifier_reference()`
- Returns tuple: `(column: String, op: FilterOp, literal: &str)`

**Error messages**:
- `"unsupported filter column expression '...': only simple column references are supported"`
- `"invalid filter clause '{}'"`

---

#### 2.3 parse_filter_value (Private)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L736-L800+)

**Signature**:
```rust
fn parse_filter_value(raw: &str) -> Result<FilterValue, String>
```

**Parsing Logic**:
- Quoted strings (single or double) → `FilterValue::Str`
- Case-insensitive `true`/`false` → `FilterValue::Bool`
- Numeric literals → `FilterValue::Int` (parse as i64)

**Error conditions**:
- Non-UTF8 encoded strings
- Unparseable numeric formats

---

### 3. Row Mask Building

#### 3.1 build_filter_mask (Private)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L139-L169)

**Signature**:
```rust
fn build_filter_mask(
    batch: &RecordBatch,
    clauses: &[FilterClause],
) -> Result<BooleanArray, String>
```

**Algorithm**:
```
for each clause:
    find column index in schema
    for each row:
        if row already marked false, skip
        evaluate clause at row
        if evaluation returns false, mark row false

return BooleanArray of accumulated row markers
```

**Error scenarios**:
- Column not found in schema → `"filter column '{}' not found"`
- evaluate_clause_at_row failures propagate

---

### 4. Row-Level Clause Evaluation

#### 4.1 evaluate_clause_at_row (Private)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L171-L245)

**Signature**:
```rust
fn evaluate_clause_at_row(
    array: &ArrayRef,
    row_idx: usize,
    clause: &FilterClause,
) -> Result<bool, String>
```

**Type Handling Matrix**:

| Data Type | Filter Value | Handling | Error Case |
|-----------|--------------|----------|-----------|
| Int64 | Int(i64) | Direct `compare_i64()` | ✓ Works |
| Int32 | Int(i64) | Cast to i64, compare | ✓ Works |
| Int16 | Int(i64) | Cast to i64, compare | ✓ Works |
| Boolean | Bool(bool) | Direct `compare_bool()` | ✗ Type mismatch if Int/Str |
| Utf8 | Str(String) | Direct `compare_str()` | ✗ Type mismatch if Int/Bool |
| Date32 | Str | Parse via `parse_date32_filter_literal()` | ✗ If parse fails |
| Date64 | Str | Parse via `parse_date64_filter_literal()` | ✗ If parse fails |
| Timestamp(unit, None) | Str | Parse via `parse_timestamp_filter_literal_for_unit()` | ✗ If parse fails |
| Timestamp(unit, Some(_)) | Any | **ERROR** | ✗ Timezone not supported |
| Any | Int | (temporal types only error) | - |
| Unmatched | Any | **ERROR** | ✗ Type mismatch |

**NULL Handling** [Line 187]:
```rust
if array.is_null(row_idx) {
    return Ok(false);  // NULL never matches any predicate
}
```

**Error Messages**:
```
"unsupported Date32 filter literal '{literal}' for column '{column}': expected quoted date/datetime"
"unsupported Date64 filter literal '{literal}' for column '{column}': expected quoted date/datetime"
"unsupported timestamp filter literal '{literal}' for column '{column}': expected quoted ISO-8601 or 'YYYY-MM-DD HH:MM:SS[.fff]'"
"unsupported temporal filter literal for column '{column}': use quoted date/time literal"
"unsupported timestamp filter for column '{column}': timezone-bearing timestamps are not supported in this phase"
"filter type mismatch for column '{column}': unsupported type {type:?}"
```

---

### 5. Type Enums

#### 5.1 FilterOp Enum
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L28-L32)

```rust
#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq,  // =
    Ne,  // !=
    Gt,  // >
    Ge,  // >=
    Lt,  // <
    Le,  // <=
}
```

---

#### 5.2 FilterValue Enum
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L20-L24)

```rust
#[derive(Debug, Clone)]
enum FilterValue {
    Int(i64),
    Bool(bool),
    Str(String),
}
```

---

#### 5.3 FilterClause Struct
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L35-L38)

```rust
#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,
}
```

---

### 6. Column Resolution

#### 6.1 resolve_schema_column_index (Public for tests)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L572-L600)

**Signature**:
```rust
pub(crate) fn resolve_schema_column_index(schema: &Schema, requested: &str) -> Option<usize>
```

**Resolution Strategy** (in order):
1. Direct lookup: `schema.index_of(requested)`
2. Normalized lookup: after `normalize_projection_identifier()`
3. Semantic fallback: `fallback_semantic_to_physical_column_index()`
   - Maps `"id"` → `"c1"` or `"*_c1"`
   - Maps `"name"` or `"value"` → `"c2"`
   - Maps `"document"` → `"*_c2"` or `"c2"`

---

#### 6.2 normalize_projection_identifier (Private)
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L631-L648)

**Processing**:
1. Strip whitespace
2. Remove quotes: `"`, backticks, brackets `[]`
3. Take suffix after last `.` (for qualified names)

---

### 7. Pipeline Architecture

#### 7.1 extract_runtime_plan
**File**: [worker/src/execution/planner.rs](worker/src/execution/planner.rs#L142-L200)

**Signature**:
```rust
pub(crate) fn extract_runtime_plan(task: &worker_service::Task) -> Result<RuntimePlan, String>
```

**Where filter_sql is extracted** [Line ~158-165]:
```rust
PhysicalOperator::Filter { predicate } => {
    let raw = match predicate {
        PhysicalExpr::Raw { sql } => sql,
        PhysicalExpr::ColumnRef { name } => name,
    };
    filter_sql = Some(raw);
}
```

**Returns**: `RuntimePlan` with `filter_sql: Option<String>`

---

#### 7.2 execute_query_task (Pipeline Orchestration)
**File**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs#L592-L710)

**Signature**:
```rust
pub(crate) async fn execute_query_task(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
    namespace: &QueryNamespace,
    result_location: &str,
) -> Result<(), String>
```

**Filter application call site** [Line ~651-653]:
```rust
if let Some(sql) = runtime_plan.filter_sql.as_deref() {
    batches = apply_filter_pipeline(&batches, sql)?;
}
```

**Execution order in pipeline**:
1. Load input batches
2. Apply column names mapping
3. **Apply filters** ← HERE
4. Apply joins
5. Apply partial aggregates
6. Apply final aggregates
7. Apply projections
8. Apply sorts
9. Apply limits
10. Persist artifacts

---

### 8. Validation Entry Points

#### 8.1 validate_filter_predicates
**File**: [worker/src/execution/query.rs](worker/src/execution/query.rs#L147-L173)

**Signature**:
```rust
fn validate_filter_predicates(operators: &[serde_json::Value]) -> Result<(), String>
```

**Process**:
- Iterates all operators looking for `Filter` type
- Extracts: `predicate.Raw.sql`
- Calls `detect_deferred_predicate_label(sql)`

**Return on detection**: `Err(format!("predicate is not supported in this phase: {}", label))`

---

#### 8.2 detect_deferred_predicate_label
**File**: [worker/src/execution/query.rs](worker/src/execution/query.rs#L120-L143)

**Signature**:
```rust
fn detect_deferred_predicate_label(sql: &str) -> Option<&'static str>
```

**Deferred Patterns** (case-insensitive):
```rust
let deferred = [
    (" between ", "BETWEEN"),
    (" in (", "IN list"),
    (" like ", "LIKE"),
    (" ilike ", "ILIKE"),
    (" exists", "EXISTS"),
    (" any ", "ANY"),
    (" all ", "ALL"),
    (" over (", "window function"),
];
```

---

### 9. Data Type Parsing Helpers

#### 9.1 parse_date32_filter_literal
**Location**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) (definition around line 750+)

**Signature**:
```rust
fn parse_date32_filter_literal(raw: &str) -> Option<i32>
```

**Accepts**: Quoted date strings, converts to i32 epoch days

---

#### 9.2 parse_date64_filter_literal
**Location**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) (definition around line 760+)

**Signature**:
```rust
fn parse_date64_filter_literal(raw: &str) -> Option<i64>
```

**Accepts**: Quoted date strings, converts to i64 epoch milliseconds

---

#### 9.3 parse_timestamp_filter_literal_for_unit
**Location**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) (definition around line 803)

**Signature**:
```rust
fn parse_timestamp_filter_literal_for_unit(raw: &str, unit: &TimeUnit) -> Option<i64>
```

**Accepts**: Quoted ISO-8601 or `YYYY-MM-DD HH:MM:SS[.fff]` format

**Returns**: Value in the specified time unit (Second, Millisecond, Microsecond, Nanosecond)

---

#### 9.4 read_timestamp_value
**Location**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) (definition around line 815+)

**Signature**:
```rust
fn read_timestamp_value(array: &ArrayRef, row_idx: usize, unit: &TimeUnit) -> Result<i64, String>
```

**Extracts**: Timestamp value from array at row_idx, returns as i64 in specified unit

---

## Error Propagation Flow

```
Flight RPC Entry
    ↓
Ticket Verification (decode_internal_ticket)
    ├─ Error: Invalid UTF-8 → Status::invalid_argument()
    └─ Error: JWT verification → Status::permission_denied()

Query Validation (validate_physical_pipeline_shape)
    ├─ Error: Unsupported operator → String("operator '...' not supported")
    ├─ Error: Deferred predicate → String("predicate is not supported in this phase: ...")
    └─ Passes → OK

Runtime Plan Extraction (extract_runtime_plan)
    └─ Error: Operator parsing → String(...)

Pipeline Execution
    └─ Filter Application (apply_filter_pipeline)
        ├─ parse_filter_clauses
        │   ├─ Error: "filter predicate is empty"
        │   ├─ Error: "filter predicate must use ASCII characters"
        │   ├─ Error: "filter predicate with OR is not supported"
        │   └─ Error: "filter predicate produced no executable clauses"
        │
        ├─ build_filter_mask
        │   └─ Error: "filter column '{}' not found"
        │
        └─ evaluate_clause_at_row
            ├─ Error: Date/Time parse failures
            ├─ Error: Type mismatches
            ├─ Error: Timezone-bearing timestamp
            └─ Error: Arrow compute errors from filter_record_batch
```

All errors are `Result<T, String>` and propagate up to abort the query task.

---

## Thread Points for Schema Metadata Integration

### Current State
- Physical plan contains schema metadata at server side
- Worker extracts only operators, discarding schema
- Filter evaluation works only with Arrow schema from RecordBatch

### Proposed Thread Point 1: RuntimePlan Extension
**File**: [worker/src/execution/planner.rs](worker/src/execution/planner.rs#L8-L26)

Add to `RuntimePlan`:
```rust
pub struct RuntimePlan {
    // ... existing fields ...
    pub(crate) schema_metadata: Option<SchemaMetadataBundle>,  // ← New
}
```

Extract in `extract_runtime_plan()` from `PhysicalPlan` if available.

### Proposed Thread Point 2: Filter Function Signature
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L72-L90)

Modify `apply_filter_pipeline()`:
```rust
pub(crate) fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
    schema_metadata: Option<&SchemaMetadata>,  // ← New parameter
) -> Result<Vec<RecordBatch>, String>
```

### Call Site Update
**File**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs#L651-L653)

```rust
if let Some(sql) = runtime_plan.filter_sql.as_deref() {
    batches = apply_filter_pipeline(
        &batches,
        sql,
        runtime_plan.schema_metadata.as_ref()  // ← Pass through
    )?;
}
```

