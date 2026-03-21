# Worker Query Execution Path: Filter Predicate Evaluation Analysis

## Overview
This document provides a detailed analysis of where and how Filter predicates are evaluated in the worker query execution pipeline, including current error handling and points for schema metadata threading.

---

## 1. Query Entry Points (Where Physical Plans with Filters Arrive)

### 1.1 Flight Service Entry Point
**File**: [worker/src/flight/server.rs](worker/src/flight/server.rs)
- **Key Struct**: `WorkerFlightService` 
- **Key Method**: Flight RPC methods (e.g., `do_get()`, `do_exchange()`)
- **Responsibility**: Decode and verify Flight tickets; extract authorization context from metadata headers
- **Extracts from headers**:
  - `session_id`
  - `rbac_user`, `rbac_role`
  - `auth_scope`
  - `query_id`

**Code signature** [L50-75]:
```rust
fn dispatch_context_from_flight_metadata(
    metadata: &tonic14::metadata::MetadataMap,
    fallback_session_id: &str,
) -> Result<crate::authz::DispatchAuthContext, Status>
```

---

### 1.2 Query Task Reception
**File**: [worker/src/execution/query.rs](worker/src/execution/query.rs)  
**Key Function**: `validate_physical_pipeline_shape()` [L195-280]

This is where physical plans first arrive and are validated. The function:
1. Extracts operator names from the JSON payload
2. Validates the complete operator pipeline shape
3. Detects unsupported predicate families

---

## 2. Physical Plan Extraction & Filter Identification

### 2.1 Runtime Plan Extraction
**File**: [worker/src/execution/planner.rs](worker/src/execution/planner.rs)

#### Key Data Structure [L8-26]:
```rust
pub(crate) struct RuntimePlan {
    pub(crate) filter_sql: Option<String>,           // ← Raw SQL from Filter operator
    pub(crate) join_spec: Option<PhysicalJoinSpec>,
    pub(crate) aggregate_partial_spec: Option<PhysicalAggregateSpec>,
    pub(crate) aggregate_final_spec: Option<PhysicalAggregateSpec>,
    pub(crate) projection_exprs: Vec<PhysicalExpr>,
    pub(crate) sort_exprs: Vec<PhysicalSortExpr>,
    pub(crate) limit_spec: Option<PhysicalLimitSpec>,
    pub(crate) has_materialize: bool,
}
```

#### Plan Extraction Function [L128-200]:
**`extract_runtime_plan(task: &worker_service::Task) -> Result<RuntimePlan, String>`**

**How Filter SQL is Captured** [Line ~158-165]:
```rust
PhysicalOperator::Filter { predicate } => {
    let raw = match predicate {
        PhysicalExpr::Raw { sql } => sql,      // ← Direct SQL predicate
        PhysicalExpr::ColumnRef { name } => name,  // ← Column reference fallback
    };
    filter_sql = Some(raw);
}
```

**Current Limitation**: 
- Only captures the raw SQL string
- **No schema_metadata is passed through** from the physical plan to the runtime context
- The `PhysicalPlan` itself is parsed but then discarded after operator extraction

---

## 3. Filter Predicate Validation (Phase 2 Subset Enforcement)

### 3.1 Validation Entry Point
**File**: [worker/src/execution/query.rs](worker/src/execution/query.rs)  
**Function**: `validate_filter_predicates(operators: &[serde_json::Value]) -> Result<(), String>` [L147-173]

**Validation Logic**:
1. Scans all operators for `Filter` type
2. Extracts predicate SQL from `{"Raw": {"sql": "..."}}`
3. Calls `detect_deferred_predicate_label(sql: &str) -> Option<&'static str>` [L120-143]

**Deferred (Unsupported) Predicates** [L127-135]:
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

**Error when detected**: Returns `Err("predicate is not supported in this phase: <LABEL>")`

---

## 4. Filter Evaluation During Pipeline Execution

### 4.1 Pipeline Orchestration
**File**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs)  
**Function**: `execute_query_task()` [L592-710]

**Filter Application in Pipeline** [Line ~651-653]:
```rust
if let Some(sql) = runtime_plan.filter_sql.as_deref() {
    batches = apply_filter_pipeline(&batches, sql)?;
}
```

**Execution Order**:
1. Load scan batches from tables
2. **Apply filters** (if present)
3. Apply joins
4. Apply aggregations
5. Apply projections
6. Apply sorting
7. Apply limits
8. Persist artifacts

---

### 4.2 Main Filter Application Function
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)

#### Public API [L72-90]:
```rust
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

**Processing Steps**:
1. **Parse**: Convert raw SQL into executable `FilterClause` structs
2. **Mask**: Build a boolean array identifying matching rows
3. **Apply**: Use Arrow's `filter_record_batch()` to extract matching rows

---

### 4.3 SQL Predicate Parsing
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)

#### Parse Filter Clauses [L127-169]:
```rust
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
```

**Restrictions**:
- ✅ Simple AND conjunctions only (OR rejected)
- ✓ ASCII characters only
- ✓ Multiple clauses: `col1 = val1 AND col2 > val2 AND ...`

#### FilterClause Structure [L35-38]:
```rust
#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,        // Eq, Ne, Gt, Ge, Lt, Le
    value: FilterValue,  // Int(i64), Bool(bool), Str(String)
}
```

#### FilterOp Enum [L28-32]:
```rust
#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}
```

---

### 4.4 Row Mask Building
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)  
**Function**: `build_filter_mask()` [L139-169]

```rust
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
                continue;  // Short-circuit: once false, stays false
            }

            let pass = evaluate_clause_at_row(array, row_idx, clause)?;
            if !pass {
                *row_flag = false;
            }
        }
    }

    Ok(BooleanArray::from(rows))
}
```

**Logic**:
- Iterates through each clause
- For each clause, iterates through each row
- **Short-circuit evaluation**: Once a row is marked false, it stays false (AND semantics)
- Returns boolean array where `true` = row passes filter

---

### 4.5 Clause-Level Evaluation
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)  
**Function**: `evaluate_clause_at_row()` [L171-245]

```rust
fn evaluate_clause_at_row(
    array: &ArrayRef,
    row_idx: usize,
    clause: &FilterClause,
) -> Result<bool, String>
```

**Data Type Match Handling**:

| Data Type | Filter Value | Behavior |
|-----------|--------------|----------|
| Int64 | Int | Direct comparison |
| Int32, Int16 | Int | Cast to i64, then compare |
| Boolean | Bool | Direct comparison |
| Utf8 | Str | String comparison |
| Date32 | Str (quoted) | Parse date32 literal, compare as i64 |
| Date64 | Str (quoted) | Parse date64 literal, compare as i64 |
| Timestamp(unit, None) | Str (quoted) | Parse ISO-8601, convert to unit, compare |
| Timestamp(unit, Some(_)) | Any | ❌ **Error**: "timezone-bearing timestamps not supported" |
| Unmatched types | Any | ❌ **Error**: "filter type mismatch" |

**NULL Handling** [L189]:
```rust
if array.is_null(row_idx) {
    return Ok(false);  // NULL never passes any filter
}
```

**Error Messages Generated**:
- `"filter column '{column}' not found"` (in build_filter_mask)
- `"unsupported Date32 filter literal '{literal}' for column '{column}': expected quoted date/datetime"`
- `"unsupported Date64 filter literal '{literal}' for column '{column}': expected quoted date/datetime"`
- `"unsupported timestamp filter literal '{literal}' for column '{column}': expected quoted ISO-8601 or 'YYYY-MM-DD HH:MM:SS[.fff]'"`
- `"unsupported temporal filter literal for column '{column}': use quoted date/time literal"`
- `"unsupported timestamp filter for column '{column}': timezone-bearing timestamps are not supported in this phase"`
- `"filter type mismatch for column '{column}': unsupported type {type:?}"`

---

## 5. Error Handling Summary

### 5.1 Error Propagation Chain
```
Flight RPC
    ↓
Query validation (validate_physical_pipeline_shape)
    ↓ Error: unsupported operator / pipeline shape / deferred predicate
    ↓
Extract runtime plan (extract_runtime_plan)
    ↓ Error: operator extraction fails
    ↓
Apply filter pipeline (apply_filter_pipeline)
    ├─ parse_filter_clauses → Error: empty, OR detected, non-ASCII
    ├─ build_filter_mask → Error: column not found
    └─ evaluate_clause_at_row → Error: type mismatch, parse failure
        ↓ All errors Result<T, String>
        ↓
Task execution fails → Status returned to Flight client
```

### 5.2 Current Error Returns
All filter functions return `Result<T, String>`:
- **No context loss**: String messages are propagated verbatim
- **No structured error info**: Errors lack error codes, metadata
- **No recovery mechanism**: All errors abort the task

---

## 6. Points for Schema Metadata Threading

### 6.1 Current State: No Schema Metadata Available to Filter

**Where schema_metadata should originate**: 
- `PhysicalPlan` struct (from server)
- Contains: column types, nullability, precision, etc.

**Current problem**:
- Physical plan is parsed in `extract_runtime_plan()` [planner.rs:142-200]
- Only raw operator/expression data is extracted
- **Schema is lost** after operators are deserialized
- Filter execution in `apply_filter_pipeline()` only receives:
  - `&[RecordBatch]` (contains Arrow schema)
  - `filter_sql: &str` (raw SQL text)

### 6.2 Where Schema Could Be Threaded

#### Option A: Modify RuntimePlan Structure
**File**: [worker/src/execution/planner.rs](worker/src/execution/planner.rs) [L8-26]

**Proposal**: Add optional schema metadata to RuntimePlan
```rust
pub(crate) struct RuntimePlan {
    pub(crate) filter_sql: Option<String>,
    pub(crate) schema_metadata: Option<???>,  // ← Add schema container
    // ... rest of fields
}
```

**Where to extract**: In `extract_runtime_plan()` [L142-200], parse `PhysicalPlan.schema_metadata` if present

#### Option B: Pass SchemaMetadata to apply_filter_pipeline()
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) [L72-90]

**Current signature**:
```rust
pub(crate) fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
) -> Result<Vec<RecordBatch>, String>
```

**Proposed signature**:
```rust
pub(crate) fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
    schema_metadata: Option<&SchemaMetadata>,  // ← New parameter
) -> Result<Vec<RecordBatch>, String>
```

**Call site**: [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs) [L651-653]
```rust
if let Some(sql) = runtime_plan.filter_sql.as_deref() {
    batches = apply_filter_pipeline(&batches, sql, runtime_plan.schema_metadata.as_ref())?;
}
```

#### Option C: Use Arrow Schema from RecordBatch (Current Approach)
The RecordBatch already contains arrow::Schema. The current approach is:
```rust
let idx = resolve_schema_column_index(batch.schema().as_ref(), &clause.column)
```

This works for basic purposes but loses semantic information:
- Original domain type annotations (e.g., "email", "UUID")
- Precision/scale for decimals
- Comment/metadata from original schema definition

---

## 7. Summary Table: Filter Evaluation Journey

| Stage | File | Function | Input | Output | Error Handling |
|-------|------|----------|-------|--------|-----------------|
| **Entry** | flight/server.rs | Flight RPC | Encrypted ticket + metadata | DispatchAuthContext | Status (tonic) |
| **Validation** | execution/query.rs | validate_filter_predicates() | Physical operators JSON | Ok(()) or error message | String (abort) |
| **Extraction** | execution/planner.rs | extract_runtime_plan() | Task payload | RuntimePlan with filter_sql | String (abort) |
| **Pipeline Setup** | execution/pipeline.rs | execute_query_task() | Condition branches | Pipeline order determined | String (abort) |
| **Parse SQL** | services/query_execution.rs | parse_filter_clauses() | filter_sql: "col1 > 5 AND col2 = 'x'" | Vec<FilterClause> | String errors (empty, OR, non-ASCII, etc.) |
| **Build Mask** | services/query_execution.rs | build_filter_mask() | RecordBatch + FilterClause[] | BooleanArray | String (column not found) |
| **Evaluate Row** | services/query_execution.rs | evaluate_clause_at_row() | ArrayRef + row_idx + clause | bool | String (type mismatch, parse failure, NULL) |
| **Apply Filter** | services/query_execution.rs | apply_filter_pipeline() (loop) | Arrow filter_record_batch | Vec<RecordBatch> | String from arrow compute |

---

## 8. Code References for Quick Navigation

| Task | File | Lines |
|------|------|-------|
| Flight entry receives query | worker/src/flight/server.rs | 50-75 |
| Validate physical plan shape | worker/src/execution/query.rs | 195-280 |
| Validate filter predicates | worker/src/execution/query.rs | 147-173 |
| Extract runtime plan | worker/src/execution/planner.rs | 142-200 |
| Main pipeline execution | worker/src/execution/pipeline.rs | 592-710 |
| Call to filter application | worker/src/execution/pipeline.rs | 651-653 |
| Filter pipeline entry | worker/src/services/query_execution.rs | 72-90 |
| Parse filter clauses | worker/src/services/query_execution.rs | 127-169 |
| Build row mask | worker/src/services/query_execution.rs | 139-169 |
| Evaluate clause at row | worker/src/services/query_execution.rs | 171-245 |
| FilterOp enum | worker/src/services/query_execution.rs | 28-32 |
| FilterClause struct | worker/src/services/query_execution.rs | 35-38 |
| RuntimePlan struct | worker/src/execution/planner.rs | 8-26 |
| resolve_schema_column_index | worker/src/services/query_execution.rs | 894-918 |

