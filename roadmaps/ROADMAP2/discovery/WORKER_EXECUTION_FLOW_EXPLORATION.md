# Worker Query Execution Flow - Comprehensive Exploration

**Date**: March 21, 2026  
**Purpose**: Understand how PhysicalPlan is received, Filter operators are evaluated, and errors propagate back to client

---

## 1. PhysicalPlan Reception & Validation

### 1.1 Entry Point: Query Task Dispatch
Location: `worker/src/services/worker_service_server.rs:174-210`

The worker receives query tasks through the gRPC `execute_task` RPC handler:
```rust
async fn execute_task(
    &self,
    request: tonic::Request<worker_service::TaskRequest>,
) -> Result<tonic::Response<worker_service::TaskResponse>, tonic::Status>
```

**Flow**:
1. Client sends `TaskRequest` containing one or more tasks
2. Service validates RBAC authorization context from request metadata
3. For query tasks, injects auth scope/user/role into task.params
4. Delegates to `crate::transactions::maestro::handle_execute_task()`

### 1.2 Task Routing
Location: `worker/src/transactions/maestro.rs:762-900`

The `handle_execute_task` function routes based on task operation type:
```rust
pub async fn handle_execute_task(
    shared: SharedData,
    req: TaskRequest,
) -> TaskResponse
```

For queries (operation == "query"):
- Calls: `crate::services::query::execute_query_task_stub(&shared, task, &session_id)`
- Returns `TaskResponse` with status/error/result_location

### 1.3 Query Stub & Namespace Extraction
Location: `worker/src/execution/query.rs:613-632`

```rust
pub(crate) async fn execute_query_task_stub(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
) -> Result<String, String>
```

**Responsibilities**:
1. Determines if task is distributed stage query or single-worker query
2. Calls `resolve_query_namespace(task)` if not a stage
3. Calls `resolve_query_namespace_from_params(task)` if stage
4. Builds deterministic result location URI
5. Delegates to `crate::execution::pipeline::execute_query_task()`

### 1.4 Physical Plan Validation & Extraction
Location: `worker/src/execution/query.rs:470-563`

Function: `resolve_query_namespace(task)` validates the canonical query payload:

**Payload Structure Checked**:
```json
{
  "version": 2,                           // Must be 2
  "statement": "Select",                  // Only Select allowed
  "namespace": {
    "database": "string",
    "schema": "string", 
    "table": "string"
  },
  "logical_plan": { /* object */ },       // Must be present
  "physical_plan": {
    "operators": [ /* array */ ]          // Must be non-empty
  },
  "schema_metadata": { /* optional */ }   // Phase 9b: Column type contract
}
```

**Validation Steps**:
1. Parse JSON payload from `task.input`
2. Check version == 2
3. Check statement == "Select"
4. Extract and validate namespace (database/schema/table)
5. Verify logical_plan is object
6. Extract physical_plan.operators array
7. Call `validate_physical_pipeline_shape(operators)` — **CRITICAL**

### 1.5 Physical Pipeline Shape Validation
Location: `worker/src/execution/query.rs:314-369`

```rust
fn validate_physical_pipeline_shape(operators: &[serde_json::Value]) 
    -> Result<(), String>
```

**Enforced Constraints** (Phase 2 linear pipeline rules):
- ✅ Must start with `TableScan`
- ✅ Must end with `Materialize`
- ✅ Exactly one `Projection` operator
- ✅ At most one `Sort` operator (must be after Projection)
- ✅ At most one `HashJoin` (must be before Projection and Sort)
- ✅ At most one `AggregatePartial` and `AggregateFinal` (paired, Partial before Final)
- ✅ `AggregateFinal` must be before Projection
- ✅ `Limit` at most once (must be after Projection and Sort if present)
- ✅ Filter predicates must use supported subset (no BETWEEN, IN, LIKE, EXISTS, OVER in Phase 2)

---

## 2. RuntimePlan Extraction & Filter Operator Processing

### 2.1 Runtime Plan Extraction
Location: `worker/src/execution/planner.rs:128-192`

```rust
pub(crate) fn extract_runtime_plan(task: &worker_service::Task) 
    -> Result<RuntimePlan, String>
```

**RuntimePlan Structure**:
```rust
pub(crate) struct RuntimePlan {
    pub(crate) filter_sql: Option<String>,                    // Raw SQL predicate
    pub(crate) schema_metadata: Option<HashMap<String, ColumnDatatypeSpec>>,  // Phase 9b
    pub(crate) join_spec: Option<PhysicalJoinSpec>,
    pub(crate) aggregate_partial_spec: Option<PhysicalAggregateSpec>,
    pub(crate) aggregate_final_spec: Option<PhysicalAggregateSpec>,
    pub(crate) projection_exprs: Vec<PhysicalExpr>,
    pub(crate) sort_exprs: Vec<PhysicalSortExpr>,
    pub(crate) limit_spec: Option<PhysicalLimitSpec>,
    pub(crate) has_materialize: bool,
}
```

**Key Extraction Points**:
1. **schema_metadata** (Phase 9b Support):
   - Reads from payload.schema_metadata if present
   - Maps to `HashMap<String, ColumnDatatypeSpec>`
   - Used for pre-filter type validation
   - Optional: if absent, filter execution skips type checking

2. **Filter Operator**:
   - Located in physical_plan.operators array
   - Extracted as: `PhysicalOperator::Filter { predicate }`
   - Predicate is either:
     - `PhysicalExpr::Raw { sql }` — raw SQL string
     - `PhysicalExpr::ColumnRef { name }` — column reference
   - Stored in RuntimePlan.filter_sql as Option<String>

### 2.2 Filter Operator Location in Operator Sequence
The physical_plan.operators array is processed sequentially in execution:
```rust
for op in operators {
    match op {
        PhysicalOperator::TableScan { .. } => {},
        PhysicalOperator::Filter { predicate } => {
            filter_sql = Some(raw);  // Extract here
        },
        PhysicalOperator::HashJoin { spec } => { ... },
        PhysicalOperator::AggregatePartial { spec } => { ... },
        PhysicalOperator::AggregateFinal { spec } => { ... },
        PhysicalOperator::Projection { expressions } => { ... },
        PhysicalOperator::Sort { keys } => { ... },
        PhysicalOperator::Limit { spec } => { ... },
        PhysicalOperator::Materialize => { ... },
    }
}
```

---

## 3. Query Execution Pipeline Structure

### 3.1 Main Execute Function
Location: `worker/src/execution/pipeline.rs:592-676`

```rust
pub(crate) async fn execute_query_task(
    shared: &SharedData,
    task: &worker_service::Task,
    session_id: &str,
    namespace: &QueryNamespace,
    result_location: &str,
) -> Result<(), String>
```

**Execution Pipeline Order** (linear):
1. **Load Input Batches**: Scan table source into Arrow RecordBatches
2. **Apply Relation Column Names**: Map stored names to batch columns
3. **Apply Filter**: If filter_sql present
4. **Apply HashJoin**: If join_spec present
5. **Apply AggregatePartial**: If aggregate_partial_spec present
6. **Apply AggregateFinal**: If aggregate_final_spec present
7. **Apply Projection**: Apply expressions
8. **Apply Sort**: ORDER BY if sort_exprs present
9. **Apply Limit**: LIMIT if limit_spec present
10. **Persist Artifacts**: Write to object storage

### 3.2 Filter Stage Details
Filter is applied early in pipeline (after table scan):
```rust
if let Some(sql) = runtime_plan.filter_sql.as_deref() {
    batches = apply_filter_pipeline(
        &batches, 
        sql, 
        runtime_plan.schema_metadata.as_ref()  // Phase 9b
    )?;
}
```

---

## 4. Filter Operator Evaluation - Deep Dive

### 4.1 Apply Filter Pipeline Entry
Location: `worker/src/services/query_execution.rs:138-170`

```rust
pub(crate) fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
    schema_metadata: Option<&HashMap<String, kionas::sql::datatypes::ColumnDatatypeSpec>>,
) -> Result<Vec<RecordBatch>, String>
```

**Phase 9b Type Validation** (NEW):
```rust
if let Some(schema) = schema_metadata {
    validate_filter_predicates_types(filter_sql, schema)?;
}
```

Where `validate_filter_predicates_types()` pre-checks that all columns referenced in filter exist in schema.

### 4.2 Filter Clause Parsing
Location: `worker/src/services/query_execution.rs:172-225`

```rust
fn parse_filter_clauses(filter_sql: &str) -> Result<Vec<FilterClause>, String>
```

**Parsing Rules**:
1. Reject empty predicates
2. Reject non-ASCII characters
3. Reject OR operators
4. Split by AND to get individual clauses
5. Parse each clause with `parse_single_clause(clause)`
6. Extract (column, op, literal) tuple
7. Build FilterClause struct with parsed value

**Supported Operators**: `=`, `<>` (!=), `>`, `>=`, `<`, `<=`

### 4.3 Row Mask Building
Location: `worker/src/services/query_execution.rs:227-257`

```rust
fn build_filter_mask(
    batch: &RecordBatch,
    clauses: &[FilterClause],
) -> Result<BooleanArray, String>
```

Algorithm:
1. Initialize boolean array: all rows = true
2. For each clause:
   - Resolve column index in batch schema
   - For each row:
     - If row already filtered (false), skip
     - Evaluate clause at row index
     - If clause fails, set row to false
3. Return final boolean mask

### 4.4 Clause-Level Evaluation
Location: `worker/src/services/query_execution.rs:259-320`

```rust
fn evaluate_clause_at_row(
    array: &ArrayRef,
    row_idx: usize,
    clause: &FilterClause,
) -> Result<bool, String>
```

**Type Matching Matrix**:
| Column Type | Literal Type | Action |
|---|---|---|
| Int64 | FilterValue::Int | i64 comparison |
| Int32 | FilterValue::Int | cast to i64, compare |
| Int16 | FilterValue::Int | cast to i64, compare |
| Boolean | FilterValue::Bool | bool comparison |
| Utf8 | FilterValue::Str | string comparison |
| Date32 | FilterValue::Str | parse date, i64 comparison |
| Date64 | FilterValue::Str | parse date64, i64 comparison |
| Timestamp | FilterValue::Str | parse ISO-8601, i64 comparison |
| NULL | any | return false (NULL always fails) |
| **mismatch** | | Return `Err(...)` |

**Error Cases**:
- Unsupported type combinations → `filter type mismatch for column '...'`
- Invalid date/time literals → `unsupported ... literal '...'`
- Timezone-bearing timestamps → `timezone-bearing timestamps not supported`
- Column not found → `filter column '...' not found`

### 4.5 Arrow Filter Application
Location via arrow-rs:
```rust
let filtered = arrow::compute::filter_record_batch(batch, &mask)?
```

Applies boolean mask to all columns in batch, removing false rows.

---

## 5. Schema Metadata Integration (Phase 9b)

### 5.1 Where Schema Metadata Exists
Location: `worker/src/execution/planner.rs:139-146`

```rust
let schema_metadata = if let Some(metadata_value) = payload.get("schema_metadata") {
    match serde_json::from_value::<HashMap<String, ColumnDatatypeSpec>>(metadata_value.clone())
    {
        Ok(map) if !map.is_empty() => Some(map),
        _ => None,
    }
} else {
    None
};
```

Structure in RuntimePlan:
```rust
pub(crate) schema_metadata: Option<HashMap<String, ColumnDatatypeSpec>>
```

### 5.2 Type Validation Function
Location: `worker/src/services/query_execution.rs:70-127`

```rust
fn validate_filter_predicates_types(
    filter_sql: &str,
    schema: &HashMap<String, kionas::sql::datatypes::ColumnDatatypeSpec>,
) -> Result<(), String>
```

**Implementation**:
1. Split filter_sql by AND clauses
2. For each clause:
   - Extract first token (operand) from each clause
   - Normalize column name
   - Check column exists in schema HashMap
3. Return error if any column missing

**Current Validation**: Existence only. Full type compatibility checking deferred to planner phase.

### 5.3 Where to Inject Schema Metadata Type Validation
**Option A: Earliest Point (Parse Time)**
- In `parse_single_clause()` - validate column exists and has compatible type
- Would allow early failure before batch processing

**Option B: Clause Evaluation (Row Evaluation)**
- In `evaluate_clause_at_row()` - use schema_metadata to augment type checking
- Could provide more context-aware error messages
- Allows per-row type coercion strategies

**Option C: Pre-Filter Scan (Between Parse and Mask Build)**
- Between `parse_filter_clauses()` and `build_filter_mask()`
- Separate concern: validation vs execution
- Could build column metadata once instead of per-row

---

## 6. Error Handling & Propagation

### 6.1 Error Type Pattern
All functions use `Result<T, String>` for error handling:
- ✅ Direct `String` error messages
- ✅ Descriptive messages for all failure cases
- ❌ No structured error codes (future enhancement)
- ❌ No error categorization (validation vs runtime vs system)

### 6.2 Query Execution Error Path
```
execute_query_task()
  ├─ extract_runtime_plan() → Err(String)
  ├─ load_input_batches() → Err(String)
  ├─ apply_filter_pipeline() → Err(String)
  │   ├─ validate_filter_predicates_types() → Err(String)
  │   ├─ parse_filter_clauses() → Err(String)
  │   ├─ build_filter_mask() → Err(String)
  │   └─ filter_record_batch() → Err(String) [Arrow]
  ├─ apply_hash_join_pipeline() → Err(String)
  ├─ apply_aggregate_*_pipeline() → Err(String)
  ├─ apply_projection_pipeline() → Err(String)
  ├─ apply_sort_pipeline() → Err(String)
  ├─ apply_limit_pipeline() → Err(String)
  └─ persist_query_artifacts() → Err(String)
```

Any error propagates upward, short-circuiting pipeline execution.

### 6.3 Error Return to Client
Location: `worker/src/transactions/maestro.rs:813-825`

```rust
if operation == "query" {
    let task = first_task.as_ref()?;
    match crate::services::query::execute_query_task_stub(&shared, task, &session_id).await {
        Ok(location) => {
            // Store result location, return success
            TaskResponse {
                status: "ok".to_string(),
                error: String::new(),
                result_location: location,
            }
        },
        Err(e) => {
            // Return error message to client
            TaskResponse {
                status: "error".to_string(),
                error: e,  // Full error String
                result_location: String::new(),
            }
        }
    }
}
```

**TaskResponse Fields**:
- `status`: "ok" or "error"
- `error`: Full error description (empty if success)
- `result_location`: Flight URI (empty if error)

### 6.4 Error Message Examples
- **Validation Stage**: `"query authorization denied for principal X with role Y on Z"`
- **Parse Stage**: `"filter predicate must use AND only: unsupported OR"` or `"filter column 'not_found' not found in table schema"`
- **Execution Stage**: `"failed to apply filter: invalid type mismatch"`
- **Storage Stage**: `"failed to read staging artifacts: storage error (details)"`

---

## 7. Type Handling in Filter Evaluation

### 7.1 Filter Value Representation
Location: `worker/src/services/query_execution.rs:20-25`

```rust
#[derive(Debug, Clone)]
enum FilterValue {
    Int(i64),
    Bool(bool),
    Str(String),
}
```

### 7.2 Type Coercion Strategy
**During Evaluation**:
- Int32/Int16 → cast to i64 for comparison
- Date32/Date64 → parsed from Str literal, compared as i64 epochs
- Timestamp(unit) → parsed from ISO-8601 Str, converted to unit-specific i64
- Utf8 → direct string comparison

**No Implicit Conversions**:
- ❌ String "123" vs Int64 123 → ERROR
- ❌ Bool 1 vs Int64 1 → ERROR
- ✅ Int32 100 vs Int64 100 → allowed (cast)

### 7.3 NULL Handling
**Current Behavior**:
- NULL column value → immediately returns false (row filtered out)
- No support for `IS NULL` or `IS NOT NULL` predicates (deferred feature)
- All comparisons short-circuit on NULL

---

## 8. Current Limitations & Future Considerations

### 8.1 Phase 2 Subset Restrictions
Filter predicates currently limited to:
- ❌ BETWEEN clauses
- ❌ IN lists
- ❌ LIKE patterns
- ❌ EXISTS subqueries
- ❌ ANY/ALL quantifiers
- ❌ Window functions
- ❌ OR operators (AND only)
- ❌ NULL checks (IS NULL/IS NOT NULL)

### 8.2 Type Coercion Gaps
- **String to Number**: Rejected (no implicit cast)
- **Temporal Boundary Values**: Date/Time parsing limited to specific formats
- **Precision Loss**: No special handling for overflow/underflow
- **Domain Types**: No support for custom/domain-specific types in Phase 9b

### 8.3 Performance Considerations
- **Row-by-row evaluation**: O(n * m) where n=rows, m=clauses
- **Type checking per-row**: Could be optimized to once per batch
- **Column lookup per-clause**: Could be pre-computed in batch iteration

---

## Summary: End-to-End Query Execution Flow Diagram

```
[Client] 
  │
  ├─→ TaskRequest (gRPC)
  │
  └─→ WorkerService::execute_task()
      │
      ├─→ RBAC Validation (metadata extraction)
      │
      ├─→ handle_execute_task() [maestro.rs]
      │
      ├─→ execute_query_task_stub() [query.rs]
      │   │
      │   ├─→ resolve_query_namespace() [query.rs]
      │   │   │
      │   │   ├─→ Parse payload JSON
      │   │   ├─→ Validate version/statement/namespace
      │   │   └─→ validate_physical_pipeline_shape() [query.rs]
      │   │       └─→ Enforces Phase 2 linear pipeline
      │   │
      │   ├─→ build_result_location() [Flight URI]
      │   │
      │   └─→ execute_query_task() [pipeline.rs]
      │       │
      │       ├─→ extract_runtime_plan() [planner.rs]
      │       │   │
      │       │   ├─→ Extract filter_sql from PhysicalOperator::Filter
      │       │   └─→ Extract schema_metadata (Phase 9b)
      │       │
      │       ├─→ load_input_batches() [Scan source]
      │       │
      │       ├─→ apply_filter_pipeline() [query_execution.rs]
      │       │   │
      │       │   ├─→ validate_filter_predicates_types() if schema_metadata
      │       │   ├─→ parse_filter_clauses()
      │       │   ├─→ For each batch:
      │       │   │   ├─→ build_filter_mask()
      │       │   │   │   └─→ For each clause, evaluate_clause_at_row()
      │       │   │   └─→ filter_record_batch(batch, mask)
      │       │   └─→ Return filtered batches
      │       │
      │       ├─→ apply_hash_join_pipeline() if join_spec
      │       ├─→ apply_aggregate_*_pipeline() if aggregate
      │       ├─→ apply_projection_pipeline()
      │       ├─→ apply_sort_pipeline()
      │       ├─→ apply_limit_pipeline()
      │       │
      │       └─→ persist_query_artifacts() [Storage]
      │
      └─→ TaskResponse (status/error/result_location)
          │
          └─→ [Result propagated to client]
```

---

## Key Integration Points for Type Validation

1. **Pre-Filter Validation**: `validate_filter_predicates_types()` in apply_filter_pipeline()
2. **Schema Threading**: RuntimePlan.schema_metadata → apply_filter_pipeline() parameter
3. **Clause Parsing**: Could inject type info during parse_filter_clauses()
4. **Row Evaluation**: evaluate_clause_at_row() has access to column type via Arrow schema
5. **Error Feedback**: All functions return Result<T, String> for error propagation

---

## Recommendations for Phase 9b Schema Metadata Injection

### Location 1: After Operator Extraction
```rust
// In execute_query_task()
let runtime_plan = extract_runtime_plan(task)?;
let schema_metadata = runtime_plan.schema_metadata.clone();  // Already present
```

### Location 2: Filter Application
```rust
// Already implemented in apply_filter_pipeline()
pub(crate) fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
    schema_metadata: Option<&HashMap<String, ColumnDatatypeSpec>>,
) -> Result<Vec<RecordBatch>, String>
```

### Location 3: Type Validation
```rust
// validate_filter_predicates_types() can be extended to:
// 1. Check column existence (current)
// 2. Validate literal type compatibility with column type
// 3. Optional: Check for implicit coercion rules
```

### Location 4: Error Messages
Current approach extends to Phase 9b:
- "type coercion violation: column '...' of type X cannot apply operator Y with literal of type Z"
- Reuse existing Err(String) pattern for consistency
