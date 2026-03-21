# Codebase Search Results: INSERT Persistence, BETWEEN/IN Validation, and Phase Gating

## 1. HOW INSERT STATEMENTS PERSIST DATA

### Server-Side Entry Point
**File**: [server/src/statement_handler/dml/insert.rs](server/src/statement_handler/dml/insert.rs#L251-L300)

`handle_insert_statement()` dispatches INSERT to worker:
- Canonicalizes table name to format: `deltalake.<schema>.<table>`
- Loads constraint metadata (NOT NULL columns, column types) from metastore
- Packages params: `table_name`, `constraint_contract_version`, `table_columns_json`, `not_null_columns_json`, `column_type_hints_json`
- Calls `run_task_for_input_with_params()` to dispatch task with operation="insert"

### Worker-Side Execution Flow
**File**: [worker/src/transactions/maestro.rs](worker/src/transactions/maestro.rs#L762-L1100)

`handle_execute_task()` orchestrates INSERT persistence:

1. **Parse & Validate**:
   - Parses INSERT payload with `parse_insert_payload()` 
   - Extracts table name, columns, and VALUES rows
   - Enforces NOT NULL constraints via `enforce_not_null_columns()`
   - Applies type coercion hints from server

2. **Derive Table URI**:
   - Extracts from task params or derives from cluster storage config
   - Pattern: `s3://{bucket}/databases/{db}/schemas/{schema}/tables/{table}`

3. **Build Arrow Batch**:
   - `build_record_batch_from_insert()` constructs Arrow RecordBatch from INSERT rows
   - Uses column type hints for type conversion

4. **Persistent Storage**:
   - Calls `write_parquet_and_commit()` in [worker/src/storage/deltalake.rs](worker/src/storage/deltalake.rs#L177-L260)

### Parquet File Writing & Staging
**File**: [worker/src/storage/deltalake.rs](worker/src/storage/deltalake.rs#L177-L260)

```
write_parquet_and_commit(shared, table_uri, record_batches) {
  1. Serialize RecordBatches to Parquet in-memory
     - ArrowWriter with WriterProperties
     - No compression by default
  
  2. Upload to object store:
     - Key path: `{table_path}/staging/{uuid}.parquet`
     - Example: `s3://bucket/databases/db/schemas/schema/tables/mytable/staging/abc-123.parquet`
  
  3. Commit to Delta log:
     - Create Delta AddFile action with path, size, modification_time, data_change=true
     - Use deltalake_core CommitBuilder API
     - Append mode (SaveMode::Append)
     - Non-partitioned write (partition_values = {})
}
```

**Staging Directory Logic**:
- Parquet files staged under `{table_uri}/staging/` before Delta commit
- File naming: `{uuid}.parquet` (deterministic UUID generation)
- Staging manifest created in [worker/src/storage/staging.rs](worker/src/storage/staging.rs)
- Promotion workflow: copy staging → final, write final manifest

---

## 2. BETWEEN AND IN PREDICATE VALIDATION & GATING

### Planner-Level Validation
**File**: [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs#L1-L70)

**Function**: `ensure_supported_predicate(predicate: &PhysicalExpr)`

```rust
Supported Predicates (Phase 2-9a):
✓ Equality: col = value, col != value, col <> value
✓ Relational: col > value, col >= value, col < value, col <= value  
✓ NULL comparison (Phase 9a): col IS NULL, col IS NOT NULL
✓ Logical: pred1 AND pred2, pred1 OR pred2, NOT pred1

Deferred (REJECTED) Predicates (Phase 9c+):
✗ BETWEEN    → Returns: UnsupportedPredicate("BETWEEN")
✗ IN list    → Returns: UnsupportedPredicate("IN list")
✗ LIKE/ILIKE → Returns: UnsupportedPredicate("LIKE"/"ILIKE")
✗ EXISTS     → Returns: UnsupportedPredicate("EXISTS")
✗ ANY/ALL    → Returns: UnsupportedPredicate("ANY"/"ALL")
✗ OVER       → Returns: UnsupportedPredicate("window function")
```

**Validation Triggers**:
- `validate_physical_plan()` calls `ensure_supported_predicate()` for every Filter operator
- Pattern matching on lowercase SQL string:
  - ` between ` → reject
  - ` in (` → reject
  - ` like ` → reject
  - ` ilike ` → reject
  - ` exists` → reject
  - ` any ` → reject
  - ` all ` → reject
  - ` over (` → reject

### Worker-Level Execution Support
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs)

Despite planner rejection, worker **HAS** BETWEEN/IN support:

**Functions Available** (Lines 370-450, 1049-1400):
- `parse_filter_clauses()` - Parses BETWEEN and IN patterns
- `parse_between_clause()` - Extracts column, lower bound, upper bound
- `parse_in_clause()` - Parses IN (val1, val2, ...) lists
- `evaluate_between_at_row()` - Evaluates BETWEEN semantics
- `evaluate_in_at_row()` - Evaluates IN membership

**FilterClause Structure** (Line 40):
```rust
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,
    value2: Option<FilterValue>,  // Phase 9c: upper bound for BETWEEN
}

enum FilterOp {
    Eq, Ne, Gt, Ge, Lt, Le,
    Between,  // Phase 9c
    In,       // Phase 9c
}

enum FilterValue {
    Int(i64),
    Str(String),
    Bool(bool),
    IntList(Vec<i64>),      // Phase 9c: IN with int list
    StrList(Vec<String>),   // Phase 9c: IN with string list
    BoolList(Vec<bool>),    // Phase 9c: IN with bool list
}
```

**Evaluation** (Lines 477-552):
```rust
match clause.op {
    FilterOp::Between => {
        if let Some(upper) = &clause.value2 {
            evaluate_between_at_row(array, row_idx, &column, &clause.value, upper)
        }
    }
    FilterOp::In => {
        evaluate_in_at_row(array, row_idx, &column, &clause.value)
    }
    // ... standard ops
}
```

---

## 3. PHASE GATING SYSTEM

### Key Finding: NO FEATURE FLAGS FOR BETWEEN/IN

✅ **What EXISTS**:
- **Hardcoded rejection** in `ensure_supported_predicate()` 
- **Pattern matching** on SQL strings (case-insensitive)
- **Test documentation** showing Phase 9a acceptance of IS NULL/IS NOT NULL

❌ **What DOES NOT EXIST**:
- No configuration flags (e.g., `enable_between`, `phase_filter`, `predicate_phase`)
- No environment variables gating BETWEEN/IN
- No runtime feature toggles in SharedData
- No per-query capability negotiation

### Actual Phase Gating Observed

1. **Phase 9a (IS/IS NOT)** - COMPLETE ✅
   - [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md#L193) documents IS/IS NOT support
   - Test cases in [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs#L634-L675)
   - Tests validate IS NULL and IS NOT NULL acceptance

2. **Phase 9b (Type Coercion FOUNDATION)** - IN PROGRESS
   - Documented: [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md#L241-L264)
   - Type contract validation added to PhysicalPlan
   - `schema_metadata` field gathers column type hints

3. **Phase 9c (BETWEEN/IN Predicates)** - DISCOVERY COMPLETE ✅ (NOT YET GATED)
   - Discovery: [roadmaps/ROADMAP2.md](roadmaps/ROADMAP2.md#L241-L264)
   - **Support code IMPLEMENTED but BLOCKED by planner validation**
   - Cannot be used until planner's `ensure_supported_predicate()` is modified

### Where To Enable BETWEEN/IN

To enable BETWEEN/IN predicates, modify [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs#L21-L50):

**Current Rejection Code** (Line 28-50):
```rust
fn ensure_supported_predicate(predicate: &PhysicalExpr) -> Result<(), PlannerError> {
    let sql = match predicate {
        PhysicalExpr::Raw { sql } => sql.to_ascii_lowercase(),
        PhysicalExpr::ColumnRef { .. } => return Ok(()),
    };

    let unsupported = [
        (" between ", "BETWEEN"),       // GATE POINT
        (" in (", "IN list"),           // GATE POINT
        (" like ", "LIKE"),
        (" ilike ", "ILIKE"),
        (" exists", "EXISTS"),
        (" any ", "ANY"),
        (" all ", "ALL"),
        (" over (", "window function"),
    ];

    for (needle, label) in unsupported {
        if sql.contains(needle) {
            return Err(PlannerError::UnsupportedPredicate(label.to_string()));
        }
    }

    Ok(())
}
```

**To Enable Phase 9c**:
```rust
// Remove or comment out BETWEEN and IN entries in unsupported slice:
let unsupported = [
    // (" between ", "BETWEEN"),     // ← ENABLE: remove this
    // (" in (", "IN list"),         // ← ENABLE: remove this
    (" like ", "LIKE"),
    (" ilike ", "ILIKE"),
    (" exists", "EXISTS"),
    (" any ", "ANY"),
    (" all ", "ALL"),
    (" over (", "window function"),
];
```

---

## 4. TEST EVIDENCE

### Phase 9a (IS NULL/IS NOT NULL) - Currently Gated
**File**: [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs#L551-L675)

✓ Tests accepting IS NULL/IS NOT NULL:
```rust
#[test]
fn accepts_is_null_predicate() { ... }

#[test]
fn accepts_is_not_null_predicate() { ... }

#[test]
fn accepts_is_null_with_and_predicate() { ... }

#[test]
fn accepts_is_not_null_with_or_predicate() { ... }
```

### Phase 9c (BETWEEN/IN) - Hardcoded Rejection
**File**: [kionas/src/planner/physical_validate.rs](kionas/src/planner/physical_validate.rs#L434-L460)

✗ Tests explicitly rejecting BETWEEN/IN:
```rust
#[test]
fn rejects_deferred_predicate_shapes() {
    let cases = vec![
        ("age BETWEEN 1 AND 10".to_string(), "BETWEEN".to_string()),
        ("country IN ('us', 'br')".to_string(), "IN list".to_string()),
        ("name LIKE 'a%'".to_string(), "LIKE".to_string()),
        ("name ILIKE 'A%'".to_string(), "ILIKE".to_string()),
        ("EXISTS (SELECT 1 FROM sales.public.users)".to_string(), "EXISTS".to_string()),
        ("score = ANY (ARRAY[1,2])".to_string(), "ANY".to_string()),
        ("score = ALL (ARRAY[1,2])".to_string(), "ALL".to_string()),
        ("row_number() OVER (PARTITION BY id) > 1".to_string(), "window function".to_string()),
    ];
    // ... test verifies all are rejected
}
```

### Worker Execution Tests
**File**: [worker/src/tests/execution_type_coercion_tests.rs](worker/src/tests/execution_type_coercion_tests.rs)

Tests TypeCoercion support at worker level (Phase 9b foundation).

---

## Summary Table

| Aspect | Status | Details |
|--------|--------|---------|
| **INSERT Persistence** | ✅ COMPLETE | Files written to `{table_uri}/staging/{uuid}.parquet`, Delta log commit via deltalake_core |
| **Staging Directory** | ✅ COMPLETE | Pattern: `{table_path}/staging/`, manifest at `staging_manifest_key()` and `final_manifest_key()` |
| **BETWEEN Support** | ✅ CODED | Worker implementation complete, planner rejects via `ensure_supported_predicate()` |
| **IN Support** | ✅ CODED | Worker implementation complete, planner rejects via `ensure_supported_predicate()` |
| **Phase Gating** | ⚠️ HARDCODED | No feature flags; rejection is pattern-matching in `ensure_supported_predicate()` line 28-50 |
| **Enable BETWEEN/IN** | ⚠️ MANUAL | Remove BETWEEN/IN from unsupported list in [physical_validate.rs](kionas/src/planner/physical_validate.rs#L28-L50), bump phase docs |

