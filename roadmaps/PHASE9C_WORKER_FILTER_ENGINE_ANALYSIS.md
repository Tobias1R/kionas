# Worker Query Execution Engine: Filter Evaluation Analysis

**Date**: March 21, 2026  
**Phase**: 9c (BETWEEN/IN Predicate Integration)  
**Status**: Discovery & Integration Planning

---

## 1. Current Implementation Overview

### Architecture
The worker filter execution uses a **4-stage pipeline** architecture:

```
Raw Filter SQL → Parse Clauses → Build Mask → Apply Filter
     ↓                ↓             ↓            ↓
  "col > 5        FilterClause    BooleanArray  RecordBatch
   AND col2='x'"   Vec            (T/F per row) filtered
```

### Key Files
| Stage | File | Function |
|-------|------|----------|
| **Planner Type Checking** | `kionas/src/planner/filter_type_checker.rs` | `check_filter_predicate_types()` |
| **Planner BETWEEN/IN Detection** | `kionas/src/planner/filter_type_checker.rs` | `validate_between_predicate()`, `validate_in_predicate()` |
| **Worker Validation** | `worker/src/services/query_execution.rs` | `validate_filter_predicates_types()` |
| **Worker Parsing** | `worker/src/services/query_execution.rs` | `parse_filter_clauses()` |
| **Worker Evaluation** | `worker/src/services/query_execution.rs` | `evaluate_clause_at_row()` |

---

## 2. How Filters Are Currently Evaluated

### 2.1 Filter Parsing Pipeline

**Entry Point**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L300-350)

```rust
pub(crate) fn apply_filter_pipeline(
    input: &[RecordBatch],
    filter_sql: &str,
    schema_metadata: Option<&HashMap<String, ColumnDatatypeSpec>>,
) -> Result<Vec<RecordBatch>, String>
```

**Process**:
1. **Type Validation** (optional, if schema_metadata provided):
   - Calls `validate_filter_predicates_types()` for column/type checking
   - Pre-validates before parsing (Phase 9b Step 6)

2. **Clause Parsing**:
   - Splits by `AND` (case-insensitive, OR not supported)
   - Rejects empty predicates, non-ASCII characters
   - Calls `parse_filter_clauses()` for all clauses

3. **Mask Building**:
   - Per batch: calls `build_filter_mask()` for boolean mask
   - Returns `Vec<bool>` with one entry per row

4. **Arrow Filtering**:
   - Applies Arrow's `filter_record_batch()` using mask
   - Preserves row order

### 2.2 Clause Parsing

**Function**: [parse_filter_clauses()](worker/src/services/query_execution.rs#L335-365)

**How it works**:
1. Splits filter SQL by `AND` operator
2. For each clause, calls `parse_single_clause()`
3. Extracts `(column, op, literal)` tuple
4. Converts literal to `FilterValue` (Int, Bool, Str)
5. Returns `Vec<FilterClause>`

**Current FilterOp Enum** [L28-32]:
```rust
#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq,    // =
    Ne,    // !=
    Gt,    // >
    Ge,    // >=
    Lt,    // <
    Le,    // <=
}
```

**FilterClause Structure** [L35-38]:
```rust
#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,  // Int(i64), Bool(bool), Str(String)
}
```

**Operator Detection** [parse_single_clause()]:
```rust
const OPS: [(&str, FilterOp); 6] = [
    (">=", FilterOp::Ge),
    ("<=", FilterOp::Le),
    ("!=", FilterOp::Ne),
    ("=", FilterOp::Eq),
    (">", FilterOp::Gt),
    ("<", FilterOp::Lt),
];
```
- Order matters: `>=` checked before `=` to avoid premature matching
- Returns error if no operator matched

---

## 3. Comparison Operators & NULL Handling

### 3.1 Comparison Implementation

**Function**: [evaluate_clause_at_row()](worker/src/services/query_execution.rs#L452-507)

**Core Logic**:
```rust
fn evaluate_clause_at_row(
    array: &ArrayRef,
    row_idx: usize,
    clause: &FilterClause,
) -> Result<bool, String>
```

**NULL Handling** [L463]:
```rust
if array.is_null(row_idx) {
    return Ok(false);  // NULL ALWAYS FAILS (Standard SQL)
}
```

### 3.2 Type Dispatch Matrix

The function uses a massive match statement on `(&clause.value, array.data_type())`:

| Column Type | Filter Value | Handler | Behavior |
|-------------|--------------|---------|----------|
| Int64 | Int(i64) | `compare_i64()` | Direct comparison |
| Int32, Int16 | Int(i64) | Cast to i64, then `compare_i64()` | Type coercion |
| Boolean | Bool(bool) | `compare_bool()` | Custom bool semantics |
| Utf8 | Str(String) | `compare_str()` | Lexicographic |
| Date32 | Str | Parse ISO-8601 → i32, `compare_i64()` | Temporal parsing |
| Date64 | Str | Parse ISO-8601 → i64, `compare_i64()` | Temporal parsing |
| Timestamp(_,None) | Str | Parse ISO-8601 → normalized i64, `compare_i64()` | Unit-based |
| Timestamp(_, Some(_)) | Any | **ERROR** | Timezone not supported |
| Any | Int | **ERROR** | Temporal types need quoted string |
| Unmatched | Any | **ERROR** | Type mismatch |

### 3.3 Comparison Functions

**Integer Comparison** [compare_i64()]:
```rust
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
```

**Boolean Comparison** [compare_bool()]:
```rust
fn compare_bool(lhs: bool, rhs: bool, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs && !rhs,      // true > false
        FilterOp::Ge => lhs == rhs || (lhs && !rhs),
        FilterOp::Lt => !lhs && rhs,      // false < true
        FilterOp::Le => lhs == rhs || (!lhs && rhs),
    }
}
```

**String Comparison** [compare_str()]:
```rust
fn compare_str(lhs: &str, rhs: &str, op: FilterOp) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Ne => lhs != rhs,
        FilterOp::Gt => lhs > rhs,        // Lexicographic
        FilterOp::Ge => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Le => lhs <= rhs,
    }
}
```

### 3.4 IS NULL / IS NOT NULL Support

**Planner-side**: [kionas/src/planner/filter_type_checker.rs](kionas/src/planner/filter_type_checker.rs#L140-170)

Pattern detection recognizes:
- `column IS NULL`
- `column IS NOT NULL`
- `NOT (column IS NULL)`

**Worker-side**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L122-185)

Recognition patterns in `validate_filter_predicates_types()`:
```rust
// Pattern 1: "column IS NULL" or "column IS NOT NULL"
if lower.ends_with("is null") || lower.ends_with("is not null")

// Pattern 2: "NOT (column IS NULL)"
if lower.starts_with("not") && lower.contains("is null")
```

However, **IS NULL is NOT currently evaluated by parse_filter_clauses()** — it returns an error:
```
"unsupported filter clause 'col IS NULL': expected one of =, !=, >, >=, <, <="
```

**TODO**: IS NULL evaluation not yet implemented in worker runtime pipeline.

---

## 4. BETWEEN/IN Operators: Current Status

### 4.1 Planner-side BETWEEN/IN Detection (COMPLETE ✅)

**File**: [kionas/src/planner/filter_type_checker.rs](kionas/src/planner/filter_type_checker.rs#L269-338)

Both operators are **recognized and type-checked** at planner time:

#### BETWEEN Validation
- **Pattern**: `"column BETWEEN lower AND upper"`
- **Checks**:
  - Column exists in schema
  - Both bounds have inferable type (bool, int, or string literal)
  - Both bounds have **same type** (homogeneity constraint)
  - Returns `TypeCoercionViolation` if anything fails
- **Function**: `validate_between_predicate()` [L269-305]

#### IN Validation
- **Pattern**: `"column IN (val1, val2, ...)"`
- **Checks**:
  - Column exists in schema
  - At least one value in list
  - All values have same type (homogeneity constraint)
  - Returns `TypeCoercionViolation` if anything fails
- **Function**: `validate_in_predicate()` [L310-360]

### 4.2 Worker-side Status: BLOCKED ❌

**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L1040-1080)

Both operators are **rejected** during parse_filter_clauses():

```rust
fn parse_single_clause(input: &str) -> Result<(String, FilterOp, &str), String> {
    const OPS: [(&str, FilterOp); 6] = [
        (">=", FilterOp::Ge), ("<=", FilterOp::Le), ("!=", FilterOp::Ne),
        ("=", FilterOp::Eq), (">", FilterOp::Gt), ("<", FilterOp::Lt),
    ];
    // ... if no operator matched:
    Err(format!(
        "unsupported filter clause '{}': expected one of =, !=, >, >=, <, <=",
        input
    ))
}
```

**Detection in validation layer**: [worker/src/execution/query.rs](worker/src/execution/query.rs#L147-173)

Early rejection happens here:
```rust
fn validate_filter_predicates(operators: &[serde_json::Value]) -> Result<(), String> {
    // ... detects and rejects BETWEEN, IN
}
```

Function `detect_deferred_predicate_label()` [L120-143]:
- Returns `Some("BETWEEN")` if ` between ` found
- Returns `Some("IN list")` if ` in (` found
- Causes immediate rejection: _"predicate is not supported in this phase"_

---

## 5. Where BETWEEN/IN Need to Be Added

### 5.1 Integration Points (Sequential Order)

#### [PRIORITY 1] Parse & Structure
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L28-38)

**Current**:
```rust
#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq, Ne, Gt, Ge, Lt, Le,
}

#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,  // Int(i64), Bool(bool), Str(String)
}
```

**Required Changes**:
1. Extend `FilterOp` enum to include `Between` and `In`
2. Extend `FilterValue` to support list types: `List(Vec<FilterValue>)` 
3. Extend `FilterClause` to handle bounds (for BETWEEN):
   ```rust
   struct FilterClause {
       column: String,
       op: FilterOp,
       value: FilterValue,
       // For BETWEEN
       upper_bound: Option<FilterValue>,
   }
   ```

#### [PRIORITY 2] Parse & Detect
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L1040-1080)

**Function**: `parse_single_clause()`

**Required Changes**:
1. Add BETWEEN pattern detection before single-operator check:
   - Pattern: `column BETWEEN literal AND literal`
   - Extract column, lower, upper bounds
   - Parse both bounds to FilterValue

2. Add IN pattern detection:
   - Pattern: `column IN (val1, val2, ...)`
   - Extract column and comma-separated values
   - Parse all values to FilterValue
   - Return as `List(Vec<FilterValue>)`

3. Fallback to current single-op matching only if patterns don't match

#### [PRIORITY 3] Type Validation
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L122-185)

**Function**: `validate_filter_predicates_types()`

**Required Changes**:
1. Add BETWEEN clause recognition:
   ```rust
   if trimmed.to_ascii_lowercase().contains(" between ") {
       // Validate column exists
       // Validate bounds are parseable and matching types
   }
   ```

2. Add IN clause recognition:
   ```rust
   if trimmed.to_ascii_lowercase().contains(" in (") {
       // Validate column exists
       // Validate all values are parseable and matching types
   }
   ```

3. Extract operand types from bounds (BETWEEN) or values (IN)
4. Call `validate_type_compatibility()` for each bound/value

#### [PRIORITY 4] Evaluation
**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L452-507)

**Function**: `evaluate_clause_at_row()`

**Required Changes**:
1. Add match arm for `FilterOp::Between`:
   ```rust
   (FilterOp::Between, FilterValue::Int(lower), FilterValue::Int(upper), column_array) => {
       if let (DataType::Int64, _) = ... {
           let lhs = downcast_required::<Int64Array>(array, "Int64")?.value(row_idx);
           Ok(compare_between_i64(lhs, lower, upper))
       }
   }
   ```

2. Add match arm for `FilterOp::In`:
   ```rust
   (FilterOp::In, FilterValue::List(values), column_array) => {
       // Convert array value to FilterValue
       // Check if value exists in list (using type-specific equality)
       Ok(values.contains(&array_value))
   }
   ```

3. Implement helper functions:
   - `compare_between_i64(value: i64, lower: i64, upper: i64) -> bool {...}`
   - `compare_between_str(value: &str, lower: &str, upper: &str) -> bool {...}`
   - `compare_between_bool(value: bool, lower: bool, upper: bool) -> bool {...}`

4. Handle temporal types (Date32, Date64, Timestamp) in BETWEEN/IN
5. NULL semantics: `NULL BETWEEN x AND y` = `false`, `NULL IN (...)` = `false`

#### [PRIORITY 5] Query Validation (Worker)
**File**: [worker/src/execution/query.rs](worker/src/execution/query.rs#L147-173)

**Function**: `validate_filter_predicates()` & `detect_deferred_predicate_label()`

**Required Changes**:
1. Remove BETWEEN and IN from `detect_deferred_predicate_label()` deferred list
2. Both should now pass validation (no longer rejected early)

---

## 6. Suggested Integration Pattern

### 6.1 BETWEEN Comparison Template

```rust
/// What: Compare value against BETWEEN bounds (inclusive).
///
/// Inputs:
/// - `value`: Column value
/// - `lower`: Lower bound (inclusive)
/// - `upper`: Upper bound (inclusive)
///
/// Output:
/// - true if lower <= value <= upper
fn compare_between_i64(value: i64, lower: i64, upper: i64) -> bool {
    value >= lower && value <= upper
}

fn compare_between_str(value: &str, lower: &str, upper: &str) -> bool {
    value >= lower && value <= upper  // Lexicographic comparison
}

fn compare_between_bool(value: bool, lower: bool, upper: bool) -> bool {
    // Boolean ordering: false < true
    let value_ord = if value { 1 } else { 0 };
    let lower_ord = if lower { 1 } else { 0 };
    let upper_ord = if upper { 1 } else { 0 };
    value_ord >= lower_ord && value_ord <= upper_ord
}
```

### 6.2 IN Matching Template

```rust
/// What: Check if value is in set of values.
///
/// Inputs:
/// - `value`: Column value as FilterValue
/// - `values`: List of allowed values
///
/// Output:
/// - true if value matches any element in values (type-safe equality)
fn value_in_set(value: &FilterValue, values: &[FilterValue]) -> bool {
    values.iter().any(|v| {
        match (value, v) {
            (FilterValue::Int(a), FilterValue::Int(b)) => a == b,
            (FilterValue::Bool(a), FilterValue::Bool(b)) => a == b,
            (FilterValue::Str(a), FilterValue::Str(b)) => a == b,
            _ => false,
        }
    })
}
```

### 6.3 Clause Parsing Strategy

```rust
fn parse_single_clause(input: &str) -> Result<(String, FilterOp, FilterValue, Option<FilterValue>), String> {
    let trimmed_lower = input.to_ascii_lowercase();
    
    // Try BETWEEN first (higher precedence)
    if let Some(between_pos) = trimmed_lower.find(" between ") {
        let column = normalize_projection_identifier(&input[..between_pos]);
        let after_between = &input[between_pos + 9..].trim();
        if let Some(and_pos) = after_between.to_ascii_lowercase().find(" and ") {
            let lower_str = after_between[..and_pos].trim();
            let upper_str = after_between[and_pos + 5..].trim();
            return Ok((
                column,
                FilterOp::Between,
                parse_filter_value(lower_str)?,
                Some(parse_filter_value(upper_str)?),
            ));
        }
    }
    
    // Try IN next
    if let Some(in_pos) = trimmed_lower.find(" in (") {
        let column = normalize_projection_identifier(&input[..in_pos]);
        let after_in = &input[in_pos + 5..].trim();
        if let Some(close_paren) = after_in.rfind(')') {
            let values_str = &after_in[..close_paren];
            let values = values_str.split(',')
                .map(|v| parse_filter_value(v.trim()))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok((
                column,
                FilterOp::In,
                FilterValue::List(values),
                None,
            ));
        }
    }
    
    // Fall back to comparison operators
    // ... existing code ...
}
```

---

## 7. Testing Considerations

### Test Coverage Requirements

1. **Planner Acceptance Tests** (✅ Already exist):
   - BETWEEN with matching bound types
   - BETWEEN with mismatched bound types (reject)
   - BETWEEN with missing column (reject)
   - IN with matching element types
   - IN with mismatched element types (reject)
   - IN with missing column (reject)
   - IN with empty list (reject)

2. **Worker Runtime Tests** (🔲 Need to add):
   - BETWEEN: `col BETWEEN 5 AND 10` on Int64 column
   - BETWEEN: `col BETWEEN 'a' AND 'z'` on Utf8 column
   - BETWEEN with NULL values (should return false)
   - BETWEEN boundary conditions (inclusive both ends)
   - IN: `col IN (1, 2, 3)` on Int64 column
   - IN: `col IN ('a', 'b', 'c')` on Utf8 column
   - IN with NULL values (should return false)
   - IN with single element
   - IN with many elements
   - Combined filters: `col BETWEEN 1 AND 10 AND col IN (5, 6, 7)`

3. **Error Handling**:
   - BETWEEN with invalid bounds (non-inferable types)
   - IN with non-parseable values
   - Timezone-bearing timestamps in BETWEEN/IN (error)
   - Type mismatch: `col IN (true, 'string')` (reject at planner)

---

## 8. Implementation Roadmap

### Phase 9c Step 2: Worker Runtime Support (Current)

**Deliverables**:
1. Extend `FilterOp` enum with `Between` and `In` variants
2. Extend `FilterValue` with `List(Vec<FilterValue>)` variant  
3. Extend `FilterClause` with optional `upper_bound` field
4. Implement BETWEEN pattern parsing in `parse_single_clause()`
5. Implement IN pattern parsing in `parse_single_clause()`
6. Add BETWEEN/IN validation in `validate_filter_predicates_types()`
7. Add BETWEEN/IN evaluation in `evaluate_clause_at_row()`
8. Remove BETWEEN/IN from deferred predicates in `detect_deferred_predicate_label()`
9. Integration testing with real query payloads

### Phase 9c Step 3: End-to-End Testing
- Planner → Worker pipeline validation
- Multiple BETWEEN/IN clauses in single filter
- Mixed predicates: `col1 > 5 AND col2 IN (1, 2) AND col3 BETWEEN 'a' AND 'z'`
- Edge cases: NULL handling, boundary conditions, type coercions

### Phase 9c Step 4: Documentation & Hardening
- Error message clarity and remediation guidance
- Performance characteristics vs. OR-equivalent queries
- NULL semantics documentation
- Supported data type matrix

---

## 9. Key Constraints & Considerations

### Design Constraints
1. **AND-only**: OR not supported, cannot use BETWEEN/IN as OR equivalent
2. **Strict Type Homogeneity**: All bounds (BETWEEN) or values (IN) must have same type
3. **NULL Semantics**: NULL in any position makes expression false
4. **Inclusive BETWEEN**: Both lower and upper bounds are inclusive
5. **Case-Insensitive Keywords**: BETWEEN/IN keywords case-insensitive, but values are not

### Performance Characteristics
- **BETWEEN**: O(1) comparison (2 boundary checks)
- **IN**: O(n) linear search in values list (could optimize to HashSet for many values)
- **NULL Filter**: Short-circuit at NULL check (no evaluation needed)

### Type Support
Current support after planner validation:
- **Integer types**: Int16, Int32, Int64
- **String types**: Utf8, Varchar
- **Temporal types**: Date32, Date64, Timestamp (no timezone)
- **Boolean**: bool

Deferred (Phase 9d+):
- Decimal precision handling in BETWEEN/IN
- Mixed-type coercions (int vs decimal)

---

## 10. File Change Summary

```
worker/src/services/query_execution.rs:
  - [L28-32] FilterOp enum: Add Between, In
  - [L35-38] FilterClause struct: Add upper_bound field, restructure value handling
  - [L122-185] validate_filter_predicates_types(): Add BETWEEN/IN pattern validation
  - [L1040-1080] parse_single_clause(): Add BETWEEN/IN parsing with fallback
  - [L452-507] evaluate_clause_at_row(): Add BETWEEN/IN match arms
  - [NEW] compare_between_i64(), compare_between_str(), compare_between_bool()
  - [NEW] value_in_set()

worker/src/execution/query.rs:
  - [L120-143] detect_deferred_predicate_label(): Remove BETWEEN/IN detection

worker/src/services/query_execution_tests.rs:
  - [NEW] Tests for BETWEEN evaluation
  - [NEW] Tests for IN evaluation
  - [NEW] Tests for combined filters with BETWEEN/IN
```

---

## 11. Success Criteria

✅ **Phase 9c Step 2 Completion Checklist**:
1. [ ] BETWEEN predicates parse without error
2. [ ] IN predicates parse without error
3. [ ] BETWEEN evaluation returns correct boolean per row
4. [ ] IN evaluation returns correct boolean per row
5. [ ] NULL semantics enforced (always false)
6. [ ] Type validation rejects mismatched bounds/values
7. [ ] Combined filters work: `col > 1 AND col BETWEEN 5 AND 10`
8. [ ] Error messages guide users to resolution
9. [ ] All test cases pass (planner + worker)
10. [ ] cargo clippy clean, cargo fmt compliant
11. [ ] No regression in existing filter operations

