# Worker Filter Engine: Quick Reference Guide

## 1. Current Code Locations (Line Numbers)

### Filter Execution Pipeline (worker/src/services/query_execution.rs)

```
FilterOp Enum:                                    L28-32
FilterValue Enum:                                 L23-26
FilterClause struct:                              L35-38

apply_filter_pipeline():                          L390-408
  - Entry point for filter processing
  - Optional schema_metadata type validation
  - Calls parse_filter_clauses() and build_filter_mask()

parse_filter_clauses():                           L410-445
  - Splits by AND, rejects OR
  - Calls parse_single_clause() per clause
  - Returns Vec<FilterClause>

parse_single_clause():                            L1040-1080
  - CRITICAL: Where operators are detected
  - Current: 6 comparison operators (=, !=, <, >, <=, >=)
  - INTEGRATION POINT: Add BETWEEN/IN pattern matching here

build_filter_mask():                              L447-477
  - Row-by-row evaluation via loop
  - Calls evaluate_clause_at_row() per row/clause
  - Returns BooleanArray

evaluate_clause_at_row():                         L479-556
  - CRITICAL: Core evaluation logic
  - NULL check (returns false)
  - Type dispatch on (FilterValue, DataType)
  - INTEGRATION POINT: Add BETWEEN/IN match arms here

Comparison functions:                             L1130-1160
  - compare_i64()       L1130-1139
  - compare_bool()      L1141-1150
  - compare_str()       L1152-1160
  - INTEGRATION POINT: Add compare_between_*() and value_in_set()

Type validation:                                  L122-185
  - validate_filter_predicates_types()
  - INTEGRATION POINT: Add BETWEEN/IN pattern recognition
  - validate_type_compatibility()
  - infer_filter_literal_type()
```

### Type Checking (Planner)

```
kionas/src/planner/filter_type_checker.rs

check_filter_predicate_types():                   L79-101
  - Entry for planner validation
  - Dispatches to validate_raw_sql_predicate()

validate_raw_sql_predicate():                     L135-200
  - IS NULL/IS NOT NULL pattern detection        L154-180
  - NOT (IS NULL) pattern detection              L182-197
  - BETWEEN pattern detection                    L199-201 ✅ IMPLEMENTED
  - IN pattern detection                         L203-205 ✅ IMPLEMENTED
  - Calls validate_between_predicate()
  - Calls validate_in_predicate()

validate_between_predicate():                     L269-305
  - Pattern: "column BETWEEN lower AND upper"
  - Validates column exists
  - Checks bounds have inferable types (homogeneous)
  - ✅ COMPLETE - validates bounds match

validate_in_predicate():                          L310-360
  - Pattern: "column IN (val1, val2, ...)"
  - Validates column exists
  - Checks all values have same type (homogeneous)
  - ✅ COMPLETE - validates element types match

infer_operand_type():                            L199-223
  - Returns "bool", "int", or "string"
  - Recognizes true/false, numeric, quoted strings
  - Used by BETWEEN/IN validators
```

### Worker Query Execution Entry

```
worker/src/execution/query.rs

validate_filter_predicates():                     L147-173
  - BLOCKS BETWEEN/IN at early validation stage
  - Calls detect_deferred_predicate_label()

detect_deferred_predicate_label():                L120-143
  - Scans for " between " → returns Some("BETWEEN")
  - Scans for " in (" → returns Some("IN list")
  - INTEGRATION POINT: Must allow BETWEEN/IN to pass
```

---

## 2. Current vs. Required Changes

### What Works Now ✅

| Aspect | Status | Implementation |
|--------|--------|-----------------|
| **Planner BETWEEN validation** | ✅ | `validate_between_predicate()` - validates structure & types |
| **Planner IN validation** | ✅ | `validate_in_predicate()` - validates structure & types |
| **Operator parsing (6 ops)** | ✅ | `parse_single_clause()` - =, !=, <, >, <=, >= |
| **NULL always fails** | ✅ | `evaluate_clause_at_row()` L463 early return |
| **Type dispatch system** | ✅ | Giant match on (FilterValue, DataType) |
| **Batch filtering** | ✅ | Arrow's `filter_record_batch()` with mask |
| **Temporal parsing** | ✅ | ISO-8601, Date32, Date64, Timestamp handling |

### What's Missing ❌

| Aspect | Status | Location | Impact |
|--------|--------|----------|--------|
| **BETWEEN pattern parsing** | ❌ | `parse_single_clause()` | Cannot extract bounds |
| **IN pattern parsing** | ❌ | `parse_single_clause()` | Cannot extract value list |
| **FilterOp::Between enum** | ❌ | `FilterOp` enum | Cannot represent operation |
| **FilterOp::In enum** | ❌ | `FilterOp` enum | Cannot represent operation |
| **FilterValue::List variant** | ❌ | `FilterValue` enum | Cannot store multiple values |
| **BETWEEN evaluation logic** | ❌ | `evaluate_clause_at_row()` | Cannot evaluate bounds |
| **IN evaluation logic** | ❌ | `evaluate_clause_at_row()` | Cannot check membership |
| **BETWEEN/IN allow list** | ❌ | `detect_deferred_predicate_label()` | Rejected early |

---

## 3. Type Flow Example: BETWEEN

```
Planner Input:  "SELECT * FROM t WHERE price BETWEEN 10 AND 100"
                        ↓
          validate_raw_sql_predicate()
                        ↓
          Detects " between " pattern
                        ↓
          validate_between_predicate()
                        ↓
          ✅ Column 'price' exists
          ✅ Bounds '10' and '100' both infer to 'int'
          ✅ Types match (homogeneous)
          ✅ Returns Ok(())
                        ↓
        Physical plan with Raw{sql: "price BETWEEN 10 AND 100"}
                        ↓
[WORKER SIDE - NOW BLOCKED]
        validate_filter_predicates() in worker/src/execution/query.rs
                        ↓
        detect_deferred_predicate_label() finds " between "
                        ↓
        ❌ Returns Err("predicate is not supported in this phase: BETWEEN")
                        ↓
        [PHASE 9C: Allow to continue]
                        ↓
        apply_filter_pipeline()
                        ↓
        parse_filter_clauses() → parse_single_clause()
                        ↓
        ❌ Cannot find =, !=, <, >, <=, >= operators
        [PHASE 9C: Add BETWEEN pattern matching]
                        ↓
        Returns FilterClause {
            column: "price",
            op: FilterOp::Between,
            value: FilterValue::Int(10),
            upper_bound: Some(FilterValue::Int(100)),
        }
                        ↓
        build_filter_mask() loops per row
                        ↓
        evaluate_clause_at_row(array, row_idx, clause)
                        ↓
        ❌ No match arm for FilterOp::Between
        [PHASE 9C: Add match arm for Between]
                        ↓
        return Ok(compare_between_i64(value, 10, 100))
                        ↓
        10 ≤ value ≤ 100 ? true : false
```

---

## 4. Key Data Structures After Phase 9c

### Extended FilterOp Enum
```rust
#[derive(Debug, Clone, Copy)]
enum FilterOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Between,  // NEW
    In,       // NEW
}
```

### Extended FilterValue Enum
```rust
#[derive(Debug, Clone)]
enum FilterValue {
    Int(i64),
    Bool(bool),
    Str(String),
    List(Vec<FilterValue>),  // NEW - for IN values
}
```

### Extended FilterClause Struct
```rust
#[derive(Debug, Clone)]
struct FilterClause {
    column: String,
    op: FilterOp,
    value: FilterValue,
    upper_bound: Option<FilterValue>,  // NEW - for BETWEEN upper
}
```

---

## 5. Integration Checklist

### Phase 9c Step 2 Tasks

```
[  ] 1. Extend FilterOp enum with Between, In variants
[  ] 2. Extend FilterValue enum with List variant  
[  ] 3. Extend FilterClause with upper_bound field
[  ] 4. Add BETWEEN pattern detection in parse_single_clause()
[  ] 5. Add IN pattern detection in parse_single_clause()
[  ] 6. Add BETWEEN clause validation in validate_filter_predicates_types()
[  ] 7. Add IN clause validation in validate_filter_predicates_types()
[  ] 8. Add FilterOp::Between match arm in evaluate_clause_at_row()
[  ] 9. Add FilterOp::In match arm in evaluate_clause_at_row()
[ ] 10. Implement compare_between_i64(), compare_between_str(), compare_between_bool()
[ ] 11. Implement value_in_set()
[ ] 12. Remove BETWEEN/IN from detect_deferred_predicate_label()
[ ] 13. Add type dispatch for temporal BETWEEN/IN (Date32, Date64, Timestamp)
[ ] 14. Write unit tests for BETWEEN evaluation
[ ] 15. Write unit tests for IN evaluation
[ ] 16. Write integration tests (planner → worker)
[ ] 17. Test NULL handling: NULL BETWEEN x AND y → false
[ ] 18. Test NULL handling: NULL IN (...) → false
[ ] 19. Test combined filters with BETWEEN/IN
[ ] 20. Run cargo fmt, cargo clippy, cargo check
```

---

## 6. Error Scenarios & Messages

### Current (Phase 9a/9b)
```
❌ unsupported filter clause 'price BETWEEN 10 AND 100': expected one of =, !=, >, >=, <, <=
❌ unsupported filter clause 'status IN (1, 2, 3)': expected one of =, !=, >, >=, <, <=
```

### After Phase 9c
```
✅ "price BETWEEN 10 AND 100" → Parses successfully
✅ "status IN (1, 2, 3)" → Parses successfully

Still rejects:
❌ "price BETWEEN 10.5 AND 'text'" → TypeMismatch (bounds not homogeneous)
❌ "status IN (1, 'text')" → TypeMismatch (values not homogeneous)
❌ "nonexistent BETWEEN 1 AND 100" → MissingColumn
❌ "nonexistent IN (1, 2)" → MissingColumn
```

---

## 7. Testing Template

### Unit Test: BETWEEN

```rust
#[test]
fn test_between_int_inclusive() {
    // Setup: Create Int64Array [5, 10, 15]
    // Query: col BETWEEN 10 AND 15
    // Expected result: [false, true, true]
}

#[test]
fn test_between_null_always_false() {
    // Setup: Create Int64Array with NULL at index 0
    // Query: col BETWEEN 5 AND 15
    // Expected result: [false, ...]
}

#[test]
fn test_between_string_lexicographic() {
    // Setup: Create StringArray ["apple", "banana", "cherry"]
    // Query: col BETWEEN "b" AND "c"
    // Expected result: [false, true, false]
}
```

### Unit Test: IN

```rust
#[test]
fn test_in_int_membership() {
    // Setup: Create Int64Array [1, 2, 5, 10]
    // Query: col IN (2, 5, 8)
    // Expected result: [false, true, true, false]
}

#[test]
fn test_in_null_always_false() {
    // Setup: Create Int64Array with NULL
    // Query: col IN (1, 2, 3)
    // Expected result: [false, ...]
}

#[test]
fn test_in_string_matching() {
    // Setup: Create StringArray ["apple", "banana", "cherry"]
    // Query: col IN ('apple', 'cherry', 'grape')
    // Expected result: [true, false, true]
}
```

---

## 8. Performance Notes

| Operator | Complexity | Notes |
|----------|-----------|-------|
| `col = x` | O(1) | Single equality check |
| `col IN (x, y, z)` | O(n) | Linear scan (n = list size) |
| `col BETWEEN x AND y` | O(1) | Two boundary checks |
| `col = a OR col = b OR col = c` | **NOT SUPPORTED** | Use IN instead |

**Optimization opportunity**: For large IN lists, could convert to HashSet in Phase 9d.

---

## 9. Files Requiring Changes Summary

```
PRIMARY FILES:
✏️  worker/src/services/query_execution.rs     [~200 lines of changes]
✏️  worker/src/execution/query.rs               [~5 lines - remove BETWEEN/IN from deferred]

TEST FILES:
✏️  worker/src/tests/services_query_execution_tests.rs  [~150 lines of new tests]

UNMODIFIED (Already Complete):
✅ kionas/src/planner/filter_type_checker.rs [BETWEEN/IN validators exist]
✅ worker/src/execution/pipeline.rs [Calls apply_filter_pipeline correctly]
```

