# Worker Filter Engine: Architecture & Flow Diagrams

## 1. Current Filter Evaluation Pipeline (Phase 9a/9b)

```
┌─────────────────────────────────────────────────────────────┐
│ INPUT: SQL Query with Filter Predicates                     │
│ "SELECT * FROM t WHERE price > 10 AND status IN [1,2]"     │
└────────────────────┬────────────────────────────────────────┘
                     │
                     ▼
        ┌────────────────────────┐
        │  PLANNER (Phase 9b)    │
        └────────────────────────┘
                     │
            ┌────────┴────────┐
            ▼                 ▼
    ✅ Predicate      ✅ IN Predicate
    Validation        Validation
    (price exists)    (❌ REJECTED)
            │                 │
            ├─────────┬───────┤
            │         │       │
            └────────┬┴───────┘
                     │
            ERROR: IN not supported (Phase 9c+)
                     │
                     ▼
    🚫 PLANNER REJECTS: "predicate is not supported in this phase: IN list"


    [IF BETWEEN/IN ARE ALLOWED TO PASS]
                     │
                     ▼
        ┌────────────────────────┐
        │  WORKER (Phase 9c)     │
        └────────────────────────┘
                     │
                     ▼
        ┌────────────────────────┐
        │ execute_query_task()   │
        └────────────────────────┘
                     │
                     ▼
        ┌────────────────────────────┐
        │ apply_filter_pipeline()    │
        │  filter_sql: "price > 10"  │
        └────────────────────────────┘
                     │
        ┌────────────┴─────────────┐
        │                          │
   (if schema_metadata)        (parse)
        │                      │
        ▼                      ▼
  validate_filter_         parse_filter_
  predicates_types()       clauses()
        │                      │
        ▼                      ▼
  validate each         parse_single_clause()
  column exists         (extracts column, op, literal)
  type matches               │
        │                    ▼
        └───────┬────────────┘
                │
                ▼
        ┌─────────────────┐
        │ build_filter_   │
        │ mask()          │
        └─────────────────┘
                │
          [For each row]
                │
                ▼
        ┌──────────────────────────┐
        │ evaluate_clause_at_row() │
        │  array[row_idx] vs value │
        └──────────────────────────┘
                │
        ┌───────┴───────────┐
        │                   │
    Is NULL?            compare_i64()
       ▼                 compare_bool()
    false             compare_str()
        │                 │
        └─────────┬───────┘
                  │
                  ▼
        ┌──────────────────┐
        │ BooleanArray[]   │
        │ [true,false,...] │
        └──────────────────┘
                  │
                  ▼
        ┌──────────────────────┐
        │ Arrow's             │
        │ filter_record_batch()│
        └──────────────────────┘
                  │
                  ▼
        ┌──────────────────────┐
        │ OUTPUT: Filtered     │
        │ RecordBatches        │
        └──────────────────────┘
```

---

## 2. Extended Pipeline for Phase 9c (BETWEEN/IN Support)

```
                INPUT: Filter SQL
                "price BETWEEN 10 AND 100"
                "status IN (1, 2, 3)"
                       │
                       ▼
        ┌──────────────────────────┐
        │ parse_single_clause()    │
        └──────────────────────────┘
                       │
        ┌──────────────┼──────────────┐
        │              │              │
    [OLD PATH]   [NEW PATH 1]   [NEW PATH 2]
        │         BETWEEN          IN
        │              │              │
        │         Find " between "    │
        │         Extract:        Find " in ("
        │         column,         Extract:
        │         lower,          column,
        │         upper           values[]
        │              │              │
        │              ▼              ▼
        │         parse each      parse each
        │         bound to        value to
        │         FilterValue     FilterValue
        │              │              │
        ▼              ▼              ▼
    Try 6 ops:    Return:         Return:
    (=,!=,>,...) FilterOp::     FilterOp::In
                 Between +       +
                 lower/upper     values_list
                 in clause           │
                       │             │
                       └─────┬───────┘
                             │
                             ▼
                   ┌─────────────────────┐
                   │ evaluate_clause_    │
                   │ at_row() NEW LOGIC: │
                   └─────────────────────┘
                             │
                ┌────────────┼────────────┐
                │            │            │
           FilterOp::    FilterOp::      FilterOp::
           Eq/Ne/Gt/...  Between        In
                │            │            │
                ▼            ▼            ▼
            (existing)  compare_     value_in_
                        between_*()  set()
                             │            │
                        lower ≤ val ≤ upper
                        [T/F per type]    
                                         val in
                                         values[]
                                         [T/F]
```

---

## 3. Type Dispatch Matrix (evaluate_clause_at_row)

```
┌────────────────────────────────────────────────────────────────┐
│         FILTER VALUE vs. ARRAY DATA TYPE DISPATCH              │
├─────────────────────┬──────────────────┬──────────────────────┤
│ Array Type          │ FilterValue      │ Handler Function     │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Int64               │ Int(i64)         │ compare_i64()        │
│ Int32, Int16        │ Int(i64)         │ compare_i64()        │
│                     │                  │ (cast to i64)        │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Boolean             │ Bool(bool)       │ compare_bool()       │
│                     │                  │ (false < true)       │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Utf8                │ Str(String)      │ compare_str()        │
│ (String)            │                  │ (lexicographic)      │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Date32              │ Str(quoted)      │ parse ISO-8601       │
│                     │                  │ compare_i64()        │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Date64              │ Str(quoted)      │ parse ISO-8601       │
│                     │                  │ compare_i64()        │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Timestamp(unit,None)│ Str(quoted)      │ parse ISO-8601       │
│                     │                  │ convert to unit      │
│                     │                  │ compare_i64()        │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Timestamp(unit,TZ)  │ Any              │ ❌ ERROR             │
│                     │                  │ Timezone not support │
├─────────────────────┼──────────────────┼──────────────────────┤
│ Any with NULL value │ Any              │ return false (always)│
│                     │                  │ (NULL fails any pred)│
├─────────────────────┼──────────────────┼──────────────────────┤
│ Unmatched type      │ Any              │ ❌ ERROR             │
│ combinations        │                  │ Type mismatch        │
└─────────────────────┴──────────────────┴──────────────────────┘

[PHASE 9c ADDITIONS]:

┌────────────────────┬──────────────────┬──────────────────────┐
│ Array Type         │ FilterValue      │ Handler              │
├────────────────────┼──────────────────┼──────────────────────┤
│ Int64              │ List(values[])   │ value_in_set()       │
│ Bool               │ List(values[])   │ (linear search)      │
│ Utf8               │ List(values[])   │                      │
│ Date32             │ List(values[])   │                      │
│ Date64             │ List(values[])   │                      │
│ Timestamp          │ List(values[])   │                      │
├────────────────────┼──────────────────┼──────────────────────┤
│ Int16/32/64        │ Int + Int        │ compare_between_i64()│
│ (BETWEEN path)     │ (lower/upper)    │ lower ≤ val ≤ upper │
│                    │                  │                      │
│ Bool               │ Bool + Bool      │ compare_between_bool│
│ Utf8               │ Str + Str        │ compare_between_str()│
│ Date32/64          │ Str + Str        │ (parse then compare) │
│ Timestamp          │ Str + Str        │                      │
└────────────────────┴──────────────────┴──────────────────────┘
```

---

## 4. NULL Semantics (Standard SQL)

```
┌──────────────────────────────────────────────────────┐
│ NULL HANDLING IN FILTERS                             │
├──────────────────────────────────────────────────────┤
│ NULL = 5               → FALSE  (not true)           │
│ NULL != 5              → FALSE  (not true)           │
│ NULL > 5               → FALSE  (not true)           │
│ NULL >= 5              → FALSE  (not true)           │
│ NULL < 5               → FALSE  (not true)           │
│ NULL <= 5              → FALSE  (not true)           │
│                                                      │
│ [PHASE 9c ADDITIONS]:                                │
│ NULL BETWEEN 1 AND 10  → FALSE  (not true)          │
│ NULL IN (1, 2, 3)      → FALSE  (not true)          │
│                                                      │
│ NULL IS NULL           → TRUE   (special case)      │
│ NULL IS NOT NULL       → FALSE  (special case)      │
│                                                      │
│ Implementation: Top of evaluate_clause_at_row()     │
│   if array.is_null(row_idx) {                       │
│       return Ok(false);  ◄──── Early exit           │
│   }                                                  │
└──────────────────────────────────────────────────────┘
```

---

## 5. Combined Filter Evaluation Example

```
QUERY: "SELECT * FROM stats WHERE created >= '2024-01-01' 
                                  AND count BETWEEN 10 AND 100
                                  AND status IN (1, 2, 3)
                                  AND region IS NOT NULL"

┌─────────────────────────────────────────┐
│ parse_filter_clauses()                  │
│ Splits by AND                           │
└─────────────────────────────────────────┘
           │
    ┌──────┴──────────┬──────────┬───────┐
    │                 │          │       │
    ▼                 ▼          ▼       ▼
Clause 1:        Clause 2:    Clause 3: Clause 4:
"created >=      "count      "status   "region IS
'2024-01-01'"    BETWEEN 10   IN (1,2,3) NOT NULL"
                 AND 100"     
    │                │          │       │
    ▼                ▼          ▼       ▼
FilterClause     FilterClause Filter   FilterClause
{                {           Clause   {
  column:        column:    {         column:
  "created"    "count"      column:   "region"
  op: Ge       op: Between  "status"  op: IsNotNull
  value:       value: 10    op: In    value: (ignored)
  "2024-01-01" upper: 100   values:   │
}              }            [1, 2, 3] │
  │              │          }         │
  └──────┬───────┴───┬──────┤─────────┘
         │           │      │
         ▼           ▼      ▼
    (ERROR: IsNotNull        (Need to implement)
     operation not yet       
     supported in current    ┌──────────────────┐
     parse_single_clause)    │ [PHASE 9c BUILD]:│
                             │ Support IS NULL  │
                             │ IS NOT NULL in   │
                             │ parse_single_()  │
                             │ pattern matching │
                             └──────────────────┘

[AFTER PHASE 9c COMPLETION]:

    ┌──────┬──────────┬──────┬────────┐
    │      │          │      │        │
    ▼      ▼          ▼      ▼        ▼
  (Clause evaluation per row in build_filter_mask)
  
  Row 0: 2024-02-15 count=50 status=1 region="US"
    → Clause1: 2024-02-15 >= 2024-01-01? ✅ true
    → Clause2: 10 ≤ 50 ≤ 100?           ✅ true
    → Clause3: 1 IN (1,2,3)?             ✅ true
    → Clause4: region IS NOT NULL?       ✅ true
    → Result: true ∧ true ∧ true ∧ true = ✅ PASS
    
  Row 1: 2023-12-01 count=5 status=2 region=NULL
    → Clause1: 2023-12-01 >= 2024-01-01? ❌ false
    → Clause2: (skipped once one fails)
    → Clause3:
    → Clause4: NULL IS NOT NULL?         ❌ false
    → Result: ❌ FAIL (any clause fails)
    
  Row 2: 2024-01-15 count=75 status=4 region="CA"
    → Clause1: 2024-01-15 >= 2024-01-01? ✅ true
    → Clause2: 10 ≤ 75 ≤ 100?           ✅ true
    → Clause3: 4 IN (1,2,3)?             ❌ false
    → Result: ❌ FAIL
  
  BooleanArray mask: [true, false, false]
         │
         ▼
  Arrow filter_record_batch(batch, [true, false, false])
         │
         ▼
  Output: Row 0 only
```

---

## 6. Phase 9c Integration Points (Detailed)

```
┌────────────────────────────────────────────────────────────────┐
│ PHASE 9C IMPLEMENTATION WORKFLOW                               │
└────────────────────────────────────────────────────────────────┘

STEP 1: Data Structure Enhancements
┌──────────────────────────────────────┐
│ worker/src/services/query_execution.rs
│ - FilterOp enum:  Add Between, In
│ - FilterValue:    Add List(Vec<_>)
│ - FilterClause:   Add upper_bound
└──────────────────────────────────────┘
         │
         ▼
STEP 2: Parsing Extension
┌──────────────────────────────────────┐
│ parse_single_clause() modifications:
│ 
│ 1. Check for " between " pattern
│    └─ Extract column, lower, upper
│       Convert to FilterClause w/
│       op: Between, upper_bound set
│ 
│ 2. Check for " in (" pattern
│    └─ Extract column, values list
│       Convert to FilterClause w/
│       op: In, value: List(values)
│ 
│ 3. Fall back to 6 comparison ops
└──────────────────────────────────────┘
         │
         ▼
STEP 3: Type Validation  
┌──────────────────────────────────────┐
│ validate_filter_predicates_types():
│ 
│ Add .contains(" between ") check
│ Add .contains(" in (") check
│ 
│ For each: validate operand type
│ inference, homogeneity constraint
└──────────────────────────────────────┘
         │
         ▼
STEP 4: Evaluation Extension
┌──────────────────────────────────────┐
│ evaluate_clause_at_row() NEW arms:
│ 
│ (FilterOp::Between, Int, Int64Array)
│   → compare_between_i64()
│ 
│ (FilterOp::Between, Str, Utf8Array)
│   → compare_between_str()
│ 
│ (FilterOp::In, List, Int64Array)
│   → value_in_set()
│ 
│ etc. for all type combinations
└──────────────────────────────────────┘
         │
         ▼
STEP 5: Permission/Gating
┌──────────────────────────────────────┐
│ worker/src/execution/query.rs
│ 
│ Remove " between " from
│ detect_deferred_predicate_label()
│ 
│ Remove " in (" from
│ detect_deferred_predicate_label()
│ 
│ Allow BETWEEN/IN to reach worker
└──────────────────────────────────────┘
         │
         ▼
STEP 6: Testing
┌──────────────────────────────────────┐
│ Comprehensive test coverage:
│ - Unit: BETWEEN on each type
│ - Unit: IN on each type
│ - NULL edge cases
│ - Integration: planner→worker
│ - Combined: multiple predicates
│ - Regression: existing predicates
└──────────────────────────────────────┘
```

---

## 7. Error Flow (What Gets Rejected)

```
┌────────────────────────────────────────────────┐
│ VALIDATION LAYERS (Left-to-Right Rejection)    │
└────────────────────────────────────────────────┘

Planner Phase:
┌─────────────────────┐
│ Structural check    │   ❌ "price BETWEEN 10 AND"
│ (find keywords)     │   ❌ "col IN ()"
└─────┬───────────────┘   ❌ "col BETWEEN 'a' AND 100" (mismatched types)
      │
      ▼
┌──────────────────────────┐
│ Column existence check   │   ❌ "nonexistent BETWEEN 1 AND 10"
│ (lookup in schema)       │   ❌ "invalid_col IN (1, 2, 3)"
└─────┬────────────────────┘
      │
      ▼
┌──────────────────────────┐
│ Operand type check       │   ❌ "col BETWEEN 'a' AND 1" (mixed types)
│ (homogeneity check)      │   ❌ "col IN (1, 'text', true)" (mixed types)
└─────┬────────────────────┘
      │
      └──► ✅ Plannervalidation passes
                │
                ▼
         [WORKER SIDE]
                │
┌───────────────┴──────────────────┐
│ detect_deferred_predicate_      │
│ label() - PHASE 9c allows pass   │
└─────┬────────────────────────────┘
      │
      ▼
┌─────────────────────────┐──────────────────────────┐
│ parse_single_clause()   │ ❌ "col BETWEEN a AND b" │
│ [NEW LOGIC]             │    (non-inferable types) │
│ - BETWEEN pattern       │                          │
│ - IN pattern            │ ❌ "col IN ()" (empty)   │
│ - Fallback to 6 ops     │                          │
└─────┬───────────────────┴──────────────────────────┘
      │
      ▼
┌─────────────────────────┐
│ Type dispatch in        │   ❌ Timestamp with TZ
│ evaluate_clause_at_row()│
│ [NEW LOGIC]             │   ❌ Mismatched types
│ - BETWEEN match arms    │      after dispatch
│ - IN match arms         │
└─────┬───────────────────┘
      │
      ▼
    ✅ Filter applied
    ✅ Rows selected/rejected
```

---

## 8. Success Indicators (Phase 9c Completion)

```
✅ FUNCTIONAL INDICATORS:

✓ Query with BETWEEN accepted (no early rejection)
✓ Query with IN accepted (no early rejection)  
✓ BETWEEN predicate evaluates correctly:
    "price BETWEEN 10 AND 100" on [5, 15, 100, 101]
    → [F, T, T, F]
✓ IN predicate evaluates correctly:
    "status IN (1, 2, 3)" on [1, 2, 4, 5]
    → [T, T, F, F]
✓ NULL always returns false:
    "col BETWEEN 1 AND 10" with NULL at row 0
    → [F, ...]
✓ Combined filters work:
    "col > 10 AND col BETWEEN 5 AND 20 AND col IN (7, 15, 25)"
✓ Type mismatches rejected:
    "col BETWEEN 'a' AND 100" ← Planner rejects
✓ All comparison operators still work:
    =, !=, <, >, <=, >= unaffected
✓ IS NULL/IS NOT NULL unaffected

✅ QUALITY INDICATORS:

✓ cargo fmt --all: No changes
✓ cargo clippy: No warnings
✓ cargo check: All pass
✓ Complexity < 25 (cyclomatic & data flow)
✓ Unit test coverage for BETWEEN
✓ Unit test coverage for IN
✓ Integration tests pass
✓ No regression in Phase 9a/9b predicates
```

