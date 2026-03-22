# SILK ROAD: Structured Predicates Discovery (Phase 1)

## Overview

**What:** Replace raw SQL string predicates with structured, typed PredicateExpr objects in the query execution pipeline.

**Why:** Current design (server sends raw SQL strings → worker re-parses) causes fragile string-matching bugs, lacks type safety, and prevents clean operator extensibility.

**Impact:** Eliminates entire class of parsing edge cases (BETWEEN+IN combinations, operator misidentification), improves type safety, enables clean addition of new operators.

---

## Current Architecture (As-Is)

### Data Flow: From SQL to Execution

```
Phase 1: Server-Side Parsing
  SQL: "SELECT * FROM table WHERE id != 1 AND price BETWEEN 50 AND 500"
    ↓
  Parse SQL → Expr tree (sqlparser crate)
    ↓
  Convert Expr to String (query_model.rs:600)
    ↓
  LogicalExpr::Raw { sql: "id != 1 AND price BETWEEN 50 AND 500" }
    ↓
  PhysicalExpr::Raw { sql: "id != 1 AND price BETWEEN 50 AND 500" }

Phase 2: Wire Protocol
  Protobuf Task {
    input: "{\"Filter\": {"predicate\": {\"Raw\": {\"sql\": \"id != 1 AND ...\"}}}}",
    ...
  }

Phase 3: Worker-Side Re-parsing
  Deserialize Task.input → extract "id != 1 AND price BETWEEN 50 AND 500"
    ↓
  split_and_respecting_between()  [BUG: AND-counting logic breaks BETWEEN+IN]
    ↓
  parse_single_clause("id != 1")  [BUG: != operator detection fails]
    ↓
  FilterClause { column, operator, literal_value }
    ↓
  evaluate_filter() → boolean mask
    ↓
  Apply mask to batch rows
```

### Issues with Current Design

| Issue | Impact | Example |
|-------|--------|---------|
| **No Type Safety** | All operators and literals are strings; no compile-time validation | `FilterOp::Ne` is just a string tag; literal "1" could be int, bool, or string |
| **String Parsing Fragility** | Regex-like matching for operators; substring edge cases | Statement 12: `WHERE id != 1` shows error `"id <"` due to operator detection mystery |
| **Complex Combination Breakdown** | Keyword-counting logic breaks with nested operators | Statement 25: `WHERE price BETWEEN 50 AND 500 AND category IN (...)` groups entire input as one clause |
| **No Operator Extensibility** | Adding new operators requires new parsing cases | Adding LIKE, REGEXP, ILIKE, etc. requires more fragile string handling |
| **Double Parsing** | SQL parsed twice (server validation + worker execution) | Wasteful; inconsistency risk if parser implementations diverge |
| **Operator Validation Inconsistency** | Server validates operators one way; worker re-validates differently | Risk of SQL passing server validation but failing at worker |

### Root Cause: Design Decision in Phase 1

From [kionas/src/planner/translate.rs](kionas/src/planner/translate.rs):
> "This phase keeps expression translation simple and deterministic by preserving projection and filter SQL text in typed expression wrappers."

**Phase 1 rationale:** Minimize complexity, serialize as strings (deterministic, simple).

**Consequence:** Worker became responsible for expression evaluation → string parsing → bugs.

---

## Future Architecture (To-Be)

### Data Flow: Structured Predicates

```
Phase 1: Server-Side (Planner)
  SQL: "id != 1 AND price BETWEEN 50 AND 500"
    ↓
  Parse SQL → sqlparser Expr tree
    ↓
  Translate Expr → PredicateExpr {
    Conjunction([
      Comparison {
        column: "id",
        operator: NotEqual,           // enum, not string
        literal: Literal::Int(1),
        column_type: Int32
      },
      Between {
        column: "price",
        lower: Literal::Int(50),
        upper: Literal::Int(500),
        column_type: Int32,
        is_negated: false
      }
    ])
  }
    ↓
  Serialize PredicateExpr to protobuf (new message type)

Phase 2: Wire Protocol
  Protobuf Task {
    filter: Some(Filter {
      predicate: Some(Predicate {
        variant: Some(conjunction_predicate),
      }),
    }),
    ...
  }

Phase 3: Worker-Side (Execution, NO RE-PARSING)
  Deserialize protobuf → PredicateExpr { ... }
    ↓
  evaluate_predicate(predicate, row_batch) {
    match predicate {
      Conjunction(clauses) → AND all clause results
      Comparison { column, operator, literal } → apply operator directly
      Between { column, lower, upper } → check bounds
      In { column, values } → check membership
      ...
    }
  }
    ↓
  Return boolean mask
    ↓
  Apply mask to batch rows
```

### Benefits

| Benefit | Impact |
|---------|--------|
| **Type Safety** | Column types known at planner time; operator compatibility validated server-side |
| **No Re-parsing** | Worker executes PredicateExpr directly; no string matching |
| **Clean Operator Support** | New operators (LIKE, REGEXP, etc.) added as enum variants; no string parsing changes |
| **Clear Separation** | Server = expression validation & translation; Worker = execution only |
| **Extensibility** | User-defined operators, expression optimization passes possible |
| **Performance** | Single parse; no worker-side string operations |

---

## Design Gaps & Blockers

### 1. **Protobuf Message Definition** ❌ [NO EXISTING TYPE]

**Current:** filter_sql sent as raw string

**Needed:**
```protobuf
message FilterPredicate {
  oneof variant {
    Conjunction conjunction = 1;
    Disjunction disjunction = 2;
    Comparison comparison = 3;
    Between between = 4;
    In in = 5;
    IsNull is_null = 6;
    IsNotNull is_not_null = 7;
    Not not = 8;
  }
}

message Conjunction {
  repeated FilterPredicate clauses = 1;
}

message Comparison {
  string column_name = 1;
  ComparisonOperator operator = 2;  // enum: Eq, Ne, Gt, Ge, Lt, Le, etc.
  FilterValue value = 3;
  DataType column_type = 4;
}

message Between {
  string column_name = 1;
  FilterValue lower = 2;
  FilterValue upper = 3;
  DataType column_type = 4;
  bool is_negated = 5;
}

message In {
  string column_name = 1;
  repeated FilterValue values = 2;
  DataType column_type = 3;
}

message FilterValue {
  oneof variant {
    int64 int_value = 1;
    string string_value = 2;
    bool bool_value = 3;
    float float_value = 4;
    // Add more types as needed: decimal, date, timestamp, etc.
  }
}

enum ComparisonOperator {
  EQUAL = 0;
  NOT_EQUAL = 1;
  GREATER_THAN = 2;
  GREATER_EQUAL = 3;
  LESS_THAN = 4;
  LESS_EQUAL = 5;
  // Add more as needed
}

enum DataType {
  UNKNOWN = 0;
  INT32 = 1;
  INT64 = 2;
  FLOAT = 3;
  DOUBLE = 4;
  STRING = 5;
  BOOL = 6;
  DATE = 7;
  TIMESTAMP = 8;
  // ... more types
}
```

### 2. **Server-Side Translation** ❌ [INCOMPLETE]

**Current:** [kionas/src/planner/translate.rs](kionas/src/planner/translate.rs) converts Expr → String

**Needed:** 
- New function: `translate_expr_to_predicate_expr(Expr) → PredicateExpr`
- Walks sqlparser Expr tree
- Builds typed PredicateExpr with proper operator mapping
- Validates column types against schema_metadata (already threaded)
- Returns descriptive errors for unsupported operators

### 3. **Operator Mapping** ❌ [NOT DEFINED]

**Current:** String operators: "!=", ">=", "=", etc.

**Needed:** Clear mapping from sqlparser.rs BinaryOp → PredicateExpr ComparisonOperator

| sqlparser BinaryOp | PredicateExpr ComparisonOperator | Notes |
|---|---|---|
| `Not(Box(Eq(...)))` → "!=" | `NotEqual` | Composed operator |
| `And` | N/A (handled by Conjunction) | |
| `Or` | N/A (handled by Disjunction) | |
| `Between` | N/A (separate Between predicate) | |
| `In` | N/A (separate In predicate) | |
| `IsNull` | N/A (separate IsNull predicate) | |
| `IsNotNull` | N/A (separate IsNotNull predicate) | |

### 4. **Worker-Side Execution** ❌ [STRING PARSING TODAY]

**Current:** [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) has `split_and_respecting_between()`, `parse_filter_clauses()`

**Needed:**
- Deserialize PredicateExpr from protobuf
- Pattern-match on PredicateExpr variant
- Execute typed predicates directly (no string parsing)
- Example: `Comparison { column, operator: NotEqual, value: 1, type: Int32 }` → direct `col != 1` evaluation

### 5. **Type Coercion & Validation** ❌ [PARTIALLY EXISTS]

**Current:** Phase 9b added type validation; Phase 9c enhanced it

**Needed:** Phase 9b type validation moved to Expr → PredicateExpr translation (server-side)
- Column type lookup via schema_metadata
- Operator compatibility checking (ensure Int != Int, not Int != String)
- Literal type coercion (ensure "1" becomes Literal::Int(1), not Literal::String("1"))
- Error messages should be clear and actionable

### 6. **Backward Compatibility** ⚠️ [MIGRATION PATH UNKNOWN]

**Question:** How to migrate during implementation?
- Old systems already deployed using raw SQL filters
- New systems need to send PredicateExpr

**Options:**
- Keep raw SQL parsing + add PredicateExpr in parallel (deprecation period)
- Do big bang refactoring (risky, fast)
- Feature flag to switch between old/new (complex)

---

## Scope: What This Road Covers

✅ **Included:**
- Protobuf message design for structured predicates
- Server-side Expr → PredicateExpr translation
- Worker-side PredicateExpr deserialization & execution
- Type coercion at translation time (server-side)
- Support for: =, !=, >, >=, <, <=, BETWEEN, NOT BETWEEN, IN, IS NULL, IS NOT NULL, AND, OR, NOT

❌ **Out of Scope:**
- User-defined operators
- Expression optimization passes (constant folding, predicate pushdown)
- Subqueries in predicates
- Function calls in filters (LIKE, REGEXP, UPPER(col), etc.)
- Scalar functions (deferred to Phase 10+)

---

## Known Dependencies

**Depends On:**
- Phase 9b: Type coercion foundation (already complete)
- Phase 9c: BETWEEN/IN predicate support via new parsing (current)
- Schema metadata threading (already in place from Phase 9b)

**Blocks:**
- New operator additions (current method requires new string parsing cases)
- Complex filter combinations (nested AND/OR with mixed operators)
- Performance optimization on filters

---

## High-Level Breakdown into Plans

### Plan 1: Protobuf & Message Design
- Files: [kionas/proto/worker_service.proto](kionas/proto/worker_service.proto) (enchance task message)
- Effort: 2-3 hours
- Output: Compiled proto files with Rust bindings

### Plan 2: Server-Side Translation (Expr → PredicateExpr)
- Files: [kionas/src/planner/predicate_translator.rs](kionas/src/planner/predicate_translator.rs) (new)
- Effort: 4-5 hours
- Output: Full Expr → PredicateExpr conversion with type checking

### Plan 3: Worker-Side Execution (PredicateExpr → Boolean Mask)
- Files: [worker/src/services/predicate_executor.rs](worker/src/services/predicate_executor.rs) (new)
- Effort: 3-4 hours
- Output: Pattern-matched PredicateExpr evaluation (no string parsing)

### Plan 4: Integration & Migration
- Files: [server/src/execution.rs](server/src/execution.rs), [worker/src/execution/planner.rs](worker/src/execution/planner.rs)
- Effort: 2-3 hours
- Output: Plumb PredicateExpr through full pipeline; deprecate raw SQL filters

### Plan 5: Testing & Validation
- Files: Unit tests (predicate_translator.rs, predicate_executor.rs), integration tests
- Effort: 2-3 hours
- Output: Comprehensive tests for all operator combinations
- DONT EXECUTE cargo test!!!


**Total Estimated Effort:** 13–18 hours

---

## Open Questions

1. **Type System:** Should we reuse DataType enum from existing codebase, or create a new FilterValueType?
- Reuse existing DataType for consistency; FilterValue can be a wrapper around it with actual value.
2. **Null Handling:** How are NULL values represented in PredicateExpr (separate IsNull predicate vs. nullable value)?
3. **User-Defined Functions:** Should the design account for future support of UPPER(col) = 'VALUE', or keep filters as simple comparisons only?
- Leave a hook for future function support, but defer actual implementation to Phase 10+.
4. **Error Messages:** Should PredicateExpr include source location info for better error reporting?
- Could be added as optional fields (line, column) in protobuf; useful for debugging but adds complexity.
5. **Versioning:** How to handle proto updates when new operators are added?
- Update affected services! no backward compatibility needed for new operators. Things need to move forward together.
---

## Evidence & References

- [Current Filter Parsing Bug Analysis](../../discover/e2e_tests_after_phase9.md#critical-failures-25)
- [Worker Service Proto](kionas/proto/worker_service.proto)
- [Server Planner Translate](kionas/src/planner/translate.rs#L157)
- [Phase 9b Type Coercion](roadmaps/ROADMAP2/discovery/discovery-phase9b.md)
- [Current Query Execution](worker/src/services/query_execution.rs#L373-L475)

---

## Next Steps

1. Review this discovery document with team
2. Decide scope refinements (is Function support needed? Null semantics?)
3. Create detailed plan documents for each of the 5 phases above
4. Establish priority: proceed immediately after Phase 9c, or defer to Phase 10?
