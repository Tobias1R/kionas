# Phase 4D Research: JOIN and Predicate Implementation Analysis

**Date**: March 27, 2026  
**Scope**: Comprehensive exploration of JOIN and predicate implementation across query model, planner, and worker execution  
**Objective**: Inform Phase 4D roadmap with precise gap analysis, architectural patterns, and implementation requirements

---

## EXECUTIVE SUMMARY

### Current State
- **JOINs**: Only INNER equi-joins implemented (hash join on simple column references)
- **Predicates**: Simple scalar comparisons supported (=, !=, <, >, <=, >=, NOT NULL, NULL, BETWEEN, IN)
- **Filter Logic**: Basic AND conjunction only; no OR, NOT, complex Boolean nesting
- **Architecture**: Layered query model → logical plan → physical plan → worker execution

### Critical Gaps (Phase 4D Targets)
1. **Non-Equi JOINs** (theta-join, cross-join with predicates)
2. **Complex Predicates** (OR/NOT nesting, aggregate predicates)
3. **OUTER JOINs** (LEFT/RIGHT/FULL with NULL-fill)
4. **Expression-Based JOIN Keys** (computed join conditions)
5. **Predicate Pushdown Optimization** (stage-local filtering)

---

## 1. CURRENT JOIN IMPLEMENTATION

### 1.1 JOIN Detection in Query Model

**File**: [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs) (Lines 61-100, 1105-1189)

#### Data Structures
```rust
pub enum QueryJoinType {
    Inner,  // Only type currently supported
}

pub struct QueryJoinKey {
    pub left: String,
    pub right: String,
}

pub struct QueryJoinSpec {
    pub join_type: QueryJoinType,
    pub right: QueryNamespace,
    pub keys: Vec<QueryJoinKey>,
}

pub struct SelectQueryModel {
    pub joins: Vec<QueryJoinSpec>,  // Flattened list of all JOINs
    // ... other fields
}
```

#### JOIN Key Extraction (`parse_join_keys`, line 1117)
```rust
fn parse_join_keys(expr: &Expr, out: &mut Vec<QueryJoinKey>) -> Result<(), QueryModelError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                parse_join_keys(left, out)?;
                parse_join_keys(right, out)?;
                Ok(())
            }
            BinaryOperator::Eq => {
                let left_key = parse_join_column_ref(left)?;
                let right_key = parse_join_column_ref(right)?;
                out.push(QueryJoinKey {
                    left: left_key,
                    right: right_key,
                });
                Ok(())
            }
            _ => Err(QueryModelError::UnsupportedJoinExpression(expr.to_string())),
        },
        _ => Err(QueryModelError::UnsupportedJoinExpression(expr.to_string())),
    }
}
```

**Key Constraint**: Only `BinaryOperator::Eq` accepted; any other operator (>, <, >=, <=, etc.) rejected.

#### JOIN Type Detection (`parse_join_specs`, line 1140)
```rust
match &join.join_operator {
    JoinOperator::Inner(constraint) => (QueryJoinType::Inner, constraint),
    JoinOperator::Join(constraint) => (QueryJoinType::Inner, constraint),
    _ => Err(QueryModelError::UnsupportedJoinType("non-inner".to_string())),  // LEFT/RIGHT/FULL rejected
}
```

#### JOIN Constraint Validation (line 1165)
```rust
match constraint {
    JoinConstraint::On(expr) => expr,
    _ => return Err(QueryModelError::UnsupportedJoinConstraint),  // USING, NATURAL rejected
}
```

### 1.2 JOIN Translation to Logical Plan

**File**: [kionas/src/planner/join_spec.rs](kionas/src/planner/join_spec.rs) (Lines 1-70)

#### Logical JOIN Spec
```rust
pub enum JoinType {
    Inner,  // Only variant supported
}

pub struct LogicalJoinSpec {
    pub join_type: JoinType,
    pub right_relation: LogicalRelation,
    pub keys: Vec<JoinKeyPair>,  // Copied from query model
}
```

**Translation**: Direct mapping from `QueryJoinSpec` → `LogicalJoinSpec` (no transformation)

### 1.3 Physical JOIN Execution

**File**: [worker/src/execution/join.rs](worker/src/execution/join.rs) (Lines 1-400+)

#### Physical JOIN Spec
```rust
pub struct PhysicalJoinSpec {
    pub join_type: JoinType,
    pub right_relation: LogicalRelation,
    pub keys: Vec<JoinKeyPair>,
}
```

#### Hash JOIN Implementation (`apply_hash_join_pipeline`, line 29)
```
Algorithm:
1. Concatenate all right batches into single table
2. Build HashMap: join_key → Vec<right_row_indices>
3. For each left row:
   - Compute join key
   - Look up in HashMap
   - For each matching right row: emit (left_idx, right_idx) pair
4. Batch fetch using Arrow::take() operator
5. Output schema: all left columns + all right columns (duplicates prefixed with table name)
```

**Constraints**:
- Requires at least one join key: `if spec.keys.is_empty() { return Err(...) }`
- Right relation metadata must be non-empty (database/schema/table)
- No predicate support beyond equality on join keys
- Single hash-join operator per pipeline (sequential for multi-join queries)

#### Test Coverage: [worker/src/tests/execution_join_tests.rs](worker/src/tests/execution_join_tests.rs)
- ✅ Deterministic match ordering
- ✅ Duplicate column renaming
- ✅ Empty result handling
- ✅ Spec validation (empty keys, empty relation)
- ❌ No tests for non-equi predicates, outer joins, or expression keys

### 1.4 JOIN Planning in Server

**File**: [server/src/planner/engine.rs](server/src/planner/engine.rs) (Lines 400-1400)

#### JOIN Family Detection (`detect_join_families`, line 912)
```rust
fn detect_join_families(node_names: &[String]) -> (bool, bool, bool) {
    let has_hash_join = node_names.iter()
        .any(|name| name.to_ascii_lowercase().contains("hashjoin"));
    let has_sort_merge_join = node_names.iter()
        .any(|name| name.to_ascii_lowercase().contains("sortmergejoin"));
    let has_nested_loop_join = node_names.iter()
        .any(|name| name.to_ascii_lowercase().contains("nestedloopjoin"));
    (has_hash_join, has_sort_merge_join, has_nested_loop_join)
}
```

**Purpose**: Detects DataFusion execution plan node types; used for capability checking (line 1329-1335):
```rust
if has_sort_merge_join || has_nested_loop_join {
    return Err(PlannerError::UnsupportedPhysicalOperator(
        "sort merge join not supported in this phase".to_string(),
    ));
}
```

#### JOIN Translation to Physical Plan (line 1382-1395)
```rust
if has_hash_join {
    let Some(join_spec) = translation_joins.first() else {
        return Err(PlannerError::InvalidPhysicalPipeline(
            "DataFusion join node detected but query model contains no join spec".to_string(),
        ));
    };
    operators.push(PhysicalOperator::HashJoin {
        spec: PhysicalJoinSpec {
            join_type: join_spec.join_type.clone(),
            right_relation: join_spec.right.clone(),
            keys: join_spec.keys.clone(),
        },
    });
}
```

**Key Functions**:
- `collect_translation_joins()` (line 445): Deduplicates joins across CTE chain
- `joins_equivalent()` (line 421): Compares join specs for deduplication
- `remap_join_key_with_cte_alias()` (line 843): Handles CTE projection aliasing

---

## 2. CURRENT PREDICATE/FILTER IMPLEMENTATION

### 2.1 Predicate Expression Model

**File**: [kionas/src/planner/predicate_expr.rs](kionas/src/planner/predicate_expr.rs) (Lines 1-500)

#### Predicate Expression Tree
```rust
pub enum PredicateExpr {
    Conjunction {
        clauses: Vec<PredicateExpr>,  // AND-only, no OR
    },
    Comparison {
        column: String,
        op: PredicateComparisonOp,      // Eq, Ne, Gt, Ge, Lt, Le
        value: PredicateValue,
    },
    Between {
        column: String,
        lower: PredicateValue,
        upper: PredicateValue,
        negated: bool,
    },
    InList {
        column: String,
        values: PredicateValue,  // Must be homogeneous type list
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    // NOT, OR, complex Boolean expressions: ❌ NOT SUPPORTED
}

pub enum PredicateComparisonOp {
    Eq, Ne, Gt, Ge, Lt, Le,
}

pub enum PredicateValue {
    Int(i64),
    Bool(bool),
    Str(String),
    IntList(Vec<i64>),
    BoolList(Vec<bool>),
    StrList(Vec<String>),
}
```

### 2.2 Predicate Parsing

#### SQL Parsing (`parse_predicate_sql`, line 270)

**Algorithm**:
1. Split by top-level AND (respecting BETWEEN...AND):
   ```rust
   fn split_and_respecting_between(filter_sql: &str) -> Vec<String>
   ```
   - Builds map of "BETWEEN...AND" positions to skip them when splitting
   - Returns Vec of individual predicates
   
2. For each clause, parse single operator:
   - `IS NULL / IS NOT NULL / NOT BETWEEN / BETWEEN / IN / =, !=, <, >, <=, >=`
   - Error on unrecognized pattern
   
3. Combine into Conjunction if multiple clauses:
   ```rust
   if nodes.len() == 1 {
       Ok(nodes.remove(0))
   } else {
       Ok(PredicateExpr::Conjunction { clauses: nodes })
   }
   ```

**Supported Literal Types**:
- Integers: `123`, `-456`
- Booleans: `true`, `false` (case-insensitive)
- Strings: `'quoted'` (inner quotes handled)
- Lists: `(val1, val2, ...)` for IN clauses

**Limitations** (see line 1495-1590):
- Single pass left-to-right parsing (no recursive descent)
- No function calls within predicates
- No type coercion (strict matching)
- No regex/pattern matching

### 2.3 Type Checking

**File**: [kionas/src/planner/filter_type_checker.rs](kionas/src/planner/filter_type_checker.rs)

#### Validation (`check_filter_predicate_types`, line 76)
```rust
pub fn check_filter_predicate_types(
    predicate: &PhysicalExpr,
    column_types: &HashMap<String, ColumnDatatypeSpec>,
) -> Result<(), PlannerError>
```

**Current Checks**:
- Column existence in schema
- IS NULL/IS NOT NULL patterns recognized
- NOT(...IS NULL) pattern recognized
- BETWEEN clause validation (column exists)
- IN clause validation (column exists)

**Deferred** (Phase 9b Step 6):
- Operand type compatibility with column type
- Implicit type coercions
- Function result types

### 2.4 Worker-Side Filter Application

**File**: [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) + [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs)

#### Filter Pipeline (`apply_filter_predicate_pipeline`, line 1367)
```rust
pub fn apply_filter_predicate_pipeline(
    batches: &[RecordBatch],
    predicate: &PredicateExpr,
    scan_hints: Option<&str>,
) -> Result<Vec<RecordBatch>, String>
```

**Algorithm**:
1. For each record batch:
   - Evaluate predicate on all rows
   - Build BooleanArray mask
   - Use Arrow's `filter_record_batch(&batch, &mask)`
   - Keep only true rows

2. Predicate Evaluation Tree:
   ```rust
   fn evaluate_predicate_expression(
       predicate: &PredicateExpr,
       batch: &RecordBatch,
   ) -> Result<BooleanArray, String>
   ```
   - Recursively evaluate clauses
   - Combine with AND logic
   - Short-circuit on null comparisons

#### Supported Backend Operations:
- Column reference lookup (by name, case-insensitive)
- Arrow array downcasting (Int64, String, Boolean, etc.)
- Scalar comparison (eq, ne, gt, ge, lt, le)
- NULL checks
- List membership testing
- BETWEEN range testing

### 2.5 Predicate Optimization (or Lack Thereof)

**Current State**: No predicate pushdown, predicate placement optimization, or early filtering.

**Timeline**:
1. Table scan loads all data
2. Filter applied after scan (late evaluation)
3. Join operates on filtered data
4. Aggregation/projection applied after join

**Impact**: High data transfer costs in multi-stage queries.

---

## 3. EXPRESSION EVALUATOR ARCHITECTURE

### 3.1 Current Expression Types

**File**: [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs)

```rust
pub enum PhysicalExpr {
    ColumnRef { name: String },      // Simple column reference: "orders_id"
    Raw { sql: String },             // Unparsed SQL: "COUNT(id)", "t1.id + 1"
    Predicate { predicate: PredicateExpr },  // Structured BUT not full expression
}
```

**Limitations**:
- No arithmetic operators (+, -, *, /)
- No function calls (SUM, COUNT, UPPER, LOWER, etc.)
- No type coercion
- Raw SQL fallback exists but not systematically evaluable

### 3.2 Window Functions (Partial Support)

**Files**: [server/src/planner/engine.rs](server/src/planner/engine.rs#L690), [worker/src/execution/window.rs](worker/src/execution/window.rs)

#### Planner Support:
- ✅ DataFusion physical plan detection
- ✅ Window frame spec parsing (`ROWS`, `RANGE`, bounds)
- ✅ Partition/order-by extraction

#### Worker Support:
- ⚠️ Basic frame computation present
- ❌ No ranking function execution
- ❌ No aggregate within window

---

## 4. DOCUMENTED GAPS & LIMITATIONS

### 4.1 Critical Gaps (Phase 4D Target)

From [roadmaps/discover/WHATS_NEXT.md](roadmaps/discover/WHATS_NEXT.md#4-gaps--constraints-what's-missing):

| Gap | Current | Required | Priority |
|-----|---------|----------|----------|
| **Non-Equi JOINs** | Only hash equi-join; no theta-join, cross-join | Implement nested-loop join; extend planner predicate routing | **CRITICAL** |
| **Complex Predicates** | Simple column comparisons; no AND/OR/NOT nesting | Extend expression evaluator for full Boolean logic | **CRITICAL** |
| **JOIN Key Expression Support** | Keys must be simple column refs; no ON expressions | Compute key expressions at runtime | High |
| **Predicate Pushdown** | No filtering across stages; late evaluation | Implement stage-local filter pushdown | High |
| **OUTER JOINs** | Hash join lacks outer semantics | Implement outer hash join + null-fill | High |
| **Scalar Functions** | Expression evaluator limited | Support function calls, type coercion | Medium |

### 4.2 Error Taxonomy

**JOIN Errors**:
- `UnsupportedJoin`: Query model extraction failed
- `UnsupportedJoinType`: Non-inner join (LEFT/RIGHT/FULL/CROSS)
- `UnsupportedJoinConstraint`: USING or NATURAL clause
- `UnsupportedJoinExpression`: Non-equi condition, function in key

**Predicate Errors**:
- `UnsupportedPredicate`: Unrecognized operator or syntax
- `TypeCoercionViolation`: Column type mismatch

---

## 5. ARCHITECTURAL PATTERNS & CODE ORGANIZATION

### 5.1 Data Flow

```
CLIENT SQL
    ↓
kionas/sql/query_model.rs
    ├─ parse_query() → SqlParser AST
    ├─ extract_select_query_model() → SelectQueryModel
    │   ├─ parse_join_specs() → Vec<QueryJoinSpec>
    │   ├─ from_clause() → QueryFromSpec
    │   └─ where_clause() → Option<String> (raw SQL)
    ↓
server/planner/engine.rs
    ├─ translate_to_kionas_plan() → PhysicalPlan
    │   ├─ derive_intent_from_exec_plan() → PlannerIntentFlags
    │   ├─ collect_translation_joins() → Vec<QueryJoinSpec>
    │   └─ build_kionas_plan_from_intent() → PhysicalPlan
    │       ├─ HashJoin { spec: PhysicalJoinSpec }
    │       └─ Filter { predicate: PhysicalExpr::Predicate(...) }
    ↓
worker/execution/pipeline.rs
    ├─ execute_query_task() → Vec<RecordBatch>
    ├─ apply_hash_join_pipeline() → Vec<RecordBatch>
    └─ apply_filter_predicate_pipeline() → Vec<RecordBatch>
    ↓
RESULT BATCHES
```

### 5.2 File Organization

| Module | Responsibility |
|--------|------------------|
| **kionas/src/sql/query_model.rs** | AST → QueryModel + JOIN/predicate extraction |
| **kionas/src/planner/join_spec.rs** | JOIN spec definitions (Query/Logical/Physical) |
| **kionas/src/planner/predicate_expr.rs** | Predicate tree definition + parsing |
| **kionas/src/planner/filter_type_checker.rs** | Type validation for predicates |
| **kionas/src/planner/physical_plan.rs** | PhysicalOperator enum (HashJoin, Filter, etc.) |
| **kionas/src/planner/physical_translate.rs** | Logical → Physical translation |
| **server/src/planner/engine.rs** | Server-side planning orchestration |
| **worker/src/execution/join.rs** | Hash join algorithm + utilities |
| **worker/src/execution/pipeline.rs** | Operator execution orchestration |
| **worker/src/services/query_execution.rs** | Filter evaluation, aggregation, etc. |

---

## 6. BLOCKERS & ANTI-PATTERNS

### 6.1 Design Constraints (Intentional)

1. **Equi-Join Only**: Hash-based algorithm fundamentally limits to equality predicates
   - Workaround for non-equi: Nested-loop join (O(n·m) complexity)
   - Optimization: Implement both; planner chooses based on selectivity

2. **AND-Only Conjunction**: Parser resists OR/NOT due to complexity
   - Risk: Incorrect evaluation in complex Boolean expressions
   - Path: Extend predicate_expr.rs with full Boolean algebra

3. **Raw SQL String Fallback**: LIMIT, complex expressions stored as SQL strings
   - Risk: Late binding, possible parsing failures at worker
   - Benefit: Flexibility during development; enables graceful degradation

4. **Late Predicate Evaluation**: Filter applied after scan
   - Risk: High data transfer in multi-stage queries
   - Opportunity: Implement pushdown in Phase 4D

### 6.2 Known Limitations (Documented Comments)

1. **worker/src/execution/join.rs:27** (Details section)
   > Current slice supports only INNER joins with one or more equi-key pairs.

2. **kionas/src/planner/predicate_expr.rs** (NO explicit limitation comment)
   > (Implied) Only AND conjunction + simple predicates

3. **server/src/planner/engine.rs:1329-1335**
   > `has_sort_merge_join || has_nested_loop_join` → UnsupportedPhysicalOperator error

---

## 7. IMPLEMENTATION REQUIREMENTS FOR PHASE 4D

### 7.1 Non-Equi JOIN Support

#### Stage 1: Query Model Extension (Lines affected: query_model.rs)

**Change**: Accept non-equi predicates in ON clause
```rust
// Current
BinaryOperator::Eq => { ... push key ... }
_ => Err(UnsupportedJoinExpression(...))

// Target
BinaryOperator::Eq | BinaryOperator::Gt | BinaryOperator::Lt | ... => {
    // Store full condition, not just keys
    keys.push(QueryJoinKey{...})  // For equi
    predicate.push(NonEquiCondition{...})  // For non-equi
}
```

**Impact**: Requires new QueryJoinSpec variant or extended struct

#### Stage 2: Planner Extension (engine.rs)

**Current**: Detects HashJoin → immediately creates HashJoin operator
**Target**: Detect join type (#rows, selectivity) → choose HashJoin vs NestedLoopJoin

**New Operator**: PhysicalOperator::NestedLoopJoin { predicate, right_relation }

#### Stage 3: Worker Execution (pipeline.rs)

**New Function**: `apply_nested_loop_join_pipeline()`
```
For each left row:
    For each right row:
        Evaluate predicate(left_row, right_row)
        If true: emit Cartesian product row
```

**Cost**: O(n·m) for full Cartesian; optimizations needed (e.g., index pruning)

### 7.2 Complex Predicate Support

#### Stage 1: Expression Tree Extension

**New Variants** in PredicateExpr:
```rust
Disjunction {      // OR
    clauses: Vec<PredicateExpr>,
},
Negation {         // NOT
    inner: Box<PredicateExpr>,
},
```

#### Stage 2: Parsing

**New function**: `parse_predicate_sql_recursive()` using LL(1) parser
- Handle operator precedence: NOT > AND > OR
- Build recursive tree instead of flat AND list

#### Stage 3: Evaluation

**New function**: `evaluate_boolean_tree()` with proper short-circuiting
- AND: short-circuit on first false
- OR: short-circuit on first true
- NOT: boolean negation

### 7.3 OUTER JOIN Support

#### Stage 1: Query Model (query_model.rs)

```rust
pub enum QueryJoinType {
    Inner,
    Left,
    Right,
    Full,
}
```

**Change detection**: Line 1150-1160
```rust
JoinOperator::LeftOuter(constraint) => QueryJoinType::Left,
JoinOperator::RightOuter(constraint) => QueryJoinType::Right,
JoinOperator::FullOuter(constraint) => QueryJoinType::Full,
```

#### Stage 2: Physical Execution

**New Function**: `apply_outer_hash_join_pipeline()`
```
1. Build right index (same as inner)
2. For each left row:
      If match found: emit matched rows (same as inner)
      If no match found: emit (left_row, NULLs...) ← outer difference
3. For LEFT: done
   For FULL: also iterate right rows without left matches, emit (NULLs..., right_row)
```

**Schema Impact**: Output columns must be nullable (LEFT: right cols; RIGHT: left cols; FULL: both)

### 7.4 Predicate Pushdown

#### Implementation Points

1. **Stage boundary detection**: Identify filter opportunities pre-scan
2. **Filter placement**: Insert Filter operator before/after stages
3. **Selectivity estimation**: Determine pushdown benefit
4. **Type checking**: Ensure filter column available at pushdown point

---

## 8. TEST COVERAGE ANALYSIS

### 8.1 Current Test Files

| File | Coverage | Gaps |
|------|----------|------|
| [worker/src/tests/execution_join_tests.rs](worker/src/tests/execution_join_tests.rs) | ✅ Equi-join determinism, schema, empty results | ❌ Non-equi, outer, expressions |
| [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs#L1300+) | ✅ Basic SELECT, WHERE, ORDER BY | ❌ OUTER JOINs, non-equi conditions |
| [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs#L1500+) | ✅ BETWEEN, IN, comparisons | ❌ OR/NOT nesting, complex expressions |

### 8.2 Test Patterns

**Query Model Tests** (query_model.rs:1300+):
```rust
#[test]
fn parses_inner_join_with_single_key() {
    let statements = parse_query("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
    // Verifies: joins[0].join_type == Inner, keys[0] == ("t1.id", "t2.id")
}

#[test]
fn rejects_left_outer_join() {
    let result = parse_query("SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id");
    // Verifies: UnsupportedJoinType error
}
```

---

## 9. SPECIFIC CODE SECTIONS FOR EXTENSION

### 9.1 Critical Extraction Points

**JOIN Detection**:
- [kionas/src/sql/query_model.rs:1140-1189](kionas/src/sql/query_model.rs#L1140-L1189): `parse_join_specs()` - Entry point for non-equi support
- [kionas/src/sql/query_model.rs:1105-1138](kionas/src/sql/query_model.rs#L1105-L1138): `parse_join_keys()` - Constraint parsing; add non-equi branches here

**Predicate Extraction**:
- [kionas/src/sql/query_model.rs:600-700](kionas/src/sql/query_model.rs#L600-L700): WHERE clause extraction; currently returns raw SQL string
- [kionas/src/planner/predicate_expr.rs:270-320](kionas/src/planner/predicate_expr.rs#L270-L320): `parse_predicate_sql()` - Main parsing function; needs recursive descent for OR/NOT

**Planner Translation**:
- [server/src/planner/engine.rs:1380-1400](server/src/planner/engine.rs#L1380-L1400): JOIN → PhysicalOperator conversion; add NestedLoopJoin case

**Worker Execution**:
- [worker/src/execution/pipeline.rs:1360-1380](worker/src/execution/pipeline.rs#L1360-L1380): Filter application orchestration
- [worker/src/execution/join.rs:29-116](worker/src/execution/join.rs#L29-L116): Hash join pipeline; model for nested-loop implementation

### 9.2 Error Handling Points

**Query Model** (query_model.rs:237-241):
```rust
pub enum QueryModelError {
    UnsupportedJoin,
    UnsupportedJoinType(String),
    UnsupportedJoinConstraint,
    UnsupportedJoinExpression(String),
    MissingJoinKeys,
}
```

**Add for Phase 4D**:
- `UnsupportedPredicate(String)` - Already exists (line 247)
- `NonEquiJoinInEquiMode` - New error for unsupported non-equi in fallback path
- `OuterJoinNotYetSupported` - Temporary blocker

---

## 10. RECOMMENDATIONS FOR PHASE 4D PLANNING

### 10.1 Execution Stages (Suggested Timeline)

**Stage 1: Complex Predicates (3-4 days)**
- Priority: Unblock WHERE clause complexity
- Simplest: Extend AND-only to full OR/NOT Boolean algebra
- Tests: 2-3 test cases per operator combination
- Files: predicate_expr.rs, pipeline.rs

**Stage 2: Non-Equi JOINs (4-5 days)**
- Priority: Enable analytical queries with inequality conditions
- Complexity: Requires nested-loop join algorithm + predicate evaluation in join context
- Tests: Hash vs nested-loop decision logic, predicate correctness
- Files: query_model.rs, engine.rs, join.rs, pipeline.rs

**Stage 3: OUTER JOINs (2-3 days)**
- Priority: Data completeness
- Complexity: NULL-fill logic, nullable schema handling
- Tests: LEFT, RIGHT, FULL semantic correctness
- Files: query_model.rs, join.rs, pipeline.rs

**Stage 4: Expression-Based Keys & Pushdown (5-7 days)**
- Priority: Query optimization
- Complexity: Expression evaluator extension, selectivity estimation
- Tests: Filter placement correctness, cost estimation validation
- Files: engine.rs, predicate_expr.rs, pipeline.rs

### 10.2 Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| **OR/NOT precedence bugs** | High | Comprehensive predicate tree tests; fuzzing |
| **Nested-loop cardinality explosion** | High | Early selectivity check; cost-based join selection |
| **NULL-fill schema mismatch** | Medium | Strict null-check at operator construction |
| **Predicate pushdown safety** | Medium | Rigorous data-flow analysis; conservative initial placement |
| **Worker compatibility** | Low | Backward-compatible payload versioning |

### 10.3 Quick Wins (Blockers to Remove)

1. **AND-only to full Boolean**: Low effort, high impact
2. **Error message clarity**: Add remediation hints (already started in filter_type_checker.rs)
3. **Test coverage**: Expand existing test suite for documented limitations

---

## 11. APPENDIX: KEY FILE LOCATIONS

### Core Files (Query Model → Execution)
- [kionas/src/sql/query_model.rs](kionas/src/sql/query_model.rs) - 1800 lines
- [kionas/src/planner/join_spec.rs](kionas/src/planner/join_spec.rs) - 70 lines
- [kionas/src/planner/predicate_expr.rs](kionas/src/planner/predicate_expr.rs) - 500 lines
- [kionas/src/planner/physical_plan.rs](kionas/src/planner/physical_plan.rs) - 300 lines
- [kionas/src/planner/filter_type_checker.rs](kionas/src/planner/filter_type_checker.rs) - 500 lines
- [server/src/planner/engine.rs](server/src/planner/engine.rs) - 2000 lines
- [worker/src/execution/join.rs](worker/src/execution/join.rs) - 400 lines
- [worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs) - 2500 lines
- [worker/src/services/query_execution.rs](worker/src/services/query_execution.rs) - 2000 lines

### Test Files
- [worker/src/tests/execution_join_tests.rs](worker/src/tests/execution_join_tests.rs) - 140 lines
- [kionas/src/sql/query_model.rs (tests section)](kionas/src/sql/query_model.rs#L1300+) - 500+ lines

### Documentation
- [roadmaps/discover/WHATS_NEXT.md](roadmaps/discover/WHATS_NEXT.md) - Phase 4D roadmap
- [docs/ARCHITECTURE_STAGE_EXTRACTION_ROUTING.md](docs/ARCHITECTURE_STAGE_EXTRACTION_ROUTING.md) - Distributed execution architecture

---

**End of Research Report**
