# Mirante Path: Discovery Investigation Plan

**Objective**: Systematically investigate 14 feature areas identified in [mirante.md](../SILK_ROAD/mirante.md) to understand implementation tradeoffs, blockers, dependencies, and success criteria. This discovery phase informs implementation sequencing and scope decisions.

**Current Status Assessment**:
- ✅ **Complete** (5): INSERT, Multi-node execution, Aggregates (partial), Arrow batching, Aliases
- 🟡 **Partial** (5): UPDATE/DELETE, Joins, Runtime optimizations, Observability, Janitor
- ❌ **Not Started** (4): UDFs, Window functions, CTEs/Subqueries, Ingestion/Connectors

---

## Feature Investigation Index

### ✅ Complete Features (Validate & Expand)

---

## 1. INSERT Statements (VALUES Only)

**Current State**: Fully implemented with VALUES clause support, NOT NULL constraint validation, Arrow batch transformation.

**Status**: ✅ Complete (VALUES only)

**Evidence**:
- [INSERT implementation](../../worker/src/transactions/maestro.rs#L1013) — Master execution logic
- [Arrow batch transformation](../../worker/src/transactions/maestro.rs#L385) — Temporal/decimal type hints
- [DML handler](../../server/src/statement_handler/dml/insert.rs) — Server-side INSERT logic

**Current Capabilities**:
- Simple INSERT VALUES statements
- Multi-row VALUES support
- NOT NULL constraint validation
- Type coercion (temporal types, decimals)
- Distributed execution to workers
- Transactional consistency (2-phase commit via maestro)

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | Low effort: Bounded scope (VALUES only), straightforward batching |
| **Performance vs Generality** | Optimized for VALUES; scalability to large batches tested in EP phases |
| **Rollout Strategy** | Incremental: VALUES implemented; SELECT-based INSERT, prepared statements deferred |
| **Architecture Alignment** | Tight integration with maestro 2-phase commit; worker exchange artifacts |

**Key Constraints**:
- VALUES-only; SELECT-based INSERT (INSERT INTO ... SELECT) deferred
- No prepared statements or parameterized queries
- Temporal/decimal type handling hardcoded (not generic type coercion framework)

**Investigation Questions**:
1. **Completeness**: Are there edge cases or data types not covered by current VALUES support (e.g., NULL handling, nested queries, expressions)?
2. **Performance**: What is throughput under bulk insert scenarios (e.g., 1M rows)? Are there worker-side batching bottlenecks?
3. **SELECT-based INSERT**: When should INSERT...SELECT be prioritized? Does it require full subquery support first?
4. **Prepared Statements**: Would prepared/parameterized queries significantly improve security or performance? Is this a Phase 13 candidate?

**Dependency Analysis**:
- ✅ Unblocked (no dependencies)
- 🔓 Enables: UPDATE/DELETE (shares DML execution patterns), Transactions improvement
- 📦 Blocked by: None

**Recommended Next Steps**:
- **Validation**: Add test coverage for edge cases (NULL handling, type boundaries, bulk scenarios)
- **Documentation**: Capture performance characteristics and scalability limits
- **Roadmap**: Defer SELECT-based INSERT and prepared statements to Phase 13+

---

## 2. Multi-Node Worker Execution

**Current State**: Fully implemented with stage-based distributed planning, topological scheduling, exchange artifacts.

**Status**: ✅ Complete

**Evidence**:
- [Distributed planner](../../kionas/src/planner/distributed_plan.rs) — Stage-based planning, topological order
- [Worker coordination](../../worker/src/execution/pipeline.rs) — Exchange artifacts, stage pipeline
- [Maestro orchestration](../../worker/src/transactions/maestro.rs) — 2-phase commit coordination

**Current Capabilities**:
- Topological stage scheduling (dependency-aware execution order)
- Exchange artifacts for cross-worker data movement
- Distributed aggregation (partial→final two-phase model)
- Hash-based partitioning for join/aggregate distribution
- 2-phase commit coordination (maestro)

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | High-complexity architecture; implementation complete but orchestration fragile |
| **Performance vs Generality** | General-purpose but assumes hash-based partitioning; shuffle overhead visible under skew |
| **Rollout Strategy** | Monolithic: all pieces required for correctness; hard to incrementally add features |
| **Architecture Alignment** | Tightly integrated with DataFusion planner; stage artifacts are core abstraction |

**Key Constraints**:
- Synchronous 2-phase commit (potential latency under network delays)
- Exchange artifacts must be serialized/deserialized (protocol overhead)
- Partitioning by hash (data skew can cause worker load imbalance)

**Investigation Questions**:
1. **Scalability**: How does performance degrade with cluster size (5, 20, 100 workers)? Are there orchestration bottlenecks?
2. **Fault Tolerance**: How are failures handled (worker crash, network partition)? Are retries/checkpoints in place?
3. **Optimization**: Are there quick wins for reducing shuffle overhead (e.g., broadcast joins, semi-joins)?
4. **Observability**: What visibility do we have into stage execution (latency, data movement)? Is this EP-1/2/3 candidate?

**Dependency Analysis**:
- ✅ Unblocked (foundation in place)
- 🔓 Enables: All distributed DDL/DML/Query operations, Joins (requires deterministic partitioning)
- 📦 Blocked by: None (operational hardening deferred to Runtime Optimizations)

**Recommended Next Steps**:
- **Validation**: Benchmark scaling to 20–50 worker cluster
- **Hardening**: Add fault tolerance (retry/checkpoint logic)
- **Observability**: Instrument stage latency and data movement (EP-1 candidate)

---

## 3. Aggregate Functions (Partial: COUNT, SUM, MIN, MAX, AVG)

**Current State**: Five aggregate functions implemented; two-phase execution (partial→final) for distributed queries.

**Status**: 🟡 Partial (5 functions only)

**Evidence**:
- [Aggregate execution engine](../../worker/src/execution/aggregate/) — COUNT, SUM, MIN, MAX, AVG
- [Two-phase aggregates](../../worker/src/execution/aggregate/aggregator.rs) — Partial accumulator, final merge
- [Planner support](../../kionas/src/planner/) — Aggregate distribution and merging

**Current Capabilities**:
- Single-partition: COUNT, SUM, MIN, MAX, AVG
- Distributed: Two-phase execution (workers compute partial, coordinator merges final)
- GROUP BY support (hash-based partitioning)
- DISTINCT within aggregates (framework in place but limited testing)

**Missing**:
- **Missing functions**: COUNT(DISTINCT), STRING_AGG, APPROX_PERCENTILE, STDDEV, VARIANCE, MEDIAN, FIRST/LAST, etc.
- **Windowing aggregates**: ROW_NUMBER(), RANK(), DENSE_RANK(), etc. (requires window function framework)
- **Custom aggregates**: No user-defined aggregate (UDAF) framework
- **Advanced GROUP BY**: GROUPING SETS, CUBE, ROLLUP (parser support only)

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | Medium effort: Each function adds ~100–200 LOC; minimal architectural change |
| **Performance vs Generality** | Highly efficient for common functions; specialized implementations (approx percentile) may require data structure optimization |
| **Rollout Strategy** | Incremental: Add one function at a time; two-phase framework already optimized for scalability |
| **Architecture Alignment** | Clean fit: Accumulator/merge pattern generalizes well; no protocol changes needed |

**Key Constraints**:
- DISTINCT aggregates require second-pass or specialized accumulator (memory overhead)
- Approximate functions (APPROX_PERCENTILE) need data structure decisions (tdigest vs quantile sketch)
- GROUPING SETS/CUBE/ROLLUP require planner changes (not just execution)

**Investigation Questions**:
1. **Prioritization**: Which missing functions are most critical? (e.g., COUNT(DISTINCT), STDDEV for analytics; STRING_AGG for reporting)
2. **DISTINCT Handling**: Should COUNT(DISTINCT) use HyperLogLog approximation or exact two-pass? Performance vs accuracy tradeoff?
3. **Approximate Functions**: Do we need APPROX_PERCENTILE or APPROX_STDDEV? Which data structure (tdigest, sampling, quantile sketch)?
4. **Advanced GROUP BY**: Is GROUPING SETS/CUBE/ROLLUP required now or deferred? Implementation scope vs query planning complexity?
5. **Custom Aggregates (UDAF)**: Should this be bundled with general UDF framework or separate?

**Dependency Analysis**:
- ✅ Unblocked (core framework in place)
- 🔓 Enables: Advanced analytics queries, Windowed aggregates (once window functions implemented)
- 📦 Blocked by: None for simple functions; UDAF requires UDF framework

**Recommended Next Steps**:
- **Phase 1**: Add COUNT(DISTINCT) and STDDEV/VARIANCE (high-value analytics functions)
- **Phase 2**: Evaluate APPROX_PERCENTILE demand; select data structure (tdigest vs quantile sketch)
- **Phase 3**: Advanced GROUP BY (GROUPING SETS/CUBE) if demanded by customers
- **Deferred**: UDAF (bundle with general UDF framework; see feature #2)

---

## 4. Arrow Batch Transformation for Inserts

**Current State**: Fully implemented with type-aware transformation, temporal/decimal support.

**Status**: ✅ Complete

**Evidence**:
- [Batch transformation](../../worker/src/transactions/maestro.rs#L385) — Temporal/decimal type hints
- [INSERT handler](../../server/src/statement_handler/dml/insert.rs) — Type coercion integration
- [Protocol](../../kionas/proto/) — Arrow schema exchange (verify proto definitions)

**Current Capabilities**:
- INSERT VALUES → Arrow RecordBatches
- Type coercion (string→timestamp, numeric precision handling)
- Decimal type support (custom scale/precision)
- Multi-row batching
- Worker-side deserialization

**Investigation Questions**:
1. **Completeness**: Are all SQL types handled (UUID, JSON, BLOB)? Any edge cases in type coercion?
2. **Performance**: Batching overhead under large inserts? Serialization/deserialization efficiency?
3. **Integration**: Does SELECT-based INSERT require different batching strategy?

**Dependency Analysis**:
- ✅ Unblocked
- 🔓 Enables: UPDATE/DELETE (can reuse batch transformation logic)
- 📦 Blocked by: None

**Recommended Next Steps**:
- **Validation**: Test with edge-case types (UUID, JSON, BLOB)
- **Documentation**: Capture type coercion rules and limitations
- **Reuse**: Leverage for UPDATE/DELETE batch transformation (Phase 13)

---

## 5. Aliases (Column & Table)

**Current State**: Parser and execution support for column/table aliases.

**Status**: ✅ Complete

**Evidence**:
- [Parser dialectdefinitions](../../kionas/src/parser/dialect.rs) — Alias rule support
- [Query planner](../../kionas/src/planner/) — Alias resolution in projection/FROM clause

**Current Capabilities**:
- Column aliases (SELECT col1 AS alias1)
- Table aliases (FROM table1 AS t1)
- Alias resolution in WHERE, GROUP BY, ORDER BY, HAVING

**Investigation Questions**:
1. **Edge cases**: Alias shadowing? Nested query alias scoping?
2. **Performance**: Any planner inefficiencies in resolving aliases?

**Dependency Analysis**:
- ✅ Unblocked
- 🔓 Enables: Subqueries (rely on alias scoping), CTEs (alias table references)
- 📦 Blocked by: None

**Recommended Next Steps**:
- **Validation**: Edge case testing (shadowing, nested scoping)
- **Documentation**: Capture alias resolution rules for subquery/CTE implementation

---

## 🟡 Partial Features (Gap Analysis & Expansion Path)

---

## 6. UPDATE/DELETE Operations

**Current State**: Deferred to Phase 13; no execution framework in place. Parser support exists.

**Status**: ❌ Not Started (framework), 🟡 Parser ready

**Evidence**:
- [DML handler](../../server/src/statement_handler/dml/) — INSERT only; UPDATE/DELETE stubs
- [Parser](../../kionas/src/parser/) — UPDATE/DELETE syntax recognized
- [Roadmap Phase 13](../../roadmaps/ROADMAP2.md) — UPDATE/DELETE planned

**Missing**:
- No execution planner for UPDATE/DELETE
- No WHERE clause evaluation for row filtering
- No transactional row locking or versioning
- No audit trail / MVCC support

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | High effortand complexity: Requires row-level locking, transactional isolation, WHERE evaluation, distributed consistency |
| **Performance vs Generality** | Generality demanded: Must handle complex WHERE + JOINs; locking strategy affects concurrency (pessimistic vs optimistic) |
| **Rollout Strategy** | Phased: Phase 1 = simple WHERE (no joins), Phase 2 = join-based updates, Phase 3 = MVCC/optimistic locking |
| **Architecture Alignment** | Requires extension to maestro (row-level coordination), executor (WHERE filtering), and planner (DML plan generation) |

**Key Constraints**:
- Deferred to Phase 13 (not immediate priority)
- WHERE clause + distributed updates = complex consistency story
- Requires solution to row-level write conflicts (pessimistic locking vs optimistic MVCC)

**Investigation Questions**:
1. **Isolation level**: What isolation guarantees do users expect (READ COMMITTED, SERIALIZABLE)? Does pessimistic locking suffice?
2. **WHERE complexity**: Should Phase 1 support only simple WHERE (single table, constant conditions) or full WHERE + JOINs?
3. **Conflict resolution**: How are write conflicts detected and resolved (optimistic retry vs pessimistic locking)? Performance implications?
4. **Audit trail**: Do we need MVCC or audit logging for UPDATE/DELETE tracking?
5. **Sequencing**: Does UPDATE depend on subquery support (correlated updates)? or independent?

**Dependency Analysis**:
- 📦 Blocked by: None (independent of other features)
- 🔓 Enables: Full CRUD operations, audit/compliance use cases
- ⚠️ Conflicts with: Aggressive query optimizations (caching, predicate pushdown) due to write concerns

**Recommended Next Steps**:
- **Phase 1 Planning** (Phase 13 pre-work):
  - Define isolation model (pessimistic vs optimistic)
  - Scope Phase 1: simple WHERE (no joins), basic conflict resolution
  - Design row-level locking protocol (maestro extension)
  - Create execution planner for simple UPDATE/DELETE
- **Phase 1 Success Criteria**:
  - Single-table UPDATE/DELETE with WHERE
  - No join predicates initially
  - Pessimistic locking or simple optimistic retry
  - Transactional consistency verified

---

## 7. Join Capability Expansion (INNER only → FULL support)

**Current State**: INNER equi-join only; LEFT/RIGHT/FULL OUTER deferred. Cross joins not supported.

**Status**: 🟡 Partial (INNER only)

**Evidence**:
- [Join execution](../../worker/src/execution/join.rs) — Deterministic hash join, INNER only
- [Join planner](../../kionas/src/planner/) — Join distribution strategy (hash-based partitioning)
- [Parser](../../kionas/src/parser/) — Full syntax support but execution limited

**Current Capabilities**:
- INNER equi-joins (ON col1 = col2)
- Deterministic hash-based partitioning (workers partition rows by join key hash)
- Multi-table inner joins (chained execution)
- Distributed join execution

**Missing**:
- **LEFT OUTER JOIN**: Requires null padding for unmatched rows from left input
- **RIGHT OUTER JOIN**: Symmetric to LEFT
- **FULL OUTER JOIN**: Requires null padding on both sides + deduplication
- **CROSS JOIN**: Cartesian product (performance concern for distributed execution)
- **ON conditions**: Non-equi predicates (e.g., t1.col > t2.col), complex boolean expressions
- **Join hints**: No way to control join algorithm (broadcast, sort-merge, hash)

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | Medium-high effort: Each join type adds 300–500 LOC; null handling adds planner logic |
| **Performance vs Generality** | Generality required: Must support all join types; outer joins → increased memory (null padding) |
| **Rollout Strategy** | Incremental: LEFT → RIGHT → FULL OUTER; non-equi joins deferred (optimizer complexity); cross join last (cartesian product perf concern) |
| **Architecture Alignment** | Fits existing hash-join framework; minimal protocol changes; requires output schema adjustment (null columns) |

**Key Constraints**:
- Null padding overhead (outer joins require wider output rows)
- Non-equi join detection necessary (current framework assumes equi-join on partition key)
- Cross join requires special handling (can't use hash partitioning; broadcast or repartition)

**Investigation Questions**:
1. **Sequencing**: Should LEFT OUTER be implemented first? Or is RIGHT OUTER equally important?
2. **Null handling**: How are nulls represented in Arrow output? Performance impact of null padding?
3. **Non-equi predicates**: Are complex ON conditions (t1.col > t2.col) required in Phase 1, or deferred?
4. **Cross joins**: How common are cross joins in real workloads? Is the performance concern justified (defer or implement broadcast)?
5. **Join hints**: Should users be able to hint join algorithm (broadcast, sort-merge) or rely on planner?
6. **Semi/Anti joins**: Are LEFT SEMI or LEFT ANTI joins needed? (Used for EXISTS/IN subqueries)

**Dependency Analysis**:
- 📦 Blocked by: None (independent)
- 🔓 Enables: Subqueries (correlated subqueries with EXISTS/IN), Complex analytics queries
- ⚠️ Interacts: Subqueries can simplify some join patterns; CTEs can express complex joins more readably

**Recommended Next Steps**:
- **Phase 1**: LEFT OUTER JOIN
  - Implement null padding in output schema
  - Extend join planner to generate null-safe probes
  - Test with large outer results (memory efficiency)
- **Phase 2**: RIGHT / FULL OUTER JOINs
  - Reuse null-padding logic
  - Add deduplication for FULL OUTER (verify correctness)
- **Phase 3**: Semi/Anti joins (for subquery support, if not addressed by subquery execution engine)
- **Deferred**: Non-equi joins (requires cost-based optimizer enhancements), Cross joins (broadcast strategy)

---

## 8. Runtime Optimizations & Hardening

**Current State**: Validation gates present; scan pruning incomplete; performance baselines not established.

**Status**: 🟡 Partial (framework in place, execution gaps)

**Evidence**:
- [Pipeline validation gates](../../worker/src/execution/pipeline.rs) — Run-time schema/constraint checks
- [Scan optimization](../../kionas/src/planner/) — Partition pruning partially implemented
- [Roadmap phases](../../roadmaps/) — EP-1/2/3/4/5 performance phases planned

**Current Optimizations**:
- Partition pruning (some conditions)
- Predicate pushdown (basic cases)
- Join order optimization (single-node; distributed order unclear)
- Stage pipelining (exchange artifacts batched)

**Missing**:
- **Cost-based optimization**: No cardinality estimation; joins use default order
- **Columnar execution**: Batch processing present but vectorization opportunities unclear
- **Memory budgets**: No spill-to-disk for large aggregates/joins
- **Adaptive optimization**: No runtime feedback to adjust plan mid-execution
- **Cache coherency**: No caching layer for selective results
- **Benchmarks**: No performance baseline (QPS, latency percentiles)

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | High effort; complex optimization decisions (cost model, vectorization, memory budgeting) |
| **Performance vs Generality** | Generality key: Optimize for diverse query patterns; targeted optimizations (OLTP vs OLAP) may diverge |
| **Rollout Strategy** | Phased (EP phases): EP-1 = validation gates + cardinality estimation setup, EP-2 = cost-based joins, EP-3 = memory budgeting, EP-4/5 = adaptive/cache |
| **Architecture Alignment** | Extends existing planner; requires statistics collection (metadata overhead); new decision points in executor (spill, adaptive) |

**Key Constraints**:
- Statistics collection requires scan of entire table (initial pass expensive)
- Adaptive optimization requires execution feedback loop (protocol changes)
- Memory budgeting decisions (spill strategy) affect I/O patterns and latency tail

**Investigation Questions**:
1. **Cost model**: Should we use DataFusion's cost model or build custom? How accurate must cardinality estimates be?
2. **Vectorization**: Are there low-hanging fruit in batch processing (SIMD, cache efficiency)?
3. **Memory limits**: What memory budgets per query/worker? How should spill-to-disk prioritize (hash tables, sort buffers)?
4. **Baselines**: What are target QPS/latency for OLTP vs OLAP workloads? How do we measure improvement?
5. **EP phases**: Do EP-1/2/3/4/5 phases need sequential execution or can they run in parallel?

**Dependency Analysis**:
- ✅ Unblocked (can progress independently)
- 🔓 Enables: Scalable analytics queries, Production deployment (observability during optimization)
- 📦 Blocked by: Observability (feedback for adaptive optimization); Statistics infrastructure (metadata)

**Recommended Next Steps**:
- **Immediate (EP-1)**: Establish performance baselines (TPC-H/DS subset), add cardinality estimation framework
- **Short-term (EP-2)**: Cost-based join ordering, memory budgeting skeleton
- **Medium-term (EP-3/4)**: Vectorization improvements, adaptive optimization, spill-to-disk
- **Long-term (EP-5)**: Advanced caching, query result caching

---

## 9. Observability & Instrumentation Hardening

**Current State**: Telemetry events present; EP-1/2/3 phases planned but not detailed.

**Status**: 🟡 Partial (framework exists, scope unclear)

**Evidence**:
- [Pipeline telemetryEvents](../../worker/src/execution/pipeline.rs) — Span/event logging present
- [Roadmap phases](../../roadmaps/) — EP observability phases documented (verify content)
- [Tracing setup](../../kionas/src/lib.rs) — Tracing crate integrated

**Current Observability**:
- Structured logging (tracing crate)
- Basic execution telemetry (stage/task level)
- Error propagation with context

**Missing**:
- **Metrics collection**: No counters/histograms for query execution (latency, throughput, errors)
- **Distributed tracing**: No correlation IDs for cross-worker traces
- **Performance profiling**: No function-level instrumentation for optimization feedback
- **Monitoring dashboard**: No real-time view of system health (worker utilization, query queue depth)
- **Alerting**: No automated alerts for SLO violations or resource exhaustion
- **Query auditing**: No logging of who ran which queries (compliance requirement)

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | Medium effort: Metrics collection straightforward; distributed tracing requires protocol changes; profiling requires careful instrumentation to avoid perf impact |
| **Performance vs Generality** | Generality demanded: Must work for OLTP and OLAP; overhead must be < 5% for low-overhead observability |
| **Rollout Strategy** | Phased (EP phases): EP-1 = metrics + distributed tracing setup, EP-2 = performance profiling, EP-3 = dashboard + alerting |
| **Architecture Alignment** | Extends protocol (correlation IDs); new metrics layer in executor; persists to telemetry backend (OpenTelemetry compatible) |

**Key Constraints**:
- Distributed tracing correlation IDs add protocol overhead (message size, parsing)
- Metrics collection has per-operation cost (precision vs performance tradeoff)
- Dashboard/alerting requires external service (Prometheus, Grafana, etc.)

**Investigation Questions**:
1. **Metrics scope**: What metrics are critical (query latency percentiles, worker CPU/memory, queue depth)? Nice-to-have?
2. **Sampling**: Should tracing be 100% or sampled? Does sampling strategy change per query type (OLTP=always, OLAP=sampled)?
3. **Profiling granularity**: How fine-grained should performance profiling be (function-level, stage-level, query-level)?
4. **Backend choice**: OpenTelemetry Jaeger, Prometheus + Grafana, or custom?
5. **Audit logging**: Are detailed query execution logs required for compliance? Retention period?
6. **Real-time vs post-mortem**: Should observability focus on real-time monitoring or post-query analysis?

**Dependency Analysis**:
- ✅ Unblocked (can progress independently)
- 🔓 Enables: Performance troubleshooting, Production deployment, SLO monitoring
- 📦 Blocked by: Runtime Optimizations (feedback loop for adaptive optimization)

**Recommended Next Steps**:
- **EP-1**: Add Prometheus-compatible metrics (query latency, worker stats), OpenTelemetry trace context
- **EP-2**: Performance profiling instrumentation (stage-level spans, histogram bucketing)
- **EP-3**: Dashboard (Grafana) + alerting setup, query audit logging
- **Ongoing**: Feedback from production monitoring to guide Runtime Optimizations

---

## 10. Janitor (Maintenance & Cleanup)

**Current State**: Partial implementation; Redis cache refresh only. No data cleanup (compaction, tombstone removal).

**Status**: 🟡 Partial (cache refresh only)

**Evidence**:
- [Janitor module](../../server/src/janitor/mod.rs) — Redis dashboard cache refresh logic
- [Scheduling](../../server/src/janitor/) — Task scheduling / run intervals

**Current Capabilities**:
- Redis cache invalidation (dashboard metadata refresh)
- Scheduled task execution

**Missing**:
- **Data cleanup**: No removal of deleted rows (tombstones), compaction of data files
- **Temporary file cleanup**: No purge of intermediate query results / spill files
- **Index maintenance**: No index fragmentation cleanup or rebuilding
- **Statistics refresh**: No automatic update of table statistics for cardinality estimation
- **Checkpoint cleanup**: No removal of old transaction checkpoint snapshots
- **Transaction log cleanup**: No purge of old WAL/transaction logs

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | Medium effort: Each cleanup task is independent; complexity depends on data structure choices (e.g., compaction strategy) |
| **Performance vs Generality** | Performance-sensitive: Cleanup must run asynchronously without impacting query workload; generality = support multiple cleanup strategies |
| **Rollout Strategy** | Incremental: Phase 1 = temporary file cleanup, Phase 2 = data compaction, Phase 3 = statistics refresh + index maintenance |
| **Architecture Alignment** | Fits existing janitor framework; requires integration with storage engine (file layout, compaction metadata) |

**Key Constraints**:
- Compaction strategy affects query performance (background I/O, file reorganization)
- Statistics refresh must be scheduled during low-traffic windows (coordination challenge)
- Checkpoint cleanup safety (must ensure no active transactions reference old snapshots)

**Investigation Questions**:
1. **Cleanup priority**: What cleanup tasks are critical for production (temporary files, tombstones, statistics)?
2. **Compaction strategy**: Should compaction be level-based, tiered, or other? How aggressive (frequency, I/O budget)?
3. **Statistics staleness**: How old can statistics be before query plans degrade? Auto-refresh frequency?
4. **Retention policies**: How long should transaction logs / checkpoints be retained? User-configurable?
5. **Performance impact**: What is acceptable background I/O for janitor tasks? How do we monitor impact?

**Dependency Analysis**:
- ✅ Unblocked (can progress independently)
- 🔓 Enables: Storage efficiency, Long-term operations, Data retention compliance
- 📦 Blocked by: Storage engine design decisions (compaction strategy, checkpoint format)

**Recommended Next Steps**:
- **Phase 1**: Temporary file cleanup + Redis cache refresh (already partial)
- **Phase 2**: Tombstone cleanup + basic compaction (coordinate with storage format)
- **Phase 3**: Statistics refresh automation + checkpoint cleanup
- **Deferred**: Advanced compaction strategies until storage performance becomes bottleneck

---

## ❌ Not-Started Features (Scope & Architecture Decisions)

---

## 11. User-Defined Functions (UDFs)

**Current State**: No registry, no execution engine. No parser support for custom functions.

**Status**: ❌ Not Started

**Evidence**:
- No UDF files found in codebase
- Parser recognizes function calls but no custom resolution
- Roadmap: UDFs not explicitly scheduled

**Missing**:
- **Function registry**: No metadata store for registered functions
- **Function signature**: No parameter type / return type definition
- **Execution engine**: No mechanism to invoke user code (SQL, Python, WASM, native?)
- **Dependency management**: No isolation or versioning for function code
- **Security model**: No permission model for who can execute which functions
- **Performance**: No caching of parsed/compiled function code

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | High effort AND complexity: Significant architectural decision (SQL vs Python vs WASM); execution sandbox; registry management |
| **Performance vs Generality** | Tension: SQL functions have predictable cost; Python/dynamic languages allow powerful logic but risks performance cliffs |
| **Rollout Strategy** | Phased, but each language is its own implementation: Phase 1 = SQL functions only, Phase 2 = Python UDFs (WASM sandbox), Phase 3 = scalar vs aggregate classification |
| **Architecture Alignment** | Requires new subsystem (function registry, compilation cache, execution sandbox); integration with planner/executor |

**Key Constraints**:
- Language choice is foundational (affects security, performance, dependency management)
- Dynamic languages (Python) require sandboxing or trust model
- Function caching and optimization adds complexity
- Parameter type inference / overload resolution affects planner

**Investigation Questions**:
1. **Language support**: Should we start with SQL-only UDFs, or include Python? What are the workload requirements?
2. **Scope**: Scalar functions only (Phase 1) or include aggregate functions (UDAFs) and table functions?
3. **Sandboxing**: If Python, do we need WASM/Wasmtime sandboxing for security, or trust users?
4. **Caching**: Should compiled/optimized UDF code be cached? How does versioning work?
5. **Permissions**: Do we need fine-grained permission model (who can execute which UDFs)?
6. **Performance**: What are performance targets (UDF invocation overhead, latency tail)?

**Dependency Analysis**:
- 📦 Blocked by: Architecture decision (language choice); no external blockers
- 🔓 Enables: Advanced analytics (custom transformations), Workflows (complex business logic), UDAFs (aggregate expansion)
- ⚠️ Interacts: Subqueries can express some UDF-like logic (CTE + CASE expressions)

**Recommended Discovery Steps** (assume SQL UDFs first):
- **Phase 0 Research**:
  - Survey customer use cases (what kinds of UDFs are needed?)
  - Evaluate SQL-only functions vs Python support
  - Define sandboxing model (trust, WASM, etc.)
  - Design function registry schema
- **Phase 1 Planning** (if SQL UDFs):
  - Define SQL function grammar (CREATE FUNCTION syntax)
  - Design registry metadata schema
  - Plan parser extensions
  - Outline planner integration (function call → SQL expression expansion or invocation)
- **Phase 1 Success Criteria**:
  - CREATE/DROP FUNCTION working
  - Scalar SQL functions invokable in SELECT
  - Function library (5–10 common functions) working

---

## 12. Window Functions

**Current State**: Parser support only (ROW_NUMBER, RANK, DENSE_RANK, etc. recognized); no execution engine.

**Status**: ❌ Not Started (execution)

**Evidence**:
- [Parser window function rules](../../kionas/src/parser/dialect.rs) — Syntax recognized
- [No executor found](../../worker/src/execution/) — No window frame execution

**Missing**:
- **Execution engine**: No window frame computation (ROWS BETWEEN, RANGE)
- **Ordering**: No support for ORDER BY within window frames
- **Partitioning**: No window PARTITION BY logic
- **Frame semantics**: ROWS vs RANGE, UNBOUNDED PRECEDING/FOLLOWING, CURRENT ROW
- **Integration with aggregates**: No window aggregate functions (SUM OVER, AVG OVER)

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | High effort; high complexity: Window semantics are intricate (frame navigation, peer groups); distributed execution adds coordination burden |
| **Performance vs Generality** | Generality required: Must support all window frame types; performance: single-node windowing feasible (one-partition assumption), distributed windowing complex |
| **Rollout Strategy** | Phased: Phase 1 = single-partition windowing + simple functions (ROW_NUMBER, RANK), Phase 2 = distributed windowing (shuffle by partition key), Phase 3 = RANGE frames |
| **Architecture Alignment** | Requires new executor stage; minimal planner changes (frame push-down); integration with sort for ORDER BY within frames |

**Key Constraints**:
- Distributed window functions require repartitioning by PARTITION BY key (shuffle overhead)
- Frame semantics are complex (peer group detection, boundary navigation)
- RANGE frames require sortable data types and boundary value semantics

**Investigation Questions**:
1. **Scope**: Implement all window functions or subset (ROW_NUMBER, RANK, DENSE_RANK, aggregates)?
2. **Distributed execution**: Should Phase 1 assume single partition (no distribution) or support PARTITION BY distributing?
3. **Frame types**: Are RANGE frames required, or do ROWS frames suffice for initial release?
4. **Integration**: Can window functions reuse aggregate execution engine or require new implementation?
5. **Performance**: What latency/throughput targets for windowed queries? Acceptable memory footprint?

**Dependency Analysis**:
- 📦 Blocked by: None (independent architecture)
- 🔓 Enables: Analytics queries (trending, rankings), CTEs can express some window-like patterns (subqueries + ROW_NUMBER workaround)
- ⚠️ Interacts: Aggregate Expansion (window aggregates use same aggregate engine)

**Recommended Discovery Steps**:
- **Phase 0 Research**:
  - Define scope (which window functions in Phase 1?)
  - Evaluate single-partition vs distributed execution trade-off
  - Survey customer use cases (common window function patterns)
- **Phase 1 Planning** (single-partition window functions):
  - Design window frame executor
  - Outline frame semantics implementation (ROWS, peer group navigation)
  - Plan integration with sorting (ORDER BY within frames)
  - Define planner changes (frame push-down, partition detection)
- **Phase 1 Success Criteria**:
  - ROW_NUMBER, RANK, DENSE_RANK working (row ordering)
  - Basic aggregates (SUM, AVG) over windows with ROWS frames
  - Single-partition assumption; multi-partition queries error or auto-partition

---

## 13. CTEs (Common Table Expressions) & Subqueries

**Current State**: Parser recognizes CTE/subquery syntax. No execution engine or planner support.

**Status**: ❌ Not Started (execution)

**Evidence**:
- [Parser CTE/subquery rules](../../kionas/src/parser/) — Syntax recognized
- [No executor logic found](../../worker/src/execution/) — No CTE evaluation framework
- [Planner](../../kionas/src/planner/) — No CTE plan generation

**Missing for CTEs**:
- **Materialization**: No decision whether to materialize CTE results or inline
- **Scope**: Multi-reference CTEs (used in multiple places); recursive CTEs
- **Execution plan**: CTE → inline as subquery or cache materialized results

**Missing for Subqueries**:
- **Scalar subqueries**: SELECT (SELECT ...) in projection
- **Correlated subqueries**: WHERE / JOIN conditions referencing outer row
- **Subquery unnesting**: Transformation to JOINs where possible (optimizer pattern)
- **Subqueries in FROM**: FROM (SELECT ...) as derived table
- **Subqueries in WHERE**: IN / EXISTS / comparison operators

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | Very high effort; very high complexity: Correlated subquery execution requires outer row passing (protocol changes); CTE materialization adds executor state; recursive CTEs require loop detection |
| **Performance vs Generality** | Generality critical: Must support all subquery types; performance: correlated subqueries inefficient without unnesting; CTE materialization vs inlining trade-off affects memory/latency |
| **Rollout Strategy** | Phased, multiple sub-phases: Phase 1 = non-correlated subqueries (FROM, simple WHERE IN), Phase 2 = unnesting optimization, Phase 3 = correlated subqueries, Phase 4 = recursive CTEs |
| **Architecture Alignment** | Requires planner extensions (subquery push-down/unnesting rules), executor integration (outer row context), protocol changes (correlation variable passing) |

**Key Constraints**:
- Correlated subqueries require passing outer row context to inner executor (protocol overhead)
- CTE materialization decisions affect memory usage (large CTE materialized = high memory cost)
- Subquery unnesting is complex optimization (requires cost model, correctness guardrails)
- Recursive CTEs require loop detection and termination logic (cycle detection)

**Investigation Questions**:
1. **Phase 1 scope**: Should we start with non-correlated subqueries (FROM/WHERE IN) or include scalars?
2. **Correlated subqueries**: How important are correlated subqueries in workload? Unnesting vs row-passing trade-off?
3. **CTE materialization**: Heuristic or cost-based decision? Should users control (WITH ... MATERIALIZED)?
4. **Recursive CTEs**: Are they required? Recursive depth limits? Cycle detection strategy?
5. **Performance**: What are acceptable latency/memory trade-offs for CTE materialization?

**Dependency Analysis**:
- 📦 Blocked by: None (independent architecture)
- 🔓 Enables: Complex analytics queries, Query readability (CTEs as named building blocks), Correlated query patterns
- ⚠️ Interacts: Subquery unnesting can simplify JOINs; window functions can replace some CTE + ROW_NUMBER patterns

**Recommended Discovery Steps**:
- **Phase 0 Research**:
  - Survey workloads: what CTEs/subqueries are most common?
  - Evaluate correlated subquery demand
  - Design subquery unnesting heuristics (cost-based or rule-based?)
  - Recurive CTE requirements (cycle detection, depth limits)
- **Phase 1 Planning** (non-correlated subqueries):
  - Design planner subquery inlining (FROM subqueries → join/union)
  - Outline executor integration (subquery evaluation in WHERE IN)
  - Plan protocol changes for subquery result handling
  - Define testing strategy (correctness verification for unnesting)
- **Phase 1 Success Criteria**:
  - FROM (SELECT ...) subqueries working
  - WHERE IN (SELECT ...) working (non-correlated)
  - Basic CTE (non-recursive) materialized or inlined

---

## 14. Ingestion Worker & Source Connectors

**Current State**: No ingestion infrastructure. No source connector framework.

**Status**: ❌ Not Started

**Evidence**:
- No ingestion files in codebase
- Worker focused on query execution
- Roadmap: Ingestion noted but not scheduled

**Missing**:
- **Source connectors**: No framework for connecting to external sources (Kafka, S3, databases, etc.)
- **Schema inference**: No automatic schema detection from source
- **Change Data Capture (CDC)**: No mechanisms for incremental ingestion
- **Ingestion worker**: No dedicated ingest-only worker process
- **Format support**: No handlers for Parquet, ORC, CSV, JSON, Avro on input
- **Deduplication**: No logic to handle duplicate records during ingestion
- **Backpressure**: No flow control when ingestion rate exceeds storage rate

**Tradeoffs Analysis**:

| Aspect | Status |
|--------|--------|
| **Effort vs Complexity** | Very high effort; high complexity: Each source connector has unique semantics; CDC adds state management; format parsers add dependencies |
| **Performance vs Generality** | Generality demanded: Support multiple sources; performance: source-dependent (network bandwidth, file I/O, parsing overhead) |
| **Rollout Strategy** | Phased, source-by-source: Phase 1 = CSV/JSON file ingestion, Phase 2 = Kafka streaming, Phase 3 = CDC (Postgres WAL, MySQL binlog), Phase 4 = foreign table federation (SQL queries on external sources) |
| **Architecture Alignment** | Requires new ingestion subsystem; integration with planner/executor for INSERT targets; new worker role (ingest vs query) |

**Key Constraints**:
- Source connector selection affects data freshness SLA (batch ingestion vs streaming vs CDC)
- Format parsing adds dependencies (Apache Arrow libraries, Kafka client, database drivers)
- CDC adds complexity (state management, transaction coordination across systems)
- Deduplication strategy (fingerprinting, primary keys) affects ingestion latency

**Investigation Questions**:
1. **Source priority**: Which sources are most critical (S3/Parquet, Kafka, databases)? CSV/JSON nice-to-have?
2. **Ingestion patterns**: Batch (periodic files) vs streaming (Kafka) vs CDC? Latency requirements?
3. **Schema handling**: Should schema be user-defined or inferred from source? Schema evolution handling?
4. **Deduplication**: How important is exactly-once semantics? Fingerprint-based vs primary-key-based deduplication?
5. **Federation vs materialization**: Should external source queries be federated (query-time) or materialized (ingested)?

**Dependency Analysis**:
- ✅ Unblocked (independent architecture; depends on INSERT/UPDATE working)
- 🔓 Enables: Data lake scenarios, Real-time analytics, Multi-source joins
- ⚠️ Interacts: RBAC/permissioning (who can ingest from which sources?), Observability (ingestion metrics)

**Recommended Discovery Steps**:
- **Phase 0 Research**:
  - Customer use cases (which sources, ingestion patterns?)
  - Source connector architecture (reusability across sources?)
  - CDC evaluation (Postgres logical replication, MySQL binlog, Debezium?)
  - Format library selection (Arrow, Polars, custom?)
- **Phase 1 Planning** (file-based ingestion):
  - Design connector API (abstract source interface)
  - Outline parser integration (CSV, JSON, Parquet)
  - Plan schema inference logic
  - Define ingestion stage in planner (INSERT target coordination)
- **Phase 1 Success Criteria**:
  - CSV/JSON files ingestible (COPY FROM or INSERT INTO ... FROM FILE)
  - Basic deduplication (hash-based fingerprinting)
  - Schema inference from first N rows

---

## Cross-Feature Dependencies & Recommended Investigation Sequence

### Dependency Graph

```
Complete (baseline):
├─ INSERT VALUES
├─ Multi-node Worker Execution (uses INSERT for distributed coordination)
├─ Aggregates (partial) — uses 2-phase execution
├─ Arrow Batching
└─ Aliases

Partial (expand from baseline):
├─ UPDATE/DELETE — uses INSERT + maestro patterns; extends WHERE filtering
├─ JOINs — uses multi-node execution + distributed aggregates
├─ Runtime Optimizations — enables performance for all queries
├─ Observability — measures performance, informs optimization
└─ Janitor — cleanup supports long-running systems

Not-Started (new capabilities):
├─ UDFs — orthogonal to core query execution; enables custom logic
├─ Window Functions — uses aggregates + sorting; can reuse 2-phase execution
├─ CTEs/Subqueries — foundational for complex queries; enables query unnesting patterns
├─ Ingestion — separate from query execution; enables data lakescenarios
```

### Recommended Investigation Order (Critical Path)

**Tier 1 (Foundation)**: Expand existing features to full coverage
- **CTEs/Subqueries**: Unlocks complex query patterns; foundation for many advanced features
- **Window Functions**: Analytics use case; depends on aggregates (already partial)
- **JOIN Expansion**: Extends query capability; foundational for complex queries

**Tier 2 (Operational Completeness)**: Complete partial features
- **UPDATE/DELETE**: Full CRUD; dependencies on WHERE filtering (covered by CTEs/subqueries)
- **Aggregate Expansion**: Additional analytics functions; builds on existing framework

**Tier 3 (System Hardening)**: Observability & optimization
- **Observability**: Enables performance troubleshooting; prerequisite for optimization feedback
- **Runtime Optimizations**: Performance foundation for production
- **Janitor**: Enables long-term operational maintenance

**Tier 4 (Advanced Capabilities)**: New subsystems
- **UDFs**: Orthogonal; can be scheduled independently
- **Ingestion**: Separate ecosystem; enables data lake use cases

### Blocking Relationships

- **CTEs/Subqueries** block:
  - Correlated subqueries in UPDATE/DELETE (WHERE conditions)
  - Complex JOIN patterns (join unnesting from subqueries)
  - Analytics queries (CTE building blocks)

- **Window Functions** block:
  - Advanced analytics (ranking, trending queries)

- **JOIN Expansion** block:
  - Multi-table queries with outer joins

---

## Investigation & Planning Summary

### Next Steps (Per Feature)

| Feature | Investigation Phase | Planning Phase | Implementation Phase |
|---------|-----------------|-------------|-------------------|
| Update/Delete | Define isolation model | Phase 13 pre-work | Phase 13 code |
| Window Functions | Phase 0: Scope functions; evaluate distributed execution | Phase 1: design executor, sort integration | Phase 1+ implementation |
| CTEs/Subqueries | Phase 0: Workload survey; unnesting strategy | Phase 1: design inlining, protocol changes | Phase 1+ implementation |
| JOINs (expand) | Phase 0: Left-outer decision | Phase 1: null padding, planner extension | Phase 1+ implementation |
| Aggregates (expand) | Phase 1: Prioritize COUNT(DISTINCT), STDDEV | Phase 2: APPROX_PERCENTILE evaluation | Phase 2+ implementation |
| UDFs | Phase 0: Language/sandbox decision | Phase 1: Registry design, parser extension | Phase 1+ implementation (SQL first) |
| Ingestion | Phase 0: Source prioritization, CDC evaluation | Phase 1: Connector API, file formats | Phase 1+ implementation |
| Observability | Phase 1: Metrics selection, tracing scope | Phase 1: OpenTelemetry setup | Parallel with optimizations |
| Runtime Optimizations | Phase 1: Cost model, benchmarking | EP-1: Cardinality estimation framework | EP-1+ implementation |
| Janitor | Phase 1: Cleanup prioritization | Phase 2: Compaction strategy | Phase 2+ implementation |

---

## Success Metrics

1. **Discovery Completeness**: All 14 features have investigation questions, tradeoffs, and dependency analysis
2. **Actionable Output**: Implementation teams can proceed to Phase 1 planning without additional clarification
3. **Risk Identification**: Blockers, performance concerns, and architectural tensions flagged for early attention
4. **Sequencing Clarity**: Critical path identified; teams understand why certain features must precede others
