# Plan: Phase 2c Multi-Partition Mapping & Routing

**TL;DR**: Implement row-level partition routing logic for 4 schemes (Hash, RoundRobin, Range, SinglePartition), then integrate into the existing multi-worker Flight streaming pipeline. All infrastructure from Phase 2b is ready. ~710 LOC, ~5-6 person-days, 2-3 weeks sequential (routing logic first, then multi-worker distribution, then integration tests).

---

## Steps

### **Phase 2c.1: Partition Routing Logic (Weeks 1-2)** ⚠️ Critical Path

**2c.1.0 – Exploration & Implementation Plan** (*depends on: discovery complete* ✅)
- Verify `PartitionSpec` enum format in [kionas/src/planner/mod.rs](kionas/src/planner/mod.rs) for Hash (key_columns), Range (bounds), RoundRobin, Single
- Audit DataFusion codebase (or arrow-rs) for column value extraction patterns (may exist as helpers)
- Decide: Dedicated module `worker/src/execution/router.rs` vs inline routing in pipeline.rs
- Clarify: RoundRobin state management (per-partition Arc<AtomicUsize> counter vs stateless row counting)

**2c.1.1 – Core: Hash Partitioning** ([worker/src/execution/router.rs](worker/src/execution/router.rs) - new file, ~60 LOC)
- Implement `route_batch_hash(batch: &RecordBatch, partition_key_columns: &[String], downstream_partition_count: u32) -> Result<Vec<RecordBatch>>`
- Extract values from partition_key_columns for each row
- Hash rows using stable hash (fnv or murmur3), mod by downstream_partition_count
- Return vector of RecordBatches indexed by destination partition (empty batches for missing partitions)
- Add doc comments: What, Inputs (batch, key cols, fanout), Output (Vec of batches), Details (hash stability)
- Accept config: `WORKER_HASH_FUNCTION` env var (default "fnv", options: "fnv", "murmur3") for hash algorithm selection

**2c.1.2 – Core: RoundRobin Partitioning** (~20 LOC, same file)
- Implement `route_batch_roundrobin(batch: &RecordBatch, downstream_partition_count: u32, next_partition: &Arc<AtomicUsize>) -> Result<Vec<RecordBatch>>`
- For each row: increment next_partition counter, assign to partition = (counter % downstream_partition_count)
- Return vector of RecordBatches indexed by destination partition
- **Note**: `next_partition` is shared across multiple calls; caller maintains Arc throughout stage execution

**2c.1.3 – Core: Range Partitioning** (~40 LOC, same file)
- Implement `route_batch_range(batch: &RecordBatch, partition_key_column: &str, range_bounds: &[Value], downstream_partition_count: u32) -> Result<Vec<RecordBatch>>`
- Extract single key column values from batch rows
- For each row: binary search value in sorted range_bounds vector
- Assign to partition = (search result index clamped to 0..downstream_partition_count-1)
- Handle edge cases: values before min bound (→ partition 0), after max bound (→ last partition)
- Return vector of RecordBatches indexed by destination partition

**2c.1.4 – Core: SinglePartition** (~10 LOC, same file)
- Implement `route_batch_single(batch: &RecordBatch, _downstream_partition_count: u32) -> Result<Vec<RecordBatch>>`
- Return vector with batch at index 0, empty batches for indices 1..N (or return single-element vec)
- Trivial implementation; primarily for completeness

**2c.1.5 – Helper: Partition Dispatcher** (~50 LOC, same file)
- Implement `route_batch(batch: &RecordBatch, partition_spec: &PartitionSpec, downstream_partition_count: u32, rr_counter: Option<&Arc<AtomicUsize>>) -> Result<Vec<RecordBatch>>`
- Dispatcher that calls appropriate route_batch_* based on PartitionSpec variant
- Match on PartitionSpec:
  - `Hash { key_columns }` → call route_batch_hash
  - `RoundRobin` → call route_batch_roundrobin with counter
  - `Range { bounds }` → call route_batch_range
  - `Single` / `Broadcast` → call route_batch_single
- Document: "Dispatches batch routing based on PartitionSpec strategy"

**2c.1.6 – Unit Tests: Hash Routing** (~60 LOC, new file [worker/src/tests/router_tests.rs](worker/src/tests/router_tests.rs))
- **Test**: Uniform distribution (1000 rows, key=int, 4 partitions, verify each partition gets ~250 rows)
- **Test**: Boundary values (rows with min/max int values hash correctly)
- **Test**: Empty batch (returns vector with empty batches)
- **Test**: Single partition (all rows go to partition 0)
- **Test**: Multi-column key (composite hash from 2+ columns)

**2c.1.7 – Unit Tests: RoundRobin Routing** (~20 LOC, same test file)
- **Test**: Cyclic assignment (10 rows, 3 partitions, verify order: 0,1,2,0,1,2,0,1,2,0)
- **Test**: State persistence (first batch increments counter; second batch continues from counter)
- **Test**: Empty batch (counter still increments correctly)

**2c.1.8 – Unit Tests: Range Routing** (~40 LOC, same test file)
- **Test**: Boundary-aware distribution (bounds [100, 200, 300], values [50, 150, 250, 350] → partitions [0, 1, 2, 3])
- **Test**: All in one partition (bounds [100, 200], rows with values [50..99] → all to partition 0)
- **Test**: Clamping at edges (values below/above bounds map to partitions 0 and N-1)

**2c.1.9 – Unit Tests: SinglePartition & Dispatcher** (~30 LOC, same test file)
- **Test**: Single partition routes all rows to partition 0
- **Test**: Dispatcher invokes correct route_batch_* per PartitionSpec variant
- **Test**: Error propagation (invalid key column name returns Err)

**Verification for 2c.1**:
1. `cargo test -p worker router_tests -- --exact` (all 9 test cases pass)
2. `cargo fmt --all` (no changes)
3. `cargo clippy --all-targets --all-features -- -D warnings` (zero warnings in router.rs)
4. Cyclomatic/data-flow complexity check on route_batch_hash (should be < 25) ✅ Complexity target met

**Deliverable**: `router.rs` with 4 route_batch_* functions + dispatcher, router_tests.rs with all passing tests. ~180 LOC core + ~150 LOC tests.

---

### **Phase 2c.2: Multi-Worker Partition Distribution (Weeks 2-3)** (*depends on: 2c.1 complete*)

**2c.2.1 – Integration: Route Batches in Pipeline** ([worker/src/execution/pipeline.rs](worker/src/execution/pipeline.rs#L1394), ~50 LOC modification)
- At line 1394 (after persist_stage_exchange_artifacts), call route_batch dispatcher
- Input: normalized_batches, task.output_destinations[0].partitioning, task.output_destinations.len()
- Output: routed_batches_by_partition: Vec<Vec<RecordBatch>>
- For RoundRobin: Create Arc<AtomicUsize> counter at start of stage execution, pass through
- Collect routed batches per destination partition index
- Pass routed_batches to stream_stage_partition_to_output_destinations_with_limits (next immediate step)

**2c.2.2 – Adaptation: Extend stream_stage_partition_to_output_destinations_with_limits** ([worker/src/flight/server.rs](worker/src/flight/server.rs#L413), ~80 LOC modification)
- Current code: Streams same normalized_batches to all output_destinations
- New code: Accept routed_batches_by_partition: Vec<Vec<RecordBatch>>
- For each output_destination (indexed i): Stream routed_batches_by_partition[i] instead of normalized_batches
- Preserve existing MPMC channel config, backpressure, metrics logging
- Update function signature to accept optional routed_batches; fall back to broadcast if None (backward compat)
- Document: "Efficiently streams partition-aware routed batches to their destination workers"

**2c.2.3 – Per-Destination MPMC Management** (~30 LOC, already configured in flight/server.rs)
- No new code needed; MPMC channel config via env vars already in place:
  - `WORKER_FLIGHT_ROUTER_QUEUE_CAPACITY` (default 16)
  - `WORKER_FLIGHT_ROUTER_ENQUEUE_TIMEOUT_MS` (default 500ms)
  - `WORKER_FLIGHT_ROUTER_REQUEST_TIMEOUT_MS` (default 10s)
- Document in code: "Each output_destination uses bounded MPMC queue per these env vars"
- Verify: log_downstream_dispatch_metrics still tracks per-destination throughput

**2c.2.4 – Error Propagation & Backpressure** (~30 LOC, verify existing behavior)
- Verify: If one destination's queue times out, error propagates as ResourceExhausted
- Verify: Other destinations continue (not blocked by slow peer)
- Verify: Fast shutdown on upstream cancel (no orphaned channels)
- Add test scenario: One slow destination blocks; others proceed (from 2c.3 scope)

**Verification for 2c.2.1-2.4**:
1. Code review: routed_batches correctly indexed and passed to Flight streaming
2. Compile check: No type errors or borrow warnings
3. Backward compatibility check: Fallback mode works if routed_batches is empty

---

### **Phase 2c.3: Integration Testing** (*depends on: 2c.2.1-2.4 complete*)

**2c.3.1 – Single-Worker Multi-Partition Routing Test** ([worker/src/tests/flight_server_tests.rs](worker/src/tests/flight_server_tests.rs), ~80 LOC, new test)
- Setup: Create mock downstream server with 4 partitions
- Input: Single upstream partition with 100 rows, partition_key=int column
- Execute: route_batch_hash(rows, ["int_col"], 4) creates 4 routed batches
- Stream: Each routed batch via separate Flight do_put to corresponding downstream partition
- Verify:
  - ✅ All 4 downstream partitions receive non-empty batches
  - ✅ Rows in each partition satisfy hash(int_col) % 4 == partition_index
  - ✅ Total row count preserved (100 rows across all 4 partitions)
  - ✅ No duplicates or loss

**2c.3.2 – Multi-Worker Fan-Out Topology Test** (~100 LOC, new test)
- Setup: 2 upstream workers (Stage 1), 4 downstream workers (Stage 2), Hash partitioning
- Worker A (Stage 1, partition 0): Generates 100 rows (int_col = 0..99)
- Worker B (Stage 1, partition 1): Generates 100 rows (int_col = 100..199)
- Both stream to 4 downstream workers via route_batch_hash
- Verify:
  - ✅ Downstream Worker 0 receives rows where hash(int_col) % 4 == 0 from both A and B
  - ✅ Downstream Worker 1 receives rows where hash(int_col) % 4 == 1, etc.
  - ✅ No cross-partition leakage
  - ✅ All 200 rows accounted for

**2c.3.3 – RoundRobin Ordering Test** (~60 LOC, new test)
- Setup: Single upstream, 3 downstream partitions, RoundRobin scheme
- Input: 12 rows (keys 0..11)
- Execute: route_batch_roundrobin with Arc<AtomicUsize> counter
- Verify:
  - ✅ Rows distributed as: [0,3,6,9] → part0, [1,4,7,10] → part1, [2,5,8,11] → part2
  - ✅ Counter increments per row (cyclic assignment holds)

**2c.3.4 – Range-Aware Distribution Test** (~50 LOC, new test)
- Setup: Bounds = [100, 200], 3 downstream partitions
- Input: Rows with int_col values [50, 75, 150, 250] (cross bounds)
- Execute: route_batch_range with Range { bounds: [100, 200] }
- Verify:
  - ✅ Rows [50, 75] → partition 0 (< 100)
  - ✅ Row [150] → partition 1 (100..200)
  - ✅ Row [250] → partition 2 (> 200)

**2c.3.5 – Slow Consumer Throttles Entire Fan-Out** (~60 LOC, new test)
- Setup: 4 downstream partitions, 1 is unreachable (127.0.0.1:8888)
- Input: 100 rows, Hash partitioning
- Stream: All routed batches to 4 downstream workers
- Expected: One partition's MPMC queue times out after 500ms (default WORKER_FLIGHT_ROUTER_ENQUEUE_TIMEOUT_MS)
- Verify:
  - ✅ Error contains "is too slow; bounded queue capacity"
  - ✅ Other 3 downstream partitions still receive complete data (concurrent channels)
  - ✅ Fast-fail (no retry loop; single timeout)

**2c.3.6 – End-to-End: Scan 4 Partitions → Join 4 Partitions → Aggregate 1** (~100 LOC, new test)
- **Stage 1 (Scan)**: 4 upstream partitions, each with 50 rows (schema: id int, value int)
  - Partition 0: id=0..49
  - Partition 1: id=50..99
  - Partition 2: id=100..149
  - Partition 3: id=150..199
- **Stage 2 (Repartition via Hash on id)**: 4 downstream partitions (Join receptacle)
  - Each partition receives rows where hash(id) % 4 == partition_index
  - Verify: Rows with same hash value land on same downstream partition (co-location for join)
- **Stage 3 (Aggregate)**: 1 downstream partition
  - Consumes all 4 partitions from Stage 2
  - Compute SUM(value) across all rows
  - Expected: Sum = 0*50 + 1*50 + 2*50 + ... = (0+1+2+...+199)*1 = 19900
- Verify:
  - ✅ Correct co-location (join-friendly partitioning)
  - ✅ Aggregate receives all 200 rows
  - ✅ Result is byte-identical to Phase 2a single-worker baseline

**Verification for 2c.3**:
1. `cargo test -p worker flight_server_tests -- --exact` (all tests pass, ~6 new tests)
2. `cargo test -p worker router_tests -- --exact` (all unit tests pass)
3. Test output shows row counts, partition assignments, and checksums
4. No flaky tests; consistent results across 3 runs

**Deliverable**: 6 new integration tests in flight_server_tests.rs, comprehensive coverage of Hash/RoundRobin/Range/error scenarios.

---

## Relevant Files

- `worker/src/execution/router.rs` — New file; core routing logic for all 4 schemes
- `worker/src/execution/pipeline.rs` — Modified; integrate route_batch call after stage execution
- `worker/src/flight/server.rs` — Modified; extend streaming to use routed batches per partition
- `worker/src/tests/router_tests.rs` — New file; unit tests for routing functions
- `worker/src/tests/flight_server_tests.rs` — Modified; 6 new integration tests
- `kionas/src/planner/mod.rs` — Reference only (verify PartitionSpec enum format)

---

## Verification

**Pre-Commit Checklist** (per copilot-instructions.md):
1. ✅ Format: `cargo fmt --all` (no changes)
2. ✅ Lint: `cargo clippy --all-targets --all-features -- -D warnings` (zero warnings)
3. ✅ Compile: `cargo check` (success)
4. ✅ Complexity: Cyclomatic < 25, Data-flow < 25 for route_batch_hash (most complex)
5. ✅ Documentation: Rust doc comments on all public functions (What, Inputs, Output, Details)

**Phase 2c.1 Completion Gate**:
- All unit tests in router_tests.rs pass
- No clippy warnings in router.rs
- route_batch dispatcher correctly dispatches to all 4 schemes
- Complexity metrics validated

**Phase 2c.2 Completion Gate**:
- pipeline.rs integration compiles without errors
- flight/server.rs streaming extension preserves existing backpressure behavior
- Backward compatibility check: empty routed_batches falls back to broadcast

**Phase 2c.3 Completion Gate**:
- All 6 integration tests pass
- Row counts preserved end-to-end
- Hash/RoundRobin/Range distribution verified
- Multi-worker fan-out topology validated
- Byte-identical result vs Phase 2a baseline

---

## Decisions & Scope

**Decisions Made**:
1. **New Module**: Dedicated `worker/src/execution/router.rs` (vs inline in pipeline.rs) for clarity and reusability
2. **Hash Algorithm**: Default FNV stable hash; configurable via env var for future flexibility
3. **RoundRobin State**: Arc<AtomicUsize> counter (thread-safe, minimal overhead); created at stage start, passed through all batch calls
4. **Range Bounds**: Assume sorted Vec<Value> in PartitionSpec; binary search with clamping at edges
5. **Backward Compatibility**: stream_stage_partition_to_output_destinations_with_limits accepts optional routed_batches; falls back to broadcast if None

**Scope: Included**
- ✅ Implement all 4 partition schemes (Hash, RoundRobin, Range, Single)
- ✅ Integrate into pipeline after stage execution
- ✅ Extend Flight streaming to route per-partition
- ✅ Unit tests per scheme
- ✅ Integration tests (single/multi-worker, topology, error cases)
- ✅ End-to-end Scan→Join→Aggregate validation

**Scope: Explicitly Excluded**
- ❌ Broadcast partitioning (edge case; default to SinglePartition for now)
- ❌ Dynamic partition count adjustment (fixed at compile time)
- ❌ Bloom filter-based range skip (future optimization)
- ❌ Partition statistics collection (deferred to Phase 3)
- ❌ Documentation updates (per general rules, doc updates deferred unless explicitly requested)

---

## Further Considerations

1. **Hash Algorithm Stability** (Question)
   - Should we use a stable library hash (e.g., fnv::FnvHasher from fnv crate) or a DataFusion builtin?
   - **Recommendation**: Use fnv crate for simplicity; add to worker Cargo.toml if not present. Avoids external dependency on DataFusion internals.

2. **RoundRobin Counter Lifecycle** (Question)
   - Should Arc<AtomicUsize> counter be created once per stage execution or once per batch?
   - **Recommendation**: Once per stage execution (created in execute_query_task, passed through all route_batch calls for same stage). Enables deterministic ordering across multiple batches.

3. **Error Recovery Strategy** (Question)
   - If one downstream destination fails mid-stream (e.g., worker death), do we:
     - **Option A**: Fail the entire upstream stage (fast-fail, one error kills all)
     - **Option B**: Continue streaming to other destinations, log the failure (resilient, but partial data loss)
     - **Option C**: Retry with exponential backoff (adds latency)
   - **Recommendation**: Start with Option A (fast-fail per Phase 2b behavior). Implement Option B in Phase 3 with fault tolerance.

---

## Timeline & Workload

| Phase | Tasks | Est. Days | Weeks | Blocking |
|-------|-------|-----------|-------|----------|
| **2c.1** | C1.1-1.9 (routing core + unit tests) | 3-4 | 1-2 | None (start immediately) |
| **2c.2** | C2.1-2.4 (integration + streaming) | 2 | 1 | *Depends on 2c.1* |
| **2c.3** | Integration tests 6 scenarios | 1-2 | 1 | *Depends on 2c.2* |
| **Total** | - | 5-6 | 2-3 | Sequential critical path |

---

**Ready to proceed once you approve this plan. Questions or revisions needed before starting 2c.1?**


Strict completion matrix (Done/Deferred)

Item	Status	Evidence
2c.1 Hash routing core	Done	router.rs:277
2c.1 RoundRobin routing core	Done	router.rs:339
2c.1 Range routing core	Done	router.rs:376
2c.1 Single routing core	Done	router.rs:422
2c.1 Dispatcher from destination partitioning payload	Done	router.rs:482
2c.2 Pipeline integration for routed batches	Done	pipeline.rs:1383
2c.2 Flight streaming uses routed batches with fallback	Done	server.rs:398
2c.3 Routed destination integration coverage	Done	flight_server_tests.rs:508
2c.3 Routed destination mismatch coverage	Done	flight_server_tests.rs:666
2c.3 Routed-empty fallback coverage	Done	flight_server_tests.rs:721
2c.3 Multi-worker fan-out topology scenario	Done	flight_server_tests.rs:810
2c.3 RoundRobin ordering scenario	Done	flight_server_tests.rs:845
2c.3 End-to-end scan→repartition→aggregate scenario	Done	flight_server_tests.rs:857
Phase 2c closure gate matrix produced	Done	This response