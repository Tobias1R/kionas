---
title: "Operational Runbook for Kionas Distributed Execution"
subtitle: "Step-by-Step Procedures and Diagnostics for Production Operations"
date: "March 25, 2026"
status: "COMPLETE"
audience: "Operations / DevOps Teams"
---

# Operational Runbook for Kionas

## Table of Contents

1. [Quick Reference](#quick-reference)
2. [Cluster Scaling](#cluster-scaling)
3. [Partition Assignment Diagnostics](#partition-assignment-diagnostics)
4. [Mid-Stream Failure Troubleshooting](#mid-stream-failure-troubleshooting)
5. [Memory Pressure Management](#memory-pressure-management)
6. [Telemetry Sign-Off Checklist](#telemetry-sign-off-checklist)
7. [Common Scenarios & Remediation](#common-scenarios--remediation)
8. [Monitoring & Observability](#monitoring--observability)
9. [Runbook Configuration](#runbook-configuration)

---

## Quick Reference

### Command Cheat Sheet

| Task | Command | Purpose |
|---|---|---|
| **Check active workers** | `curl http://consul:8500/v1/catalog/service/worker` | List live workers in Consul |
| **Register new worker** | `curl -X PUT http://consul:8500/v1/agent/service/register -d @worker_reg.json` | Add worker to cluster |
| **De-register worker** | `curl -X PUT http://consul:8500/v1/agent/service/deregister/worker-W4` | Remove worker (graceful) |
| **Dump query plan** | `./client --dump-plan "SELECT * FROM t"` | View stage extraction |
| **Check worker memory** | `curl -s worker:9090/metrics \| grep memory_usage_bytes` | Prometheus metrics |
| **Restart worker** | `docker restart kionas-worker1` | Graceful restart (in-flight queries timeout) |
| **View server logs** | `docker logs kionas-server \| grep "Stage"` | Search for stage-related events |
| **Monitor telemetry** | `./client --query "SELECT * FROM t" --telemetry-format json` | Detailed telemetry output |

---

## Cluster Scaling

### 2.1 Adding a Worker to an Active Cluster

#### Prerequisites
- New worker binary deployed and running on target host
- Worker listens on standard ports (50051 for interops, 50052 for Flight)
- Network connectivity verified: `ping worker-new.local`

#### Procedure

**Step 1: Verify new worker is running**

```bash
# SSH to new worker host
ssh worker-new.local

# Check if worker process is alive
ps aux | grep kionas-worker

# Verify ports are open
netstat -tlnp | grep LISTEN | grep -E "50051|50052"

# Expected output:
# tcp    0    0 0.0.0.0:50051    0.0.0.0:*    LISTEN    12345/kionas-worker
# tcp    0    0 0.0.0.0:50052    0.0.0.0:*    LISTEN    12345/kionas-worker
```

**Step 2: Register worker with Consul**

```bash
# Create registration JSON
cat > /tmp/worker_registration.json << 'EOF'
{
  "ID": "worker-W4",
  "Name": "worker",
  "Address": "worker-new.local",
  "Port": 50051,
  "Tags": ["kionas", "worker"],
  "Check": {
    "HTTP": "http://worker-new.local:50051/health",
    "Interval": "10s",
    "Timeout": "5s",
    "DeregisterCriticalServiceAfter": "30s"
  }
}
EOF

# Register with Consul
curl -X PUT http://consul-server:8500/v1/agent/service/register \
  -d @/tmp/worker_registration.json

echo "Worker registered"
```

**Step 3: Verify registration**

```bash
# Wait 10 seconds for health check
sleep 10

# Verify worker appears in service catalog
curl -s http://consul-server:8500/v1/catalog/service/worker | jq '.[] | {ID, Address, Status}'

# Expected output:
# {
#   "ID": "worker-W4",
#   "Address": "worker-new.local",
#   "Status": "passing"
# }
```

**Step 4: Server auto-detection**

```bash
# Server queries Consul every 60 seconds
# Monitor server logs for detection event
docker logs kionas-server | tail -20 | grep -i "worker"

# Expected log line:
# [INFO] Detected new worker: W4 (worker-new.local:50051)
```

**Step 5: Validate with query**

```bash
# Run a distributed query (e.g., hash join) to trigger routing updates
./client --query "SELECT * FROM a JOIN b ON a.id = b.id LIMIT 5"

# Check if query succeeds
# If yes: New worker is integrated ✅
```

#### Rollback (if issues arise)

```bash
# De-register worker immediately
curl -X PUT http://consul-server:8500/v1/agent/service/deregister/worker-W4

# Verify removal (may take up to 30 seconds)
sleep 35
curl -s http://consul-server:8500/v1/catalog/service/worker | jq 'length'

# Expected: Worker count decreased by 1
```

#### Expected Timeline
- **Step 1–2**: 2–3 minutes
- **Step 3–4**: 1–2 minutes (+ 30 sec health check window)
- **Step 5**: <1 minute
- **Total**: 5–10 minutes from startup to full integration

---

### 2.2 Removing a Worker from an Active Cluster

#### Procedure

**Step 1: Drain ongoing queries (optional)**

```bash
# If graceful drain is needed:
# 1. Stop accepting new queries (application level)
# 2. Wait for in-flight queries to complete (check query count)

# Check current query count
curl -s worker:9090/metrics | grep query_count

# Wait for count to approach 0 (may take 5–10 minutes for long-running queries)
```

**Step 2: De-register from Consul**

```bash
# Remove worker from service catalog
curl -X PUT http://consul-server:8500/v1/agent/service/deregister/worker-W3

# Verify removal
sleep 5
curl -s http://consul-server:8500/v1/catalog/service/worker | jq '.[] | {ID, Address}'
```

**Step 3: Shut down worker process**

```bash
# SSH to worker host
ssh worker-W3.local

# Graceful shutdown (drains in-flight batches)
docker stop kionas-worker1 --time 60

# Verify process is gone
ps aux | grep kionas-worker | grep -v grep
# Expected: No output (process exited)
```

**Step 4: Verify partition reassignment**

```bash
# Next distributed query will use remaining workers
# Server will recompute partition assignment (excludes removed worker)

./client --query "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id"

# Monitor logs
docker logs kionas-server | grep -E "partition|routing"

# Expected: No errors; query completes with new partitioning
```

#### Impact on In-Flight Queries

**Before de-registration**:
- In-flight stages targeting removed worker will timeout (30 sec default)
- Client receives error: `"Worker W3 disconnected"`
- Application retries query (if configured)

**Mitigation**: Drain queries before de-registering (Step 1) or allow timeout window before shutdown.

#### Recovery: Bringing Worker Back

```bash
# Repeat Section 2.1 (Adding a Worker)
# Server detects re-registration within 60 seconds
# No manual partition rebalancing needed (automatic)
```

---

### 2.3 Rolling Restart of All Workers

Use this procedure during deployment (e.g., code updates, configuration changes).

#### Procedure

**Step 1: Notify applications**

```bash
# Send message to ops: "Cluster maintenance window 10 AM–10:30 AM"
# Long-running queries (>5 min) should be cancelled
```

**Step 2: Restart workers one at a time**

```bash
# For each worker (W1, W2, W3, ...):

WORKER_NAME="W1"

# 1. De-register from Consul
curl -X PUT http://consul-server:8500/v1/agent/service/deregister/worker-${WORKER_NAME}

# 2. Wait for in-flight queries to timeout (30 sec)
sleep 35

# 3. Restart worker process
docker restart kionas-${WORKER_NAME}

# 4. Wait for health check to pass (10 sec default)
sleep 15

# 5. Verify re-registration
curl -s http://consul-server:8500/v1/catalog/service/worker | jq '.[] | select(.ID | contains("'${WORKER_NAME}'"))'

# 6. Move to next worker
sleep 10  # Brief pause before next worker
```

**Step 3: Post-restart validation**

```bash
# After all workers restarted, run test queries
./client --query "SELECT COUNT(*) FROM a"  # Single-worker
./client --query "SELECT COUNT(*) FROM a JOIN b ON a.id = b.id"  # Distributed

# Monitor logs for errors
docker logs kionas-server | grep -i "error" | head -10
docker logs kionas-worker1 | grep -i "error" | head -10
```

#### Timeline
- Per worker: 3–5 minutes (including health check + buffer)
- 4 workers: 15–20 minutes total

---

## Partition Assignment Diagnostics

### 3.1 Inspecting Stage → Partition Layout

#### Command: View Query Execution Plan

```bash
# Extract and display stage/partition information
./client --dump-plan "SELECT o.id, c.name, SUM(o.amount) 
                       FROM orders o 
                       JOIN customers c ON o.customer_id = c.id 
                       GROUP BY o.id, c.name"
```

#### Expected Output

```json
{
  "stages": [
    {
      "stage_id": 0,
      "operator": "TableScan",
      "partition_count": 1,
      "partition_strategy": "single",
      "workers": ["W1", "W2", "W3"],
      "description": "Initial scan on each worker"
    },
    {
      "stage_id": 1,
      "operator": "Join",
      "partition_count": 12,
      "partition_strategy": "hash",
      "partition_key": "customer_id",
      "partition_assignment": {
        "0": "W1",
        "1": "W2",
        "2": "W3",
        "3": "W1",
        "4": "W2",
        "5": "W3",
        "6": "W1",
        "7": "W2",
        "8": "W3",
        "9": "W1",
        "10": "W2",
        "11": "W3"
      },
      "workers": ["W1", "W2", "W3"]
    },
    {
      "stage_id": 2,
      "operator": "Aggregate",
      "partition_count": 4,
      "partition_strategy": "hash",
      "partition_key": "customer_id",
      "workers": ["W1", "W2", "W3"]
    }
  ]
}
```

#### Interpretation Guide

| Field | Meaning | Expected Values |
|---|---|---|
| `stage_id` | Stage number (0=scan, 1+=shuffled) | 0, 1, 2, ... |
| `operator` | Logical operator type | TableScan, Join, Aggregate, Sort, ... |
| `partition_count` | Number of partitions for this stage | 1 (single), 4, 8, 12, 16 (common) |
| `partition_strategy` | How data is divided | "single", "hash", "range", "roundrobin" |
| `partition_key` | Columns used for hashing | e.g., "customer_id", "order_date" |
| `partition_assignment` | Where each partition runs | {"0": "W1", "1": "W2", ...} |

#### Common Patterns

**Pattern 1: Single-worker query (no shuffles)**
```json
{
  "stages": [
    {
      "stage_id": 0,
      "partition_count": 1,
      "partition_strategy": "single"
    }
  ]
}
```
✅ Expected for: `SELECT * FROM t WHERE x > 5` (no joins, no aggregates)

**Pattern 2: Join query (two shuffles)**
```json
{
  "stages": [
    {stage_id: 0, partition_strategy: "single"},
    {stage_id: 1, partition_strategy: "hash"},  // Join shuffle
    {stage_id: 2, partition_strategy: "hash"}   // Aggregate shuffle
  ]
}
```
✅ Expected for: `SELECT * FROM a JOIN b ... GROUP BY ...`

**Pattern 3: Uneven partition assignment (4 workers, 12 partitions)**
```json
{
  "partition_assignment": {
    "0": "W1", "1": "W2", "2": "W3", "3": "W4",  // Round 1
    "4": "W1", "5": "W2", "6": "W3", "7": "W4",  // Round 2
    "8": "W1", "9": "W2", "10": "W3", "11": "W4" // Round 3
  }
}
```
✅ Expected behavior: Load balanced via round-robin (each worker gets 3 partitions)

---

### 3.2 Debugging Uneven Partition Distribution

#### Symptom: "One worker is much slower than others"

**Step 1: Extract partition plan**

```bash
./client --dump-plan "SELECT * FROM a JOIN b ON a.id = b.id" | jq '.stages[1].partition_assignment' > /tmp/partitions.json

# Count partitions per worker
cat /tmp/partitions.json | jq 'to_entries[] | "\(.value)": .key' | sort | uniq -c
```

**Step 2: Check worker data sizes**

```bash
# If partition assignment looks correct, data skew might be the issue

# Estimate data per partition
./client --query "SELECT partition_id, COUNT(*) as row_count FROM (
  SELECT CAST(hash(customer_id) AS INT) % 12 as partition_id
  FROM a
) GROUP BY partition_id ORDER BY row_count DESC"

# Output should show relatively balanced row counts
# If skewed: Data is not uniformly distributed on partition key
```

**Step 3: Monitor worker metrics during execution**

```bash
# In a separate terminal, run query and monitor workers
for worker in W1 W2 W3; do
  echo "=== $worker ==="
  curl -s ${worker}:9090/metrics | grep -E "query_duration|memory_used_bytes" | head -3
done
```

#### Possible Causes & Remediation

| Symptom | Cause | Remediation |
|---|---|---|
| Partition counts uneven (e.g., W1 gets 5, W2 gets 3 partitions) | Poor modulo distribution or worker count not aligned | Document expected distribution; verify worker list is stable |
| All partitions assigned to one worker | Worker discovery returned single worker | Check Consul service catalog; ensure all workers registered |
| Data heavily skewed on partition key | Partition key has non-uniform distribution (e.g., 90% NULL) | Use different partition key; apply data preprocessing |

---

## Mid-Stream Failure Troubleshooting

### 4.1 Diagnosing Query Failures

#### Common Error Messages

| Error | Stage | Root Cause | Action |
|---|---|---|---|
| `"Worker W2 disconnected mid-shuffle"` | 1+ | Network issue or worker crash | Check worker health; retry query |
| `"Timeout waiting for Stage 1 output"` | 1+ | Slow downstream worker or memory pressure | Monitor memory; increase timeout or add workers |
| `"Partition ID out of bounds"` | 1+ | Hash collision or routing misconfiguration | Verify partition count matches destination map |
| `"Invalid stage_id in params"` | 1+ | Version mismatch between server and worker | Ensure all binaries are same version |

### 4.2 Failure Scenario: "Worker W2 Disconnected Mid-Aggregate"

#### Symptom

```
Client query fails with:
Query failed: Worker W2 disconnected during Stage 2 (Aggregate)
Latency: 5.2s (timed out at 30s)
```

#### Diagnosis Steps

**Step 1: Check worker health**

```bash
# Option A: Check Consul
curl -s http://consul-server:8500/v1/catalog/service/worker | jq '.[] | {ID, Status}'

# Option B: Direct ping
ssh worker-W2.local -c "ps aux | grep kionas-worker"
ps aux | grep kionas-worker | grep -v grep
# Expected: Process should be running
```

**Step 2: Check worker logs**

```bash
# View recent error logs
docker logs kionas-worker2 --tail=100 | grep -i "error\|panic\|fail"

# Look for patterns:
# - "OOM killed" → Memory pressure
# - "Connection reset" → Network issue
# - "Stage timeout" → Processing took too long
```

**Step 3: Check network connectivity**

```bash
# From server to worker
ping worker-W2.local
curl -v http://worker-W2.local:50052/health

# Expected: TCP connection established within 100ms
```

**Step 4: Monitor worker resource utilization**

```bash
# SSH to worker
ssh worker-W2.local

# Check memory
free -h
# Expected: Available memory > 1GB

# Check CPU
top -b -n 1 | head -10
# Expected: CPU usage < 80%

# Check disk
df -h /
# Expected: Disk space > 20%
```

#### Remediation

**Option A: Retry query (often succeeds)**

```bash
./client --query "SELECT ..." --retry-count 3

# Kionas will retry up to 3 times with exponential backoff
# Often succeeds if failure was transient
```

**Option B: Increase timeout**

```bash
# Temporarily increase stage timeout (negotiate with ops)
export KIONAS_STAGE_TIMEOUT_MS=60000  # 60 seconds instead of 30

./client --query "SELECT ..."
```

**Option C: Reduce query load**

```bash
# Reduce batch size to lower memory pressure
# Edit kionas-worker2.json:
# "execution": {
#   "batch_size_rows": 4096  // reduced from 8192
# }

# Restart worker
docker restart kionas-worker2

# Retry query
./client --query "SELECT ..."
```

**Option D: Check & fix worker**

```bash
# If worker is stuck:
docker inspect kionas-worker2 | jq '.State'

# If status is "running" but unresponsive:
docker restart kionas-worker2 --time 60

# Verify restart succeeded
docker logs kionas-worker2 | tail -5 | grep "Started\|Ready"
```

#### Prevention

```bash
# Monitor worker metrics proactively
curl -s worker:9090/metrics | grep memory_used_bytes | grep -o '[0-9]\+' | \
  awk '{if ($1 > 8589934592) print "WARNING: Memory > 8GB"}'

# Set up alerts (e.g., Prometheus):
# alert if memory_used_bytes > 8GB or cpu > 80%
```

---

## Memory Pressure Management

### 5.1 Identifying Memory Pressure

#### Metrics to Monitor

```bash
# Primary indicator: Memory usage per stage
./client --query "SELECT ..." --telemetry-format json | jq '.stages[] | {stage_id, memory_peak_mb}'

# Expected output:
# {
#   "stage_id": 0,
#   "memory_peak_mb": 128
# },
# {
#   "stage_id": 1,
#   "memory_peak_mb": 512
# }
```

#### Warning Thresholds

| Metric | Threshold | Action |
|---|---|---|
| Per-stage memory | > 80% of configured limit | Reduce batch size by 25% |
| Worker total memory | > 85% of system memory | Add worker to cluster |
| Memory spill detected | Yes | Check data distribution; may indicate skew |

### 5.2 Reducing Memory Usage

#### Option 1: Reduce Batch Size

```bash
# Edit worker config (all workers)
# kionas-worker1.json, kionas-worker2.json, etc.

# Current:
# "execution": {"batch_size_rows": 8192}

# Reduced:
# "execution": {"batch_size_rows": 4096}

# Restart workers (one at a time, rolling restart)
docker restart kionas-worker1

# Verify
docker logs kionas-worker1 | tail -5 | grep "batch_size\|Ready"
```

**Trade-off**: Lower throughput (~10% loss) but safer memory utilization

#### Option 2: Add Worker to Cluster

```bash
# Adds parallelism; partitions are distributed across more workers
# Each worker processes fewer partitions; memory pressure decreases

# Procedure: Follow Section 2.1 (Adding a Worker)

# Verify effect
./client --query "SELECT COUNT(*) FROM a JOIN b ... GROUP BY ..." --telemetry-format json

# Compare telemetry:
# Before: memory_peak_mb: 600
# After:  memory_peak_mb: 350  (distributed across 4 workers instead of 3)
```

#### Option 3: Optimize Data Layout

```bash
# If memory spike on specific column, consider compression
# Example: UUID column (128 bits) → dictionary encoding (8 bits after dedup)

# Step 1: Check column stats
./client --query "SELECT COUNT(DISTINCT column_name) FROM table" 

# If distinct count << total rows: High compression potential

# Step 2: Apply encoding (at table creation time)
-- CREATE TABLE with dictionary encoding
-- (requires DataFusion integration; check docs)
```

### 5.3 Memory Spill Handling

Kionas may spill aggregation data to disk if memory threshold exceeded.

#### Detecting Spills

```bash
# Check worker logs
docker logs kionas-worker1 | grep -i "spill"

# Expected if spill occurs:
# [WARN] Stage 2 Aggregate memory spill triggered (50MB → disk)
```

#### Performance Impact

- **With spill**: Query latency +20–50% (disk I/O slower than RAM)
- **Mitigation**: Reduce batch size or add workers before spill occurs

#### Configuration

```json
// kionas-worker1.json
{
  "execution": {
    "memory_tracking": {
      "enabled": true,
      "spill_threshold_mb": 512,
      "sample_interval_batches": 10
    }
  }
}
```

---

## Telemetry Sign-Off Checklist

### 6.1 Pre-Deployment Validation

Before deploying Kionas to production, operators must validate telemetry across all query types.

#### Checklist

- [ ] **Telemetry Keys Present**: Run a query and verify all expected keys appear in output
  
  ```bash
  ./client --query "SELECT * FROM a JOIN b ON a.id = b.id GROUP BY a.col" \
           --telemetry-format json | jq 'keys' | grep -E "distributed_dag_metrics_json|datafusion_stage_count|distributed_stage_count"
  
  # Expected output (all three keys present):
  # "distributed_dag_metrics_json": {...}
  # "datafusion_stage_count": 3
  # "distributed_stage_count": 2
  ```

- [ ] **Latency Breakdown Per-Stage**: Confirm client can parse and display stage latencies
  
  ```bash
  ./client --query "SELECT COUNT(*) FROM a" --telemetry-format json | jq '.stages[] | {stage_id, queue_ms, exec_ms, network_ms}'
  
  # Expected output:
  # {
  #   "stage_id": 0,
  #   "queue_ms": 5,
  #   "exec_ms": 120,
  #   "network_ms": 0
  # }
  ```

- [ ] **Memory Usage Tracked**: Verify no memory spikes or OOM killers on moderately-sized queries
  
  ```bash
  # Run aggregate query (~1M rows)
  ./client --query "SELECT col, SUM(val) FROM t1 GROUP BY col" \
           --telemetry-format json | jq '.stages[] | {stage_id, memory_peak_mb}'
  
  # Expected: memory_peak_mb < configured limit (no spills)
  # Check worker logs for OOM warnings
  docker logs kionas-worker1 | grep -i "OOM\|memory"
  # Expected: No OOM messages
  ```

- [ ] **Network Bandwidth < 50% of Link Capacity**: Confirm network utilization is reasonable
  
  ```bash
  # Run distributed query (joins); monitor network metrics
  # Collect metrics over 60 seconds
  
  for i in {1..10}; do
    ./client --query "SELECT COUNT(*) FROM a JOIN b JOIN c ... GROUP BY col"
    sleep 6
  done
  
  # Check network telemetry
  ./client --query "SELECT ..." --telemetry-format json | jq '.stages[] | {stage_id, network_bytes_transferred}'
  
  # Expected interpretation:
  # - SCAN stage: network_bytes ≈ 0 (no shuffle)
  # - JOIN stage: network_bytes ≈ 2-5x input table size (hash redistribution)
  # - If > 100MB for small dataset: This may indicate inefficiency
  ```

- [ ] **All Workers Integrated**: Confirm all workers appear in partition assignment
  
  ```bash
  ./client --dump-plan "SELECT ..." | jq '.stages[] | select(.partition_strategy == "hash") | .workers[]'
  
  # Expected output (if 3 workers):
  # "W1"
  # "W2"
  # "W3"
  # No "W4" or undefined values
  ```

- [ ] **Query Types Validated**: Test with SCAN, FILTER, JOIN, AGGREGATE, SORT
  
  ```bash
  # Test each type independently
  
  echo "=== SCAN ==="
  ./client --query "SELECT * FROM a LIMIT 10" --telemetry-format json | jq '.status'
  
  echo "=== FILTER ==="
  ./client --query "SELECT * FROM a WHERE col > 100 LIMIT 10" --telemetry-format json | jq '.status'
  
  echo "=== JOIN ==="
  ./client --query "SELECT * FROM a JOIN b ON a.id = b.id LIMIT 10" --telemetry-format json | jq '.status'
  
  echo "=== AGGREGATE ==="
  ./client --query "SELECT col, COUNT(*) FROM a GROUP BY col LIMIT 10" --telemetry-format json | jq '.status'
  
  echo "=== SORT ==="
  ./client --query "SELECT * FROM a ORDER BY col DESC LIMIT 10" --telemetry-format json | jq '.status'
  
  # Expected: All should return status: "success"
  ```

#### Sign-Off

```
Telemetry Validation Sign-Off
Date: _______________
Operator Name: _______________
Result: ☐ PASS  ☐ FAIL (describe issues below)

Issues (if any):
_________________________________________________________________

Recommendation: ☐ Proceed to Production  ☐ Investigate Issues
```

---

## Common Scenarios & Remediation

### 7.1 Query Hangs (No Output for 60+ Seconds)

#### Symptoms
- Client shows "Waiting for response..." after 60 seconds
- No error message
- Worker logs appear normal

#### Diagnosis

```bash
# Check if query is executing on workers
for worker in W1 W2 W3; do
  echo "=== $worker ==="
  curl -s ${worker}:9090/metrics | grep query_running_count
done

# Expected: query_running_count > 0 on at least one worker

# If query_running_count = 0 everywhere: Query stuck in server (planner/dispatcher)
# If query_running_count > 0 on one worker: Query executing but not progressing
```

#### Remediation

**Option A: Check downstream worker connectivity**

```bash
# Query may be waiting for Stage 2 output from W2
# Check if W2 is reachable

ping worker-W2.local

# Check if Flight port is open
telnet worker-W2.local 50052
# Expected: Connected immediately
```

**Option B: Kill and retry**

```bash
# Interrupt query
Ctrl+C

# Retry (often succeeds if transient network issue)
./client --query "..."
```

**Option C: Check memory pressure on workers**

```bash
# Worker may be paused due to memory management
for worker in W1 W2 W3; do
  echo "=== $worker ==="
  ssh ${worker}.local -c "free -h | head -2"
  ssh ${worker}.local -c "top -b -n 1 | head -3"
done
```

---

### 7.2 Partition Assignment Mismatch Error

#### Symptom

```
Error: "Partition 5 not found in destination map"
```

#### Diagnosis

```bash
# This indicates stage extraction didn't match worker destination map

# Step 1: Check plan
./client --dump-plan "SELECT ..." | jq '.stages[1].partition_count'
# Expected: 12

# Step 2: Check destinations were built with same partition count
# (This is usually internal; check server logs)
docker logs kionas-server | grep -i "partition"
```

#### Remediation

```bash
# Usually indicates a bug; restart server should clear
docker restart kionas-server

# Retry query
./client --query "..."
```

---

### 7.3 Uneven Query Latency (Same Query, Different Times)

#### Scenario
- Query 1: 2 seconds
- Query 2: 5 seconds
- Query 3: 2.1 seconds

(Same query, same data, significant 2.5x variance)

#### Investigation

```bash
# Check if data is being cached
# If Query 2 added rows to table between Query 1 and 2:

SELECT COUNT(*) FROM a;
-- After Query 1: 1000000 rows
-- After Query 2: 2000000 rows  <- Double the data!

# If data size changed, latency variance is expected (linear scaling)

# If data unchanged, check worker load
for i in {1..3}; do
  echo "=== Run $i ==="
  /usr/bin/time -f "Elapsed: %e sec" ./client --query "SELECT ..."
  
  # Check if other processes consuming CPU
  top -b -n 1 | head -10 | grep -E "worker|system"
done
```

---

## Monitoring & Observability

### 8.1 Setting Up Prometheus Metrics

#### Worker Metrics Endpoint

Workers expose metrics on port `9090`:

```bash
curl http://worker-W1.local:9090/metrics | head -20

# Expected output (Prometheus format):
# # HELP query_count Total queries executed
# # TYPE query_count gauge
# query_count{worker="W1"} 42
#
# # HELP memory_used_bytes Memory used (bytes)
# # TYPE memory_used_bytes gauge
# memory_used_bytes{worker="W1"} 536870912
#
# # HELP stage_latency_ms Stage execution latency (ms)
# # TYPE stage_latency_ms histogram
# stage_latency_ms_bucket{le="100",stage="0"} 10
# stage_latency_ms_bucket{le="1000",stage="0"} 35
```

#### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'kionas-workers'
    static_configs:
      - targets: ['worker-W1.local:9090', 'worker-W2.local:9090', 'worker-W3.local:9090']
    relabel_configs:
      - source_labels: [__address__]
        regex: '(worker-W[0-9]+)\..*'
        target_label: 'worker'

  - job_name: 'kionas-server'
    static_configs:
      - targets: ['server.local:8888']
```

#### Alert Rules

```yaml
# alerts.yml
groups:
  - name: kionas
    rules:
      - alert: WorkerMemoryHigh
        expr: memory_used_bytes > 8589934592
        for: 2m
        annotations:
          summary: "Worker {{ $labels.worker }} memory > 8GB"

      - alert: WorkerCrashing
        expr: increase(query_count[5m]) == 0 AND up == 0
        annotations:
          summary: "Worker {{ $labels.worker }} appears crashed"

      - alert: QueryTimeout
        expr: query_latency_p99_ms > 60000
        for: 5m
        annotations:
          summary: "99th percentile query latency > 60s"
```

---

### 8.2 Log Aggregation with ELK Stack

#### Shipping Worker Logs

```bash
# On each worker, pipe logs to Filebeat
docker logs kionas-worker1 -f | filebeat -config.stdout -strict.perms=false \
  -c - > /dev/null 2>&1 &
```

#### Sample Queries (Elasticsearch/Kibana)

```
# Find errors in last 1 hour
timestamp > now - 1h AND level = "ERROR"

# Find Stage 1 timeouts
timestamp > now - 24h AND message = "*timeout*" AND stage_id = 1

# Find memory pressure events
timestamp > now - 7d AND message = "*spill*"
```

---

## Runbook Configuration

### 9.1 Environment-Specific Settings

#### Development Cluster (1–2 workers)

```json
{
  "execution": {
    "batch_size_rows": 8192,
    "stage_timeout_ms": 60000
  },
  "memory_tracking": {
    "enabled": true,
    "spill_threshold_mb": 512
  }
}
```

#### Staging Cluster (3–4 workers)

```json
{
  "execution": {
    "batch_size_rows": 16384,
    "stage_timeout_ms": 30000
  },
  "memory_tracking": {
    "enabled": true,
    "spill_threshold_mb": 1024
  }
}
```

#### Production Cluster (5+ workers)

```json
{
  "execution": {
    "batch_size_rows": 32768,
    "stage_timeout_ms": 30000
  },
  "memory_tracking": {
    "enabled": true,
    "spill_threshold_mb": 2048
  }
}
```

### 9.2 Handling Special Cases

#### Case 1: Very Large Table (>1GB)

```bash
# Reduce batch size to avoid OOM
# kionas-worker1.json:
"batch_size_rows": 4096  # from 8192

# Increase partition count for better distribution
# kionas-metastore.json:
"stage_extraction": {
  "bucket_count_multiplier": 8    # from 4, gives 32 partitions
}
```

#### Case 2: CPU-Bound Query (Join/Aggregate on large dataset)

```bash
# Batch size less important; increase to maximize throughput
"batch_size_rows": 32768

# Add more workers to increase parallelism
# Procedure: Section 2.1
```

#### Case 3: Network-Constrained Environment

```bash
# Reduce batch size to lower network packet size
"batch_size_rows": 2048

# Use range partitioning instead of hash (may reduce shuffle data)
# kionas-metastore.json:
"stage_extraction": {
  "prefer_partition_strategy": "range"
}
```

---

## Appendix: Troubleshooting Decision Tree

```
Query failed
 ├─ Error message contains "Worker ... disconnected"
 │  └─ Go to: Section 4.2 (Mid-Stream Failure)
 │
 ├─ Error message contains "Partition ... out of bounds"
 │  └─ Go to: Section 7.2 (Partition Mismatch)
 │
 ├─ No error; query hangs (60+ seconds)
 │  └─ Go to: Section 7.1 (Query Hangs)
 │
 ├─ Query succeeds but slow (2–3x slower than expected)
 │  ├─ Check worker load: Section 8.1 (Monitoring)
 │  ├─ Check partition distribution: Section 3.2 (Diagnostics)
 │  └─ Check memory pressure: Section 5.1 (Memory Management)
 │
 └─ Intermittent failures (sometimes succeeds, sometimes fails)
    ├─ Retry with: --retry-count 3
    ├─ Check network: Section 4.2 (Failures)
    └─ Check if adding worker helps (reduced load)
```

---

## Document Revision History

| Date | Author | Change |
|---|---|---|
| March 25, 2026 | AI (M4.2) | Initial version; 9 sections, 2000+ words |

---

**Document Status**: ✅ COMPLETE

This operational runbook should be reviewed by operators and DevOps teams before Phase 2d production deployment.
