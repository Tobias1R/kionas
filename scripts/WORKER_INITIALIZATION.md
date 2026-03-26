# Worker Initialization & Consul Registration Guide

This directory contains scripts for initializing Kionas workers and registering them with Consul for service discovery.

## Overview

Kionas uses Consul for worker service discovery. When a worker is deployed, it must be registered with Consul so that:
- The server can discover all available workers
- The server can route queries to healthy workers
- Health checks automatically detect and remove failed workers

These scripts provide an easy way to register, deregister, and manage worker services.

## Scripts

### 1. `register_worker.sh` — Register a Single Worker

Register a new worker with Consul.

**Usage:**
```bash
# Basic form (positional arguments)
./register_worker.sh W1 worker1.local 50051

# Extended form (named arguments)
./register_worker.sh --worker-id W1 --host worker1.local --port 50051 --consul http://consul:8500

# With custom health check settings
./register_worker.sh W2 worker2.local 50051 --health-interval 5s --deregister-after 15s

# Verbose output
./register_worker.sh W1 worker1.local 50051 --verbose
```

**What it does:**
1. Verifies Consul is reachable
2. Creates a service registration JSON with:
   - Worker ID (e.g., "W1")
   - Hostname and port
   - Health check configuration
   - Metadata (version, flight port, registration timestamp)
3. Posts registration to Consul's `/v1/agent/service/register` endpoint

**Output:**
```
[1/3] Verifying Consul connectivity...
✓ Consul is reachable
[2/3] Preparing service registration...
[3/3] Registering worker service with Consul...
✓ Worker registered successfully
  Service ID: worker-W1
  Address: worker1.local:50051
```

**Environment variables:**
- `CONSUL_URL` — Override Consul URL (default: http://localhost:8500)
- `WORKER_ID` — Override worker ID
- `WORKER_HOST` — Override worker host
- `WORKER_PORT` — Override worker port

---

### 2. `register_cluster.sh` — Register All Workers

Register all workers in the cluster at once by reading configuration files.

**Usage:**
```bash
# Use defaults
./register_cluster.sh

# Custom Consul URL
./register_cluster.sh --consul http://consul-server:8500

# Custom configs directory
./register_cluster.sh --configs-dir /etc/kionas/configs

# Verbose output
./register_cluster.sh --verbose
```

**What it does:**
1. Finds all `kionas-worker*.json` and `kionas-worker*.toml` files in the configs directory
2. Parses each file to extract worker ID, hostname, and port
3. Calls `register_worker.sh` for each worker
4. Displays summary of successful/failed registrations

**Prerequisites:**
- `jq` must be installed (for JSON parsing)
- Worker config files (e.g., `kionas-worker1.json`, `kionas-worker2.json`) must exist

**Output:**
```
╔════════════════════════════════════════════════════════════╗
║  Kionas Cluster Worker Registration                        ║
╚════════════════════════════════════════════════════════════╝

Configuration:
  Consul URL: http://localhost:8500
  Configs Dir: ./configs

Found 3 worker configuration file(s):
  - kionas-worker1.json
  - kionas-worker2.json
  - kionas-worker3.json

[1/3] Processing kionas-worker1.json...
✓ Worker registered successfully
  Service ID: worker-W1
  Address: worker1.local:50051

...

═══════════════════════════════════════════════════════════
Registration Summary:
  Total: 3
  Success: 3
  Failed: 0

✓ All workers registered successfully

Verification:
To verify registration, run:
  curl -s http://localhost:8500/v1/catalog/service/worker | jq '.[] | {ID, Address, Port}'
```

---

### 3. `deregister_worker.sh` — Deregister Workers

Remove one or more workers from Consul.

**Usage:**
```bash
# Deregister single worker
./deregister_worker.sh W1

# Deregister multiple workers
./deregister_worker.sh W1 W2 W3

# Deregister all workers
./deregister_worker.sh --all

# Deregister without confirmation
./deregister_worker.sh --all --force

# Custom Consul URL
./deregister_worker.sh -c http://consul-server:8500 W1
```

**What it does:**
1. Lists workers to be deregistered
2. Prompts for confirmation (unless `--force` used)
3. Calls Consul's `/v1/agent/service/deregister` endpoint for each worker
4. Displays summary

**⚠️ Warning:** In-flight queries on deregistered workers will timeout after ~30 seconds.

**Output:**
```
[1/3] Verifying Consul connectivity...
✓ Consul is reachable
[2/3] Identifying workers to deregister...

Workers to deregister:
  - W1
  - W2

⚠ This action will deregister 2 worker(s)
⚠ In-flight queries on these workers will timeout
Continue? (yes/no): yes
[3/3] Deregistering workers...

✓ Deregistered: W1 (service-id: worker-W1)
✓ Deregistered: W2 (service-id: worker-W2)

Deregistration Summary:
  Total: 2
  Success: 2
  Failed: 0

✓ All workers deregistered successfully
```

---

### 4. `worker_service_mgmt.sh` — Manage & Query Worker Services

Query and monitor worker services in Consul.

**Usage:**
```bash
# List all workers (table format)
./worker_service_mgmt.sh list

# List in JSON format
./worker_service_mgmt.sh list json

# Show health status of all workers
./worker_service_mgmt.sh status

# Show detailed health for specific worker
./worker_service_mgmt.sh health W1

# Show detailed information
./worker_service_mgmt.sh info W1

# Verify connectivity to all workers
./worker_service_mgmt.sh verify

# Count registered workers
./worker_service_mgmt.sh count
```

**Output examples:**

**List:**
```
Registered Workers: 3

ID     Address              Hostname             Port
────────────────────────────────────────────────────────
W1     192.168.1.10         worker1              50051
W2     192.168.1.11         worker2              50051
W3     192.168.1.12         worker3              50051
```

**Status:**
```
Worker Health Status: 3 registered

ID     Address              Status
──────────────────────────────────────────
W1     192.168.1.10:50051   passing
W2     192.168.1.11:50051   passing
W3     192.168.1.12:50051   passing
```

**Info:**
```
═══════════════════════════════════════════════════════════
Worker Information: W1
═══════════════════════════════════════════════════════════

Service Details:
  ID:            worker-W1
  Address:       192.168.1.10
  Interops Port: 50051
  Flight Port:   50052
  Node:          worker1
  Status:        passing

Tags:
  - kionas
  - worker
  - W1

Metadata:
  version: 2.0
  flight_port: 50052
  registered_at: 2026-03-25T14:30:00Z

Health Checks:
  Service Check: passing
```

**Verify:**
```
Verifying connectivity to all workers...

Checking W1 (192.168.1.10:50051)... ✓ Reachable
Checking W2 (192.168.1.11:50051)... ✓ Reachable
Checking W3 (192.168.1.12:50051)... ✓ Reachable
```

---

## Quick Start Guide

### 1. Initialize a New Development Cluster

```bash
# Generate worker configs (if not already present)
python3 scripts/generate_worker_configs.py

# Register all workers with Consul
./scripts/register_cluster.sh
```

### 2. Add a New Worker to Running Cluster

```bash
# 1. Deploy worker binary and start process on new host
ssh worker-new.local
docker run -d --name kionas-worker4 \
  -e WORKER_ID=W4 \
  -e CONSUL_URL=http://consul:8500 \
  kionas-worker:latest

# 2. Register with Consul
./scripts/register_worker.sh W4 worker-new.local 50051

# 3. Verify
./scripts/worker_service_mgmt.sh list
./scripts/worker_service_mgmt.sh verify
```

### 3. Remove Worker for Maintenance

```bash
# 1. Deregister from Consul
./scripts/deregister_worker.sh W2

# 2. Wait for in-flight queries to timeout (30 sec)
sleep 35

# 3. Stop worker process
docker stop kionas-worker2

# 4. Perform maintenance...
# 5. Restart worker
docker start kionas-worker2

# 6. Re-register with Consul
./scripts/register_worker.sh W2 worker2.local 50051
```

### 4. Monitor Worker Health

```bash
# Check all workers
./scripts/worker_service_mgmt.sh status

# Get detailed info
./scripts/worker_service_mgmt.sh info W1

# Test connectivity
./scripts/worker_service_mgmt.sh verify

# Count workers
./scripts/worker_service_mgmt.sh count
```

---

## Integration with Operational Procedures

These scripts are referenced in the Operational Runbook (`docs/OPERATIONAL_RUNBOOK.md`):

- **Section 2.1 (Adding a Worker)**: Use `register_worker.sh`
- **Section 2.2 (Removing a Worker)**: Use `deregister_worker.sh`
- **Section 2.3 (Rolling Restart)**: Use both registration scripts
- **Section 3 (Diagnostics)**: Use `worker_service_mgmt.sh list`

---

## Troubleshooting

### Issue: "Connection refused"

```bash
# Verify Consul is running
curl http://localhost:8500/v1/status/leader
```

**Solution**: Start Consul or check URL:
```bash
export CONSUL_URL=http://consul-server:8500
./scripts/register_worker.sh W1 worker1.local 50051
```

### Issue: "No workers found"

```bash
./worker_service_mgmt.sh count
# Output: 0
```

**Solution**: Ensure worker configs exist and registration ran:
```bash
# Generate configs
python3 generate_worker_configs.py

# Register
./register_cluster.sh
```

### Issue: Worker shows "critical" status

```bash
./worker_service_mgmt.sh status
# Shows: W1 ... critical
```

**Solution**: Check worker connectivity:
```bash
./worker_service_mgmt.sh verify
./worker_service_mgmt.sh health W1
```

Typical causes:
- Worker process crashed
- Network connectivity issue
- Health check timeout (increase `--health-timeout`)

---

## Health Check Configuration

Each registered worker includes a TCP health check:

```json
{
  "Check": {
    "TCP": "worker1.local:50051",
    "Interval": "10s",           // Check every 10 seconds
    "Timeout": "5s",              // Timeout after 5 seconds
    "DeregisterCriticalServiceAfter": "30s"  // Remove if critical for 30s
  }
}
```

**To customize health checks at registration time:**

```bash
./register_worker.sh W1 worker1.local 50051 \
  --health-interval 5s \
  --health-timeout 3s \
  --deregister-after 60s
```

---

## Script Requirements

All scripts require:
- `bash` 4.0+
- `curl`
- `jq` (for `register_cluster.sh` and `worker_service_mgmt.sh`)
- Consul server at `$CONSUL_URL` (default: http://localhost:8500)

To install dependencies:

```bash
# Ubuntu/Debian
apt-get install curl jq

# macOS
brew install curl jq

# CentOS/RHEL
yum install curl jq
```

---

## Reference

### Environment Variables

- `CONSUL_URL` — Consul API endpoint (default: http://localhost:8500)
- `CONFIGS_DIR` — Worker configs directory (default: ./configs)

### Consul API Endpoints Used

- `GET /v1/status/leader` — Check Consul health
- `PUT /v1/agent/service/register` — Register service
- `PUT /v1/agent/service/deregister/{id}` — Deregister service
- `GET /v1/catalog/service/worker` — List workers
- `GET /v1/health/service/worker` — Get health checks

### Worker Service Structure

```json
{
  "ID": "worker-W1",
  "Name": "worker",
  "Address": "worker1.local",
  "Port": 50051,
  "Tags": ["kionas", "worker", "W1"],
  "Meta": {
    "version": "2.0",
    "flight_port": "50052",
    "registered_at": "2026-03-25T14:30:00Z"
  }
}
```

---

## Document Status

✅ **COMPLETE** — Ready for production use

Last updated: March 25, 2026
