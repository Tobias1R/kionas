# Worker Configuration & Registration Workflow

This document explains the enhanced worker initialization workflow that combines:
1. **Configuration Management** — Upload worker configs to Consul KV store
2. **Service Registration** — Register worker services for discovery

## Overview: Three Workflow Options

### Option 1: Full Workflow (Recommended for Single Worker)

**Use `configure_worker.sh`:**

```bash
./configure_worker.sh ./configs/kionas-worker1.json
```

**What it does:**
1. ✅ Validates the JSON configuration file
2. ✅ Extracts worker metadata (ID, hostname, port)
3. ✅ Uploads config to Consul KV at `kionas/configs/<HOSTNAME>`
4. ✅ Registers worker service with Consul

**Output:**
```
╔════════════════════════════════════════════════════════════╗
║  Worker Configuration & Registration                       ║
╚════════════════════════════════════════════════════════════╝

Configuration file: kionas-worker1.json

[1/3] Extracting worker information from configuration...
✓ Extracted worker information:
  Worker ID:  W1
  Hostname:   kionas-worker1
  Port:       50051

[2/3] Uploading configuration to Consul...
✓ Configuration uploaded to Consul
  Key: kionas/configs/kionas-worker1

[3/3] Registering worker service with Consul...
✓ Worker registered successfully
  Service ID: worker-W1
  Address: kionas-worker1:50051

═══════════════════════════════════════════════════════════
✓ Configuration and registration complete!

Configured Worker:
  ID:       W1
  Hostname: kionas-worker1
  Port:     50051

Consul KV:
  Key: kionas/configs/kionas-worker1
  Retrieve: curl -s http://localhost:8500/v1/kv/kionas/configs/kionas-worker1 | jq -r '.[].Value' | base64 -d
```

### Option 2: Register All Workers (For Clusters)

**Use `register_cluster.sh`:**

```bash
./register_cluster.sh
```

**What it does:**
1. ✅ Finds all `kionas-worker*.json` files
2. ✅ **Uploads each config to Consul KV** (newly added!)
3. ✅ Registers each worker service
4. ✅ Shows summary

**Key Difference from Option 1:**
- `register_cluster.sh` processes all workers at once
- Each config is uploaded to `kionas/configs/<HOSTNAME>`
- Each worker is registered as a service
- Good for cluster initialization

### Option 3: Separate Operations

If you need more control, use individual steps:

```bash
# Step 1: Upload only (skip registration)
./configure_worker.sh ./configs/kionas-worker1.json --skip-registration

# Step 2: Register only (assumes config already uploaded)
./configure_worker.sh ./configs/kionas-worker1.json --skip-upload

# Or use the registration script directly
./register_worker.sh W1 worker1.local 50051
```

---

## Understanding the Config Upload

### Why Upload Configs to Consul?

When worker configs are stored in Consul KV, the server can:
- Retrieve worker config on-demand
- Audit configuration history (Consul stores versions)
- Update configs without restarting workers (future enhancement)
- Provide single source of truth for cluster state

### Config Path Structure

```
Consul KV Store:
├── kionas/
│   ├── cluster                           # cluster.json
│   ├── configs/
│   │   ├── kionas-worker1                # worker1 config (from kionas-worker1.json)
│   │   ├── kionas-worker2                # worker2 config
│   │   ├── kionas-warehouse              # warehouse config
│   │   └── kionas-metastore              # metastore config
│   └── ...
```

**Key** = `kionas/configs/<HOSTNAME>`
**Value** = JSON content of the config file (base64 encoded by Consul)

### Retrieving Configs from Consul

```bash
# Get config for worker1
curl -s http://localhost:8500/v1/kv/kionas/configs/kionas-worker1 | jq -r '.[].Value' | base64 -d | jq .

# List all configs
curl -s http://localhost:8500/v1/kv/kionas/configs?recurse | jq '.[] | {key: .Key, size: (.Value | length)}'

# Example output:
# {
#   "key": "kionas/configs/kionas-worker1",
#   "size": 512
# }
```

---

## Complete Example: Initialize 2-Worker Cluster

```bash
# Starting state: No workers registered
./worker_service_mgmt.sh list
# Output: No workers registered

# Step 1: Configure and register all workers at once
./register_cluster.sh

# Output:
# Found 2 worker configuration file(s):
#   - kionas-worker1.json
#   - kionas-worker2.json
#
# [1/2] Processing kionas-worker1.json...
#   ✓ Configuration uploaded to Consul KV
#   ✓ Worker registered successfully
#
# [2/2] Processing kionas-worker2.json...
#   ✓ Configuration uploaded to Consul KV
#   ✓ Worker registered successfully
#
# Registration Summary:
#   Total: 2
#   Success: 2
#   Failed: 0
```

## Workflow Integration

### For Development

```bash
# One-time setup
./init_workers.sh
# Handles: config generation + Consul upload + registration + verification
```

### For Cluster Scaling (Add New Worker)

```bash
# Deploy new worker binary to host
ssh worker4.local
docker run -d kionas-worker:latest

# Configure and register (uploads config + registers service)
./configure_worker.sh ./configs/kionas-worker4.json

# Verify
./worker_service_mgmt.sh list
```

### For Rolling Updates

```bash
# For each worker:
./deregister_worker.sh W1           # Remove from service catalog
sleep 35                             # Wait for timeout
docker restart kionas-worker1        # Update/restart process
./configure_worker.sh ./configs/kionas-worker1.json  # Re-register
```

---

## Configuration File Format

Worker config file must be valid JSON with this structure:

```json
{
  "mode": "worker",
  "services": {
    "interops": {
      "host": "worker-hostname",
      "port": 50051,
      "tls_cert": "...",
      "tls_key": "...",
      "mode": "https"
    }
  },
  "execution": {
    "router": { ... },
    "memory_tracking": { ... }
  }
}
```

**Key fields extracted:**
- `services.interops.host` — Worker hostname (uploaded as config key)
- `services.interops.port` — Worker port (used for registration)

**Worker ID derivation:**
Priority order (uses first available):
1. CLI argument: `--worker-id W1`
2. Filename: `kionas-worker1.json` → `W1`
3. Hostname: `kionas-worker1` → `W1`
4. Default: `W1`

---

## Troubleshooting

### Issue: "Could not extract worker host from config"

**Cause**: Config file doesn't have expected fields

**Solution**: Ensure config has:
```json
{
  "services": {
    "interops": {
      "host": "hostname",
      "port": 50051
    }
  }
}
```

### Issue: Config uploaded but service not registered

**Check:**
```bash
# Verify config is in Consul
curl -s http://localhost:8500/v1/kv/kionas/configs/worker-name | jq .

# Check service registration
curl -s http://localhost:8500/v1/catalog/service/worker | jq '.[] | {ID, Address}'
```

**Typical causes:**
- Worker hostname/port mismatch (use `--host` and `--port` overrides)
- Consul connectivity issue

### Issue: "Worker ID collision" or duplicate workers

**Solution**: Each worker needs unique ID:
```bash
# Explicitly set worker ID
./configure_worker.sh ./config.json --worker-id W3
```

---

## API Reference

### configure_worker.sh

```bash
Usage: ./configure_worker.sh CONFIG_FILE [OPTIONS]

Options:
  -c, --consul URL        Consul URL (default: http://localhost:8500)
  -i, --worker-id ID      Override worker ID
  -H, --host HOSTNAME     Override hostname
  -p, --port PORT         Override port
  --skip-upload           Skip Consul KV upload
  --skip-registration     Skip service registration
  -v, --verbose           Verbose output
```

### Config Upload Endpoint

```
PUT /v1/kv/kionas/configs/<HOSTNAME>

Headers:
  Content-Type: application/octet-stream

Body:
  Raw JSON configuration file content
```

### Config Retrieval

```
GET /v1/kv/kionas/configs/<HOSTNAME>

Response:
[
  {
    "LockIndex": 0,
    "Key": "kionas/configs/kionas-worker1",
    "Flags": 0,
    "Value": "eyJtb2RlIjogIndvcmtlciIsIC4uLn0=",  # base64-encoded
    "CreateIndex": 1234,
    "ModifyIndex": 1234
  }
]
```

---

## Data Flow Diagram

```
┌─────────────────────────────────────────────────────────┐
│ Local: kionas-worker1.json                              │
│ {                                                       │
│   "services": {                                         │
│     "interops": {                                       │
│       "host": "worker1.local",                          │
│       "port": 50051                                     │
│     }                                                   │
│   }                                                     │
│ }                                                       │
└──────────────────────┬──────────────────────────────────┘
                       │
                       ├─→ configure_worker.sh
                       │
        ┌──────────────┴──────────────┐
        │                             │
        ▼                             ▼
  ┌──────────────┐            ┌──────────────┐
  │ Upload to    │            │ Register     │
  │ Consul KV    │            │ Service      │
  └──────────────┘            └──────────────┘
        │                             │
        ▼                             ▼
  ┌──────────────────────────────────────────┐
  │ Consul KV: kionas/configs/worker1.local  │
  │ Consul Catalog: service worker-W1        │
  │                                          │
  │ Now server can:                          │
  │ - Discover worker W1 at port 50051       │
  │ - Retrieve config from KV store          │
  │ - Route queries to worker                │
  └──────────────────────────────────────────┘
```

---

## Summary

| Task | Command | Uploads Config? | Registers Service? |
|---|---|---|---|
| Initialize all workers | `./init_workers.sh` | ✅ | ✅ |
| Register all from configs | `./register_cluster.sh` | ✅ (NEW!) | ✅ |
| Configure single worker | `./configure_worker.sh config.json` | ✅ | ✅ |
| Upload config only | `./configure_worker.sh config.json --skip-registration` | ✅ | ❌ |
| Register service only | `./configure_worker.sh config.json --skip-upload` | ❌ | ✅ |
| Register service (direct) | `./register_worker.sh W1 host port` | ❌ | ✅ |

---

## Document Status

✅ **Complete** — Configuration upload workflow fully integrated

Last updated: March 25, 2026
