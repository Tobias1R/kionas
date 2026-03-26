#!/bin/bash
# Quick Reference: Worker Scripts Guide
# For immediate access to all worker management commands

# Display usage guide
if [[ "$1" == "-h" ]] || [[ "$1" == "--help" ]] || [[ -z "$1" ]]; then
  cat << 'EOF'
╔═══════════════════════════════════════════════════════════════════════════╗
║                   KIONAS WORKER MANAGEMENT SCRIPTS                        ║
║                        Quick Reference Guide                              ║
╚═══════════════════════════════════════════════════════════════════════════╝

📋 AVAILABLE SCRIPTS
────────────────────────────────────────────────────────────────────────────

1. init_workers.sh
   Full cluster initialization (one-time setup)
   - Generates configs
   - Uploads to Consul
   - Registers all workers
   - Verifies connectivity
   
   Usage:
     ./init_workers.sh
     ./init_workers.sh --register-only
     ./init_workers.sh --verify-only

2. configure_worker.sh ⭐ NEW
   Configure and register a single worker
   - Extracts worker info from config
   - Uploads config to Consul KV
   - Registers as service
   
   Usage:
     ./configure_worker.sh ./configs/kionas-worker1.json
     ./configure_worker.sh config.json --skip-registration
     ./configure_worker.sh config.json --worker-id W1

3. register_cluster.sh
   Register all workers at once
   - Finds all kionas-worker*.json files
   - Uploads each config to Consul
   - Registers all services
   - Batch operation
   
   Usage:
     ./register_cluster.sh
     ./register_cluster.sh -v (verbose)
     ./register_cluster.sh -c http://consul:8500

4. register_worker.sh
   Register a single worker service (service only)
   - Direct Consul service registration
   - Requires worker info (ID, host, port)
   - No config upload
   
   Usage:
     ./register_worker.sh W1 worker1.local 50051
     ./register_worker.sh -i W2 -H worker2.local -p 50051

5. deregister_worker.sh
   Remove workers from Consul
   - Graceful removal
   - Confirmation prompt
   - Supports multiple workers or --all
   
   Usage:
     ./deregister_worker.sh W1
     ./deregister_worker.sh W1 W2 W3
     ./deregister_worker.sh --all --force

6. worker_service_mgmt.sh
   Query and monitor worker services
   - List all workers
   - Show health status
   - Verify connectivity
   - Get worker details
   
   Usage:
     ./worker_service_mgmt.sh list
     ./worker_service_mgmt.sh status
     ./worker_service_mgmt.sh health W1
     ./worker_service_mgmt.sh verify
     ./worker_service_mgmt.sh info W1

────────────────────────────────────────────────────────────────────────────

🎯 COMMON WORKFLOWS

1. FIRST-TIME CLUSTER SETUP
   ─────────────────────────────────────────────────────────────────────
   Goal: Initialize 3-worker cluster from scratch
   
   Commands:
     ./init_workers.sh
   
   What happens:
     ✓ Generates worker configs
     ✓ Uploads configs to Consul
     ✓ Registers all services
     ✓ Verifies connectivity
   
   Time: ~2 minutes

2. ADD NEW WORKER TO RUNNING CLUSTER
   ─────────────────────────────────────────────────────────────────────
   Goal: Add worker W4 to existing 3-worker cluster
   
   Steps:
     a) Deploy worker binary to new host
     b) ./configure_worker.sh ./configs/kionas-worker4.json
     c) ./worker_service_mgmt.sh list
   
   What happens:
     ✓ Uploads W4 config to Consul
     ✓ Registers W4 service
     ✓ W4 joins cluster (server auto-discovers)
   
   Time: ~1 minute

3. UPDATE WORKER CONFIGURATION
   ─────────────────────────────────────────────────────────────────────
   Goal: Change batch size for worker W1
   
   Steps:
     a) Edit configs/kionas-worker1.json
     b) ./configure_worker.sh ./configs/kionas-worker1.json
     c) Restart worker: docker restart kionas-worker1
   
   What happens:
     ✓ Uploads updated config to Consul
     ✓ Re-registers service (health checks reset)
   
   Time: ~1 minute + restart time

4. ROLLING RESTART (e.g., code update)
   ─────────────────────────────────────────────────────────────────────
   Goal: Restart all workers without downtime
   
   For each worker W1, W2, W3:
     a) ./deregister_worker.sh W1
     b) sleep 35
     c) docker restart kionas-worker1
     d) ./configure_worker.sh ./configs/kionas-worker1.json
   
   What happens:
     ✓ Worker removed from service registry
     ✓ In-flight queries timeout gracefully
     ✓ Worker restarts with new code
     ✓ Worker re-registers and rejoins cluster
   
   Time: ~5 minutes per worker (3–4 workers = 15–20 minutes)

5. REMOVE WORKER FOR MAINTENANCE
   ─────────────────────────────────────────────────────────────────────
   Goal: Safely remove W3, perform maintenance, bring back
   
   Offline:
     a) ./deregister_worker.sh W3
     b) sleep 35
     c) docker stop kionas-worker3
     d) [do maintenance work]
     e) docker start kionas-worker3
     f) ./configure_worker.sh ./configs/kionas-worker3.json
   
   What happens:
     ✓ W3 removed from service registry
     ✓ Ongoing queries fail gracefully
     ✓ Maintenance performed
     ✓ W3 returns to cluster after restart
   
   Time: 1 minute + maintenance time

6. MONITOR CLUSTER HEALTH
   ─────────────────────────────────────────────────────────────────────
   Goal: Check all workers are healthy
   
   Commands:
     ./worker_service_mgmt.sh list        # Show all workers
     ./worker_service_mgmt.sh status      # Show health status
     ./worker_service_mgmt.sh verify      # Test connectivity
   
   Interpretation:
     - Status "passing" = ✅ Healthy
     - Status "critical" = ❌ Needs attention
     - Connectivity test shows TCP access

────────────────────────────────────────────────────────────────────────────

📊 SCRIPT QUICK REFERENCE

Configuration Management:
  configure_worker.sh    │ Upload config + register    │ Single worker
  register_cluster.sh    │ Upload all + register all   │ All workers
  [populate_consul.sh]   │ Upload configs to KV        │ Configs only

Service Registration:
  register_worker.sh     │ Register service only       │ Single worker
  register_cluster.sh    │ Register all services       │ All workers
  deregister_worker.sh   │ Remove services             │ Single/multiple

Service Query:
  worker_service_mgmt.sh │ List, status, health, verify│ All/specific

Orchestration:
  init_workers.sh        │ Full initialization         │ One-time setup

────────────────────────────────────────────────────────────────────────────

🔧 CONFIGURATION EXTRAS

Environment Variables (set before running scripts):
  CONSUL_URL          Override Consul endpoint (default: http://localhost:8500)
  CONFIGS_DIR         Override configs directory (default: ./configs)

Consul KV Structure (where configs are stored):
  kionas/configs/kionas-worker1    ← Config for worker1
  kionas/configs/kionas-worker2    ← Config for worker2
  kionas/configs/kionas-worker3    ← Config for worker3

Consul Service Structure (where services are registered):
  Name: worker
  ID:   worker-W1, worker-W2, worker-W3, ...
  Port: 50051 (interops), 50052 (flight)
  Tags: ["kionas", "worker", "W1"]

────────────────────────────────────────────────────────────────────────────

📈 TYPICAL TIMELINE

Activity                    Command                             Time
─────────────────────────────────────────────────────────────────────
Initialize cluster         ./init_workers.sh                   2–3 min
Add single worker           ./configure_worker.sh config.json   1 min
Restart single worker       deregister + restart + configure    3–5 min
Rolling restart (3 workers) For each worker                     15–20 min
Remove + maintain worker    deregister + maintain + register    1 + admin
Query cluster health        worker_service_mgmt.sh status       <1 sec

────────────────────────────────────────────────────────────────────────────

💡 TIPS & TRICKS

1. Verbose Output
   Add -v or --verbose flag to see detailed logs
   Example: ./init_workers.sh --verbose

2. Dry-Run (Check only)
   Use --skip-registration to validate config without registering
   Example: ./configure_worker.sh config.json --skip-registration

3. Override Values
   Use -H, -p flags to override extracted config values
   Example: ./configure_worker.sh config.json -H custom.host -p 9999

4. Batch Operations
   register_cluster.sh processes all workers; no loop needed
   Use with -v for visibility into each worker

5. Service Discovery
   Server auto-discovers workers from Consul every 60 seconds
   Query immediately registers; takes effect next query cycle

────────────────────────────────────────────────────────────────────────────

📚 DOCUMENTATION

Full guides:
  WORKER_INITIALIZATION.md        ← Detailed script reference
  WORKER_CONFIGURATION_WORKFLOW.md ← Configuration upload workflow
  docs/OPERATIONAL_RUNBOOK.md     ← Operator procedures

────────────────────────────────────────────────────────────────────────────

Need help with a specific script? Run:
  ./script_name.sh --help

Example:
  ./configure_worker.sh --help
  ./init_workers.sh --help
  ./worker_service_mgmt.sh --help

EOF
  exit 0
fi

# If argument provided, show specific script help
case "$1" in
  init)
    ./init_workers.sh --help
    ;;
  configure)
    ./configure_worker.sh --help
    ;;
  register)
    if [[ "$2" == "cluster" ]]; then
      ./register_cluster.sh --help
    else
      ./register_worker.sh --help
    fi
    ;;
  deregister)
    ./deregister_worker.sh --help
    ;;
  mgmt)
    ./worker_service_mgmt.sh --help
    ;;
  *)
    echo "Unknown script: $1"
    echo "Run with no arguments for full guide: $(basename "$0")"
    exit 1
    ;;
esac
