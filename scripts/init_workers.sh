#!/bin/bash
# Unified Worker Initialization & Registration Script
# Initializes all workers in a Kionas cluster with proper Consul registration
#
# This script handles the complete worker setup workflow:
# 1. Generate worker configuration files (if needed)
# 2. Upload configs to Consul KV store
# 3. Register worker services in Consul catalog
# 4. Verify all workers are healthy and reachable
#
# Usage:
#   ./init_workers.sh                           # Full initialization
#   ./init_workers.sh --config-only            # Only generate configs
#   ./init_workers.sh --register-only           # Only register (configs must exist)
#   ./init_workers.sh --verify-only             # Only verify existing registrations

set -e

# Configuration
CONSUL_URL="${CONSUL_URL:-http://localhost:8500}"
CONFIGS_DIR="${CONFIGS_DIR:-./configs}"
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_GENERATOR="$SCRIPTS_DIR/generate_cluster_config.py"
WORKER_CONFIG_GENERATOR="$SCRIPTS_DIR/generate_worker_configs.py"
CONSUL_POPULATOR="$SCRIPTS_DIR/populate_consul.sh"
CLUSTER_REGISTRAR="$SCRIPTS_DIR/register_cluster.sh"
WORKER_MGR="$SCRIPTS_DIR/worker_service_mgmt.sh"

# Options
SKIP_CONFIG_GEN=0
SKIP_CONSUL_UPLOAD=0
SKIP_REGISTRATION=0
SKIP_VERIFY=0
CONFIG_ONLY=0
REGISTER_ONLY=0
VERIFY_ONLY=0
VERBOSE=0

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

usage() {
  cat << EOF
Usage: $(basename "$0") [OPTIONS]

Initialize and register all workers in a Kionas cluster.

Options:
  --config-only          Only generate worker configs (skip registration/verify)
  --register-only        Only register workers (skip config generation/verify)
  --verify-only          Only verify existing registrations
  --skip-config-gen      Skip config generation step
  --skip-consul-upload   Skip uploading configs to Consul KV
  --skip-registration    Skip worker service registration
  --skip-verify          Skip final verification
  -c, --consul URL       Consul API URL (default: http://localhost:8500)
  -d, --configs-dir PATH Config directory (default: ./configs)
  -v, --verbose          Enable verbose output
  -h, --help             Show this help message

Environment Variables:
  CONSUL_URL             Override Consul URL
  CONFIGS_DIR            Override configs directory

Examples:
  # Full initialization workflow
  ./init_workers.sh

  # Only generate configs
  ./init_workers.sh --config-only

  # Only register (configs already exist)
  ./init_workers.sh --register-only

  # Full workflow with custom Consul URL
  ./init_workers.sh -c http://consul-server:8500

  # Verbose output
  ./init_workers.sh --verbose

EOF
  exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --config-only)
      CONFIG_ONLY=1
      SKIP_CONSUL_UPLOAD=1
      SKIP_REGISTRATION=1
      SKIP_VERIFY=1
      shift
      ;;
    --register-only)
      REGISTER_ONLY=1
      SKIP_CONFIG_GEN=1
      SKIP_CONSUL_UPLOAD=1
      SKIP_VERIFY=1
      shift
      ;;
    --verify-only)
      VERIFY_ONLY=1
      SKIP_CONFIG_GEN=1
      SKIP_CONSUL_UPLOAD=1
      SKIP_REGISTRATION=1
      shift
      ;;
    --skip-config-gen)
      SKIP_CONFIG_GEN=1
      shift
      ;;
    --skip-consul-upload)
      SKIP_CONSUL_UPLOAD=1
      shift
      ;;
    --skip-registration)
      SKIP_REGISTRATION=1
      shift
      ;;
    --skip-verify)
      SKIP_VERIFY=1
      shift
      ;;
    -c|--consul)
      CONSUL_URL="$2"
      shift 2
      ;;
    -d|--configs-dir)
      CONFIGS_DIR="$2"
      shift 2
      ;;
    -v|--verbose)
      VERBOSE=1
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      echo -e "${RED}Unknown option: $1${NC}"
      usage
      ;;
  esac
done

# Banner
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Kionas Worker Initialization & Registration              ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

# Step 0: Verify prerequisites
echo -e "${YELLOW}[0/5]${NC} Verifying prerequisites..."

missing_deps=0

if ! command -v curl &> /dev/null; then
  echo -e "${RED}❌ curl is required but not installed${NC}"
  missing_deps=1
fi

if ! command -v python3 &> /dev/null; then
  echo -e "${RED}❌ python3 is required but not installed${NC}"
  missing_deps=1
fi

if ! command -v jq &> /dev/null; then
  echo -e "${RED}❌ jq is required but not installed${NC}"
  missing_deps=1
fi

if [ $missing_deps -eq 1 ]; then
  exit 1
fi

echo -e "${GREEN}✓${NC} All prerequisites satisfied"
echo ""

# Step 1: Generate configs
if [ $SKIP_CONFIG_GEN -eq 0 ]; then
  echo -e "${YELLOW}[1/5]${NC} Generating cluster configurations..."
  
  if [ ! -f "$CONFIG_GENERATOR" ]; then
    echo -e "${RED}❌ Config generator not found: $CONFIG_GENERATOR${NC}"
    exit 1
  fi
  
  if python3 "$CONFIG_GENERATOR"; then
    echo -e "${GREEN}✓${NC} Cluster configuration generated"
  else
    echo -e "${RED}❌ Failed to generate cluster configuration${NC}"
    exit 1
  fi
  
  echo ""
  
  if [ ! -f "$WORKER_CONFIG_GENERATOR" ]; then
    echo -e "${RED}❌ Worker config generator not found: $WORKER_CONFIG_GENERATOR${NC}"
    exit 1
  fi
  
  if python3 "$WORKER_CONFIG_GENERATOR"; then
    echo -e "${GREEN}✓${NC} Worker configurations generated"
  else
    echo -e "${RED}❌ Failed to generate worker configurations${NC}"
    exit 1
  fi
  
  echo ""
else
  echo -e "${YELLOW}[1/5]${NC} Skipping config generation..."
  echo ""
fi

# Step 2: Upload configs to Consul
if [ $SKIP_CONSUL_UPLOAD -eq 0 ]; then
  echo -e "${YELLOW}[2/5]${NC} Uploading configurations to Consul..."
  
  if [ ! -f "$CONSUL_POPULATOR" ]; then
    echo -e "${RED}❌ Consul populator script not found: $CONSUL_POPULATOR${NC}"
    exit 1
  fi
  
  # Run populator with environment variables
  if CONSUL_URL="$CONSUL_URL" bash "$CONSUL_POPULATOR"; then
    echo -e "${GREEN}✓${NC} Configurations uploaded to Consul"
  else
    echo -e "${RED}❌ Failed to upload configurations to Consul${NC}"
    exit 1
  fi
  
  echo ""
else
  echo -e "${YELLOW}[2/5]${NC} Skipping Consul upload..."
  echo ""
fi

# Step 3: Register worker services
if [ $SKIP_REGISTRATION -eq 0 ]; then
  echo -e "${YELLOW}[3/5]${NC} Registering worker services..."
  
  if [ ! -f "$CLUSTER_REGISTRAR" ]; then
    echo -e "${RED}❌ Cluster registrar script not found: $CLUSTER_REGISTRAR${NC}"
    exit 1
  fi
  
  # Build command
  cmd="bash '$CLUSTER_REGISTRAR' -c '$CONSUL_URL' -d '$CONFIGS_DIR'"
  
  if [ $VERBOSE -eq 1 ]; then
    cmd="$cmd --verbose"
  fi
  
  if eval "$cmd"; then
    echo -e "${GREEN}✓${NC} Worker services registered"
  else
    echo -e "${RED}❌ Failed to register worker services${NC}"
    exit 1
  fi
  
  echo ""
else
  echo -e "${YELLOW}[3/5]${NC} Skipping worker registration..."
  echo ""
fi

# Step 4: Verify registrations
if [ $SKIP_VERIFY -eq 0 ]; then
  echo -e "${YELLOW}[4/5]${NC} Verifying worker registrations..."
  
  # Wait a moment for registrations to settle
  sleep 2
  
  if [ ! -f "$WORKER_MGR" ]; then
    echo -e "${RED}❌ Worker manager script not found: $WORKER_MGR${NC}"
    exit 1
  fi
  
  # Show status
  echo ""
  CONSUL_URL="$CONSUL_URL" bash "$WORKER_MGR" status
  
  echo -e "${GREEN}✓${NC} Verification complete"
  
  echo ""
else
  echo -e "${YELLOW}[4/5]${NC} Skipping verification..."
  echo ""
fi

# Step 5: Test connectivity
if [ $SKIP_VERIFY -eq 0 ]; then
  echo -e "${YELLOW}[5/5]${NC} Testing worker connectivity..."
  
  echo ""
  CONSUL_URL="$CONSUL_URL" bash "$WORKER_MGR" verify
  
  echo -e "${GREEN}✓${NC} Connectivity tests complete"
  
  echo ""
else
  echo -e "${YELLOW}[5/5]${NC} Skipping connectivity tests..."
  echo ""
fi

# Summary
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ Initialization complete!${NC}"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "  1. Start the Kionas server:"
echo "     docker run -d --name kionas-server \\"
echo "       -e CONSUL_URL=http://consul:8500 \\"
echo "       kionas-server:latest"
echo ""
echo "  2. Run verification query:"
echo "     ./client --query 'SELECT * FROM a JOIN b ... LIMIT 5'"
echo ""
echo -e "${YELLOW}Useful commands:${NC}"
echo "  List all workers:    ./worker_service_mgmt.sh list"
echo "  Show worker status:  ./worker_service_mgmt.sh status"
echo "  Verify connectivity: ./worker_service_mgmt.sh verify"
echo "  Get worker info:     ./worker_service_mgmt.sh info W1"
echo ""

exit 0
