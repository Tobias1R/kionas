#!/bin/bash
# Cluster Worker Registration Script
# Registers all workers in a Kionas cluster with Consul service discovery
#
# This script reads worker configuration from kionas-worker*.json files
# and registers each worker as a service in Consul.
#
# Usage:
#   ./register_cluster.sh                    # use defaults
#   ./register_cluster.sh --consul http://consul:8500 --configs-dir /workspace/configs

set -e

# Configuration
CONSUL_URL="${CONSUL_URL:-http://localhost:8500}"
CONFIGS_DIR="${CONFIGS_DIR:-./configs}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTER_SCRIPT="$SCRIPT_DIR/register_worker.sh"
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

Options:
  -c, --consul URL          Consul API URL (default: http://localhost:8500)
  -d, --configs-dir PATH    Path to configs directory (default: ./configs)
  -v, --verbose             Enable verbose output
  -h, --help                Show this help message

Environment Variables:
  CONSUL_URL                Override default Consul URL
  CONFIGS_DIR               Override default configs directory

Examples:
  # Register all workers (use defaults)
  ./register_cluster.sh

  # Register with custom Consul and configs path
  ./register_cluster.sh -c http://consul-server:8500 -d /etc/kionas/configs

  # Verbose output
  ./register_cluster.sh -v

EOF
  exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
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

# Validate prerequisites
if [ ! -d "$CONFIGS_DIR" ]; then
  echo -e "${RED}❌ Configs directory not found: $CONFIGS_DIR${NC}"
  exit 1
fi

if [ ! -f "$REGISTER_SCRIPT" ]; then
  echo -e "${RED}❌ Worker registration script not found: $REGISTER_SCRIPT${NC}"
  exit 1
fi

if ! command -v jq &> /dev/null; then
  echo -e "${RED}❌ jq is required but not installed${NC}"
  exit 1
fi

# Banner
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Kionas Cluster Worker Registration                        ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo -e "${YELLOW}Configuration:${NC}"
echo "  Consul URL: $CONSUL_URL"
echo "  Configs Dir: $CONFIGS_DIR"
echo ""

# Find all worker config files
WORKER_CONFIGS=($(find "$CONFIGS_DIR" -maxdepth 1 -name "kionas-worker*.json" -o -name "kionas-worker*.toml" | sort))

if [ ${#WORKER_CONFIGS[@]} -eq 0 ]; then
  echo -e "${YELLOW}⚠${NC} No worker configuration files found in $CONFIGS_DIR"
  echo "   Expected: kionas-worker*.json or kionas-worker*.toml"
  exit 1
fi

echo -e "${YELLOW}Found ${#WORKER_CONFIGS[@]} worker configuration file(s):${NC}"
for config in "${WORKER_CONFIGS[@]}"; do
  echo "  - $(basename "$config")"
done
echo ""

# Extract worker details and register
TOTAL=${#WORKER_CONFIGS[@]}
SUCCESS=0
FAILED=0

for (( i = 0; i < TOTAL; i++ )); do
  config_file="${WORKER_CONFIGS[$i]}"
  index=$((i + 1))
  
  echo -e "${YELLOW}[$index/$TOTAL]${NC} Processing $(basename "$config_file")..."
  
  # Parse config file (JSON or TOML)
  # Extract worker ID, host, port from filename or config
  # Format: kionas-worker{ID}.json or kionas-worker{ID}.toml
  
  filename=$(basename "$config_file")
  # Extract ID from filename (e.g., "kionas-worker1.json" -> "1")
  WORKER_ID_NUM=$(echo "$filename" | sed -E 's/kionas-worker([0-9]+)\.(json|toml)/\1/')
  WORKER_ID="W${WORKER_ID_NUM}"
  
  # Extract host and port from config
  if [[ "$config_file" == *.json ]]; then
    # JSON config
    WORKER_HOST=$(jq -r '.server.bind_host // "localhost"' "$config_file" 2>/dev/null || echo "localhost")
    WORKER_PORT=$(jq -r '.server.bind_port // 50051' "$config_file" 2>/dev/null || echo "50051")
  elif [[ "$config_file" == *.toml ]]; then
    # TOML config (basic parsing)
    WORKER_HOST=$(grep -oP '^\s*bind_host\s*=\s*"\K[^"]+' "$config_file" 2>/dev/null || echo "localhost")
    WORKER_PORT=$(grep -oP '^\s*bind_port\s*=\s*\K[0-9]+' "$config_file" 2>/dev/null || echo "50051")
  else
    echo -e "${RED}  ❌ Unsupported config format${NC}"
    FAILED=$((FAILED + 1))
    continue
  fi
  
  # Fallback: use standard defaults if parsing failed
  if [ "$WORKER_HOST" = "null" ] || [ -z "$WORKER_HOST" ]; then
    WORKER_HOST="worker${WORKER_ID_NUM}.local"
  fi
  if [ "$WORKER_PORT" = "null" ] || [ -z "$WORKER_PORT" ]; then
    WORKER_PORT="50051"
  fi
  
  if [ $VERBOSE -eq 1 ]; then
    echo -e "  ${YELLOW}[DEBUG]${NC} Extracted: ID=$WORKER_ID, Host=$WORKER_HOST, Port=$WORKER_PORT"
  fi
  
  # Step 3a: Upload config to Consul KV
  KV_KEY="kionas/configs/$WORKER_HOST"
  if [ $VERBOSE -eq 1 ]; then
    echo -e "  ${YELLOW}[DEBUG]${NC} Uploading config to Consul KV: $KV_KEY"
  fi
  
  KV_HTTP_CODE=$(curl -sw '%{http_code}' -X PUT \
    --data-binary @"$config_file" \
    "$CONSUL_URL/v1/kv/$KV_KEY" \
    -o /dev/null 2>/dev/null)
  
  if [ "$KV_HTTP_CODE" = "200" ]; then
    echo -e "  ${GREEN}✓${NC} Configuration uploaded to Consul KV"
  else
    echo -e "  ${YELLOW}⚠${NC} Configuration upload to Consul failed (HTTP $KV_HTTP_CODE)"
  fi
  
  # Step 3b: Register worker service
  if [ $VERBOSE -eq 1 ]; then
    if "$REGISTER_SCRIPT" -c "$CONSUL_URL" -i "$WORKER_ID" -H "$WORKER_HOST" -p "$WORKER_PORT" --verbose; then
      SUCCESS=$((SUCCESS + 1))
    else
      FAILED=$((FAILED + 1))
    fi
  else
    if "$REGISTER_SCRIPT" -c "$CONSUL_URL" -i "$WORKER_ID" -H "$WORKER_HOST" -p "$WORKER_PORT"; then
      SUCCESS=$((SUCCESS + 1))
    else
      echo -e "  ${RED}❌ Registration failed${NC}"
      FAILED=$((FAILED + 1))
    fi
  fi
  
  echo ""
done

# Summary
echo -e "${YELLOW}═══════════════════════════════════════════════════════════${NC}"
echo -e "${YELLOW}Registration Summary:${NC}"
echo -e "  Total: $TOTAL"
echo -e "  ${GREEN}Success: $SUCCESS${NC}"
echo -e "  ${RED}Failed: $FAILED${NC}"

if [ $FAILED -eq 0 ]; then
  echo ""
  echo -e "${GREEN}✓ All workers registered successfully${NC}"
  echo ""
  echo -e "${YELLOW}Verification:${NC}"
  echo "To verify registration, run:"
  echo "  curl -s $CONSUL_URL/v1/catalog/service/worker | jq '.[] | {ID, Address, Port}'"
  exit 0
else
  echo ""
  echo -e "${RED}❌ Some registrations failed${NC}"
  exit 1
fi
