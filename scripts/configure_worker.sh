#!/bin/bash
# Worker Configuration Upload & Registration Script
# 
# This script provides a unified approach to:
# 1. Upload worker configuration to Consul KV store
# 2. Extract worker metadata from config
# 3. Register worker service with Consul
#
# Usage:
#   ./configure_worker.sh /path/to/kionas-worker1.json
#   ./configure_worker.sh /path/to/config.json --consul http://consul:8500 --worker-id W1
#   ./configure_worker.sh /path/to/config.json --skip-registration  # Only upload config
#   ./configure_worker.sh /path/to/config.json --skip-upload        # Only register service

set -e

# Configuration
WORKER_CONFIG=""
CONSUL_URL="${CONSUL_URL:-http://localhost:8500}"
CONFIGS_KV_PREFIX="kionas/configs"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REGISTER_SCRIPT="$SCRIPT_DIR/register_worker.sh"

# Options
WORKER_ID=""
OVERRIDE_HOST=""
OVERRIDE_PORT=""
SKIP_UPLOAD=0
SKIP_REGISTRATION=0
VERBOSE=0

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

usage() {
  cat << EOF
Usage: $(basename "$0") CONFIG_FILE [OPTIONS]

Upload worker configuration to Consul and register service.

Positional Arguments:
  CONFIG_FILE             Path to worker configuration JSON file (required)

Options:
  -c, --consul URL        Consul API URL (default: http://localhost:8500)
  -i, --worker-id ID      Override worker ID (default: extracted from config/filename)
  -H, --host HOSTNAME     Override worker hostname (default: from config)
  -p, --port PORT         Override worker port (default: from config)
  --skip-upload           Skip uploading config to Consul KV
  --skip-registration     Skip registering worker service
  -v, --verbose           Enable verbose output
  -h, --help              Show this help message

Environment Variables:
  CONSUL_URL              Override default Consul URL

Examples:
  # Full workflow: upload config and register service
  ./configure_worker.sh ./configs/kionas-worker1.json

  # Upload only (extract info but don't register)
  ./configure_worker.sh ./configs/kionas-worker1.json --skip-registration

  # Register service only (assuming config already uploaded)
  ./configure_worker.sh ./configs/kionas-worker1.json --skip-upload

  # Custom Consul URL
  ./configure_worker.sh ./configs/kionas-worker1.json -c http://consul-server:8500

  # Override extracted values
  ./configure_worker.sh ./configs/kionas-worker1.json \
    --worker-id W1 \
    --host worker1.example.com \
    --port 50051

  # Verbose output
  ./configure_worker.sh ./configs/kionas-worker1.json --verbose

EOF
  exit 1
}

# Parse arguments
if [ $# -lt 1 ]; then
  echo -e "${RED}Error: CONFIG_FILE required${NC}"
  usage
fi

WORKER_CONFIG="$1"
shift

while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--consul)
      CONSUL_URL="$2"
      shift 2
      ;;
    -i|--worker-id)
      WORKER_ID="$2"
      shift 2
      ;;
    -H|--host)
      OVERRIDE_HOST="$2"
      shift 2
      ;;
    -p|--port)
      OVERRIDE_PORT="$2"
      shift 2
      ;;
    --skip-upload)
      SKIP_UPLOAD=1
      shift
      ;;
    --skip-registration)
      SKIP_REGISTRATION=1
      shift
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

# Validate config file
if [ ! -f "$WORKER_CONFIG" ]; then
  echo -e "${RED}❌ Configuration file not found: $WORKER_CONFIG${NC}"
  exit 1
fi

if ! command -v jq &> /dev/null; then
  echo -e "${RED}❌ jq is required but not installed${NC}"
  exit 1
fi

# Validate it's valid JSON
if ! jq empty "$WORKER_CONFIG" 2>/dev/null; then
  echo -e "${RED}❌ Invalid JSON in configuration file${NC}"
  exit 1
fi

# Banner
echo -e "${BLUE}╔════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  Worker Configuration & Registration                       ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════════════════════════╝${NC}"
echo ""

echo -e "${YELLOW}Configuration file:${NC} $(basename "$WORKER_CONFIG")"
echo ""

# Step 1: Extract worker information from config
echo -e "${YELLOW}[1/3]${NC} Extracting worker information from configuration..."

# Extract from config (service.interops section)
CONFIG_HOST=$(jq -r '.services.interops.host // .server.bind_host // empty' "$WORKER_CONFIG" 2>/dev/null || echo "")
CONFIG_PORT=$(jq -r '.services.interops.port // .server.bind_port // empty' "$WORKER_CONFIG" 2>/dev/null || echo "")

# Allow CLI overrides
WORKER_HOST="${OVERRIDE_HOST:-$CONFIG_HOST}"
WORKER_PORT="${OVERRIDE_PORT:-$CONFIG_PORT}"

# Validate extracted values
if [ -z "$WORKER_HOST" ]; then
  echo -e "${RED}❌ Could not extract worker host from config${NC}"
  echo "   Expected: services.interops.host or server.bind_host"
  exit 1
fi

if [ -z "$WORKER_PORT" ]; then
  echo -e "${RED}❌ Could not extract worker port from config${NC}"
  echo "   Expected: services.interops.port or server.bind_port"
  exit 1
fi

# Extract or derive worker ID
if [ -z "$WORKER_ID" ]; then
  # Try to extract from filename (e.g., "kionas-worker1.json" -> "W1")
  filename=$(basename "$WORKER_CONFIG")
  if [[ $filename =~ kionas-worker([0-9]+) ]]; then
    WORKER_ID="W${BASH_REMATCH[1]}"
  else
    # Try to derive from hostname (e.g., "kionas-worker1" -> "W1")
    if [[ $WORKER_HOST =~ worker([0-9]+) ]]; then
      WORKER_ID="W${BASH_REMATCH[1]}"
    else
      # Fall back to numeric suffix from hostname
      WORKER_ID="W1"
    fi
  fi
fi

echo -e "${GREEN}✓${NC} Extracted worker information:"
echo "  Worker ID:  $WORKER_ID"
echo "  Hostname:   $WORKER_HOST"
echo "  Port:       $WORKER_PORT"
echo ""

# Step 2: Upload configuration to Consul
if [ $SKIP_UPLOAD -eq 0 ]; then
  echo -e "${YELLOW}[2/3]${NC} Uploading configuration to Consul..."
  
  # Verify Consul connectivity
  if ! curl -sf "$CONSUL_URL/v1/status/leader" > /dev/null 2>&1; then
    echo -e "${RED}❌ Failed to connect to Consul at $CONSUL_URL${NC}"
    exit 1
  fi
  
  # Upload config to kionas/configs/<HOSTNAME>
  KV_KEY="$CONFIGS_KV_PREFIX/$WORKER_HOST"
  
  if [ $VERBOSE -eq 1 ]; then
    echo -e "${YELLOW}[DEBUG]${NC} Uploading to: $KV_KEY"
    echo -e "${YELLOW}[DEBUG]${NC} Consul URL: $CONSUL_URL"
  fi
  
  # Upload with curl
  HTTP_CODE=$(curl -sw '%{http_code}' -X PUT \
    --data-binary @"$WORKER_CONFIG" \
    "$CONSUL_URL/v1/kv/$KV_KEY" \
    -o /dev/null)
  
  if [ "$HTTP_CODE" = "200" ]; then
    echo -e "${GREEN}✓${NC} Configuration uploaded to Consul"
    echo "  Key: $KV_KEY"
  else
    echo -e "${RED}❌ Failed to upload configuration (HTTP $HTTP_CODE)${NC}"
    exit 1
  fi
  
  echo ""
else
  echo -e "${YELLOW}[2/3]${NC} Skipping configuration upload..."
  echo ""
fi

# Step 3: Register worker service
if [ $SKIP_REGISTRATION -eq 0 ]; then
  echo -e "${YELLOW}[3/3]${NC} Registering worker service with Consul..."
  
  # Check if register script exists
  if [ ! -f "$REGISTER_SCRIPT" ]; then
    echo -e "${RED}❌ Worker registration script not found: $REGISTER_SCRIPT${NC}"
    exit 1
  fi
  
  # Build registration command
  cmd="bash '$REGISTER_SCRIPT' -c '$CONSUL_URL' -i '$WORKER_ID' -H '$WORKER_HOST' -p '$WORKER_PORT'"
  
  if [ $VERBOSE -eq 1 ]; then
    cmd="$cmd --verbose"
  fi
  
  if eval "$cmd"; then
    echo ""
  else
    echo -e "${RED}❌ Failed to register worker service${NC}"
    exit 1
  fi
  
  echo ""
else
  echo -e "${YELLOW}[3/3]${NC} Skipping worker registration..."
  echo ""
fi

# Summary
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✓ Configuration and registration complete!${NC}"
echo ""
echo -e "${YELLOW}Configured Worker:${NC}"
echo "  ID:       $WORKER_ID"
echo "  Hostname: $WORKER_HOST"
echo "  Port:     $WORKER_PORT"
echo ""

if [ $SKIP_UPLOAD -eq 0 ]; then
  echo -e "${YELLOW}Consul KV:${NC}"
  echo "  Key: $CONFIGS_KV_PREFIX/$WORKER_HOST"
  echo "  Retrieve: curl -s $CONSUL_URL/v1/kv/$KV_KEY | jq -r '\''.[].Value\'' | base64 -d"
  echo ""
fi

if [ $SKIP_REGISTRATION -eq 0 ]; then
  echo -e "${YELLOW}Consul Service:${NC}"
  echo "  Service ID: worker-$WORKER_ID"
  echo "  Retrieve: curl -s $CONSUL_URL/v1/catalog/service/worker | jq '.[] | select(.ID == \"worker-$WORKER_ID\")"
  echo ""
fi

exit 0
