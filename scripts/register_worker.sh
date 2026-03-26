#!/bin/bash
# Worker Service Registration Script
# Registers a Kionas worker with Consul for service discovery
# 
# Usage:
#   ./register_worker.sh --worker-id W1 --host worker1.local --port 50051 --consul http://localhost:8500
#   ./register_worker.sh W1 worker1.local 50051  # short form

set -e

# Default values
CONSUL_URL="${CONSUL_URL:-http://localhost:8500}"
WORKER_ID=""
WORKER_HOST=""
WORKER_PORT="50051"
HEALTH_CHECK_INTERVAL="${HEALTH_CHECK_INTERVAL:-10s}"
HEALTH_CHECK_TIMEOUT="${HEALTH_CHECK_TIMEOUT:-5s}"
DEREGISTER_AFTER="${DEREGISTER_AFTER:-30s}"
VERBOSE=0

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Print usage
usage() {
  cat << EOF
Usage: $(basename "$0") [OPTIONS] [WORKER_ID] [WORKER_HOST] [WORKER_PORT]

Short form (positional arguments):
  $(basename "$0") W1 worker1.local 50051

Long form (named arguments):
  $(basename "$0") --worker-id W1 --host worker1.local --port 50051 --consul http://consul:8500

Options:
  -i, --worker-id ID              Worker identifier (e.g., W1, W2)
  -H, --host HOSTNAME             Worker hostname or IP address
  -p, --port PORT                 Worker interops port (default: 50051)
  -c, --consul URL                Consul API URL (default: http://localhost:8500)
      --health-interval INTERVAL  Health check interval (default: 10s)
      --health-timeout TIMEOUT    Health check timeout (default: 5s)
      --deregister-after DURATION Deregister after this duration of failures (default: 30s)
  -v, --verbose                   Enable verbose output
  -h, --help                      Show this help message

Environment Variables:
  CONSUL_URL                      Override default Consul URL
  WORKER_ID                       Override worker ID
  WORKER_HOST                     Override worker host
  WORKER_PORT                     Override worker port

Examples:
  # Register worker W1 on worker1.local
  ./register_worker.sh W1 worker1.local 50051

  # Register with custom Consul URL
  ./register_worker.sh -c http://consul-server:8500 -i W2 -H worker2.local -p 50051

  # Register with custom health check settings
  ./register_worker.sh W3 worker3.local 50051 --health-interval 5s --deregister-after 15s
EOF
  exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -i|--worker-id)
      WORKER_ID="$2"
      shift 2
      ;;
    -H|--host)
      WORKER_HOST="$2"
      shift 2
      ;;
    -p|--port)
      WORKER_PORT="$2"
      shift 2
      ;;
    -c|--consul)
      CONSUL_URL="$2"
      shift 2
      ;;
    --health-interval)
      HEALTH_CHECK_INTERVAL="$2"
      shift 2
      ;;
    --health-timeout)
      HEALTH_CHECK_TIMEOUT="$2"
      shift 2
      ;;
    --deregister-after)
      DEREGISTER_AFTER="$2"
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
      # Positional arguments: WORKER_ID HOST PORT
      if [ -z "$WORKER_ID" ]; then
        WORKER_ID="$1"
      elif [ -z "$WORKER_HOST" ]; then
        WORKER_HOST="$1"
      elif [ "$WORKER_PORT" = "50051" ]; then
        WORKER_PORT="$1"
      fi
      shift
      ;;
  esac
done

# Validate required arguments
if [ -z "$WORKER_ID" ] || [ -z "$WORKER_HOST" ]; then
  echo -e "${RED}Error: Missing required arguments (WORKER_ID and WORKER_HOST)${NC}"
  usage
fi

# Verbose output
if [ $VERBOSE -eq 1 ]; then
  echo -e "${YELLOW}[DEBUG] Configuration:${NC}"
  echo "  Consul URL: $CONSUL_URL"
  echo "  Worker ID: $WORKER_ID"
  echo "  Host: $WORKER_HOST"
  echo "  Port: $WORKER_PORT"
  echo "  Health check interval: $HEALTH_CHECK_INTERVAL"
  echo "  Health check timeout: $HEALTH_CHECK_TIMEOUT"
  echo "  Deregister after: $DEREGISTER_AFTER"
fi

# Verify Consul connectivity
echo -e "${YELLOW}[1/3]${NC} Verifying Consul connectivity..."
if ! curl -sf "$CONSUL_URL/v1/status/leader" > /dev/null 2>&1; then
  echo -e "${RED}❌ Failed to connect to Consul at $CONSUL_URL${NC}"
  exit 1
fi
echo -e "${GREEN}✓${NC} Consul is reachable"

# Create registration payload
echo -e "${YELLOW}[2/3]${NC} Preparing service registration..."

# Use TCP health check for worker interops port
REGISTRATION_JSON=$(cat <<EOF
{
  "ID": "worker-${WORKER_ID}",
  "Name": "worker",
  "Address": "${WORKER_HOST}",
  "Port": ${WORKER_PORT},
  "Tags": ["kionas", "worker", "${WORKER_ID}"],
  "Meta": {
    "version": "2.0",
    "flight_port": "$((WORKER_PORT + 1))",
    "registered_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  },
  "Check": {
    "TCP": "${WORKER_HOST}:${WORKER_PORT}",
    "Interval": "${HEALTH_CHECK_INTERVAL}",
    "Timeout": "${HEALTH_CHECK_TIMEOUT}",
    "DeregisterCriticalServiceAfter": "${DEREGISTER_AFTER}"
  }
}
EOF
)

if [ $VERBOSE -eq 1 ]; then
  echo -e "${YELLOW}[DEBUG] Registration payload:${NC}"
  echo "$REGISTRATION_JSON" | jq '.'
fi

# Register worker with Consul
echo -e "${YELLOW}[3/3]${NC} Registering worker service with Consul..."

RESPONSE=$(curl -s -w "\n%{http_code}" -X PUT \
  -H "Content-Type: application/json" \
  -d "$REGISTRATION_JSON" \
  "$CONSUL_URL/v1/agent/service/register")

HTTP_CODE=$(echo "$RESPONSE" | tail -1)
BODY=$(echo "$RESPONSE" | head -n -1)

if [ "$HTTP_CODE" = "200" ]; then
  echo -e "${GREEN}✓${NC} Worker registered successfully"
  echo -e "  Service ID: worker-${WORKER_ID}"
  echo -e "  Address: ${WORKER_HOST}:${WORKER_PORT}"
  exit 0
else
  echo -e "${RED}❌ Registration failed (HTTP $HTTP_CODE)${NC}"
  if [ -n "$BODY" ]; then
    echo -e "${RED}Response: $BODY${NC}"
  fi
  exit 1
fi
