#!/bin/bash
# Worker Service Deregistration Script
# Deregisters a Kionas worker from Consul service discovery
#
# Usage:
#   ./deregister_worker.sh W1                    # deregister single worker
#   ./deregister_worker.sh --all                 # deregister all workers
#   ./deregister_worker.sh -c http://consul:8500 W1 W2 W3

set -e

# Configuration
CONSUL_URL="${CONSUL_URL:-http://localhost:8500}"
WORKERS_TO_REMOVE=()
DEREGISTER_ALL=0
FORCE=0
VERBOSE=0

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

usage() {
  cat << EOF
Usage: $(basename "$0") [OPTIONS] [WORKER_IDS...]

Deregister one or more workers from Consul service discovery.

Options:
  -c, --consul URL       Consul API URL (default: http://localhost:8500)
  -a, --all              Deregister all workers
  -f, --force            Skip confirmation prompt
  -v, --verbose          Enable verbose output
  -h, --help             Show this help message

Positional Arguments:
  WORKER_IDS             Worker IDs to deregister (e.g., W1 W2 W3)

Environment Variables:
  CONSUL_URL             Override default Consul URL

Examples:
  # Deregister single worker W1
  $(basename "$0") W1

  # Deregister multiple workers
  $(basename "$0") W1 W2 W3

  # Deregister all workers (with confirmation)
  $(basename "$0") --all

  # Deregister all workers without confirmation
  $(basename "$0") --all --force

  # Deregister with custom Consul URL
  $(basename "$0") -c http://consul-server:8500 W1 W2

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
    -a|--all)
      DEREGISTER_ALL=1
      shift
      ;;
    -f|--force)
      FORCE=1
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
      # Positional arguments (worker IDs)
      WORKERS_TO_REMOVE+=("$1")
      shift
      ;;
  esac
done

# Verify Consul connectivity
echo -e "${YELLOW}[1/3]${NC} Verifying Consul connectivity..."
if ! curl -sf "$CONSUL_URL/v1/status/leader" > /dev/null 2>&1; then
  echo -e "${RED}❌ Failed to connect to Consul at $CONSUL_URL${NC}"
  exit 1
fi
echo -e "${GREEN}✓${NC} Consul is reachable"

# Get list of workers to deregister
echo -e "${YELLOW}[2/3]${NC} Identifying workers to deregister..."

if [ $DEREGISTER_ALL -eq 1 ]; then
  # Fetch all registered workers from Consul
  RESPONSE=$(curl -sf "$CONSUL_URL/v1/catalog/service/worker")
  
  if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
    echo -e "${YELLOW}⚠${NC} No workers currently registered in Consul"
    exit 0
  fi
  
  # Extract worker IDs
  WORKERS_TO_REMOVE=($(echo "$RESPONSE" | jq -r '.[] | .ID' | sed 's/worker-//' | sort))
  
  if [ ${#WORKERS_TO_REMOVE[@]} -eq 0 ]; then
    echo -e "${YELLOW}⚠${NC} No workers found to deregister"
    exit 0
  fi
elif [ ${#WORKERS_TO_REMOVE[@]} -eq 0 ]; then
  echo -e "${RED}❌ No workers specified. Use --all to deregister all workers.${NC}"
  usage
fi

# Display workers to be deregistered
echo ""
echo -e "${YELLOW}Workers to deregister:${NC}"
for worker in "${WORKERS_TO_REMOVE[@]}"; do
  echo "  - $worker"
done
echo ""

# Confirmation
if [ $FORCE -eq 0 ]; then
  echo -e "${RED}⚠ This action will deregister ${#WORKERS_TO_REMOVE[@]} worker(s)${NC}"
  echo -e "${RED}⚠ In-flight queries on these workers will timeout${NC}"
  read -p "Continue? (yes/no): " -r CONFIRM
  if [[ ! "$CONFIRM" =~ ^[Yy][Ee][Ss]?$ ]]; then
    echo "Cancelled."
    exit 0
  fi
fi

# Deregister workers
echo -e "${YELLOW}[3/3]${NC} Deregistering workers..."
echo ""

SUCCESS=0
FAILED=0

for worker in "${WORKERS_TO_REMOVE[@]}"; do
  SERVICE_ID="worker-$worker"
  
  if [ $VERBOSE -eq 1 ]; then
    echo -e "${YELLOW}[DEBUG]${NC} Deregistering: $SERVICE_ID"
  fi
  
  # Call deregister API
  HTTP_CODE=$(curl -sw '%{http_code}' -X PUT \
    "$CONSUL_URL/v1/agent/service/deregister/$SERVICE_ID" \
    -o /dev/null)
  
  if [ "$HTTP_CODE" = "200" ]; then
    echo -e "${GREEN}✓${NC} Deregistered: $worker (service-id: $SERVICE_ID)"
    SUCCESS=$((SUCCESS + 1))
  else
    echo -e "${RED}❌ Failed to deregister: $worker (HTTP $HTTP_CODE)${NC}"
    FAILED=$((FAILED + 1))
  fi
done

# Summary
echo ""
echo -e "${YELLOW}Deregistration Summary:${NC}"
echo "  Total: ${#WORKERS_TO_REMOVE[@]}"
echo -e "  ${GREEN}Success: $SUCCESS${NC}"
echo -e "  ${RED}Failed: $FAILED${NC}"

if [ $FAILED -eq 0 ]; then
  echo ""
  echo -e "${GREEN}✓ All workers deregistered successfully${NC}"
  echo ""
  echo -e "${YELLOW}Note:${NC} Deregistered workers will be removed from Consul"
  echo "  after the deregister_critical_service_after timeout (typically 30s)"
  exit 0
else
  echo ""
  echo -e "${RED}❌ Some deregistrations failed${NC}"
  exit 1
fi
