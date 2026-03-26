#!/bin/bash
# Worker Service Management Utility
# Query and manage worker services in Consul
#
# Usage:
#   ./worker_service_mgmt.sh list               # list all workers
#   ./worker_service_mgmt.sh status             # show health status
#   ./worker_service_mgmt.sh health W1          # detailed health for worker
#   ./worker_service_mgmt.sh verify             # verify all workers reachable

set -e

# Configuration
CONSUL_URL="${CONSUL_URL:-http://localhost:8500}"
COMMAND="${1:-list}"
WORKER_ID="${2:-}"
OUTPUT_FORMAT="${OUTPUT_FORMAT:-table}"

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
GRAY='\033[0;37m'
NC='\033[0m'

usage() {
  cat << EOF
Usage: $(basename "$0") [COMMAND] [OPTIONS]

Worker service management and diagnostics.

Commands:
  list [FORMAT]          List all registered workers (table|json)
  status                 Show health status of all workers
  health WORKER_ID       Show detailed health check status for worker
  verify                 Verify connectivity to all registered workers
  info WORKER_ID         Show detailed information for worker
  count                  Print count of registered workers
  -h, --help             Show this help message

Environment Variables:
  CONSUL_URL             Override default Consul URL (http://localhost:8500)

Examples:
  # List all workers
  $(basename "$0") list

  # List workers in JSON format
  $(basename "$0") list json

  # Show health status of all workers
  $(basename "$0") status

  # Show detailed health for W1
  $(basename "$0") health W1

  # Verify connectivity to all workers
  $(basename "$0") verify

  # Get detailed info for W2
  $(basename "$0") info W2

  # Count registered workers
  $(basename "$0") count

EOF
  exit 1
}

# Verify Consul connectivity
verify_consul() {
  if ! curl -sf "$CONSUL_URL/v1/status/leader" > /dev/null 2>&1; then
    echo -e "${RED}❌ Failed to connect to Consul at $CONSUL_URL${NC}"
    exit 1
  fi
}

# List all registered workers
list_workers() {
  local format="${1:-table}"
  
  verify_consul
  
  RESPONSE=$(curl -sf "$CONSUL_URL/v1/catalog/service/worker")
  
  if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
    echo -e "${YELLOW}No workers registered${NC}"
    return 0
  fi
  
  if [ "$format" = "json" ]; then
    echo "$RESPONSE" | jq '.'
  else
    # Table format
    local count=$(echo "$RESPONSE" | jq '. | length')
    echo -e "${BLUE}Registered Workers: $count${NC}"
    echo ""
    printf "%-6s %-25s %-8s\n" "ID" "Address" "Port"
    printf "%.0s-" {1..45}; echo ""
    
    echo "$RESPONSE" | jq -r '.[] | "\(.ServiceID | gsub("worker-";"")) \(.ServiceAddress) \(.ServicePort)"' | \
    while read id address port; do
      printf "%-6s %-25s %-8s\n" "$id" "$address" "$port"
    done
  fi
}

# Show health status
show_status() {
  verify_consul
  
  RESPONSE=$(curl -sf "$CONSUL_URL/v1/catalog/service/worker")
  
  if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
    echo -e "${YELLOW}No workers registered${NC}"
    return 0
  fi
  
  local count=$(echo "$RESPONSE" | jq '. | length')
  echo -e "${BLUE}Worker Health Status: $count registered${NC}"
  echo ""
  
  printf "%-6s %-25s %-15s\n" "ID" "Address" "Status"
  printf "%.0s-" {1..50}; echo ""
  
  echo "$RESPONSE" | jq -r '.[] | "\(.ServiceID | gsub("worker-";"")) \(.ServiceAddress):\(.ServicePort) passing"' | \
  while read id addr status; do
    printf "%-6s %-25s ${GREEN}%-15s${NC}\n" "$id" "$addr" "$status"
  done
  echo ""
}

# Show detailed health for specific worker
show_health() {
  local worker_id="$1"
  
  if [ -z "$worker_id" ]; then
    echo -e "${RED}Error: Worker ID required${NC}"
    usage
  fi
  
  verify_consul
  
  # Get worker info
  RESPONSE=$(curl -sf "$CONSUL_URL/v1/catalog/service/worker" | jq ".[] | select(.ID == \"worker-$worker_id\")")
  
  if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
    echo -e "${RED}❌ Worker not found: $worker_id${NC}"
    exit 1
  fi
  
  echo -e "${BLUE}Worker: $worker_id${NC}"
  echo ""
  echo "$RESPONSE" | jq '{
    ID,
    Address,
    Node,
    Port: .ServicePort,
    Tags: .ServiceTags,
    Meta: .ServiceMeta
  }'
  echo ""
}

# Show detailed info for specific worker
show_info() {
  local worker_id="$1"
  
  if [ -z "$worker_id" ]; then
    echo -e "${RED}Error: Worker ID required${NC}"
    usage
  fi
  
  verify_consul
  
  RESPONSE=$(curl -sf "$CONSUL_URL/v1/catalog/service/worker" | jq ".[] | select(.ID == \"worker-$worker_id\")")
  
  if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
    echo -e "${RED}❌ Worker not found: $worker_id${NC}"
    exit 1
  fi
  
  local address=$(echo "$RESPONSE" | jq -r '.Address')
  local port=$(echo "$RESPONSE" | jq -r '.ServicePort')
  local flight_port=$((port + 1))
  
  echo -e "${BLUE}═══════════════════════════════════════════════${NC}"
  echo -e "${BLUE}Worker Information: $worker_id${NC}"
  echo -e "${BLUE}═══════════════════════════════════════════════${NC}"
  echo ""
  
  echo -e "${YELLOW}Service Details:${NC}"
  echo "  ID:            $(echo "$RESPONSE" | jq -r '.ID')"
  echo "  Address:       $address"
  echo "  Interops Port: $port"
  echo "  Flight Port:   $flight_port"
  echo "  Node:          $(echo "$RESPONSE" | jq -r '.Node')"
  echo "  Status:        $(echo "$RESPONSE" | jq -r '.Status')"
  echo ""
  
  echo -e "${YELLOW}Tags:${NC}"
  echo "$RESPONSE" | jq -r '.ServiceTags[]' | sed 's/^/  - /'
  echo ""
  
  echo -e "${YELLOW}Metadata:${NC}"
  echo "$RESPONSE" | jq -r '.ServiceMeta | to_entries[] | "  \(.key): \(.value)"'
  echo ""
  
  # Try to get health check status
  echo -e "${YELLOW}Health Checks:${NC}"
  CHECKS=$(curl -sf "$CONSUL_URL/v1/health/service/worker" | jq ".Checks[] | select(.ServiceID == \"worker-$worker_id\")")
  if [ -n "$CHECKS" ]; then
    echo "$CHECKS" | jq -r '.[] | "  \(.Name): \(.Status)"'
  else
    echo "  No health checks found"
  fi
  echo ""
}

# Verify connectivity to all workers
verify_workers() {
  verify_consul
  
  RESPONSE=$(curl -sf "$CONSUL_URL/v1/catalog/service/worker")
  
  if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
    echo -e "${YELLOW}No workers registered${NC}"
    return 0
  fi
  
  echo -e "${BLUE}Verifying connectivity to all workers...${NC}"
  echo ""
  
  local success=0
  local failed=0
  
  echo "$RESPONSE" | jq -r '.[] | "\(.ID | gsub("worker-";"")) \(.Address) \(.ServicePort)"' | \
  while read id address port; do
    echo -n "Checking $id ($address:$port)... "
    
    if timeout 5 bash -c "echo >/dev/tcp/$address/$port" 2>/dev/null; then
      echo -e "${GREEN}✓ Reachable${NC}"
    else
      echo -e "${RED}✗ Unreachable${NC}"
    fi
  done
  echo ""
}

# Count registered workers
count_workers() {
  verify_consul
  
  RESPONSE=$(curl -sf "$CONSUL_URL/v1/catalog/service/worker")
  
  if [ -z "$RESPONSE" ] || [ "$RESPONSE" = "null" ]; then
    echo "0"
  else
    echo "$RESPONSE" | jq '. | length'
  fi
}

# Parse command
case "$COMMAND" in
  list)
    list_workers "$OUTPUT_FORMAT"
    ;;
  status)
    show_status
    ;;
  health)
    show_health "$WORKER_ID"
    ;;
  info)
    show_info "$WORKER_ID"
    ;;
  verify)
    verify_workers
    ;;
  count)
    count_workers
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo -e "${RED}Unknown command: $COMMAND${NC}"
    usage
    ;;
esac
