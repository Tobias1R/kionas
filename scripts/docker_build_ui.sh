#!/bin/bash

# What: Build and prepare ui_backend Docker image with React frontend
#
# Inputs:
# - $1: Optional - "prod" for production build, defaults to "dev"
#
# Output:
# - Docker image: kionas-ui-backend (tagged appropriately)
#
# Details:
# - For dev: Uses docker-compose dev environment
# - For prod: Uses multi-stage Dockerfile.ui-backend

set -e

BUILD_TYPE="${1:-dev}"
IMAGE_NAME="kionas-ui-backend"

echo "============================================================"
echo "Building ui_backend Docker image (${BUILD_TYPE})"
echo "============================================================"
echo ""

if [ "$BUILD_TYPE" = "prod" ]; then
    echo "[PROD] Using multi-stage Dockerfile.ui-backend for optimized build..."
    docker build -f docker/Dockerfile.ui-backend -t ${IMAGE_NAME}:latest -t ${IMAGE_NAME}:prod .
    echo "[PROD] Image built successfully: ${IMAGE_NAME}:latest"
    
elif [ "$BUILD_TYPE" = "dev" ]; then
    echo "[DEV] Using docker-compose dev environment..."
    docker-compose -f docker/kionas.docker-compose.yaml build ui-backend
    echo "[DEV] Dev environment prepared successfully"
    
else
    echo "Error: Invalid build type '${BUILD_TYPE}'"
    echo "Usage: $0 [dev|prod]"
    exit 1
fi

echo ""
echo "============================================================"
echo "Build completed successfully!"
echo "============================================================"
echo ""
echo "Next steps:"
if [ "$BUILD_TYPE" = "prod" ]; then
    echo "1. Run: docker run -p 8081:8081 --net warehouse_network ${IMAGE_NAME}:latest"
else
    echo "1. Run: docker-compose -f docker/kionas.docker-compose.yaml up ui-backend"
fi
echo "2. Access at: http://localhost:8081"
echo ""
