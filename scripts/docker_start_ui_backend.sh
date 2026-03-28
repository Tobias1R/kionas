#!/bin/bash

# What: Orchestrate frontend build and ui_backend startup in Docker
#
# Inputs:
# - Environment variables from docker-compose
#
# Output:
# - Running ui_backend service serving React frontend
#
# Details:
# - Builds React app with npm
# - Prepares dist directory with build artifacts
# - Starts ui_backend binary

set -e

echo "[ui-backend] Preparing environment..."

# Navigate to workspace
cd /workspace

echo "[ui-backend] Building frontend React application..."
cd ui_backend/frontend

# Install dependencies
npm ci

# Build React app
npm run build

echo "[ui-backend] Frontend build completed"

# Prepare dist directory
echo "[ui-backend] Preparing static assets..."
cd /workspace
mkdir -p ui_backend/dist
cp -r ui_backend/frontend/build/* ui_backend/dist/

echo "[ui-backend] Starting ui_backend service..."
echo "[ui-backend] Listening on: ${UI_BACKEND_BIND}:${UI_BACKEND_PORT}"

# Start the ui_backend binary
/workspace/docker-target/debug/ui_backend
