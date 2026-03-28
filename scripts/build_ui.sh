#!/bin/bash

# What: Build frontend React app and prepare deployment
# Output: Frontend build artifacts copied to ui_backend/dist
# Details: Installs deps, builds Vite bundle, copies to dist directory

set -e

echo "============================================================"
echo "Kionas UI Integration Build Script (Linux/Mac)"
echo "============================================================"
echo ""

# Check if we're in the right directory
if [ ! -f "ui_backend/frontend/package.json" ]; then
    echo "Error: ui_backend/frontend/package.json not found"
    echo "Please run this script from the Kionas workspace root"
    exit 1
fi

echo "[1/3] Installing frontend dependencies..."
cd ui_backend/frontend
npm ci
if [ $? -ne 0 ]; then
    echo "Error: npm ci failed"
    cd ../..
    exit 1
fi
echo "[1/3] Dependencies installed successfully"

echo ""
echo "[2/3] Building React frontend with Vite..."
npm run build
if [ $? -ne 0 ]; then
    echo "Error: npm run build failed"
    cd ../..
    exit 1
fi
echo "[2/3] Frontend build completed successfully"

echo ""
echo "[3/3] Copying build artifacts to ui_backend/dist..."

# Navigate back to workspace root
cd ../..

# Remove old dist directory if it exists
if [ -d "ui_backend/dist" ]; then
    echo "Removing old dist directory..."
    rm -rf "ui_backend/dist"
fi

# Create new dist directory
mkdir -p "ui_backend/dist"

# Copy build output
echo "Copying: ui_backend/frontend/build/* to ui_backend/dist/"
cp -r ui_backend/frontend/build/* ui_backend/dist/
if [ $? -ne 0 ]; then
    echo "Error: Failed to copy build artifacts"
    exit 1
fi

echo "[3/3] Build artifacts copied successfully"

echo ""
echo "============================================================"
echo "Build completed successfully!"
echo "============================================================"
echo ""
echo "Next steps:"
echo "1. Run: cargo build --release --bin ui_backend"
echo "2. Binary will serve frontend from ui_backend/dist"
echo "3. Access at: http://localhost:8081"
echo ""
