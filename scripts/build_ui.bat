@echo off
REM What: Build frontend React app and prepare deployment
REM Output: Frontend build artifacts copied to ui_backend/dist
REM Details: Installs deps, builds Vite bundle, copies to dist directory

setlocal enabledelayedexpansion

echo ============================================================
echo Kionas UI Integration Build Script (Windows)
echo ============================================================
echo.

REM Check if we're in the right directory
if not exist "ui_backend\frontend\package.json" (
    echo Error: ui_backend\frontend\package.json not found
    echo Please run this script from the Kionas workspace root
    exit /b 1
)

echo [1/3] Installing frontend dependencies...
cd ui_backend\frontend
call npm ci
if !errorlevel! neq 0 (
    echo Error: npm ci failed
    cd ..\..
    exit /b 1
)
echo [1/3] Dependencies installed successfully

echo.
echo [2/3] Building React frontend with Vite...
call npm run build
if !errorlevel! neq 0 (
    echo Error: npm run build failed
    cd ..\..
    exit /b 1
)
echo [2/3] Frontend build completed successfully

echo.
echo [3/3] Copying build artifacts to ui_backend\dist...

REM Navigate back to workspace root
cd ..\..

REM Remove old dist directory if it exists
if exist "ui_backend\dist" (
    echo Removing old dist directory...
    rmdir /s /q "ui_backend\dist"
)

REM Create new dist directory
mkdir "ui_backend\dist"

REM Copy build output
echo Copying: ui_backend\frontend\build\* to ui_backend\dist\
xcopy "ui_backend\frontend\build\*" "ui_backend\dist" /E /I /Y
if !errorlevel! neq 0 (
    echo Error: Failed to copy build artifacts
    exit /b 1
)

echo [3/3] Build artifacts copied successfully

echo.
echo ============================================================
echo Build completed successfully!
echo ============================================================
echo.
echo Next steps:
echo 1. Run: cargo build --release --bin ui_backend
echo 2. Binary will serve frontend from ui_backend\dist
echo 3. Access at: http://localhost:8081
echo.

endlocal
