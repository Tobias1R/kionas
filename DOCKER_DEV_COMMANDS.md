# Docker Commands for UI Development

## Quick Start

### 1. Build Backend Binary (One-Time)

```bash
cd c:\code\kionas
cargo build --bin ui_backend
```

**Expected Output:**
```
   Compiling ui_backend v0.1.0
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 2m
```

---

## Option A: Docker Dev Environment

### Start Dev Stack

```bash
cd c:\code\kionas

# Start services in background
docker-compose -f docker/docker-compose.dev.yaml up -d

# Watch logs
docker-compose -f docker/docker-compose.dev.yaml logs -f frontend
```

**Expected Output:**
```
[+] Running 4/4
 ✓ Network dev_network  Created
 ✓ Container kionas-redis-dev      Started
 ✓ Container kionas-ui-backend-dev  Started
 ✓ Container kionas-frontend-dev    Started

[kionas-frontend-dev] Installing dependencies...
[kionas-frontend-dev] added 972 packages in 5s
[kionas-frontend-dev] Starting Vite dev server...
[kionas-frontend-dev] ➜  Local:   http://0.0.0.0:5173/
[kionas-frontend-dev] ➜  Press h for help
```

### Access Services

- **Frontend:** http://localhost:5173
- **Backend:** http://localhost:8081
- **Health Check:** `curl http://localhost:8081/health`

### Edit & Test

```bash
# Edit a React component
code ui_backend/frontend/src/app/layouts/Dashboard.tsx

# Save (Ctrl+S) → Browser auto-refreshes within 1-2 seconds
```

### View Backend Logs

```bash
docker-compose -f docker/docker-compose.dev.yaml logs -f ui-backend
```

### Stop Services

```bash
docker-compose -f docker/docker-compose.dev.yaml down
```

**Clean up volumes (optional):**
```bash
docker-compose -f docker/docker-compose.dev.yaml down -v
```

---

## Option B: Local Development (Recommended for Speed)

### Terminal 1: Start Backend

```bash
cd c:\code\kionas
cargo run --bin ui_backend
```

**Expected Output:**
```
[Server] Listening on 0.0.0.0:8081
[Server] Health check endpoint: GET /health
```

### Terminal 2: Start Frontend

```bash
cd c:\code\kionas\ui_backend\frontend

npm install
npm run dev
```

**Expected Output:**
```
  VITE v7.3.1  ready in 257ms

  ➜  Local:   http://localhost:5173/
  ➜  press h to show help
```

### Access & Test

- **Frontend:** http://localhost:5173 (auto-refresh on save)
- **Backend:** http://localhost:8081 (API)

---

## Troubleshooting

### Frontend Won't Start (Docker)

```bash
# Check logs
docker-compose -f docker/docker-compose.dev.yaml logs frontend

# Rebuild
docker-compose -f docker/docker-compose.dev.yaml build --no-cache frontend

# Restart
docker-compose -f docker/docker-compose.dev.yaml restart frontend
```

### Backend Won't Connect (Docker)

```bash
# Verify backend is running
docker-compose -f docker/docker-compose.dev.yaml logs ui-backend

# Test API endpoint inside container
docker exec kionas-ui-backend-dev curl -s http://localhost:8081/health
```

### Port Already in Use

```bash
# Port 5173 in use
netstat -ano | findstr :5173
taskkill /PID <PID> /F

# Port 8081 in use
netstat -ano | findstr :8081
taskkill /PID <PID> /F
```

### Changes Not Reflecting

```bash
# Hard refresh browser
Ctrl+Shift+R (Chrome/Firefox)
Cmd+Shift+R (Mac)

# Or clear browser cache
# DevTools → Application → Clear storage → Clear site data
```

---

## Testing API Calls

### Get Server Stats

```bash
curl http://localhost:8081/dashboard/key?name=server_stats
```

**Response:**
```json
{
  "field1": "value1",
  "field2": "value2"
}
```

### Get Sessions

```bash
curl http://localhost:8081/dashboard/key?name=sessions
```

### Get Workers

```bash
curl http://localhost:8081/dashboard/key?name=workers
```

### Health Check

```bash
curl http://localhost:8081/health
```

**Response:**
```
ok
```

---

## Common Workflows

### Develop a New Dashboard Widget

```bash
# 1. Start dev environment (pick Option A or B above)

# 2. Edit component
code ui_backend/frontend/src/app/shared-components/MyWidget.tsx

# 3. Save (Ctrl+S)

# 4. Browser auto-refreshes (1-2 seconds)

# 5. Check browser console for errors
# Open DevTools: F12 → Console

# 6. Test API calls
# DevTools → Network → filter by /dashboard
# Verify response is correct JSON

# 7. Repeat until satisfied
```

### Debug TypeScript Errors

```bash
# Terminal 2 (frontend)
# Watch for compilation errors:
# "npm run dev" will show:
# [error] src/app/layouts/Dashboard.tsx (5:3): Property 'foo' does not exist

# Fix the error, save
# Auto-recompile within 100ms
```

### Backend Changes

```bash
# 1. Edit Rust code
code ui_backend/src/main.rs

# 2. Terminal 1 (backend)
# Stop: Ctrl+C
# Recompile: cargo build --bin ui_backend
# Run: cargo run --bin ui_backend

# 3. Test API
curl http://localhost:8081/dashboard/key?name=test
```

---

## Docker Compose Log Management

### View All Logs

```bash
docker-compose -f docker/docker-compose.dev.yaml logs
```

### Follow Logs (Real-Time)

```bash
# All services
docker-compose -f docker/docker-compose.dev.yaml logs -f

# Specific service
docker-compose -f docker/docker-compose.dev.yaml logs -f frontend
docker-compose -f docker/docker-compose.dev.yaml logs -f ui-backend
docker-compose -f docker/docker-compose.dev.yaml logs -f redis
```

### Last N Lines

```bash
docker-compose -f docker/docker-compose.dev.yaml logs --tail=50
```

---

## Environment Setup Check

```bash
# Verify Node.js
node --version   # Should be >= 22.12.0

# Verify npm
npm --version    # Should be >= 10.9.0

# Verify Rust
rustc --version  # Should work
cargo --version

# Verify Docker
docker --version

# Verify docker-compose
docker-compose --version
```

---

## Quick Reference

| Task | Command |
|------|---------|
| Build backend | `cargo build --bin ui_backend` |
| Start local frontend | `cd ui_backend/frontend && npm run dev` |
| Start local backend | `cargo run --bin ui_backend` |
| Start Docker dev stack | `docker-compose -f docker/docker-compose.dev.yaml up -d` |
| View logs | `docker-compose -f docker/docker-compose.dev.yaml logs -f` |
| Stop Docker stack | `docker-compose -f docker/docker-compose.dev.yaml down` |
| Access frontend | http://localhost:5173 |
| Access backend | http://localhost:8081 |
| Test API | `curl http://localhost:8081/dashboard/key?name=servers` |
| Health check | `curl http://localhost:8081/health` |

