# UI Dashboard Development Guide

**Document:** UI_INTEGRATION_DEVELOPMENT.md  
**Date Created:** March 28, 2026  
**Last Updated:** March 28, 2026  

---

## Table of Contents

1. [Architecture](#architecture)
2. [Local Development](#local-development)
3. [Building](#building)
4. [API Integration](#api-integration)
5. [Testing](#testing)
6. [Troubleshooting](#troubleshooting)

---

## Architecture

### System Overview

```
┌─────────────────────────┐
│   React Frontend        │
│  (Vite + Material UI)   │
│   on ui_backend/        │
│   frontend/             │
└────────────┬────────────┘
             │
             │ HTTP (JSON)
             ▼
┌─────────────────────────┐
│   ui_backend Binary     │
│  (Rust + Axum)          │
│  Port: 8081             │
│  
│  Endpoints:             │
│  - GET /                │ (index.html)
│  - GET /assets/*        │ (JS, CSS, fonts)
│  - GET /health          │ (health check)
│  - GET /dashboard/key   │ (Redis fetch)
└────────────┬────────────┘
             │
             │ Redis IPC
             ▼
┌─────────────────────────┐
│    Redis Database       │
│  (Status data)          │
└─────────────────────────┘
```

### Frontend Structure

```
ui_backend/
├── frontend/                       # React application
│   ├── src/
│   │   ├── app/                   # Route definitions
│   │   │   ├── (control-panel)/   # Dashboard routes
│   │   │   │   └── example/       # Example dashboard
│   │   │   └── (public)/          # Public pages
│   │   ├── components/            # Reusable React components
│   │   ├── hooks/                 # Custom React hooks
│   │   ├── utils/                 # Helper functions
│   │   ├── @mock-utils/           # Mock API (development)
│   │   └── index.tsx              # React root
│   ├── public/                    # Static assets
│   ├── build/                     # Production build output (generated)
│   ├── package.json
│   ├── vite.config.mts            # Vite build config
│   └── tsconfig.json
│
├── src/
│   └── main.rs                    # Rust backend binary
├── static/
│   ├── index.html                 # Legacy entry point (embedded)
│   ├── styles.css                 # Legacy CSS (embedded)
│   └── app.js                     # Legacy app code (embedded)
│
└── dist/                          # Vite build artifacts (at runtime)
```

### Tech Stack

**Frontend:**
- React 18
- TypeScript
- Vite (build tool)
- Material UI (components)
- React Router (SPA routing)
- TanStack Query (data fetching)
- Mock Service Worker (development mocking)

**Backend:**
- Axum web framework
- Tokio async runtime
- Redis async driver
- Rust 1.84 (edition 2024)

---

## Local Development

### Prerequisites

- **Node.js:** >=22.12.0 (check: `node --version`)
- **npm:** >=10.9.0 (check: `npm --version`)
- **Rust:** 1.84+ (check: `rustc --version`)
- **Redis:** Running locally or via Docker

### Setup

#### 1. Start Redis (if not already running)

```bash
# Using Docker
docker run -d -p 6379:6379 redis:latest

# Or use docker-compose
docker-compose -f docker/kionas.docker-compose.yaml up redis
```

#### 2. Start Backend (in terminal 1)

```bash
cd /workspace
cargo run --bin ui_backend

# Expected output:
# [INFO] starting ui_backend on 0.0.0.0:8081
```

#### 3. Start Frontend Dev Server (in terminal 2)

```bash
cd /workspace/ui_backend/frontend

# Install dependencies (first time only)
npm ci

# Start dev server with hot reload
npm run dev

# Expected output:
#   VITE v7.3.1  ready in 1000 ms
#   ➜  Local:   http://localhost:5173/
```

#### 4. Open Browser

- **Frontend dev:** `http://localhost:5173` (Vite dev server with hot reload)
- **Backend served:** `http://localhost:8081` (production-like, static)

### Development Workflow

1. **Edit React components** in `ui_backend/frontend/src/`
2. **Vite auto-reloads** on file save (hot module replacement)
3. **Backend changes:** Edit `ui_backend/src/main.rs`, then restart with `cargo run`
4. **Add new API endpoints:** Update backend, then call from React components

---

## Building

### Build Frontend Only

```bash
cd ui_backend/frontend

# Build optimized production bundle
npm run build

# Output: build/ directory with ~12.77 MB of assets
```

### Build Backend Binary

```bash
# Development build
cargo build --bin ui_backend

# Release (optimized) build
cargo build --release --bin ui_backend

# Output: target/debug/ui_backend or target/release/ui_backend
```

### Full Integration Build

Use the provided build scripts:

**Windows:**
```bash
./scripts/build_ui.bat
```

**Linux/Mac:**
```bash
./scripts/build_ui.sh
```

**Docker:**
```bash
# Development containers (with npm build inside)
docker-compose -f docker/kionas.docker-compose.yaml build ui-backend

# Production image (multi-stage optimized)
docker build -f docker/Dockerfile.ui-backend -t kionas-ui-backend:latest .
```

---

## API Integration

### Available Backend Endpoints

| Endpoint | Method | Purpose | Response |
|----------|--------|---------|----------|
| `/` | GET | Serve index.html | HTML |
| `/styles.css` | GET | Serve CSS stylesheet | CSS |
| `/app.js` | GET | Serve JS bundle | JavaScript |
| `/health` | GET | Health check | "ok" (200) |
| `/dashboard/key` | GET | Fetch Redis key | JSON (200/404/503) |

### Dashboard Key Query

**Request:**
```
GET /dashboard/key?name=server_stats
```

**Valid Aliases:**
- `server_stats` — Server runtime statistics
- `sessions` — Active session metadata
- `tokens` — Authentication token metrics
- `workers` — Worker node status
- `consul_cluster_summary` — Consul cluster health

**Response (200 OK):**
```json
{
  "uptime_seconds": 3600,
  "memory_mb": 256,
  "connections": 42
}
```

**Response (404 Not Found):**
```
key not found
```

**Response (503 Service Unavailable):**
```
redis unavailable
```

### Making API Calls from React

**Using Fetch API:**
```typescript
async function getDashboardData(alias: string) {
  const response = await fetch(
    `/dashboard/key?name=${encodeURIComponent(alias)}`
  );
  
  if (!response.ok) {
    throw new Error(`API error: ${response.status}`);
  }
  
  return response.json();
}
```

**Using TanStack Query:**
```typescript
import { useQuery } from '@tanstack/react-query';

function useDashboardData(alias: string) {
  return useQuery({
    queryKey: ['dashboard', alias],
    queryFn: async () => {
      const response = await fetch(`/dashboard/key?name=${alias}`);
      if (!response.ok) throw new Error('Failed to fetch');
      return response.json();
    },
    refetchInterval: 15000,  // Every 15 seconds
  });
}

// Usage in component:
const { data, isLoading, error } = useDashboardData('server_stats');
```

### Adding New Endpoints

**Backend (Rust):**

```rust
// In ui_backend/src/main.rs

async fn my_new_handler(State(state): State<AppState>) -> Response {
    // Fetch data from Redis or compute
    (StatusCode::OK, "response data").into_response()
}

// In Router initialization:
.route("/my/endpoint", get(my_new_handler))
```

**Frontend (React):**

```typescript
// In any React component
const response = await fetch('/my/endpoint');
const data = await response.json();
```

---

## Testing

### Frontend Testing

**Run Linter:**
```bash
cd ui_backend/frontend
npm run lint
npm run lint:fix  # Auto-fix issues
```

**Run Tests (if configured):**
```bash
npm test
```

**Manual Testing:**
- Open `http://localhost:5173` in browser
- Check browser DevTools console for errors
- Verify all dashboard panels render

### Backend Testing

**Run Tests:**
```bash
cargo test --bin ui_backend
```

**Manual Testing:**
```bash
# Health check
curl http://localhost:8081/health

# Dashboard API
curl "http://localhost:8081/dashboard/key?name=server_stats"

# Check logs
tail -f /tmp/kionas-ui.log
```

### Integration Testing

See [docs/UI_INTEGRATION_TESTING.md](UI_INTEGRATION_TESTING.md) for comprehensive E2E testing procedures.

---

## Troubleshooting

### Frontend Issues

#### "Module not found" error

**Problem:** Vite can't find a module or component

**Solution:**
```bash
# Clear node_modules and rebuild
rm -rf ui_backend/frontend/node_modules package-lock.json
cd ui_backend/frontend
npm ci
npm run dev
```

#### Dev server shows blank page

**Problem:** App loads but displays nothing

**Solutions:**
- Check browser console for JavaScript errors
- Verify backend is running and accessible
- Clear browser cache: `Ctrl+Shift+Delete` (Chrome/Firefox)
- Check that React Router paths match

#### Slow development server

**Problem:** Hot reload takes >5 seconds

**Solutions:**
- Check CPU and disk usage
- Reduce TypeScript type-checking: adjust `tsconfig.json`
- Use `npm run dev -- --open=false` to skip browser auto-open

### Backend Issues

#### "Port 8081 already in use"

**Problem:** Another process is using port 8081

**Solutions:**
```bash
# Find process using port 8081
lsof -i :8081  # Mac/Linux
netstat -ano | findstr :8081  # Windows

# Kill process
kill -9 <PID>  # Mac/Linux
taskkill /PID <PID> /F  # Windows

# Use different port
UI_BACKEND_PORT=8082 cargo run --bin ui_backend
```

#### Redis connection errors

**Problem:** Backend can't connect to Redis

**Solutions:**
```bash
# Check Redis is running
redis-cli ping  # Should return PONG

# Verify Redis URL
echo $REDIS_URL
# Default: redis://localhost:6379/2

# Connect directly
redis-cli -n 2
```

### API Issues

#### CORS errors in browser console

**Problem:** Frontend can't reach backend due to CORS

**Solution:**
- Backend should be on same origin as frontend in dev
- For dev server on :5173 and backend on :8081, check backend CORS headers
- Current Axum config may need CORS middleware if needed

#### Dashboard shows empty panels

**Problem:** API returns 404 or no data

**Solutions:**
1. Check Redis has data: `redis-cli get dashboard:server_stats`
2. Verify alias is correct: server_stats, sessions, tokens, workers, consul_cluster_summary
3. Check backend logs: Do they show Redis read errors?
4. Ensure Redis is running and accessible

### Build Issues

#### `npm run build` fails

**Problem:** Vite build process fails

**Solutions:**
```bash
# Check Node version
node --version  # Should be >=22.12.0

# Clear cache
rm -rf ui_backend/frontend/build node_modules/.vite

# Try again
npm run build

# Check output
ls -la ui_backend/frontend/build/
```

#### `cargo build` fails

**Problem:** Rust compilation fails

**Solutions:**
```bash
# Update Rust
rustup update

# Check dependencies
cargo tree --depth 1

# Clean rebuild
cargo clean
cargo build --bin ui_backend
```

---

## Development Tips

### Hot Reload

- **Frontend (Vite):** Auto-refreshes on file save
- **Backend (Axum):** Requires manual restart
- Use `cargo watch` for automatic restart on backend changes:
  ```bash
  cargo install cargo-watch
  cargo watch -x 'run --bin ui_backend'
  ```

### Debugging

**Browser DevTools:**
- F12 → Console tab for JavaScript errors
- Network tab to see API calls and responses
- Storage tab to inspect localStorage, cookies, IndexedDB

**IDE:**
- VS Code: Install Rust Analyzer extension for code hints
- Set breakpoints in Rust code and run with `rust-gdb`

### Performance

- Use browser DevTools Performance tab to profile React renders
- Check Network tab to measure API response times
- Check Memory tab for leaks over time

---

## References

- [Vite Documentation](https://vitejs.dev/)
-[React Documentation](https://react.dev/)
- [Material-UI Documentation](https://mui.com/)
- [Axum Documentation](https://docs.rs/axum/)
- [Tokio Documentation](https://tokio.rs/)

---

## Next Steps

1. Start dev servers (frontend + backend)
2. Modify a component and verify hot reload
3. Add a new API endpoint to fetch additional data
4. Deploy to Docker environment for full testing

