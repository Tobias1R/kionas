# UI Development Guide - Hot Reload Workflow

**Last Updated:** March 28, 2026  
**Status:** Ready for Development

---

## Quick Start: Local Development (Recommended for Speed)

**Best for:** Rapid iteration, debugging, immediate feedback

### Prerequisites
```bash
node --version    # >= 22.12.0
npm --version     # >= 10.9.0
rustc --version   # >= 1.84
cargo --version
```

### Setup (One-Time)

```bash
# Terminal 1: Build Rust backend
cd c:\code\kionas
cargo build --bin ui_backend

# Terminal 2: Start React dev server
cd c:\code\kionas\ui_backend\frontend
npm install
npm run dev
```

### Result
- **Frontend:** http://localhost:5173 (Auto-refresh on save)
- **Backend:** http://localhost:8081 (API calls)

### Workflow

1. Edit React component in VSCode:
   ```tsx
   // ui_backend/frontend/src/app/layouts/Dashboard.tsx
   export default function Dashboard() {
     return <h1>New Feature Here</h1>
   }
   ```

2. Save file (Ctrl+S)

3. Browser auto-refreshes in ~1 second

4. See your changes immediately ✓

### Backend Changes

If you modify Rust code:

```bash
# Terminal 1
cargo build --bin ui_backend

# Terminal 2 (restart)
# Stop: Ctrl+C
# Run: cargo run --bin ui_backend
```

---

## Docker Development (For Testing Container Setup)

**Best for:** Testing Docker environment, multi-service integration

### One-Time Setup

```bash
# Build backend binary
cd c:\code\kionas
cargo build --bin ui_backend
```

### Start Dev Environment

```bash
cd c:\code\kionas

# Using docker-compose
docker-compose -f docker/docker-compose.dev.yaml up -d

# Wait for services to start
docker-compose -f docker/docker-compose.dev.yaml logs frontend

# Check health
curl http://localhost:5173
curl http://localhost:8081/health
```

### Access
- **Frontend:** http://localhost:5173 (Vite dev server)
- **Backend:** http://localhost:8081 (API)
- **Logs:** `docker-compose -f docker/docker-compose.dev.yaml logs -f frontend`

### Workflow

Same as local development, but in Docker containers:

1. Edit files in VSCode (`ui_backend/frontend/src/**/*.tsx`)
2. Vite dev server inside container detects changes
3. Browser auto-refreshes within 1-2 seconds
4. Backend API available at http://localhost:8081

### Cleanup

```bash
docker-compose -f docker/docker-compose.dev.yaml down

# Remove volumes (optional, keeps node_modules)
docker-compose -f docker/docker-compose.dev.yaml down -v
```

---

## Architecture: Dev vs Production

### Development (What We're Doing Now)
```
VSCode (src files)
    ↓
Vite Dev Server (:5173)
    ↓
Hot Module Reload (HMR)
    ↓
Browser auto-refresh
    ↓
Backend API (:8081)
```

**Characteristics:**
- Source map debugging
- Unminified code
- TypeScript live compilation
- CSS hot reload
- Fast iteration: 1-2s per change

### Production (Future)
```
npm run build → dist/ (12.77 MB)
    ↓
Rust binary embeds dist/
    ↓
Single binary (:8081)
    ↓
Optimized: minified + gzipped
```

**Characteristics:**
- Minified bundle (~2.8 MB JS)
- Gzipped assets (~700 KB)
- No dev server
- Single artifact to deploy

---

## Common Development Tasks

### Run Frontend Tests

```bash
cd ui_backend/frontend

# Unit tests
npm test

# Watch mode (re-run on changes)
npm test -- --watch
```

### Run Rust Tests

```bash
cd c:\code\kionas

# All tests
cargo test

# Specific binary
cargo test --bin ui_backend

# Watch mode (requires cargo-watch)
cargo watch -x test
```

### Format Code

```bash
# Frontend (TypeScript/React)
cd ui_backend/frontend
npm run format

# Backend (Rust)
cd c:\code\kionas
cargo fmt --all
clippy --all-targets --all-features -- -D warnings
```

### Debug Frontend

1. Open DevTools: F12
2. Go to Sources tab
3. Set breakpoints in compiled JS
4. Or use VSCode debugger: Debug → Start Debugging

### Debug Backend

```bash
# Terminal 1: Start with debug info
RUST_LOG=debug cargo run --bin ui_backend

# Terminal 2: Send test request
curl http://localhost:8081/health
```

---

## Troubleshooting

### Issue: "npm: command not found" in Docker
**Solution:** Rebuild the image
```bash
docker-compose -f docker/docker-compose.dev.yaml build --no-cache frontend
```

### Issue: "Cannot GET /" on 5173
**Solution:** Wait for Vite to start (~5-10s)
```bash
docker-compose -f docker/docker-compose.dev.yaml logs frontend
# Watch for: "Local: http://..."
```

### Issue: API calls fail with CORS error
**Solution:** Ensure Vite is proxying to backend
- Check `vite.config.ts` has server.proxy for `/dashboard`
- Verify backend is accessible: `curl http://localhost:8081/health`

### Issue: Changes not reflecting in browser
**Solution:** Vite not detecting changes
```bash
# Try:
1. Hard refresh: Ctrl+Shift+R (clears cache)
2. Stop/restart Vite: kill terminal, npm run dev
3. Check file was saved: ls -la ui_backend/frontend/src/
```

### Issue: "Cannot find module 'tsc'"
**Solution:** npm dependencies not installed
```bash
cd ui_backend/frontend
node node_modules/.bin/tsc --version
# If fails: npm ci
```

---

## Development Best Practices

1. **Keep backend running:** Leave `cargo run` in terminal 1
2. **Hot reload first:** Change UI, see result in 1-2s
3. **Then test API:** Ensure /dashboard/key returns expected data
4. **Use Browser DevTools:** Network tab shows real requests
5. **Check Redux/Query state:** React DevTools extension helps

---

## Next Steps

Once satisfied with local development:

1. **Build for production:**
   ```bash
   npm run build    # Creates dist/ (12.77 MB)
   ```

2. **Embed in binary (future):**
   ```bash
   # Copy dist → ui_backend/static
   # Re-compile: cargo build --release --bin ui_backend
   # Result: Single 50-70 MB binary with all assets
   ```

3. **Deploy to Docker:**
   ```bash
   # Use Dockerfile.ui-backend (production multi-stage)
   docker build -f docker/Dockerfile.ui-backend -t kionas/ui-backend:v1.0 .
   docker push kionas/ui-backend:v1.0
   ```

---

## References

- [Vite HMR Documentation](https://vitejs.dev/guide/hmr.html)
- [React Fast Refresh](https://react-fast-refresh.netlify.app/)
- [Cargo Watch](https://github.com/watchexec/cargo-watch)
- [Docker Compose Reference](https://docs.docker.com/compose/compose-file/03-compose-file/)

