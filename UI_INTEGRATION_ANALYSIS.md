# UI Integration Analysis: Vitejs Skeleton + ui_backend Binary

**Date:** March 28, 2026  
**Status:** Pre-Integration Analysis  
**Scope:** Integrating Vitejs React frontend with existing Rust backend webserver

---

## Executive Summary

The project has two disconnected UI components:
1. **Current backend** (`ui_backend`): Axum webserver serving pre-built static files (index.html, styles.css, app.js)
2. **New Vitejs skeleton** (`ui_backend/frontend`): Full React 19 + TypeScript app, never built or integrated

**Key Finding:** ✅ **The existing `ui_backend` binary CAN serve the Vitejs build output.** A separate Node webserver is **NOT required** for production. The binary is flexible enough to serve the built React app.

---

## Current Architecture

### Backend (Rust)
- **Location:** `ui_backend/src/main.rs`
- **Framework:** Axum 0.8.4
- **Port:** 8081 (configurable)
- **Static files approach:** Files baked in at compile-time using `include_str!()`
- **Current source:** `ui_backend/static/` (index.html, styles.css, app.js)

### Frontend (React/Vitejs)
- **Location:** `ui_backend/frontend/`
- **Framework:** React 19.2.4 + TypeScript + Vite
- **Build output:** `ui_backend/frontend/build/` (does not exist yet)
- **Dev server:** Port 3000 (not exposed in docker-compose)
- **Build command:** `npm run build` (requires Node >=22.12.0)

### Docker Setup
- **Container:** `docker-compose kionas.docker-compose.yaml` line 250
- **Service:** `ui-backend` on port 8081
- **Current command:** Runs precompiled binary from `docker-target/debug/ui_backend`
- **No frontend build step** currently in docker-compose or Dockerfile.dev

---

## Integration Findings

### 1. **Do We Need Another Container Running a Webserver?**

**Answer: NO** (for production)

**Reasoning:**
- The Rust binary already serves static files with `axum`
- Vite's production output is just static files (HTML, JS, CSS, assets)
- No need for a separate Node.js container or HTTP server
- The binary can be the **single HTTP endpoint** for both backend APIs and frontend assets

**Exception:** During **development**, you might want to run the Vite dev server separately for:
- Hot module replacement (HMR)
- Faster iteration without recompiling the Rust binary
- Direct access to source maps and unminified code

### 2. **Can Our Binary Serve the Required Files?**

**Answer: YES**, with a minor modification

**Current mechanism:**
```rust
// Baked-in static files using include_str!()
let body = match path.as_str() {
    "/" => include_str!("../static/index.html"),
    "/styles.css" => include_str!("../static/styles.css"),
    "/app.js" => include_str!("../static/app.js"),
    _ => DEFAULT_404,
};
```

**How to serve Vitejs output:**
1. Build the React app: `npm run build` → outputs to `ui_backend/frontend/build/`
2. **Option A (Recommended):** Copy built files into `ui_backend/static/` before building the binary
   - Files get baked into binary at compile-time
   - Binary is self-contained, no external files needed
   - Efficient in containerized environments
   
3. **Option B (Alternative):** Modify the binary to serve from filesystem
   - More flexible for development (hot reload possible)
   - Slightly less efficient at runtime
   - Requires the `build/` folder to exist when binary runs

### 3. **Integration Points**

The current backend serves these **dashboard data endpoints** that the React app needs:

| Endpoint | Purpose | Used by |
|----------|---------|---------|
| `GET /health` | Health check | Monitoring |
| `GET /dashboard/key?name=server_stats` | Server metrics (memory, CPU, sessions) | Dashboard widget |
| `GET /dashboard/key?name=sessions` | Active session list | Sessions table |
| `GET /dashboard/key?name=tokens` | Auth token snapshots | Tokens table |
| `GET /dashboard/key?name=workers` | Worker nodes registry | Workers table |
| `GET /dashboard/key?name=consul_cluster_summary` | Cluster metadata | Cluster info |

**All endpoints already exist and return JSON.** The React app needs to call these same endpoints (no changes needed).

---

## Integration Strategies

### Strategy 1: Build-Time Integration (Recommended for Production)

**Steps:**
1. Build the Vitejs app: `npm run build` → `ui_backend/frontend/build/`
2. Copy all files from `build/` to `ui_backend/static/`
3. Update `ui_backend/src/main.rs` to serve from the combined static directory
4. Compile Rust binary (files are embedded)
5. Deploy the binary—everything is self-contained

**Pros:**
- ✅ Single deployable artifact (the binary)
- ✅ Fast startup, no disk I/O for static files
- ✅ Works perfectly in containers
- ✅ No external dependencies at runtime

**Cons:**
- ❌ Binary size increases with embedded assets
- ❌ Need to rebuild binary to update frontend

---

### Strategy 2: Filesystem Serving (Better for Development)

**Steps:**
1. Build the Vitejs app: `npm run build`
2. Modify `ui_backend/src/main.rs` to serve from `frontend/build/` in filesystem (not embedded)
3. Compile binary
4. Run binary with `frontend/build/` accessible at runtime

**Pros:**
- ✅ Smaller binary size
- ✅ Can update frontend without recompiling binary
- ✅ Easier to debug assets

**Cons:**
- ❌ Requires `build/` folder at runtime
- ❌ More I/O overhead
- ❌ Not ideal for containers (needs volume mounting)

---

### Strategy 3: Dual Dev Environment (Development Only)

**For fast iteration during development:**

1. **Terminal 1:** Start Vite dev server
   ```bash
   cd ui_backend/frontend
   npm install
   npm run dev                    # Runs on http://localhost:3000
   ```

2. **Terminal 2:** Start Rust backend
   ```bash
   cd ui_backend
   cargo run                      # Runs on http://localhost:8081, but serves nothing yet
   ```

3. **Browser:** Open `http://localhost:3000` (dev server with HMR)
4. **Config Vite as proxy:** Point API calls to `http://localhost:8081/dashboard/*`

**Note:** This requires updating `vite.config.mts` to proxy backend requests. See example below.

---

## Docker Container Modifications Needed

### Current Config ❌
```yaml
ui-backend:
  container_name: kionas-ui-backend
  ports:
    - 8081:8081
  command: ["bash", "-c", "/workspace/docker-target/debug/ui_backend"]
```

**Issue:** No frontend build step.

### Recommended Config ✅
```yaml
ui-backend:
  container_name: kionas-ui-backend
  ports:
    - 8081:8081
  environment:
    UI_BACKEND_BIND: 0.0.0.0
    UI_BACKEND_PORT: 8081
    REDIS_URL: redis://kionas-redis:6379/2
    NODEJS_VERSION: "22"
  volumes:
    - ..:/workspace:cached
  depends_on:
    warehouse: { condition: service_started }
  command: >
    bash -c "
    cd /workspace/ui_backend/frontend &&
    npm ci &&
    npm run build &&
    cp -r build/* ../static/ &&
    /workspace/docker-target/debug/ui_backend
    "
```

**What changed:**
1. Added Node build step before Binary runs
2. Copy Vite output to `static/`
3. Binary serves the combined app

### Alternative: Multi-Stage Docker Build

Create `docker/Dockerfile.ui-backend`:
```dockerfile
# Stage 1: Build React app
FROM node:22-slim AS frontend-builder
WORKDIR /app/frontend
COPY ui_backend/frontend/package*.json ./
RUN npm ci && npm run build

# Stage 2: Serve with Rust backend
FROM rust:latest
WORKDIR /workspace
COPY . .
COPY --from=frontend-builder /app/frontend/build ./ui_backend/static/
RUN cargo build --release --bin ui_backend
CMD ["/workspace/target/release/ui_backend"]
```

---

## File Structure After Integration

```
ui_backend/
├── src/
│   └── main.rs                 # Serves static files (unchanged)
├── static/                     # All served files (currently has placeholder files)
│   ├── index.html              # React app root HTML
│   ├── assets/
│   │   ├── index-*.js          # Vite-bundled React code
│   │   ├── index-*.css         # Vite-bundled CSS
│   │   └── *.woff2             # Fonts
│   ├── favicon.ico
│   └── manifest.json
├── frontend/                   # Source React project
│   ├── src/                    # React components & logic
│   ├── public/                 # Static assets
│   ├── package.json
│   ├── vite.config.mts
│   ├── tsconfig.json
│   └── build/                  # Output (git-ignored, created by npm run build)
└── Cargo.toml
```

---

## Implementation Roadmap

### Phase 1: Setup (Local Development)
- [ ] Install Node.js >=22.12.0 in dev environment
- [ ] Navigate to `ui_backend/frontend/` and run `npm install`
- [ ] Run `npm run build` to verify build succeeds
- [ ] Verify `ui_backend/frontend/build/` contains output

### Phase 2: Code Review (Vitejs Skeleton)
- [ ] Review React components in `ui_backend/frontend/src/`
- [ ] Identify API calls that need to point to backend endpoints
- [ ] Update API base URL in frontend config
- [ ] Test component imports and dependencies

### Phase 3: Integration (Binary Modification)
- [ ] Create a script to copy `frontend/build/*` → `static/`
- [ ] Update `ui_backend/src/main.rs` to serve all files from `static/`
- [ ] Handle 404s for SPA routing (redirect to `index.html`)
- [ ] Test locally: `cargo run --bin ui_backend` and check `http://localhost:8081`

### Phase 4: Docker Integration
- [ ] Add frontend build step to `kionas.docker-compose.yaml`
- [ ] Or create multi-stage Dockerfile for ui_backend
- [ ] Test container build: `docker-compose build ui-backend`
- [ ] Verify `docker-compose up` runs the full stack

### Phase 5: Verification
- [ ] Frontend loads on `http://localhost:8081`
- [ ] API calls to `/dashboard/key` work correctly
- [ ] React components render dashboards with real data
- [ ] No console errors or 404s

---

## Technology Stack After Integration

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Frontend** | React 19 + TypeScript | UI components, state management |
| **Build Tool** | Vite | Fast bundling, HMR during dev |
| **Styling** | Tailwind CSS + Skeleton UI | Design system, component library |
| **Backend** | Axum (Rust) | HTTP server, API endpoints |
| **Data Source** | Redis | Real-time metrics dashboard |
| **Deployment** | Docker | Container orchestration |

---

## Bandwidth Requirements

### Development
- **Vite dev server:** ~3-5 MB/s (HMR)
- **Node deps:** ~500 MB per install
- **Build time:** ~30-45 seconds

### Production
- **Binary size:** ~100-150 MB (includes embedded assets)
- **Runtime memory:** ~50-100 MB (Rust binary is lightweight)
- **First page load:** <1 second (all assets embedded)

---

## Conclusion

**Can the ui_backend binary serve the Vitejs frontend?**
✅ **YES, absolutely.** The binary already serves static files and can easily be extended to serve the built React app.

**Do we need another container?**
❌ **NO.** A single Rust binary is sufficient for production. During development, optionally run Vite dev server separately for faster iteration.

**Recommended approach:**
1. **Development:** Build frontend locally, run Vite dev server + Rust backend separately
2. **Production:** Build frontend as part of docker-compose, embed in binary
3. **Deployment:** Single containerized Axum binary serving both API and frontend

---

## Next Steps

1. **Approval:** Confirm integration strategy (Build-Time vs Filesystem)
2. **Phase 1 Setup:** Install Node and build frontend skeleton  
3. **API Review:** Map frontend component API calls to existing backend endpoints
4. **Implementation:** Modify binary to serve combined static directory
5. **Testing:** Verify dashboard functionality with real Redis data

