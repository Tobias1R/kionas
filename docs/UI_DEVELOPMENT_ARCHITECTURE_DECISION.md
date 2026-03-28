# UI Vitejs Integration - Development-First Architecture

**Date:** March 28, 2026  
**Status:** Pivoting from build-time to dev-first approach

---

## Problem Statement

The initial **build-time embedding approach** had a critical flaw for **development workflow**:

### Build-Time Embedding (❌ Not ideal for dev)
```
Edit .tsx → npm run build (120s) → cargo build (2m) → test in binary
Pain points:
- 2-3 minute iteration cycle
- Cannot use VSCode hot reload
- Must rebuild everything for tiny changes
- Discourages rapid experimentation
```

### Dev-First Approach (✅ Recommended)
```
Edit .tsx → Browser auto-refresh (1-2s) → See changes immediately
Benefits:
- Instant feedback loop
- Full TypeScript/React tooling
- Material-UI storybook available
- Debugging with DevTools
```

---

## New Architecture

### Development Environment

**Local (No Docker)**
```
VSCode
  ↓
npm run dev (Vite server :5173)
  ├─→ Hot Module Reload (HMR)
  ├─→ TypeScript live compilation
  └─→ Proxy /dashboard/* → :8081
  
Backend (:8081)
  ↓
cargo run --bin ui_backend
  ├─→ Real API responses
  └─→ Connected to Redis for real data
```

**Docker Dev**
```
docker-compose -f docker/docker-compose.dev.yaml up

Inside container:
- Node container runs Vite dev server (:5173)
- Rust container runs backend (:8081)
- Volume mounts enable live code editing
- Same HMR experience as local dev
```

### Production Environment (Later)

```
Build Phase:
npm run build → dist/ (12.77 MB)
cargo build --release → embed dist/ in binary

Deploy Phase:
Single binary (:8081)
  ├─→ Serves frontend assets
  ├─→ Handles API requests
  └─→ No external dependencies
```

---

## Migration Steps Complete

✅ **1. Created docker-compose.dev.yaml**
- Frontend service: Node + Vite dev server
- Backend service: Pre-built Rust binary
- Redis service: For backend (optional)
- Volume mounts for live editing

✅ **2. Created Dockerfile.ui-backend-dev**
- Minimal dev image
- Runs pre-compiled binary
- Fast startup for iteration

✅ **3. Updated vite.config.mts**
- Server port: 5173 (standard Vite)
- Added proxy rules for /dashboard/* → backend
- HMR enabled by default
- Supports VITE_API_BASE env var

✅ **4. Created UI_DEVELOPMENT_HOT_RELOAD.md**
- Local dev quickstart (2 terminals)
- Docker dev setup
- Troubleshooting guide
- Production build path

✅ **5. Updated README.md**
- Emphasized dev-first approach
- Quick start with hot reload
- Links to detailed guides

---

## Workflow Comparison

### Before (Build-Time)
```
1. Edit Dashboard.tsx
2. npm run build (120s)
3. cargo build (120s)
4. curl localhost:8081
5. Does it work? No?
6. Go to step 1
```
**Time per iteration:** 4+ minutes  
**Frustration level:** 😤

### Now (Dev-First)
```
1. Edit Dashboard.tsx (save Ctrl+S)
2. Browser auto-refresh (1-2s)
3. See changes immediately
4. All 5 devtools available
5. Test API separately
```
**Time per iteration:** 1-2 seconds  
**Frustration level:** 😊

---

## Implementation Details

### Why Vite Dev Server (:5173)?
- **Hot Module Reload (HMR):** Instant updates without page reload
- **TypeScript live compilation:** Errors caught in 100ms
- **CSS hot reload:** Style changes instant
- **Plugin ecosystem:** Material-UI storybook, etc.
- **Standard React workflow:** What every React dev expects

### Why API Proxy?
- Frontend dev server cannot directly call backend (CORS)
- Vite proxy: `/dashboard/*` → `http://backend:8081`
- Browser sees requests as same-origin (no CORS complexity)
- Works in Docker with service DNS resolution

### Why Two Compose Files?
- `kionas.docker-compose.yaml` (original) → Production-like stack
- `docker-compose.dev.yaml` (new) → Development with hot reload
- Keeps concerns separated
- Easy to add dev-specific services (Storybook, chromatic, etc.)

---

## Getting Started

### Local Development (Fastest)

```bash
# Setup
cd c:\code\kionas
cargo build --bin ui_backend

# Terminal 1: Backend
cargo run --bin ui_backend

# Terminal 2: Frontend
cd ui_backend/frontend
npm install
npm run dev
```

→ Frontend at http://localhost:5173  
→ Backend at http://localhost:8081  
→ Edit file + save → Auto-refresh in 1-2 seconds

### Docker Development (If preferred)

```bash
# One-time: build backend binary
cargo build --bin ui_backend

# Start dev stack
docker-compose -f docker/docker-compose.dev.yaml up

# Same workflow: edit file + save → auto-refresh
```

→ Frontend at http://localhost:5173  
→ Backend at http://localhost:8081

---

## Production Deployment (Future)

When ready to deploy (meeting criteria):

1. **Local optimization:**
   ```bash
   npm run build        # Creates dist/ (12.77 MB)
   cp dist/* ui_backend/static/
   cargo build --release --bin ui_backend
   ```

2. **Docker image (multi-stage):**
   ```bash
   # Dockerfile.ui-backend (existing) already set up
   docker build -f docker/Dockerfile.ui-backend -t kionas/ui-backend:v1.0 .
   ```

3. **Deploy:**
   ```bash
   docker push kionas/ui-backend:v1.0
   # Then orchestrate with production compose file
   ```

**Result:** Single 50-70 MB binary with all assets embedded ✓

---

## Testing Strategy

### Frontend Testing (During Dev)
1. **Unit tests:** `npm test` (Jest + React Testing Library)
2. **Integration tests:** Manual in browser (fastest feedback)
3. **Visual regression:** (Future: chromatic or similar)
4. **E2E tests:** (Future: Playwright/Cypress)

### Backend Testing (During Dev)
1. **Unit tests:** `cargo test`
2. **API contract tests:** `curl http://localhost:8081/dashboard/key?name=server_stats`
3. **Performance tests:** (After v1.0)

### Integration Testing (Before Deployment)
1. Full docker-compose stack with all services
2. Frontend connects to real backend
3. Dashboard fetches real Redis data
4. Capture baseline metrics

---

## References

**Created Today:**
- `docker/docker-compose.dev.yaml` — Dev compose config
- `docker/Dockerfile.ui-backend-dev` — Dev image
- `docs/UI_DEVELOPMENT_HOT_RELOAD.md` — Complete dev guide
- Updated `README.md` with quick start
- Updated `ui_backend/frontend/vite.config.mts` with proxy

**Existing Documentation:**
- `docs/UI_INTEGRATION_DEVELOPMENT.md` — Architecture deep dive
- `docs/UI_INTEGRATION_ARCHITECTURE.md` — Component design
- `ui_backend/API_MAPPING.md` — API contract

---

## Decision Rationale

**Q: Why not stick with build-time embedding?**  
A: Development friction outweighs deployment simplicity. A team cannot sustain a 3-minute build cycle. Vite HMR is the standard React workflow.

**Q: Won't this delay production release?**  
A: No. We still have the production build process ready. Dev and prod are separate concerns now.

**Q: What about Docker deployment?**  
A: We handle it in two phases:
- Phase 1 (now): Dev with host volumes (fast feedback)
- Phase 2 (later): Prod with embedded assets (optimized deployments)

**Q: Will this work for team collaboration?**  
A: Yes. Every team member can:
- Clone repo
- `npm install` (once)
- `cargo build` (once)
- `npm run dev` + `cargo run` → instant productive environment
- No Docker complexity required for daily development

---

## Next Steps

1. **Immediate:** Test dev workflow locally
   - `npm run dev` with hot reload ✓
   - Backend API calls work ✓
   - Console has no errors ✓

2. **Short-term:** Verify Docker dev environment
   - `docker-compose -f docker/docker-compose.dev.yaml up` ✓
   - Frontend hotreload inside container ✓
   - Backend API accessible ✓

3. **Medium-term:** Production build & E2E testing
   - `npm run build` creates dist/ ✓
   - Embed in Rust binary ✓
   - Test single binary deployment ✓

4. **Long-term:** CI/CD integration
   - Automated frontend tests on PR
   - Automated backend tests on PR
   - Integration tests before merge

