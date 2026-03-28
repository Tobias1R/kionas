# UI Vitejs Integration - Verification & Testing Guide

**Date:** March 28, 2026  
**Phase:** 6 - Verification & Testing  
**Status:** Testing Plan Created  

---

## Testing Phases

### Phase 6.1-6.4: Environment Setup

#### Checklist

- [ ] **6.1 - Stop all running containers**
  ```bash
  docker-compose -f docker/kionas.docker-compose.yaml down
  ```

- [ ] **6.2 - Clean build artifacts (optional)**
  ```bash
  rm -rf ui_backend/dist ui_backend/frontend/build
  ```

- [ ] **6.3 - Run full docker-compose stack**
  ```bash
  docker-compose -f docker/kionas.docker-compose.yaml up -d
  ```

- [ ] **6.4 - Wait for services to stabilize**
  - Allow 30-60 seconds for all services to start
  - Check logs: `docker-compose logs -f ui-backend`

---

### Phase 6.5-6.10: Functional Verification

#### Test 6.5: Frontend Loads

- **Action:** Open browser to `http://localhost:8081`
- **Expected:** React app loads, dashboard visible
- **Success Criteria:**
  - [ ] Page loads within 2 seconds
  - [ ] No 404 errors in console
  - [ ] React DevTools show component tree

#### Test 6.6: UI Renders Correctly

- **Action:** Visual inspection of dashboard
- **Expected:** 5 widget panels render with correct styling
- **Success Criteria:**
  - [ ] All 5 panels present (server_stats, sessions, tokens, workers, consul_cluster_summary)
  - [ ] Layout is responsive and readable
  - [ ] Material Design styling applied correctly
  - [ ] No visual glitches or missing elements

#### Test 6.7: API Calls Work

- **Action:** Open browser DevTools → Network tab, observe calls
- **Expected:** Dashboard makes API requests to `/dashboard/key`
- **Success Criteria:**
  - [ ] GET requests to `/dashboard/key?name=<alias>`
  - [ ] Responses are 200 OK or 404 (expected if Redis keys empty)
  - [ ] Response time < 200ms per request
  - [ ] All 5 aliases queried in parallel

#### Test 6.8: No 404/CORS Errors

- **Action:** Check browser console for errors
- **Expected:** No network or CORS errors
- **Success Criteria:**
  - [ ] Console shows no red errors
  - [ ] No "Failed to fetch" messages
  - [ ] No CORS policy violations
  - [ ] All asset requests return 200 (CSS, JS, images, fonts)

#### Test 6.9: React Router Navigation

- **Action:** Click navigation links in app
- **Expected:** Routes change without page reload
- **Success Criteria:**
  - [ ] URL changes in address bar
  - [ ] Page content updates
  - [ ] Browser back/forward buttons work
  - [ ] No 404 errors when navigating

#### Test 6.10: Responsive Design

- **Action:** Resize browser window, test mobile view
- **Expected:** Layout adapts to different screen sizes
- **Success Criteria:**
  - [ ] Desktop view: 1920px width works
  - [ ] Tablet view: 768px width is readable
  - [ ] Mobile view: 375px width usable (hamburger menu appears)
  - [ ] No horizontal scrolling on mobile

---

### Phase 6.11-6.12: Documentation & Baselines

#### Baseline Metrics to Capture

**Performance Metrics:**
- [ ] Page load time (first contentful paint): _________ ms
- [ ] Dashboard API response time (5 calls parallel): _________ ms
- [ ] Asset sizes:
  - index.html: _________ KB
  - Main JS bundle: _________ KB
  - Main CSS bundle: _________ KB
  - Total assets folder: _________ MB

**Infrastructure Metrics:**
- [ ] Docker image size: _________ MB
- [ ] Container startup time: _________ seconds
- [ ] Memory usage at idle: _________ MB
- [ ] CPU usage at idle: _________ %

**Known Issues to Document:**
- Issue 1: _________________________________
- Issue 2: _________________________________
- Issue 3: _________________________________

---

## Automated Test Commands

### Run Full Test Suite

```bash
cd /workspace

# Build frontend
./scripts/build_ui.bat  # Windows
./scripts/build_ui.sh   # Linux/Mac

# Run local dev server
cd ui_backend/frontend
npm run dev

# In another terminal, run backend
cd /workspace
cargo run --bin ui_backend
```

### Quick Verification

```bash
# Test 1: Binary compiles
cargo check --bin ui_backend

# Test 2: Build script works
./scripts/build_ui.bat

# Test 3: Docker-compose config valid
docker-compose -f docker/kionas.docker-compose.yaml config

# Test 4: Health endpoint responsive
curl http://localhost:8081/health

# Test 5: Dashboard endpoint works
curl http://localhost:8081/dashboard/key?name=server_stats
```

---

## Integration Test Scenarios

### Scenario 1: Cold Start

**Objective:** Verify system startup from scratch

1. Stop all containers
2. Clean all build artifacts
3. Start docker-compose
4. Wait for ui-backend startup
5. Verify frontend loads within 2 seconds
6. Dashboard shows valid data (or 404 if Redis empty)

**Pass Criteria:** Page loads and dashboards render without errors

### Scenario 2: Data Flow

**Objective:** Verify real API data displays correctly

1. Start Redis with sample data
2. Start ui-backend
3. Navigate to dashboard
4. Observe data in JSON panels
5. Verify "Pretty-print" formatting is readable

**Pass Criteria:** JSON displays correctly in all 5 panels

### Scenario 3: Error Handling

**Objective:** Verify graceful error handling

1. Stop Redis
2. Click "Refresh" button
3. Observe error status in panels
4. Restart Redis
5. Click "Refresh" again
6. Verify recovery to normal state

**Pass Criteria:** UI shows appropriate status (Degraded → OK)

### Scenario 4: Navigation

**Objective:** Verify React Router works for SPA

1. Click links to different routes
2. Verify URL changes
3. Use browser back button
4. Verify page loads from new route
5. No full page reload occurs

**Pass Criteria:** All navigation works without page refresh

---

## Performance Benchmarks

### Target Metrics

| Metric | Target | Acceptable | Notes |
|--------|--------|-----------|-------|
| Page Load Time | < 1.5s | < 2s | First Contentful Paint |
| Dashboard Refresh | < 300ms | < 500ms | All 5 calls parallel |
| Binary Size | < 100MB | < 200MB | Including assets |
| Container Size | < 500MB | < 1GB | Docker image |
| Startup Time | < 3s | < 5s | From container start to ready |
| Memory (Idle) | < 50MB | < 100MB | After startup stabilizes |

---

## Known Limitations & Future Improvements

### Current Limitations
- No authentication (dashboard accessible to anyone on port 8081)
- No real-time updates (15-second polling interval)
- Limited to 5 predefined Redis keys
- No data export functionality
- No persistence across restarts

### Planned Enhancements (Future)
- [ ] RBAC authentication layer
- [ ] WebSocket real-time updates
- [ ] Custom dashboard layouts
- [ ] Data export (CSV, JSON)
- [ ] Dashboard state persistence
- [ ] Alert thresholds and notifications

---

## Rollback Plan

If issues are discovered:

1. **Frontend Issues:** Revert `ui_backend/src/main.rs` changes
2. **Build Issues:** Fallback to old static/*  files
3. **Docker Issues:** Use previous docker-compose version
4. **Data Loss:** None expected; no data stored in ui_backend

---

## Sign-Off

| Role | Name | Date | Status |
|------|------|------|--------|
| QA Lead | [Name] | [ ] | [ ] Approved |
| DevOps Lead | [Name] | [ ] | [ ] Approved |
| Project Lead | [Name] | [ ] | [ ] Approved |

---

## Test Execution Log

### Date: March 28, 2026

**Test Environment:** Windows DevLocal + Docker Desktop

**Tests Completed:**
- [x] Phase 6.1: Container cleanup - N/A (local dev)
- [x] Phase 6.2: Clean artifacts - Completed
- [ ] Phase 6.3: Run docker-compose - Pending (requires Docker setup)
- [ ] Phase 6.4: Wait for stabilization - Pending
- [ ] Phase 6.5-6.10: Functional verification - Pending
- [ ] Phase 6.11: Performance baseline - Pending
- [ ] Phase 6.12: Known issues documentation - Pending

**Notes:**
- All code compiles successfully
- Build scripts work on Windows/Linux
- Docker-compose configuration valid
- Ready for full E2E testing in Docker environment

---

## Next Steps

1. Deploy to Docker environment
2. Run all functional tests (6.5-6.10)
3. Capture performance metrics (6.11-6.12)
4. Document any issues found
5. Sign off on completion
6. Proceed to Phase 7 (Performance Review)
