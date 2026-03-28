# API Mapping: Frontend to Backend

**Date Created:** March 28, 2026  
**Status:** Complete  
**Last Updated:** March 28, 2026  

---

## Overview

This document maps all API calls made by the React frontend to their corresponding backend endpoints in the `ui_backend` Rust server.

### Current Backend Endpoints

| Endpoint | Method | Handler | Purpose |
|----------|--------|---------|---------|
| `/` | GET | `index_handler` | Serves `index.html` (React app entry point) |
| `/styles.css` | GET | `styles_handler` | Serves CSS stylesheet |
| `/app.js` | GET | `app_js_handler` | Serves JavaScript bundle |
| `/health` | GET | `health_handler` | Health check endpoint |
| `/dashboard/key` | GET | `get_dashboard_key` | Fetches Redis key value for dashboard widgets |

---

## Frontend API Calls

### Dashboard Widget Refresh Cycle

**Source File:** `static/app.js`  
**Function:** `fetchAlias(alias)` → `refreshDashboard()`  
**Query Interval:** 15 seconds (configurable as `POLL_INTERVAL_MS`)

#### Supported Aliases (Redis Keys)

The frontend calls `/dashboard/key?name=<alias>` with these aliases:

| Alias | Backend Mapping | Purpose | Data Format |
|-------|-----------------|---------|------------|
| `server_stats` | Redis key `REDIS_UI_DASHBOARD_SERVER_STATS_KEY` | Server runtime statistics | JSON object |
| `sessions` | Redis key `REDIS_UI_DASHBOARD_SESSIONS_KEY` | Active session metadata | JSON object |
| `tokens` | Redis key `REDIS_UI_DASHBOARD_TOKENS_KEY` | Authentication token metrics | JSON object |
| `workers` | Redis key `REDIS_UI_DASHBOARD_WORKERS_KEY` | Worker node status | JSON object |
| `consul_cluster_summary` | Redis key `REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY` | Consul cluster health | JSON object |

### API Call Details

#### GET /dashboard/key?name=\<alias\>

**Purpose:** Fetch a single Redis key value for dashboard display

**Request Parameters:**
- `name` (required): One of: `server_stats`, `sessions`, `tokens`, `workers`, `consul_cluster_summary`

**Response Codes:**
- `200 OK` — Key found; response is JSON object representing the key value
- `400 Bad Request` — Invalid alias name
- `404 Not Found` — Key not yet stored in Redis
- `503 Service Unavailable` — Backend cannot connect to Redis

**Response Format:**
```json
{
  "field1": "value1",
  "field2": "value2",
  ...
}
```

**Error Response Format:**
```
Content-Type: text/plain
<error message>
```

**Frontend Handling:**
- `200` → Display JSON (pretty-printed in panel)
- `404` → Show "Empty" status with message "No key value in Redis yet."
- `503` → Show "Degraded" status with message "Backend cannot reach Redis."
- Other errors → Show "Error" status with returned message

---

## Frontend Integration Points

### Component: Dashboard

**Location:** `static/index.html`, `static/app.js`

**Functionality:**
- Renders 5 interactive panels, one per alias
- Each panel shows:
  - Status indicator (`OK`, `Empty`, `Error`, `Degraded`, `Loading`)
  - JSON content (when available)
  - Last refresh timestamp

**Polling Behavior:**
- Initial refresh: On page load
- Continuous refresh: Every 15 seconds (`refreshDashboard()`)
- Manual refresh: Click "Refresh" button (`refreshBtn` event listener)
- All 5 aliases fetched in parallel: `Promise.all()`

---

## Missing/Future Endpoints

**None identified.** The current frontend only requires `/dashboard/key` endpoint, which is already implemented.

---

## React Frontend Architecture

### Current Structure

The React frontend is built as a **Fuse React** template application with the following layout:

**Directories:**
- `src/app/` — Application routes and layout
  - `(control-panel)/example/` — Example control panel route
- `src/components/` — Reusable React components
- `src/hooks/` — Custom React hooks
- `src/utils/` — Utility functions
- `src/@mock-utils/` — MSW (Mock Service Worker) configuration for development

**Key Files:**
- `src/index.tsx` — React root entry point
- `src/app/App.tsx` — Main app component
- `public/mockServiceWorker.js` — Service worker for mocking API calls in development

### Current API Integration Approach

**Development:** Mock API with MSW (Mock Service Worker)  
**Production:** Direct API calls to backend at `GET /dashboard/key`

---

## Configuration

### Backend Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `UI_BACKEND_BIND` | `0.0.0.0` | Server bind address |
| `UI_BACKEND_PORT` | `8081` | Server port |
| `REDIS_URL_ENV` | `redis://kionas-redis:6379/2` | Redis connection URL |
| `REDIS_POOL_SIZE_ENV` | `8` | Redis connection pool size |

### Frontend Configuration

No environment variables required at runtime. All configuration is hardcoded in:
- `static/app.js` — Aliases array, poll interval (15s)
- `vite.config.mts` — Build configuration

---

## Build Specifications

### Frontend Build Output

- **Bundle Size:** ~5-6 MB (including fonts, icons, images)  
- **Main JS:** `index-*.js` (2.8 MB minified + gzipped)  
- **Main CSS:** `index-*.css` (41.5 KB minified)  
- **Entry Point:** `build/index.html`  
- **Assets:** Embedded in `build/assets/` directory

### Backend Build Output

- **Binary:** `ui_backend` (Axum server)  
- **Size:** Embedded static files only (~100 KB for HTML/CSS/JS)  
- **Runtime Dependencies:** Redis pool, Tokio async runtime

---

## Deployment Model

### Build-Time Integration (Option A)

All frontend assets embedded in backend binary:

1. **Build Phase:**
   - Frontend: `npm run build` → generates `build/` directory  
   - Backend: `cargo build` embeds files via `include_str!()` macro  

2. **Deployment:**
   - Single binary: `ui_backend`  
   - No external files needed  
   - Container includes all assets  

3. **Serving:**
   - Static files served from compiled-in `&'static str` constants  
   - Zero disk I/O for static assets  

---

## Testing Checklist

### Frontend Functionality

- [ ] Page loads at `http://localhost:8081`
- [ ] All 5 dashboard panels render
- [ ] Initial data populates within 2 seconds
- [ ] Status indicators change based on endpoint response (200, 404, 503)
- [ ] Manual "Refresh" button works
- [ ] Auto-refresh continues every 15 seconds
- [ ] JSON renders correctly with syntax highlighting

### Backend Functionality

- [ ] `GET /` returns HTML (200)
- [ ] `GET /styles.css` returns CSS (200)
- [ ] `GET /app.js` returns JS (200)
- [ ] `GET /health` returns "ok" (200)
- [ ] `GET /dashboard/key?name=server_stats` returns JSON (200 or 404)
- [ ] `GET /dashboard/key?name=invalid` returns error (400)
- [ ] Redis pool handles concurrent requests
- [ ] 503 errors handled gracefully when Redis unavailable

### Performance Baselines

- [ ] Page load time: < 2 seconds
- [ ] Dashboard refresh: < 500ms (all 5 calls parallel)
- [ ] Binary size: < 500 MB
- [ ] Startup time: < 5 seconds

---

## Future Enhancements (Out of Scope)

1. **Additional endpoints** as needed by new dashboard features
2. **Authentication** for dashboard access (future RBAC work)
3. **Real-time updates** via WebSocket instead of polling
4. **Custom dashboard layouts** for different user roles
5. **Data export** functionality (CSV, JSON download)

---

## Version History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-03-28 | AI Agent | Initial API mapping created during Phase 2 |

