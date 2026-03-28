![Kionas Logo](logo.png)

# Kionas

**TL;DR**
- Kionas is a Rust-native, modular data platform prototype (server, worker, metastore, client) focused on pluggable storage, gRPC interop, and coordinated transactions. It is early-stage and welcomes contributions.

**What Do We Have So Far**
- **Crates**: : workspace crates including [server](server), [worker](worker), [metastore](metastore), [client](client), and the shared `kionas` crate.
- **Staging**: : storage staging helpers and a staging manifest workflow.
- **Minio provider**: : S3/MinIO-compatible storage provider implementation.
- **Transactions**: : `transactions::maestro` for coordination of prepare/commit/abort flows.
- **Interops pool**: : pooled inter-service clients via `deadpool`.
- **gRPC / tonic**: : service definitions and servers generated/implemented with `tonic`.
- **TLS**: : `rustls`-based TLS integration and cert helper scripts.
- **Tests**: : unit and integration tests for core components (see `worker/tests`).

**Quickstart**
- **Prereqs**: Rust toolchain (rustup), Cargo, Docker (recommended for MinIO), Node.js >=22.12.0 (for UI), optional: OpenSSL for cert helpers.
- Build the workspace: `cargo build --workspace`
- Run a service locally (examples):
  - `cargo run -p metastore`
  - `cargo run -p server`
  - `cargo run -p worker -- <worker_id>`
  - `cargo run -p ui_backend` (starts dashboard on http://localhost:8081)
- Start compose stack: `docker compose -f docker/kionas.docker-compose.yaml up --build`

**UI Dashboard**

The `ui_backend` provides a React-based admin dashboard for monitoring Kionas services.

**Quick Start (Development with Hot Reload)**
```bash
# Terminal 1: Build and run backend
cargo build --bin ui_backend
cargo run --bin ui_backend

# Terminal 2: Start frontend dev server with HMR
cd ui_backend/frontend
npm install
npm run dev
```
Access dashboard at `http://localhost:5173` (edit `.tsx` files → auto-refresh in browser)

**Features:**
  - Real-time dashboard widgets for server stats, sessions, tokens, workers, Consul cluster status
  - Auto-refreshing data from Redis (every 15 seconds)
  - React Router SPA with responsive Material Design UI
  - RESTful API: `GET /dashboard/key?name=<alias>`

**Production Build:**
```bash
./scripts/build_ui.bat    # Windows
./scripts/build_ui.sh     # Linux/Mac
cargo run --bin ui_backend  # Access http://localhost:8081
```

For advanced setup, Docker dev environment, and troubleshooting, see [docs/UI_DEVELOPMENT_HOT_RELOAD.md](docs/UI_DEVELOPMENT_HOT_RELOAD.md) and [docs/UI_INTEGRATION_DEVELOPMENT.md](docs/UI_INTEGRATION_DEVELOPMENT.md).

**Development**
- **Format**: : `cargo fmt --all`
- **Lint**: : `cargo clippy --all-targets --all-features -- -D warnings`
- **Build check**: : `cargo check`
- **Tests**: : `cargo test --workspace -- --test-threads=1`

**Configuration**
- Examples and runtime configs live in the `configs/` folder. Check each crate's `main.rs` for env vars and overrides.

**Contributing**
- See [CONTRIBUTING.md](CONTRIBUTING.md) for PR guidelines, code style, and testing expectations. We welcome issues, small PRs, and design discussions.

**Contact**
- Open an issue or PR on this repository for bugs, questions, or to propose changes.

---

Thanks for checking out Kionas — contributions and feedback are warmly welcome.
