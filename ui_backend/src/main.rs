use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use deadpool::managed::Pool;
use kionas::constants::{
    REDIS_POOL_SIZE_ENV, REDIS_UI_DASHBOARD_CLUSTER_INFO_KEY,
    REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY, REDIS_UI_DASHBOARD_SERVER_STATS_KEY,
    REDIS_UI_DASHBOARD_SESSIONS_KEY, REDIS_UI_DASHBOARD_TOKENS_KEY, REDIS_UI_DASHBOARD_WORKERS_KEY,
    REDIS_URL_ENV,
};
use redis::AsyncCommands;
use serde::Deserialize;
use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use tokio::fs;

const DEFAULT_REDIS_URL: &str = "redis://redis:6379/2";
const DEFAULT_REDIS_POOL_SIZE: usize = 8;
const DEFAULT_UI_BACKEND_BIND: &str = "0.0.0.0";
const DEFAULT_UI_BACKEND_PORT: u16 = 8081;
const INDEX_HTML: &str = include_str!("../static/index.html");
const STYLES_CSS: &str = include_str!("../static/styles.css");
const APP_JS: &str = include_str!("../static/app.js");

type RedisPool = Pool<RedisConnectionManager>;

struct RedisConnectionManager {
    redis_url: String,
}

impl deadpool::managed::Manager for RedisConnectionManager {
    type Type = redis::aio::MultiplexedConnection;
    type Error = Box<dyn std::error::Error + Send + Sync>;

    fn create(&self) -> impl std::future::Future<Output = Result<Self::Type, Self::Error>> + Send {
        let redis_url = self.redis_url.clone();
        async move {
            let client = redis::Client::open(redis_url.as_str())?;
            let conn = client.get_multiplexed_async_connection().await?;
            Ok(conn)
        }
    }

    async fn recycle(
        &self,
        _obj: &mut Self::Type,
        _metrics: &deadpool::managed::Metrics,
    ) -> Result<(), deadpool::managed::RecycleError<Self::Error>> {
        Ok(())
    }
}

#[derive(Clone)]
struct AppState {
    redis_pool: RedisPool,
}

#[derive(Deserialize)]
struct KeyQuery {
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    env_logger::init();

    let state = AppState {
        redis_pool: build_redis_pool()?,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/dashboard/key", get(get_dashboard_key))
        // Default routes for legacy embedded assets
        .route("/", get(index_handler))
        .route("/styles.css", get(styles_handler))
        .route("/app.js", get(app_js_handler))
        // Catch-all for SPA fallback routing (Axum 0.8+ syntax: /{*path})
        .route("/{*path}", get(spa_static_handler))
        .with_state(state);

    let bind_host =
        env::var("UI_BACKEND_BIND").unwrap_or_else(|_| DEFAULT_UI_BACKEND_BIND.to_string());
    let bind_port = env::var("UI_BACKEND_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_UI_BACKEND_PORT);

    let addr: SocketAddr = format!("{}:{}", bind_host, bind_port).parse()?;
    log::info!("starting ui_backend on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

/// What: Get static directory path for serving Vite build artifacts.
///
/// Inputs:
/// - None.
///
/// Output:
/// - PathBuf to the static assets directory.
/// - Defaults to `./dist` or `../frontend/build` if available.
///
/// Details:
/// - Checks multiple possible paths to support both dev and prod deployments.
/// - Falls back to current directory if configured directory doesn't exist.
fn get_static_dir() -> PathBuf {
    // Check for dist directory (production build output)
    if PathBuf::from("./dist").exists() {
        return PathBuf::from("./dist");
    }
    // Check for frontend/build (development)
    if PathBuf::from("../frontend/build").exists() {
        return PathBuf::from("../frontend/build");
    }
    // Check for ui_backend/frontend/build (from workspace root)
    if PathBuf::from("ui_backend/frontend/build").exists() {
        return PathBuf::from("ui_backend/frontend/build");
    }
    // Default fallback
    PathBuf::from(".")
}

/// What: Get MIME type for a file based on extension.
///
/// Inputs:
/// - path: File path or extension string.
///
/// Output:
/// - MIME type string appropriate for HTTP Content-Type header.
///
/// Details:
/// - Supports common web assets: JS, CSS, JSON, images, fonts, SVG.
/// - Defaults to application/octet-stream for unknown types.
fn mime_type_for_path(path: &str) -> &'static str {
    match path {
        p if p.ends_with(".html") => "text/html; charset=utf-8",
        p if p.ends_with(".css") => "text/css; charset=utf-8",
        p if p.ends_with(".js") => "application/javascript; charset=utf-8",
        p if p.ends_with(".json") => "application/json",
        p if p.ends_with(".svg") => "image/svg+xml",
        p if p.ends_with(".png") => "image/png",
        p if p.ends_with(".jpg") || p.ends_with(".jpeg") => "image/jpeg",
        p if p.ends_with(".gif") => "image/gif",
        p if p.ends_with(".webp") => "image/webp",
        p if p.ends_with(".ico") => "image/x-icon",
        p if p.ends_with(".woff") => "font/woff",
        p if p.ends_with(".woff2") => "font/woff2",
        p if p.ends_with(".ttf") => "font/ttf",
        p if p.ends_with(".eot") => "application/vnd.ms-fontobject",
        p if p.ends_with(".otf") => "font/otf",
        p if p.ends_with(".txt") => "text/plain; charset=utf-8",
        p if p.ends_with(".map") => "application/json",
        _ => "application/octet-stream",
    }
}

/// What: Serve static files with SPA fallback routing.
///
/// Inputs:
/// - Path: Requested file path (e.g., "assets/index-abc123.js").
///
/// Output:
/// - File content with correct Content-Type header if found.
/// - index.html if file not found (SPA fallback for React Router).
/// - 404 if file is a directory or truly inaccessible.
///
/// Details:
/// - Supports serving Vite build output files with asset hashing.
/// - For SPA apps, all non-existent routes return index.html.
/// - Prevents directory traversal with basic path sanitization.
async fn spa_static_handler(Path(path): Path<String>) -> Response {
    let static_dir = get_static_dir();

    // Security: Prevent directory traversal
    if path.contains("../") || path.contains("..\\") {
        return (StatusCode::BAD_REQUEST, "invalid path").into_response();
    }

    let file_path = static_dir.join(&path);

    // Try to serve the requested file
    match fs::read(&file_path).await {
        Ok(content) => {
            let mime = mime_type_for_path(&path);
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                mime.parse()
                    .unwrap_or_else(|_| "application/octet-stream".parse().unwrap()),
            );
            (StatusCode::OK, headers, content).into_response()
        }
        Err(_) => {
            // If file not found, check if request might be for a React route
            // Serve index.html for SPA routing (only for requests without extensions)
            if !path.contains('.') || path.ends_with(".map") {
                match fs::read(static_dir.join("index.html")).await {
                    Ok(content) => {
                        let mut headers = HeaderMap::new();
                        headers.insert(
                            header::CONTENT_TYPE,
                            "text/html; charset=utf-8".parse().unwrap(),
                        );
                        return (StatusCode::OK, headers, content).into_response();
                    }
                    Err(e) => {
                        log::warn!("failed to fallback to index.html: {}", e);
                        return (StatusCode::NOT_FOUND, "not found").into_response();
                    }
                }
            }
            (StatusCode::NOT_FOUND, "not found").into_response()
        }
    }
}

fn build_redis_pool() -> Result<RedisPool, String> {
    let redis_url = env::var(REDIS_URL_ENV)
        .ok()
        .and_then(|url| to_status_db_url(&url))
        .unwrap_or_else(|| DEFAULT_REDIS_URL.to_string());

    let pool_size = env::var(REDIS_POOL_SIZE_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_REDIS_POOL_SIZE);

    let manager = RedisConnectionManager { redis_url };
    deadpool::managed::Pool::builder(manager)
        .max_size(pool_size)
        .build()
        .map_err(|error| format!("redis pool build error: {error}"))
}

/// What: Serve main index.html entry point.
///
/// Inputs:
/// - None.
///
/// Output:
/// - HTML response with embedded or served index.html.
///
/// Details:
/// - Uses embedded INDEX_HTML constant for reliable fallback.
/// - Proper Content-Type header set for HTML.
async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// What: Serve CSS stylesheet.
///
/// Inputs:
/// - None.
///
/// Output:
/// - CSS response with proper Content-Type header.
///
/// Details:
/// - Uses embedded STYLES_CSS constant.
/// - Sets charset UTF-8 for proper text encoding.
async fn styles_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLES_CSS,
    )
}

/// What: Serve JavaScript bundle.
///
/// Inputs:
/// - None.
///
/// Output:
/// - JavaScript response with proper Content-Type header.
///
/// Details:
/// - Uses embedded APP_JS constant.
/// - Sets charset UTF-8 for proper text encoding.
async fn app_js_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        APP_JS,
    )
}

/// What: Health check endpoint for deployment orchestration.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Plain text "ok" response with 200 status.
///
/// Details:
/// - Used by Kubernetes, Docker, and orchestration systems.
/// - Indicates the backend service is running and responsive.
async fn health_handler() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}

fn to_status_db_url(url: &str) -> Option<String> {
    let parsed = url::Url::parse(url).ok()?;
    let mut rebuilt = format!("{}://", parsed.scheme());
    if !parsed.username().is_empty() {
        rebuilt.push_str(parsed.username());
        if let Some(password) = parsed.password() {
            rebuilt.push(':');
            rebuilt.push_str(password);
        }
        rebuilt.push('@');
    }

    let host = parsed.host_str()?;
    rebuilt.push_str(host);
    if let Some(port) = parsed.port() {
        rebuilt.push(':');
        rebuilt.push_str(port.to_string().as_str());
    }

    rebuilt.push_str("/2");
    Some(rebuilt)
}

/// What: Handle dashboard key retrieval from Redis.
///
/// Inputs:
/// - State: Application state containing Redis connection pool.
/// - Query: KeyQuery with `name` parameter (alias for Redis key).
///
/// Output:
/// - 200 OK with JSON payload if key exists.
/// - 400 Bad Request if alias is invalid.
/// - 404 Not Found if key doesn't exist in Redis.
/// - 503 Service Unavailable if Redis connection fails.
///
/// Details:
/// - Translates friendly aliases (server_stats, sessions, etc.) to Redis keys.
/// - Enforces connection pooling for efficient Redis access.
/// - Proper error handling with clear messages for debugging.
async fn get_dashboard_key(
    State(state): State<AppState>,
    Query(query): Query<KeyQuery>,
) -> Response {
    let is_workers_query = query.name == "workers";
    let redis_key = match key_alias_to_redis_key(query.name.as_str()) {
        Some(key) => key,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "unsupported key alias; expected one of: server_stats,sessions,tokens,workers,cluster_info,consul_cluster_summary",
            )
                .into_response();
        }
    };

    let mut conn = match state.redis_pool.get().await {
        Ok(conn) => conn,
        Err(error) => {
            log::warn!("failed to acquire redis pooled connection: {}", error);
            return (StatusCode::SERVICE_UNAVAILABLE, "redis unavailable").into_response();
        }
    };

    if is_workers_query {
        match read_workers_from_scan(&mut conn).await {
            Ok(payload) => {
                return (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json")],
                    payload,
                )
                    .into_response();
            }
            Err(error) => {
                log::warn!(
                    "workers scan retrieval failed; falling back to cached workers key: {}",
                    error
                );
            }
        }
    }

    let value: redis::RedisResult<Option<String>> = conn.get(redis_key).await;
    match value {
        Ok(Some(payload)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            payload,
        )
            .into_response(),
        Ok(None) => {
            if is_workers_query {
                (
                    StatusCode::OK,
                    [(header::CONTENT_TYPE, "application/json")],
                    "[]",
                )
                    .into_response()
            } else {
                (StatusCode::NOT_FOUND, "key not found").into_response()
            }
        }
        Err(error) => {
            log::warn!("redis read failed for key {}: {}", redis_key, error);
            (StatusCode::SERVICE_UNAVAILABLE, "redis read failed").into_response()
        }
    }
}

/// What: Read worker system info snapshots directly using Redis SCAN pattern lookup.
///
/// Inputs:
/// - `connection`: Active Redis connection.
///
/// Output:
/// - JSON array string containing worker sys-info payloads.
///
/// Details:
/// - Scans keys matching kionas:worker:*:sys_info.
/// - Returns an empty JSON array when no keys are present.
/// - Skips malformed worker payloads while preserving healthy entries.
async fn read_workers_from_scan(
    connection: &mut redis::aio::MultiplexedConnection,
) -> Result<String, String> {
    let mut cursor: u64 = 0;
    let mut keys: Vec<String> = Vec::new();

    loop {
        let (next_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("kionas:worker:*:sys_info")
            .arg("COUNT")
            .arg(100_u32)
            .query_async(connection)
            .await
            .map_err(|error| format!("redis scan failed: {error}"))?;

        keys.extend(batch);
        if next_cursor == 0 {
            break;
        }
        cursor = next_cursor;
    }

    let mut workers: Vec<serde_json::Value> = Vec::with_capacity(keys.len());
    for key in keys {
        let raw: Option<String> = connection
            .get(key.as_str())
            .await
            .map_err(|error| format!("redis get failed for key {key}: {error}"))?;

        if let Some(body) = raw {
            match serde_json::from_str::<serde_json::Value>(body.as_str()) {
                Ok(value) => workers.push(value),
                Err(error) => {
                    log::warn!(
                        "skipping malformed worker sys_info payload for key {}: {}",
                        key,
                        error
                    );
                }
            }
        }
    }

    serde_json::to_string(&workers)
        .map_err(|error| format!("serialize workers array failed: {error}"))
}

/// What: Translate user-friendly alias to Redis hash key constant.
///
/// Inputs:
/// - name: Dashboard key alias (server_stats, sessions, tokens, workers, cluster_info, consul_cluster_summary).
///
/// Output:
/// - Some(&'static str) with Redis key constant if alias is valid.
/// - None if alias is not recognized.
///
/// Details:
/// - Centralizes alias validation for dashboard endpoints.
/// - References to kionas crate constants ensure consistency across codebase.
fn key_alias_to_redis_key(name: &str) -> Option<&'static str> {
    match name {
        "server_stats" => Some(REDIS_UI_DASHBOARD_SERVER_STATS_KEY),
        "sessions" => Some(REDIS_UI_DASHBOARD_SESSIONS_KEY),
        "tokens" => Some(REDIS_UI_DASHBOARD_TOKENS_KEY),
        "workers" => Some(REDIS_UI_DASHBOARD_WORKERS_KEY),
        "cluster_info" => Some(REDIS_UI_DASHBOARD_CLUSTER_INFO_KEY),
        "consul_cluster_summary" => Some(REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY),
        _ => None,
    }
}
