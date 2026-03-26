use axum::Router;
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::routing::get;
use deadpool::managed::Pool;
use kionas::constants::{
    REDIS_POOL_SIZE_ENV, REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY,
    REDIS_UI_DASHBOARD_SERVER_STATS_KEY, REDIS_UI_DASHBOARD_SESSIONS_KEY,
    REDIS_UI_DASHBOARD_TOKENS_KEY, REDIS_UI_DASHBOARD_WORKERS_KEY, REDIS_URL_ENV,
};
use redis::AsyncCommands;
use serde::Deserialize;
use std::env;
use std::net::SocketAddr;

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
        .route("/", get(index_handler))
        .route("/styles.css", get(styles_handler))
        .route("/app.js", get(app_js_handler))
        .route("/health", get(health_handler))
        .route("/dashboard/key", get(get_dashboard_key))
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

async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn styles_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, "text/css; charset=utf-8")],
        STYLES_CSS,
    )
}

async fn app_js_handler() -> impl IntoResponse {
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        APP_JS,
    )
}

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

async fn get_dashboard_key(
    State(state): State<AppState>,
    Query(query): Query<KeyQuery>,
) -> Response {
    let redis_key = match key_alias_to_redis_key(query.name.as_str()) {
        Some(key) => key,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                "unsupported key alias; expected one of: server_stats,sessions,tokens,workers,consul_cluster_summary",
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

    let value: redis::RedisResult<Option<String>> = conn.get(redis_key).await;
    match value {
        Ok(Some(payload)) => (
            StatusCode::OK,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            payload,
        )
            .into_response(),
        Ok(None) => (StatusCode::NOT_FOUND, "key not found").into_response(),
        Err(error) => {
            log::warn!("redis read failed for key {}: {}", redis_key, error);
            (StatusCode::SERVICE_UNAVAILABLE, "redis read failed").into_response()
        }
    }
}

fn key_alias_to_redis_key(name: &str) -> Option<&'static str> {
    match name {
        "server_stats" => Some(REDIS_UI_DASHBOARD_SERVER_STATS_KEY),
        "sessions" => Some(REDIS_UI_DASHBOARD_SESSIONS_KEY),
        "tokens" => Some(REDIS_UI_DASHBOARD_TOKENS_KEY),
        "workers" => Some(REDIS_UI_DASHBOARD_WORKERS_KEY),
        "consul_cluster_summary" => Some(REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY),
        _ => None,
    }
}
