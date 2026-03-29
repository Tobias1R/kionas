use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Mutex;

const QUERY_DURATION_BUCKETS_MS: [f64; 11] = [
    10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 30000.0, 60000.0,
];

/// What: Label set used for warehouse-scoped metrics.
///
/// Inputs:
/// - `warehouse`: Warehouse name associated with the metric sample.
///
/// Output:
/// - Encodable label set for Prometheus family metrics.
///
/// Details:
/// - Warehouse value is normalized before metric updates to avoid empty labels.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct WarehouseLabel {
    warehouse: String,
}

/// What: Label set used for query status and warehouse metrics.
///
/// Inputs:
/// - `warehouse`: Warehouse name associated with the query.
/// - `status`: Query outcome status (`success` or `error`).
///
/// Output:
/// - Encodable label set for Prometheus family metrics.
///
/// Details:
/// - Keeps query counters and query duration histogram aligned on dimensions.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct QueryStatusLabel {
    warehouse: String,
    status: String,
}

/// What: Server-scoped Prometheus metrics registry and metric handles.
///
/// Inputs:
/// - Constructed once during server startup and shared via Arc.
///
/// Output:
/// - Metric handles used by statement handlers and janitor updates.
///
/// Details:
/// - Registry is wrapped with async mutex only for text encoding in /metrics endpoint.
/// - Metric handles are independently thread-safe and can be updated without registry lock.
#[derive(Debug)]
pub struct ServerPrometheusMetrics {
    pub registry: Arc<Mutex<Registry>>,
    queries_total: Family<QueryStatusLabel, Counter>,
    queries_active: Family<WarehouseLabel, Gauge>,
    query_duration_ms: Family<QueryStatusLabel, Histogram>,
    sessions_active: Gauge,
    process_cpu_percent: Gauge,
    process_memory_mb: Gauge,
}

/// What: Query status used for Prometheus labeling.
///
/// Inputs:
/// - Set by statement handler as query succeeds or fails.
///
/// Output:
/// - Determines `status` label value on counter and histogram updates.
///
/// Details:
/// - Defaults to `Error` when used by guard acquisition for safety.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrometheusQueryStatus {
    Success,
    Error,
}

impl PrometheusQueryStatus {
    fn as_label_value(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Error => "error",
        }
    }
}

/// What: RAII guard for one in-flight query in Prometheus metrics.
///
/// Inputs:
/// - Acquired when query dispatch starts.
///
/// Output:
/// - On drop, decrements active gauge, increments query counter, and records duration histogram.
///
/// Details:
/// - Defaults outcome to `Error` unless caller marks `Success`.
pub struct PrometheusQueryGuard {
    metrics: Arc<ServerPrometheusMetrics>,
    warehouse: String,
    started_at: Instant,
    pub status: PrometheusQueryStatus,
}

impl PrometheusQueryGuard {
    /// What: Acquire query guard and increment active query gauge.
    ///
    /// Inputs:
    /// - `metrics`: Shared server Prometheus metrics.
    /// - `warehouse`: Warehouse name associated with this query.
    ///
    /// Output:
    /// - Guard whose drop finalizes query metrics.
    ///
    /// Details:
    /// - Uses normalized warehouse label to avoid empty dimensions.
    pub fn acquire(metrics: Arc<ServerPrometheusMetrics>, warehouse: &str) -> Self {
        let normalized = normalize_warehouse(warehouse);
        metrics
            .queries_active
            .get_or_create(&WarehouseLabel {
                warehouse: normalized.clone(),
            })
            .inc();
        Self {
            metrics,
            warehouse: normalized,
            started_at: Instant::now(),
            status: PrometheusQueryStatus::Error,
        }
    }
}

impl Drop for PrometheusQueryGuard {
    fn drop(&mut self) {
        let status_value = self.status.as_label_value().to_string();
        let labels = QueryStatusLabel {
            warehouse: self.warehouse.clone(),
            status: status_value,
        };

        self.metrics
            .queries_active
            .get_or_create(&WarehouseLabel {
                warehouse: self.warehouse.clone(),
            })
            .dec();
        self.metrics.queries_total.get_or_create(&labels).inc();
        self.metrics
            .query_duration_ms
            .get_or_create(&labels)
            .observe(self.started_at.elapsed().as_secs_f64() * 1000.0);
    }
}

impl ServerPrometheusMetrics {
    /// What: Create and register all server Prometheus metrics.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Shared metrics container ready for query and janitor updates.
    ///
    /// Details:
    /// - Metric names use `kionas_` prefix and snake_case naming.
    pub fn new() -> Arc<Self> {
        let mut registry = Registry::default();

        let queries_total = Family::<QueryStatusLabel, Counter>::default();
        registry.register(
            "kionas_queries_total",
            "Total number of server queries by status and warehouse",
            queries_total.clone(),
        );

        let queries_active = Family::<WarehouseLabel, Gauge>::default();
        registry.register(
            "kionas_queries_active",
            "Current number of active server queries by warehouse",
            queries_active.clone(),
        );

        let query_duration_ms = Family::<QueryStatusLabel, Histogram>::new_with_constructor(|| {
            Histogram::new(QUERY_DURATION_BUCKETS_MS.into_iter())
        });
        registry.register(
            "kionas_query_duration_ms",
            "Server query duration in milliseconds",
            query_duration_ms.clone(),
        );

        let sessions_active = Gauge::default();
        registry.register(
            "kionas_sessions_active",
            "Current number of active sessions",
            sessions_active.clone(),
        );

        let process_cpu_percent = Gauge::default();
        registry.register(
            "kionas_process_cpu_percent",
            "Current server process CPU utilization percent",
            process_cpu_percent.clone(),
        );

        let process_memory_mb = Gauge::default();
        registry.register(
            "kionas_process_memory_mb",
            "Current server process memory usage in MB",
            process_memory_mb.clone(),
        );

        Arc::new(Self {
            registry: Arc::new(Mutex::new(registry)),
            queries_total,
            queries_active,
            query_duration_ms,
            sessions_active,
            process_cpu_percent,
            process_memory_mb,
        })
    }

    /// What: Set current active session count.
    ///
    /// Inputs:
    /// - `value`: Session count snapshot.
    ///
    /// Output:
    /// - Updates `kionas_sessions_active` gauge.
    pub fn set_sessions_active(&self, value: usize) {
        self.sessions_active
            .set(i64::try_from(value).unwrap_or(i64::MAX));
    }

    /// What: Set current process CPU usage percent.
    ///
    /// Inputs:
    /// - `cpu_percent`: Process CPU usage percent snapshot.
    ///
    /// Output:
    /// - Updates `kionas_process_cpu_percent` gauge.
    pub fn set_process_cpu_percent(&self, cpu_percent: f32) {
        self.process_cpu_percent.set(cpu_percent.round() as i64);
    }

    /// What: Set current process memory usage in MB.
    ///
    /// Inputs:
    /// - `memory_mb`: Process memory in MB.
    ///
    /// Output:
    /// - Updates `kionas_process_memory_mb` gauge.
    pub fn set_process_memory_mb(&self, memory_mb: u64) {
        self.process_memory_mb
            .set(i64::try_from(memory_mb).unwrap_or(i64::MAX));
    }
}

/// What: Start HTTP endpoint exposing Prometheus metrics for server process.
///
/// Inputs:
/// - `registry`: Shared metrics registry to encode.
/// - `port`: TCP port to bind for GET /metrics.
///
/// Output:
/// - Starts serving until process shutdown or binding/serve error.
///
/// Details:
/// - Returns an error string for startup/serve failures so caller can log and continue.
pub async fn serve_metrics_endpoint(
    registry: Arc<Mutex<Registry>>,
    port: u16,
) -> Result<(), String> {
    let app = Router::new()
        .route("/metrics", get(metrics_handler))
        .with_state(registry);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port))
        .await
        .map_err(|error| {
            format!("failed to bind server metrics endpoint on port {port}: {error}")
        })?;

    axum::serve(listener, app)
        .await
        .map_err(|error| format!("server metrics endpoint exited with error: {error}"))
}

/// What: Encode and return Prometheus metrics exposition payload.
///
/// Inputs:
/// - `registry`: Shared metrics registry state.
///
/// Output:
/// - HTTP response with Prometheus text format body.
///
/// Details:
/// - Returns 500 payload if encoding fails.
async fn metrics_handler(State(registry): State<Arc<Mutex<Registry>>>) -> impl IntoResponse {
    let mut body = String::new();
    let registry_guard = registry.lock().await;

    if let Err(error) = encode(&mut body, &registry_guard) {
        let message = format!("prometheus encoding failed: {error}");
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/plain; charset=utf-8")],
            message,
        );
    }

    (
        axum::http::StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
}

fn normalize_warehouse(warehouse: &str) -> String {
    let trimmed = warehouse.trim();
    if trimmed.is_empty() {
        "unknown".to_string()
    } else {
        trimmed.to_string()
    }
}
