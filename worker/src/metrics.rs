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
use tokio::sync::Mutex;

const STAGE_EXEC_BUCKETS_MS: [f64; 9] =
    [5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0];

/// What: Label set for worker-scoped metrics.
///
/// Inputs:
/// - `worker_id`: Worker identifier owning the metric series.
///
/// Output:
/// - Encodable label set for Prometheus family metrics.
///
/// Details:
/// - Worker id is captured at startup and remains stable for process lifetime.
#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct WorkerLabel {
    worker_id: String,
}

/// What: Worker-scoped Prometheus metrics registry and handles.
///
/// Inputs:
/// - Constructed once at worker startup using stable worker id.
///
/// Output:
/// - Shared metric handles for service, execution, and monitoring call sites.
///
/// Details:
/// - Registry mutex is only required for /metrics encoding.
#[derive(Debug)]
pub struct WorkerPrometheusMetrics {
    pub registry: Arc<Mutex<Registry>>,
    worker_id: String,
    stages_total: Family<WorkerLabel, Counter>,
    partitions_total: Family<WorkerLabel, Counter>,
    stages_active: Family<WorkerLabel, Gauge>,
    partitions_active: Family<WorkerLabel, Gauge>,
    bytes_scanned_total: Family<WorkerLabel, Counter>,
    rows_produced_total: Family<WorkerLabel, Counter>,
    stage_exec_ms_total: Family<WorkerLabel, Counter>,
    stage_exec_ms_histogram: Family<WorkerLabel, Histogram>,
    cpu_percent: Family<WorkerLabel, Gauge>,
    memory_used_mb: Family<WorkerLabel, Gauge>,
    thread_count: Family<WorkerLabel, Gauge>,
}

/// What: RAII guard that tracks one active worker stage in Prometheus metrics.
///
/// Inputs:
/// - Acquired when task stage execution starts.
///
/// Output:
/// - On drop, decrements active stage gauge and increments stage total counter.
///
/// Details:
/// - Complements existing atomic `StageGuard` used by in-process counters.
pub struct PrometheusStageGuard(Arc<WorkerPrometheusMetrics>);

impl PrometheusStageGuard {
    /// What: Acquire stage guard and mark stage as active.
    ///
    /// Inputs:
    /// - `metrics`: Shared worker metrics.
    ///
    /// Output:
    /// - Guard value that finalizes stage counters on drop.
    pub fn acquire(metrics: Arc<WorkerPrometheusMetrics>) -> Self {
        metrics.stage_active_inc();
        Self(metrics)
    }
}

impl Drop for PrometheusStageGuard {
    fn drop(&mut self) {
        self.0.stage_active_dec();
        self.0.inc_stages_total();
    }
}

/// What: RAII guard that tracks one active worker partition in Prometheus metrics.
///
/// Inputs:
/// - Acquired when staged partition work starts.
///
/// Output:
/// - On drop, decrements active partition gauge and increments partition total counter.
///
/// Details:
/// - Complements existing atomic `PartitionGuard` used by in-process counters.
pub struct PrometheusPartitionGuard(Arc<WorkerPrometheusMetrics>);

impl PrometheusPartitionGuard {
    /// What: Acquire partition guard and mark partition as active.
    ///
    /// Inputs:
    /// - `metrics`: Shared worker metrics.
    ///
    /// Output:
    /// - Guard value that finalizes partition counters on drop.
    pub fn acquire(metrics: Arc<WorkerPrometheusMetrics>) -> Self {
        metrics.partition_active_inc();
        Self(metrics)
    }
}

impl Drop for PrometheusPartitionGuard {
    fn drop(&mut self) {
        self.0.partition_active_dec();
        self.0.inc_partitions_total();
    }
}

impl WorkerPrometheusMetrics {
    /// What: Create and register all worker Prometheus metrics.
    ///
    /// Inputs:
    /// - `worker_id`: Stable worker identifier for metric label dimensions.
    ///
    /// Output:
    /// - Shared worker metrics container ready for runtime updates.
    ///
    /// Details:
    /// - Metric names use `kionas_` prefix and snake_case naming.
    pub fn new(worker_id: &str) -> Arc<Self> {
        let mut registry = Registry::default();

        let stages_total = Family::<WorkerLabel, Counter>::default();
        registry.register(
            "kionas_worker_stages_total",
            "Total number of worker stages completed",
            stages_total.clone(),
        );

        let partitions_total = Family::<WorkerLabel, Counter>::default();
        registry.register(
            "kionas_worker_partitions_total",
            "Total number of worker partitions completed",
            partitions_total.clone(),
        );

        let stages_active = Family::<WorkerLabel, Gauge>::default();
        registry.register(
            "kionas_worker_active_stages",
            "Current number of active worker stages",
            stages_active.clone(),
        );

        let partitions_active = Family::<WorkerLabel, Gauge>::default();
        registry.register(
            "kionas_worker_active_partitions",
            "Current number of active worker partitions",
            partitions_active.clone(),
        );

        let bytes_scanned_total = Family::<WorkerLabel, Counter>::default();
        registry.register(
            "kionas_worker_bytes_scanned_total",
            "Total bytes scanned by worker stages",
            bytes_scanned_total.clone(),
        );

        let rows_produced_total = Family::<WorkerLabel, Counter>::default();
        registry.register(
            "kionas_worker_rows_produced_total",
            "Total rows produced by worker stages",
            rows_produced_total.clone(),
        );

        let stage_exec_ms_total = Family::<WorkerLabel, Counter>::default();
        registry.register(
            "kionas_worker_stage_exec_ms_total",
            "Total stage execution time in milliseconds",
            stage_exec_ms_total.clone(),
        );

        let stage_exec_ms_histogram =
            Family::<WorkerLabel, Histogram>::new_with_constructor(|| {
                Histogram::new(STAGE_EXEC_BUCKETS_MS.into_iter())
            });
        registry.register(
            "kionas_stage_exec_ms",
            "Worker stage execution duration histogram in milliseconds",
            stage_exec_ms_histogram.clone(),
        );

        let cpu_percent = Family::<WorkerLabel, Gauge>::default();
        registry.register(
            "kionas_worker_cpu_percent",
            "Current worker process CPU utilization percent",
            cpu_percent.clone(),
        );

        let memory_used_mb = Family::<WorkerLabel, Gauge>::default();
        registry.register(
            "kionas_worker_memory_used_mb",
            "Current worker process memory usage in MB",
            memory_used_mb.clone(),
        );

        let thread_count = Family::<WorkerLabel, Gauge>::default();
        registry.register(
            "kionas_worker_thread_count",
            "Current worker thread count",
            thread_count.clone(),
        );

        Arc::new(Self {
            registry: Arc::new(Mutex::new(registry)),
            worker_id: worker_id.to_string(),
            stages_total,
            partitions_total,
            stages_active,
            partitions_active,
            bytes_scanned_total,
            rows_produced_total,
            stage_exec_ms_total,
            stage_exec_ms_histogram,
            cpu_percent,
            memory_used_mb,
            thread_count,
        })
    }

    /// What: Increment total bytes scanned metric.
    ///
    /// Inputs:
    /// - `bytes`: Number of bytes to add.
    ///
    /// Output:
    /// - Updates monotonic bytes scanned counter.
    pub fn inc_bytes_scanned_total(&self, bytes: u64) {
        self.bytes_scanned_total
            .get_or_create(&self.worker_label())
            .inc_by(bytes);
    }

    /// What: Increment total rows produced metric.
    ///
    /// Inputs:
    /// - `rows`: Number of rows to add.
    ///
    /// Output:
    /// - Updates monotonic rows produced counter.
    pub fn inc_rows_produced_total(&self, rows: u64) {
        self.rows_produced_total
            .get_or_create(&self.worker_label())
            .inc_by(rows);
    }

    /// What: Increment cumulative stage execution milliseconds metric.
    ///
    /// Inputs:
    /// - `duration_ms`: Stage duration to add in milliseconds.
    ///
    /// Output:
    /// - Updates monotonic stage execution time counter.
    pub fn inc_stage_exec_ms_total(&self, duration_ms: u64) {
        self.stage_exec_ms_total
            .get_or_create(&self.worker_label())
            .inc_by(duration_ms);
    }

    /// What: Observe one stage execution duration sample.
    ///
    /// Inputs:
    /// - `duration_ms`: Stage execution duration in milliseconds.
    ///
    /// Output:
    /// - Records one value into stage execution histogram.
    pub fn observe_stage_exec_ms(&self, duration_ms: u64) {
        self.stage_exec_ms_histogram
            .get_or_create(&self.worker_label())
            .observe(duration_ms as f64);
    }

    /// What: Set active stage gauge.
    ///
    /// Inputs:
    /// - `active`: Current active stage count.
    ///
    /// Output:
    /// - Updates active stages gauge.
    pub fn set_active_stages(&self, active: u32) {
        self.stages_active
            .get_or_create(&self.worker_label())
            .set(i64::from(active));
    }

    /// What: Set active partition gauge.
    ///
    /// Inputs:
    /// - `active`: Current active partition count.
    ///
    /// Output:
    /// - Updates active partitions gauge.
    pub fn set_active_partitions(&self, active: u32) {
        self.partitions_active
            .get_or_create(&self.worker_label())
            .set(i64::from(active));
    }

    /// What: Set worker process CPU percent gauge.
    ///
    /// Inputs:
    /// - `cpu_percent`: CPU usage percentage.
    ///
    /// Output:
    /// - Updates worker CPU gauge.
    pub fn set_cpu_percent(&self, cpu_percent: f32) {
        self.cpu_percent
            .get_or_create(&self.worker_label())
            .set(cpu_percent.round() as i64);
    }

    /// What: Set worker process memory usage gauge in MB.
    ///
    /// Inputs:
    /// - `memory_mb`: Memory usage in MB.
    ///
    /// Output:
    /// - Updates worker memory gauge.
    pub fn set_memory_used_mb(&self, memory_mb: u64) {
        self.memory_used_mb
            .get_or_create(&self.worker_label())
            .set(i64::try_from(memory_mb).unwrap_or(i64::MAX));
    }

    /// What: Set worker thread count gauge.
    ///
    /// Inputs:
    /// - `thread_count`: Number of process threads.
    ///
    /// Output:
    /// - Updates worker thread gauge.
    pub fn set_thread_count(&self, thread_count: u32) {
        self.thread_count
            .get_or_create(&self.worker_label())
            .set(i64::from(thread_count));
    }

    fn stage_active_inc(&self) {
        self.stages_active.get_or_create(&self.worker_label()).inc();
    }

    fn stage_active_dec(&self) {
        self.stages_active.get_or_create(&self.worker_label()).dec();
    }

    fn partition_active_inc(&self) {
        self.partitions_active
            .get_or_create(&self.worker_label())
            .inc();
    }

    fn partition_active_dec(&self) {
        self.partitions_active
            .get_or_create(&self.worker_label())
            .dec();
    }

    fn inc_stages_total(&self) {
        self.stages_total.get_or_create(&self.worker_label()).inc();
    }

    fn inc_partitions_total(&self) {
        self.partitions_total
            .get_or_create(&self.worker_label())
            .inc();
    }

    fn worker_label(&self) -> WorkerLabel {
        WorkerLabel {
            worker_id: self.worker_id.clone(),
        }
    }
}

/// What: Start HTTP endpoint exposing Prometheus metrics for worker process.
///
/// Inputs:
/// - `registry`: Shared metrics registry to encode.
/// - `port`: TCP port to bind for GET /metrics.
///
/// Output:
/// - Starts serving until process shutdown or binding/serve error.
///
/// Details:
/// - Returns an error string so worker startup can log and continue.
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
            format!("failed to bind worker metrics endpoint on port {port}: {error}")
        })?;

    axum::serve(listener, app)
        .await
        .map_err(|error| format!("worker metrics endpoint exited with error: {error}"))
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
