use arrow::record_batch::RecordBatch;

/// What: Stage latency breakdown emitted by worker query execution.
///
/// Inputs:
/// - `stage_id`: Distributed stage identifier handled by this worker task.
/// - `queue_ms`: Milliseconds spent waiting between enqueue and worker execution start.
/// - `exec_ms`: Milliseconds spent in in-worker pipeline execution before downstream streaming.
/// - `network_ms`: Milliseconds spent streaming stage output to downstream destinations.
///
/// Output:
/// - Structured latency payload used by server/client observability reporting.
#[derive(Debug, Clone)]
pub struct StageLatencyMetrics {
    pub stage_id: u32,
    pub queue_ms: u128,
    pub exec_ms: u128,
    pub network_ms: u128,
}

/// What: Per-stage memory metrics sampled during worker pipeline execution.
///
/// Inputs:
/// - Tracks batch allocations and sampled row counts during one stage task.
///
/// Output:
/// - Memory telemetry payload with peak batch memory estimate and spill-threshold status.
#[derive(Debug, Clone)]
pub struct StageMemoryMetrics {
    pub stage_id: u32,
    pub enabled: bool,
    pub spill_threshold_bytes: usize,
    pub sampled_batches: usize,
    pub sampled_rows: usize,
    pub peak_batch_bytes: usize,
    pub spill_threshold_exceeded: bool,
}

/// What: Per-stage network transfer metrics for downstream Flight dispatch.
///
/// Inputs:
/// - `stage_id`: Stage identifier.
/// - `downstream_endpoint_count`: Number of downstream Flight endpoints targeted.
/// - `downstream_write_ms`: Total elapsed write time across downstream endpoint dispatches.
/// - `downstream_queued_frames`: Total Flight frames queued across destinations.
/// - `downstream_queued_bytes`: Total encoded bytes queued across destinations.
///
/// Output:
/// - Network telemetry payload consumable by server/client benchmark reporting.
#[derive(Debug, Clone)]
pub struct StageNetworkMetrics {
    pub downstream_endpoint_count: usize,
    pub downstream_write_ms: u128,
    pub downstream_queued_frames: usize,
    pub downstream_queued_bytes: usize,
}

impl StageMemoryMetrics {
    /// What: Build an empty memory metrics accumulator for one stage.
    ///
    /// Inputs:
    /// - `stage_id`: Stage identifier.
    /// - `enabled`: Whether memory tracking is active.
    /// - `spill_threshold_mb`: Spill threshold in MB used for telemetry checks.
    ///
    /// Output:
    /// - Zeroed memory metrics accumulator.
    pub fn new(stage_id: u32, enabled: bool, spill_threshold_mb: usize) -> Self {
        Self {
            stage_id,
            enabled,
            spill_threshold_bytes: spill_threshold_mb.saturating_mul(1024 * 1024),
            sampled_batches: 0,
            sampled_rows: 0,
            peak_batch_bytes: 0,
            spill_threshold_exceeded: false,
        }
    }

    /// What: Sample one batch allocation for memory telemetry.
    ///
    /// Inputs:
    /// - `batch`: Arrow record batch produced during stage execution.
    ///
    /// Output:
    /// - Updates in-place counters and peak memory estimate.
    pub fn track_batch_allocation(&mut self, batch: &RecordBatch) {
        if !self.enabled {
            return;
        }

        self.sampled_batches = self.sampled_batches.saturating_add(1);
        self.sampled_rows = self.sampled_rows.saturating_add(batch.num_rows());

        let batch_bytes = batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum::<usize>();

        if batch_bytes > self.peak_batch_bytes {
            self.peak_batch_bytes = batch_bytes;
        }

        if self.spill_threshold_bytes > 0 && batch_bytes >= self.spill_threshold_bytes {
            self.spill_threshold_exceeded = true;
        }
    }
}

/// What: Aggregated execution telemetry for one worker stage task.
///
/// Inputs:
/// - `latency`: Queue/exec/network latency metrics.
/// - `memory`: Optional memory metrics when tracking is enabled.
///
/// Output:
/// - Single telemetry payload encoded into result-location params and logs.
#[derive(Debug, Clone)]
pub struct StageExecutionTelemetry {
    pub latency: StageLatencyMetrics,
    pub memory: Option<StageMemoryMetrics>,
    pub network: Option<StageNetworkMetrics>,
}
