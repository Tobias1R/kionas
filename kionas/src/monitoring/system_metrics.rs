use sysinfo::{DiskExt, ProcessExt, System, SystemExt};

/// What: Current point-in-time worker system metrics.
///
/// Inputs:
/// - Constructed by collect_system_metrics.
///
/// Output:
/// - Memory, CPU, and disk values normalized for dashboard transport.
///
/// Details:
/// - Memory and disk values are stored in megabytes.
#[derive(Debug, Clone, PartialEq)]
pub struct SystemMetrics {
    pub memory_used_mb: u64,
    pub memory_total_mb: u64,
    pub cpu_percent: f32,
    pub thread_count: u32,
    pub disk_used_mb: u64,
    pub disk_total_mb: u64,
}

/// What: Collect current CPU, memory, and disk metrics.
///
/// Inputs:
/// - None.
///
/// Output:
/// - SystemMetrics snapshot with bounded CPU percentage.
///
/// Details:
/// - Uses sysinfo to gather host stats.
/// - Aggregates all visible disks for total/used values.
pub fn collect_system_metrics() -> SystemMetrics {
    let mut system = System::new_all();
    system.refresh_all();

    let total_memory = system.total_memory();
    let used_memory = system.used_memory();

    // Per-process CPU usage gives worker-specific signal instead of host-wide average.
    let current_process = sysinfo::get_current_pid()
        .ok()
        .and_then(|pid| system.process(pid));

    let cpu_count = system.cpus().len().max(1) as f32;
    let cpu_percent = current_process
        .map(|process| (process.cpu_usage() / cpu_count).clamp(0.0, 100.0))
        .unwrap_or(0.0);

    let thread_count = {
        #[cfg(target_os = "linux")]
        {
            current_process
                .map(|process| u32::try_from(process.tasks.len()).unwrap_or(u32::MAX))
                .unwrap_or(0)
        }

        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    };

    let disk_total_bytes: u64 = system.disks().iter().map(DiskExt::total_space).sum();
    let disk_available_bytes: u64 = system.disks().iter().map(DiskExt::available_space).sum();
    let disk_used_bytes = disk_total_bytes.saturating_sub(disk_available_bytes);

    SystemMetrics {
        memory_used_mb: used_memory / 1024,
        memory_total_mb: total_memory / 1024,
        cpu_percent,
        thread_count,
        disk_used_mb: disk_used_bytes / (1024 * 1024),
        disk_total_mb: disk_total_bytes / (1024 * 1024),
    }
}
