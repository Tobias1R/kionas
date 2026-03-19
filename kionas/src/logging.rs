use chrono::Local;
use env_logger::{Builder, Target};
use log::LevelFilter;
use std::io::Write;

/// Initialize the logger based on configuration
pub fn init_logging(
    level: &str,
    output: &str,
    _format: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Extract logging settings
    let level = level;
    let output = output;

    // Map level string to LevelFilter
    let log_level = match level.to_string().as_str() {
        "trace" => LevelFilter::Trace,
        "debug" => LevelFilter::Debug,
        "info" => LevelFilter::Info,
        "warn" => LevelFilter::Warn,
        "error" => LevelFilter::Error,
        _ => LevelFilter::Info,
    };

    // Configure logger
    let mut builder = Builder::new();
    builder.filter(None, log_level);

    // Set output target
    match output.to_string().to_lowercase().as_str() {
        "stdout" => builder.target(Target::Stdout),
        "stderr" => builder.target(Target::Stderr),
        _ => builder.target(Target::Stdout), // Default to stdout
    };

    // Set log format
    builder.format(move |buf, record| {
        writeln!(
            buf,
            "[{}][{}][{}] {}",
            record.level(),
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            record.target(),
            record.args()
        )
    });

    // Initialize logger
    builder.init();

    Ok(())
}
