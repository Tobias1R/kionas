pub(crate) mod aggregate;
pub(crate) mod artifacts;
pub(crate) mod join;
pub(crate) mod pipeline;
pub(crate) mod pipeline_core;
#[cfg(test)]
pub(crate) mod pipeline_exchange_io;
#[cfg(test)]
pub(crate) mod pipeline_exchange_partitioning;
pub(crate) mod pipeline_exchange_validation;
pub(crate) mod pipeline_relation_columns;
pub(crate) mod pipeline_scan_io;
pub(crate) mod pipeline_scan_pruning;
pub(crate) mod pipeline_telemetry;
pub(crate) mod pipeline_union;
pub(crate) mod pipeline_upstream;
pub(crate) mod planner;
pub(crate) mod query;
pub(crate) mod router;
pub(crate) mod window;
pub(crate) mod window_frame;
pub(crate) mod window_functions;
