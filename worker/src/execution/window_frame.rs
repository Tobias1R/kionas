use kionas::planner::{PhysicalWindowFrameBound, PhysicalWindowFrameSpec, PhysicalWindowFrameUnit};

/// What: Resolved row-index boundaries for one window frame evaluation.
///
/// Inputs:
/// - `start`: Inclusive frame start row index.
/// - `end`: Inclusive frame end row index.
///
/// Output:
/// - Concrete index interval used by window function evaluation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResolvedWindowFrame {
    pub(crate) start: usize,
    pub(crate) end: usize,
}

/// What: Resolve row-based frame boundaries for one partition row.
///
/// Inputs:
/// - `frame`: Optional frame specification from physical plan.
/// - `current_row`: Current row index in sorted partition.
/// - `partition_len`: Total number of rows in partition.
///
/// Output:
/// - Inclusive row-index range for the current frame.
///
/// Details:
/// - Defaults to `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
/// - Returns `Ok(None)` when the resolved frame is empty.
/// - `RANGE` frame unit is rejected in this increment with an actionable error.
pub(crate) fn resolve_window_frame(
    frame: Option<&PhysicalWindowFrameSpec>,
    current_row: usize,
    partition_len: usize,
) -> Result<Option<ResolvedWindowFrame>, String> {
    if partition_len == 0 {
        return Ok(None);
    }

    let default = PhysicalWindowFrameSpec {
        unit: PhysicalWindowFrameUnit::Rows,
        start_bound: PhysicalWindowFrameBound::UnboundedPreceding,
        end_bound: PhysicalWindowFrameBound::CurrentRow,
    };
    let frame = frame.unwrap_or(&default);

    if !matches!(frame.unit, PhysicalWindowFrameUnit::Rows) {
        return Err("window frame unit RANGE is not supported yet; use ROWS frames".to_string());
    }

    let start = resolve_start(&frame.start_bound, current_row);
    let end = resolve_end(&frame.end_bound, current_row, partition_len);

    if start > end || start >= partition_len {
        return Ok(None);
    }

    let bounded_end = end.min(partition_len.saturating_sub(1));
    Ok(Some(ResolvedWindowFrame {
        start,
        end: bounded_end,
    }))
}

fn resolve_start(bound: &PhysicalWindowFrameBound, current_row: usize) -> usize {
    match bound {
        PhysicalWindowFrameBound::UnboundedPreceding => 0,
        PhysicalWindowFrameBound::Preceding { offset } => {
            current_row.saturating_sub(usize::try_from(*offset).unwrap_or(usize::MAX))
        }
        PhysicalWindowFrameBound::CurrentRow => current_row,
        PhysicalWindowFrameBound::Following { offset } => {
            current_row.saturating_add(usize::try_from(*offset).unwrap_or(usize::MAX))
        }
        PhysicalWindowFrameBound::UnboundedFollowing => usize::MAX,
    }
}

fn resolve_end(
    bound: &PhysicalWindowFrameBound,
    current_row: usize,
    partition_len: usize,
) -> usize {
    match bound {
        PhysicalWindowFrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
        PhysicalWindowFrameBound::Following { offset } => {
            current_row.saturating_add(usize::try_from(*offset).unwrap_or(usize::MAX))
        }
        PhysicalWindowFrameBound::CurrentRow => current_row,
        PhysicalWindowFrameBound::Preceding { offset } => {
            current_row.saturating_sub(usize::try_from(*offset).unwrap_or(usize::MAX))
        }
        PhysicalWindowFrameBound::UnboundedPreceding => 0,
    }
}
