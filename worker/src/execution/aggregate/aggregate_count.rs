/// What: Update COUNT aggregate partial state from one input value.
///
/// Inputs:
/// - `state`: Mutable running count.
/// - `include_row`: When `true`, increments count by one.
///
/// Output:
/// - Updated running count.
pub(crate) fn update_partial_count(state: &mut i64, include_row: bool) {
    if include_row {
        *state += 1;
    }
}

/// What: Merge one upstream COUNT partial value into final COUNT state.
///
/// Inputs:
/// - `state`: Mutable final count.
/// - `upstream_partial`: Partial count value from upstream partition.
///
/// Output:
/// - Updated final count.
pub(crate) fn merge_final_count(state: &mut i64, upstream_partial: i64) {
    *state += upstream_partial;
}
