/// What: Update SUM aggregate partial state from one numeric input value.
///
/// Inputs:
/// - `state`: Mutable running sum.
/// - `value`: Optional numeric input. `None` is ignored for SQL NULL semantics.
///
/// Output:
/// - Updated running sum.
pub(crate) fn update_partial_sum(state: &mut f64, value: Option<f64>) {
    if let Some(value) = value {
        *state += value;
    }
}

/// What: Merge one upstream SUM partial value into final SUM state.
///
/// Inputs:
/// - `state`: Mutable final sum.
/// - `upstream_partial`: Partial sum from upstream partition.
///
/// Output:
/// - Updated final sum.
pub(crate) fn merge_final_sum(state: &mut f64, upstream_partial: f64) {
    *state += upstream_partial;
}
