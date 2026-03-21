/// What: Update MAX aggregate partial state from one numeric input value.
///
/// Inputs:
/// - `state`: Mutable optional maximum value.
/// - `value`: Optional numeric input. `None` is ignored for SQL NULL semantics.
///
/// Output:
/// - Updated maximum value.
pub(crate) fn update_partial_max(state: &mut Option<f64>, value: Option<f64>) {
    if let Some(value) = value {
        match state {
            Some(current) if value > *current => *current = value,
            Some(_) => {}
            None => *state = Some(value),
        }
    }
}

/// What: Merge one upstream MAX partial value into final MAX state.
///
/// Inputs:
/// - `state`: Mutable optional maximum value.
/// - `upstream_partial`: Optional partial maximum from upstream partition.
///
/// Output:
/// - Updated final maximum value.
pub(crate) fn merge_final_max(state: &mut Option<f64>, upstream_partial: Option<f64>) {
    update_partial_max(state, upstream_partial);
}
