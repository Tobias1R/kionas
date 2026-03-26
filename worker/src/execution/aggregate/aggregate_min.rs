/// What: Update MIN aggregate partial state from one numeric input value.
///
/// Inputs:
/// - `state`: Mutable optional minimum value.
/// - `value`: Optional numeric input. `None` is ignored for SQL NULL semantics.
///
/// Output:
/// - Updated minimum value.
pub(crate) fn update_partial_min(state: &mut Option<f64>, value: Option<f64>) {
    if let Some(value) = value {
        match state {
            Some(current) if value < *current => *current = value,
            Some(_) => {}
            None => *state = Some(value),
        }
    }
}

/// What: Merge one upstream MIN partial value into final MIN state.
///
/// Inputs:
/// - `state`: Mutable optional minimum value.
/// - `upstream_partial`: Optional partial minimum from upstream partition.
///
/// Output:
/// - Updated final minimum value.
pub(crate) fn merge_final_min(state: &mut Option<f64>, upstream_partial: Option<f64>) {
    update_partial_min(state, upstream_partial);
}
