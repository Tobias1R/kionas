/// What: Update AVG aggregate partial state from one numeric input value.
///
/// Inputs:
/// - `sum_state`: Mutable running sum.
/// - `count_state`: Mutable running count of non-null rows.
/// - `value`: Optional numeric input. `None` is ignored for SQL NULL semantics.
///
/// Output:
/// - Updated sum and count state.
pub(crate) fn update_partial_avg(sum_state: &mut f64, count_state: &mut i64, value: Option<f64>) {
    if let Some(value) = value {
        *sum_state += value;
        *count_state += 1;
    }
}

/// What: Merge one upstream AVG partial state into final AVG state.
///
/// Inputs:
/// - `sum_state`: Mutable final sum.
/// - `count_state`: Mutable final count.
/// - `upstream_sum`: Upstream partial sum.
/// - `upstream_count`: Upstream partial count.
///
/// Output:
/// - Updated final sum and count.
pub(crate) fn merge_final_avg(
    sum_state: &mut f64,
    count_state: &mut i64,
    upstream_sum: f64,
    upstream_count: i64,
) {
    *sum_state += upstream_sum;
    *count_state += upstream_count;
}

/// What: Finalize AVG value from merged sum/count states.
///
/// Inputs:
/// - `sum_state`: Final aggregate sum.
/// - `count_state`: Final aggregate count.
///
/// Output:
/// - Optional finalized average. Returns `None` for empty non-null input set.
pub(crate) fn finalize_avg(sum_state: f64, count_state: i64) -> Option<f64> {
    if count_state == 0 {
        return None;
    }

    Some(sum_state / count_state as f64)
}
