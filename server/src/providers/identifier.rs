/// What: Normalize SQL identifiers for deterministic provider and metadata lookups.
///
/// Inputs:
/// - `raw`: Identifier text that may include SQL quoting.
///
/// Output:
/// - Lowercased canonical identifier with quoting removed.
///
/// Details:
/// - This is shared across planner and statement handlers to avoid normalization drift.
pub fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}
