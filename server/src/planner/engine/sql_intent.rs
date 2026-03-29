use crate::planner::engine::types::PlannerIntentFlags;

/// What: Remove SQL literal and comment content before lightweight clause detection.
///
/// Inputs:
/// - `sql`: Canonical SQL text.
///
/// Output:
/// - Lowercased SQL text where literal/comment bodies are replaced with spaces.
///
/// Details:
/// - This is used only by fallback intent detection and avoids false positives from
///   keyword-like text in `'string literals'`, `-- line comments`, and `/* block comments */`.
fn sanitize_sql_for_clause_detection(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        let ch = bytes[i] as char;

        if ch == '\'' {
            out.push(' ');
            i += 1;
            while i < bytes.len() {
                let current = bytes[i] as char;
                if current == '\'' {
                    if i + 1 < bytes.len() && (bytes[i + 1] as char) == '\'' {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            continue;
        }

        if ch == '-' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '-' {
            out.push(' ');
            i += 2;
            while i < bytes.len() {
                let current = bytes[i] as char;
                i += 1;
                if current == '\n' {
                    out.push('\n');
                    break;
                }
            }
            continue;
        }

        if ch == '/' && i + 1 < bytes.len() && (bytes[i + 1] as char) == '*' {
            out.push(' ');
            i += 2;
            while i + 1 < bytes.len() {
                if (bytes[i] as char) == '*' && (bytes[i + 1] as char) == '/' {
                    i += 2;
                    break;
                }
                i += 1;
            }
            continue;
        }

        out.push(ch.to_ascii_lowercase());
        i += 1;
    }

    out
}

/// What: Collapse SQL whitespace to single spaces for stable clause keyword matching.
///
/// Inputs:
/// - `sql`: Lowercased/sanitized SQL text.
///
/// Output:
/// - SQL text with all whitespace runs normalized to single ASCII spaces.
///
/// Details:
/// - This allows fallback intent detection to match clauses even when SQL is formatted
///   across lines or tabs.
fn normalize_sql_whitespace_for_clause_detection(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// What: Derive planner intent flags from SQL text when DataFusion physical planning is unavailable.
///
/// Inputs:
/// - `sql`: Canonical SQL string from query model.
///
/// Output:
/// - Best-effort intent flags based on SQL clause presence.
///
/// Details:
/// - Used only as a fallback for unresolved relation planning errors.
/// - Join family defaults to hash-join compatibility for current worker contract.
pub(crate) fn derive_intent_from_sql_text(sql: &str) -> PlannerIntentFlags {
    let sanitized = sanitize_sql_for_clause_detection(sql);
    let normalized = normalize_sql_whitespace_for_clause_detection(&sanitized);
    let has_join = normalized.contains(" join ");
    let has_limit = normalized.contains(" limit ")
        || normalized.contains(" fetch first ")
        || normalized.contains(" fetch next ");
    let has_aggregate = normalized.contains(" group by ")
        || normalized.contains("count(")
        || normalized.contains("sum(")
        || normalized.contains("min(")
        || normalized.contains("max(")
        || normalized.contains("avg(");
    let has_window = normalized.contains(" over (");
    let has_union = normalized.contains(" union ");

    PlannerIntentFlags {
        has_scan: true,
        has_filter: normalized.contains(" where "),
        has_projection: true,
        has_sort: normalized.contains(" order by "),
        has_limit,
        has_hash_join: has_join,
        has_sort_merge_join: false,
        has_nested_loop_join: false,
        has_aggregate,
        has_window,
        has_union,
        union_child_count: 0,
    }
}
