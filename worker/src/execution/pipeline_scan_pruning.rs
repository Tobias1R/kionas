use crate::execution::pipeline::ScanPruningOutcome;
use crate::execution::planner::{RuntimeScanHints, RuntimeScanMode};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
struct PredicateComparisonNode {
    column: String,
    op: String,
    value: String,
}

#[derive(Debug, Clone)]
enum PredicateAstNode {
    Comparison(PredicateComparisonNode),
    And(Vec<PredicateAstNode>),
}

#[derive(Debug, Clone)]
enum PredicateLiteralValue {
    Number(f64),
    String(String),
    Bool(bool),
}

#[derive(Debug, Clone)]
struct DeltaFileStats {
    min_values: serde_json::Map<String, Value>,
    max_values: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone)]
struct DeltaPruningCacheEntry {
    cached_at: Instant,
    latest_version: u64,
    file_stats: HashMap<String, DeltaFileStats>,
}

const DELTA_PRUNING_CACHE_TTL: Duration = Duration::from_secs(30);

static DELTA_PRUNING_CACHE: LazyLock<Mutex<HashMap<String, DeltaPruningCacheEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn get_cached_delta_pruning_metadata(
    cache_key: &str,
) -> Option<(u64, HashMap<String, DeltaFileStats>)> {
    let mut cache = DELTA_PRUNING_CACHE.lock().ok()?;
    let entry = cache.get(cache_key)?;
    if entry.cached_at.elapsed() > DELTA_PRUNING_CACHE_TTL {
        cache.remove(cache_key);
        return None;
    }

    Some((entry.latest_version, entry.file_stats.clone()))
}

fn put_cached_delta_pruning_metadata(
    cache_key: String,
    latest_version: u64,
    file_stats: HashMap<String, DeltaFileStats>,
) {
    if let Ok(mut cache) = DELTA_PRUNING_CACHE.lock() {
        cache.insert(
            cache_key,
            DeltaPruningCacheEntry {
                cached_at: Instant::now(),
                latest_version,
                file_stats,
            },
        );
    }
}

fn parse_predicate_ast_from_hints(scan_hints_json: &str) -> Result<PredicateAstNode, &'static str> {
    let hints: Value = serde_json::from_str(scan_hints_json).map_err(|_| "hints_invalid")?;
    let ast = hints.get("predicate_ast").ok_or("predicate_ast_missing")?;
    parse_predicate_ast_node(ast).ok_or("predicate_ast_missing")
}

fn parse_predicate_ast_node(node: &Value) -> Option<PredicateAstNode> {
    let kind = node.get("kind")?.as_str()?;
    match kind {
        "comparison" => Some(PredicateAstNode::Comparison(PredicateComparisonNode {
            column: node.get("column")?.as_str()?.to_string(),
            op: node.get("op")?.as_str()?.to_string(),
            value: node.get("value")?.as_str()?.to_string(),
        })),
        "and" => {
            let terms = node
                .get("terms")?
                .as_array()?
                .iter()
                .filter_map(parse_predicate_ast_node)
                .collect::<Vec<_>>();
            if terms.is_empty() {
                None
            } else {
                Some(PredicateAstNode::And(terms))
            }
        }
        _ => None,
    }
}

fn table_prefix_from_source_prefix(source_prefix: &str) -> Option<String> {
    source_prefix
        .strip_suffix("staging/")
        .map(ToString::to_string)
}

fn delta_log_prefix_from_source_prefix(source_prefix: &str) -> Option<String> {
    table_prefix_from_source_prefix(source_prefix).map(|prefix| format!("{}_delta_log/", prefix))
}

async fn latest_delta_log_version(
    provider: &std::sync::Arc<dyn crate::storage::StorageProvider + Send + Sync>,
    delta_log_prefix: &str,
) -> Result<Option<u64>, String> {
    let keys = provider.list_objects(delta_log_prefix).await.map_err(|e| {
        format!(
            "failed to list delta log objects for {}: {}",
            delta_log_prefix, e
        )
    })?;

    let mut latest = None::<u64>;
    for key in keys {
        if !key.ends_with(".json") {
            continue;
        }
        let file_name = key.rsplit('/').next().unwrap_or_default();
        let stem = file_name.strip_suffix(".json").unwrap_or_default();
        if stem.len() != 20 || !stem.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }
        if let Ok(version) = stem.parse::<u64>() {
            latest = Some(latest.map_or(version, |current| current.max(version)));
        }
    }

    Ok(latest)
}

async fn load_delta_file_stats_for_version(
    provider: &std::sync::Arc<dyn crate::storage::StorageProvider + Send + Sync>,
    delta_log_prefix: &str,
    table_prefix: &str,
    version: u64,
) -> Result<HashMap<String, DeltaFileStats>, String> {
    let delta_log_key = format!("{}{:020}.json", delta_log_prefix, version);
    let bytes = provider
        .get_object(&delta_log_key)
        .await
        .map_err(|e| format!("failed to read delta log {}: {}", delta_log_key, e))?
        .ok_or_else(|| format!("delta log object not found: {}", delta_log_key))?;
    let content = String::from_utf8(bytes)
        .map_err(|e| format!("delta log {} is not valid UTF-8: {}", delta_log_key, e))?;

    let mut file_stats = HashMap::<String, DeltaFileStats>::new();

    for line in content.lines().filter(|line| !line.trim().is_empty()) {
        let row: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let add = match row.get("add") {
            Some(value) => value,
            None => continue,
        };

        let relative_path = match add.get("path").and_then(Value::as_str) {
            Some(value) if !value.trim().is_empty() => value,
            _ => continue,
        };

        let stats_raw = match add.get("stats").and_then(Value::as_str) {
            Some(value) if !value.trim().is_empty() => value,
            _ => continue,
        };

        let stats_value: Value = match serde_json::from_str(stats_raw) {
            Ok(value) => value,
            Err(_) => continue,
        };

        let min_values = match stats_value.get("minValues").and_then(Value::as_object) {
            Some(map) => map.clone(),
            None => continue,
        };
        let max_values = match stats_value.get("maxValues").and_then(Value::as_object) {
            Some(map) => map.clone(),
            None => continue,
        };

        let relative_key = relative_path.trim_start_matches('/');
        let full_key = format!("{}{}", table_prefix, relative_key);
        file_stats.insert(
            full_key,
            DeltaFileStats {
                min_values,
                max_values,
            },
        );
    }

    Ok(file_stats)
}

fn normalize_column_name(column: &str) -> String {
    column
        .trim()
        .trim_matches('"')
        .trim_matches('`')
        .rsplit('.')
        .next()
        .unwrap_or(column)
        .to_string()
}

fn parse_predicate_literal(raw: &str) -> Option<PredicateLiteralValue> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(stripped) = trimmed
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            trimmed
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
    {
        return Some(PredicateLiteralValue::String(stripped.to_string()));
    }

    if trimmed.eq_ignore_ascii_case("true") {
        return Some(PredicateLiteralValue::Bool(true));
    }
    if trimmed.eq_ignore_ascii_case("false") {
        return Some(PredicateLiteralValue::Bool(false));
    }

    trimmed
        .parse::<f64>()
        .ok()
        .map(PredicateLiteralValue::Number)
}

fn compare_value_and_literal(value: &Value, literal: &PredicateLiteralValue) -> Option<Ordering> {
    match literal {
        PredicateLiteralValue::Number(right) => value.as_f64()?.partial_cmp(right),
        PredicateLiteralValue::String(right) => Some(value.as_str()?.cmp(right)),
        PredicateLiteralValue::Bool(right) => Some(value.as_bool()?.cmp(right)),
    }
}

fn is_comparison_definitely_false(
    comparison: &PredicateComparisonNode,
    stats: &DeltaFileStats,
) -> bool {
    let column = normalize_column_name(&comparison.column);
    let min_value = match stats.min_values.get(&column) {
        Some(value) => value,
        None => return false,
    };
    let max_value = match stats.max_values.get(&column) {
        Some(value) => value,
        None => return false,
    };
    let literal = match parse_predicate_literal(&comparison.value) {
        Some(value) => value,
        None => return false,
    };

    let min_cmp = compare_value_and_literal(min_value, &literal);
    let max_cmp = compare_value_and_literal(max_value, &literal);

    match comparison.op.as_str() {
        "=" => {
            matches!(max_cmp, Some(Ordering::Less)) || matches!(min_cmp, Some(Ordering::Greater))
        }
        ">" => matches!(max_cmp, Some(Ordering::Less | Ordering::Equal)),
        ">=" => matches!(max_cmp, Some(Ordering::Less)),
        "<" => matches!(min_cmp, Some(Ordering::Greater | Ordering::Equal)),
        "<=" => matches!(min_cmp, Some(Ordering::Greater)),
        "!=" => {
            matches!(min_cmp, Some(Ordering::Equal)) && matches!(max_cmp, Some(Ordering::Equal))
        }
        _ => false,
    }
}

fn predicate_definitely_false_for_file(
    predicate: &PredicateAstNode,
    stats: &DeltaFileStats,
) -> bool {
    match predicate {
        PredicateAstNode::Comparison(node) => is_comparison_definitely_false(node, stats),
        PredicateAstNode::And(terms) => terms
            .iter()
            .any(|term| predicate_definitely_false_for_file(term, stats)),
    }
}

fn prune_keys_by_delta_stats(
    keys: &[String],
    stats: &HashMap<String, DeltaFileStats>,
    predicate: &PredicateAstNode,
) -> Vec<String> {
    keys.iter()
        .filter(|key| match stats.get(key.as_str()) {
            Some(entry) => !predicate_definitely_false_for_file(predicate, entry),
            None => true,
        })
        .cloned()
        .collect()
}

pub(crate) async fn maybe_prune_scan_keys(
    provider: &std::sync::Arc<dyn crate::storage::StorageProvider + Send + Sync>,
    source_prefix: &str,
    keys: &[String],
    scan_hints: &RuntimeScanHints,
) -> ScanPruningOutcome {
    if scan_hints.mode != RuntimeScanMode::MetadataPruned {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "mode_full_requested",
            cache_hit: None,
        };
    }

    if !scan_hints.pruning_eligible {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "ineligible_or_missing_reason",
            cache_hit: None,
        };
    }

    let Some(scan_hints_json) = scan_hints.pruning_hints_json.as_deref() else {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "hints_missing",
            cache_hit: None,
        };
    };

    let predicate_ast = match parse_predicate_ast_from_hints(scan_hints_json) {
        Ok(value) => value,
        Err(reason) => {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason,
                cache_hit: None,
            };
        }
    };

    let Some(delta_log_prefix) = delta_log_prefix_from_source_prefix(source_prefix) else {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "source_prefix_invalid",
            cache_hit: None,
        };
    };
    let Some(table_prefix) = table_prefix_from_source_prefix(source_prefix) else {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "table_prefix_invalid",
            cache_hit: None,
        };
    };

    let cache_key = table_prefix.clone();
    if let Some((cached_version, cached_stats)) = get_cached_delta_pruning_metadata(&cache_key) {
        if let Some(pin) = scan_hints.delta_version_pin
            && pin > 0
            && pin != cached_version
        {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_pin_mismatch",
                cache_hit: Some(true),
            };
        }

        if cached_stats.is_empty() {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_stats_unavailable",
                cache_hit: Some(true),
            };
        }

        return ScanPruningOutcome {
            keys: prune_keys_by_delta_stats(keys, &cached_stats, &predicate_ast),
            effective_mode: RuntimeScanMode::MetadataPruned,
            reason: "pruning_applied",
            cache_hit: Some(true),
        };
    }

    let latest_version = match latest_delta_log_version(provider, &delta_log_prefix).await {
        Ok(Some(version)) => version,
        Ok(None) | Err(_) => {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_version_unavailable",
                cache_hit: Some(false),
            };
        }
    };

    if let Some(pin) = scan_hints.delta_version_pin
        && pin > 0
        && pin != latest_version
    {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "delta_pin_mismatch",
            cache_hit: Some(false),
        };
    }

    let stats = match load_delta_file_stats_for_version(
        provider,
        &delta_log_prefix,
        &table_prefix,
        latest_version,
    )
    .await
    {
        Ok(value) => value,
        Err(_) => {
            return ScanPruningOutcome {
                keys: keys.to_vec(),
                effective_mode: RuntimeScanMode::Full,
                reason: "delta_stats_unavailable",
                cache_hit: Some(false),
            };
        }
    };

    if stats.is_empty() {
        return ScanPruningOutcome {
            keys: keys.to_vec(),
            effective_mode: RuntimeScanMode::Full,
            reason: "delta_stats_unavailable",
            cache_hit: Some(false),
        };
    }

    put_cached_delta_pruning_metadata(cache_key, latest_version, stats.clone());

    ScanPruningOutcome {
        keys: prune_keys_by_delta_stats(keys, &stats, &predicate_ast),
        effective_mode: RuntimeScanMode::MetadataPruned,
        reason: "pruning_applied",
        cache_hit: Some(false),
    }
}
