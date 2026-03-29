use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;

type ConstraintCacheEntries = HashMap<(String, String, String), CachedConstraints>;

/// What: Cached normalized table constraints used by INSERT validation.
///
/// Inputs:
/// - `table_columns`: Canonical ordered table column names.
/// - `not_null_columns`: Canonical non-null constrained columns.
/// - `column_type_hints`: Canonical type hints keyed by normalized column name.
/// - `cached_at`: Timestamp marking when cache entry was populated.
///
/// Output:
/// - Immutable cache value cloned on lookup hit.
///
/// Details:
/// - `cached_at` is compared against cache TTL to determine freshness.
#[derive(Debug, Clone)]
pub struct CachedConstraints {
    pub table_columns: Vec<String>,
    pub not_null_columns: Vec<String>,
    pub column_type_hints: HashMap<String, String>,
    pub cached_at: Instant,
}

/// What: In-memory cache for relation constraints used by INSERT execution.
///
/// Inputs:
/// - `database`, `schema`, `table` key tuples for entries.
///
/// Output:
/// - Fast read-path cache hits for table constraints.
///
/// Details:
/// - Uses a coarse-grained async `RwLock` and TTL-based eviction checks.
#[derive(Debug, Clone)]
pub struct TableConstraintCache {
    entries: Arc<RwLock<ConstraintCacheEntries>>,
    ttl: Duration,
}

impl TableConstraintCache {
    /// What: Create a new table constraint cache with a configured TTL.
    ///
    /// Inputs:
    /// - `ttl`: Maximum age for cache entries.
    ///
    /// Output:
    /// - Empty cache instance ready for shared use.
    ///
    /// Details:
    /// - A zero TTL effectively disables cache reuse.
    pub fn new(ttl: Duration) -> Self {
        Self {
            entries: Arc::new(RwLock::new(HashMap::new())),
            ttl,
        }
    }

    /// What: Read a cache entry if present and not expired.
    ///
    /// Inputs:
    /// - `database`: Canonical database.
    /// - `schema`: Canonical schema.
    /// - `table`: Canonical table.
    ///
    /// Output:
    /// - `Some(CachedConstraints)` on hit, otherwise `None`.
    ///
    /// Details:
    /// - Expired entries are removed before returning `None`.
    pub async fn get(
        &self,
        database: &str,
        schema: &str,
        table: &str,
    ) -> Option<CachedConstraints> {
        let key = (database.to_string(), schema.to_string(), table.to_string());

        {
            let guard = self.entries.read().await;
            if let Some(entry) = guard.get(&key)
                && entry.cached_at.elapsed() < self.ttl
            {
                return Some(entry.clone());
            }
        }

        let mut guard = self.entries.write().await;
        if let Some(entry) = guard.get(&key)
            && entry.cached_at.elapsed() < self.ttl
        {
            return Some(entry.clone());
        }
        guard.remove(&key);
        None
    }

    /// What: Insert or replace a cache entry for a table namespace.
    ///
    /// Inputs:
    /// - `database`: Canonical database.
    /// - `schema`: Canonical schema.
    /// - `table`: Canonical table.
    /// - `table_columns`: Canonical ordered table columns.
    /// - `not_null_columns`: Canonical non-null constrained columns.
    /// - `column_type_hints`: Canonical type hints.
    ///
    /// Output:
    /// - Entry becomes available for subsequent read hits.
    ///
    /// Details:
    /// - Existing value for the key is overwritten.
    pub async fn insert(
        &self,
        database: &str,
        schema: &str,
        table: &str,
        table_columns: Vec<String>,
        not_null_columns: Vec<String>,
        column_type_hints: HashMap<String, String>,
    ) {
        let key = (database.to_string(), schema.to_string(), table.to_string());
        let value = CachedConstraints {
            table_columns,
            not_null_columns,
            column_type_hints,
            cached_at: Instant::now(),
        };

        let mut guard = self.entries.write().await;
        guard.insert(key, value);
    }

    /// What: Remove a table namespace from the cache.
    ///
    /// Inputs:
    /// - `database`: Canonical database.
    /// - `schema`: Canonical schema.
    /// - `table`: Canonical table.
    ///
    /// Output:
    /// - Returns `true` when an entry was removed.
    ///
    /// Details:
    /// - Missing keys are treated as successful no-op invalidations.
    pub async fn invalidate(&self, database: &str, schema: &str, table: &str) -> bool {
        let key = (database.to_string(), schema.to_string(), table.to_string());
        let mut guard = self.entries.write().await;
        guard.remove(&key).is_some()
    }
}
