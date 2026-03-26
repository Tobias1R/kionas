use kionas::config::WarehousePoolTierConfig;
use serde::Serialize;
use std::time::SystemTime;
use uuid::Uuid;

/// What: Active warehouse pool with registered worker members.
///
/// Inputs:
/// - Constructed from a pool name and tier name
///
/// Output:
/// - In-memory pool record used for worker registration and session snapshots
///
/// Details:
/// - Pools are created lazily on first worker registration
/// - Membership updates bump the `updated_at` timestamp
/// - Health is evaluated against tier template constraints
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct WarehousePool {
    pub name: String,
    pub tier: String,
    pub members: Vec<String>,
    pub uuid: Uuid,
    pub created_at: SystemTime,
    pub updated_at: SystemTime,
}

impl WarehousePool {
    /// What: Create a new empty warehouse pool for the given tier.
    ///
    /// Inputs:
    /// - `name`: Pool name such as compute_xl
    /// - `tier`: Tier identifier such as xl
    ///
    /// Output:
    /// - New `WarehousePool` with no members
    ///
    /// Details:
    /// - Pool UUID is generated once and remains stable for the pool lifetime
    pub fn new(name: String, tier: String) -> Self {
        Self {
            name,
            tier,
            members: Vec::new(),
            uuid: Uuid::new_v4(),
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
        }
    }

    /// What: Add a worker member to the pool.
    ///
    /// Inputs:
    /// - `worker_name`: Worker name to register in the pool
    ///
    /// Output:
    /// - `Ok(())` when inserted or already present
    ///
    /// Details:
    /// - Operation is idempotent for duplicate registrations
    pub fn add_member(&mut self, worker_name: String) {
        if self.members.contains(&worker_name) {
            return;
        }
        self.members.push(worker_name);
        self.updated_at = SystemTime::now();
    }

    /// What: Remove a worker member from the pool.
    ///
    /// Inputs:
    /// - `worker_name`: Worker name to remove
    ///
    /// Output:
    /// - `Ok(())` when removed, otherwise an error string
    ///
    /// Details:
    /// - This is useful for worker shutdown and janitor cleanup paths
    #[allow(dead_code)]
    pub fn remove_member(&mut self, worker_name: &str) -> Result<(), String> {
        if let Some(position) = self.members.iter().position(|member| member == worker_name) {
            self.members.remove(position);
            self.updated_at = SystemTime::now();
            return Ok(());
        }
        Err(format!(
            "Worker '{}' not present in pool '{}'.",
            worker_name, self.name
        ))
    }

    /// What: Evaluate whether this pool satisfies tier membership constraints.
    ///
    /// Inputs:
    /// - `tier_config`: Tier template with minimum and maximum members
    ///
    /// Output:
    /// - `true` when pool size is within bounds, otherwise `false`
    ///
    /// Details:
    /// - Health check is used during registration guardrails and diagnostics
    #[allow(dead_code)]
    pub fn is_healthy(&self, tier_config: &WarehousePoolTierConfig) -> bool {
        let member_count = self.members.len();
        member_count >= tier_config.min_members && member_count <= tier_config.max_members
    }
}

/// What: Pool health snapshot used by diagnostics and monitoring paths.
///
/// Inputs:
/// - Produced by state lookup from active pools and tier templates
///
/// Output:
/// - Serializable health status for a single pool
///
/// Details:
/// - Contains both current membership and configured bounds
#[derive(Debug, Clone, Serialize)]
#[allow(dead_code)]
pub struct PoolHealth {
    pub pool_name: String,
    pub tier: String,
    pub current_members: usize,
    pub min_members: usize,
    pub max_members: usize,
    pub is_healthy: bool,
}
