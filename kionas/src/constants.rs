/*
 * Constants used throughout the server module
 */

/*
 * Key for storing cluster configuration in Consul
 */
pub const CONSUL_CLUSTER_KEY: &str = "kionas/cluster";
/*
 * Key prefix for storing worker-specific configuration in Consul
 * Full key format: kionas/configs/<node_id>
 */
pub const CONSUL_NODE_CONFIG_PREFIX: &str = "kionas/configs/";

/*
 * Redis database indexes
 */
pub const REDIS_DB_SESSION: u8 = 0;
pub const REDIS_DB_CONFIG: u8 = 1;
pub const REDIS_DB_STATUS: u8 = 2;

/*
 * Redis runtime configuration
 */
pub const REDIS_URL_ENV: &str = "REDIS_URL";
pub const REDIS_POOL_SIZE_ENV: &str = "REDIS_POOL_SIZE";

/*
 * Worker task-result cache keys
 */
pub const REDIS_TASK_RESULT_KEY_PREFIX: &str = "kionas:worker:task_result";
pub const REDIS_TASK_RESULT_TTL_SECONDS: u64 = 86_400;

/*
 * Session and auth token TTL
 */
pub const REDIS_SESSION_TTL_SECONDS: u64 = 604_800; // 7 days
pub const REDIS_SESSION_TTL_ENV: &str = "KIONAS_SESSION_TTL_SECONDS";

/*
 * MinIO artifact cleanup retention policies
 */
pub const STAGE_EXCHANGE_RETENTION_SECONDS: u64 = 259_200; // 3 days
pub const STAGE_EXCHANGE_RETENTION_ENV: &str = "KIONAS_STAGE_EXCHANGE_RETENTION_SECONDS";

pub const QUERY_RESULT_RETENTION_SECONDS: u64 = 604_800; // 7 days
pub const QUERY_RESULT_RETENTION_ENV: &str = "KIONAS_QUERY_RESULT_RETENTION_SECONDS";

pub const TRANSACTION_STAGING_RETENTION_SECONDS: u64 = 86_400; // 1 day
pub const TRANSACTION_STAGING_RETENTION_ENV: &str = "KIONAS_TRANSACTION_STAGING_RETENTION_SECONDS";

pub const CLEANUP_JOB_INTERVAL_SECONDS: u64 = 3_600; // 1 hour
pub const CLEANUP_JOB_INTERVAL_ENV: &str = "KIONAS_CLEANUP_JOB_INTERVAL_SECONDS";

/*
 * UI dashboard cache keys
 */
pub const REDIS_UI_DASHBOARD_SERVER_STATS_KEY: &str = "kionas:ui:dashboard:server_stats";
pub const REDIS_UI_DASHBOARD_SESSIONS_KEY: &str = "kionas:ui:dashboard:sessions";
pub const REDIS_UI_DASHBOARD_TOKENS_KEY: &str = "kionas:ui:dashboard:tokens";
pub const REDIS_UI_DASHBOARD_WORKERS_KEY: &str = "kionas:ui:dashboard:workers";
pub const REDIS_UI_DASHBOARD_CONSUL_SUMMARY_KEY: &str =
    "kionas:ui:dashboard:consul_cluster_summary";
