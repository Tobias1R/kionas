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
