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
