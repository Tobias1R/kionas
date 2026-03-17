/// Manifest types and key helpers for staging/final prefixes.

/// Build full staging prefix: "staging/{staging_prefix}/{tx_id}/"
pub fn staging_prefix_key(staging_prefix: &str, tx_id: &str) -> String {
    format!("staging/{}/{}/", staging_prefix, tx_id)
}

/// Staging manifest key: "staging/{staging_prefix}/manifest_{tx_id}.json"
pub fn staging_manifest_key(staging_prefix: &str, tx_id: &str) -> String {
    format!("staging/{}/manifest_{}.json", staging_prefix, tx_id)
}

/// Final prefix: "final/{staging_prefix}/{tx_id}/"
pub fn final_prefix_key(staging_prefix: &str, tx_id: &str) -> String {
    format!("final/{}/{}/", staging_prefix, tx_id)
}

/// Final manifest key: "final/{staging_prefix}/manifest_{tx_id}.json"
pub fn final_manifest_key(staging_prefix: &str, tx_id: &str) -> String {
    format!("final/{}/manifest_{}.json", staging_prefix, tx_id)
}
