use crate::config::ClusterConfig;

#[test]
fn cluster_config_accepts_legacy_version_alias() {
    let raw = r#"
    {
      "cluster_name": "local-dev-cluster",
      "cluster_id": "cluster-001",
      "version": "1.0.0",
      "nodes": ["node-1"],
      "master": "127.0.0.1:8500",
      "storage": {
        "storage_type": "minio",
        "bucket": "warehouse",
        "region": "us-east-1",
        "endpoint": "http://localhost:9000",
        "access_key": "root",
        "secret_key": "rootpassword"
      }
    }
    "#;

    let parsed: ClusterConfig =
        serde_json::from_str(raw).expect("legacy version alias should parse");

    assert_eq!(parsed.cluster_name, "local-dev-cluster");
    assert_eq!(parsed.cluster_id, "cluster-001");
    assert_eq!(parsed.cluster_version, "1.0.0");
}
