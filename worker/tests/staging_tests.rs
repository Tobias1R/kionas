use serde_json::json;
use worker::storage::mock::MockProvider;
use worker::storage::staging;

#[tokio::test]
async fn test_stage_promote_abort_roundtrip() {
    let prov = MockProvider::new().into_arc();
    let tasks = vec![
        json!({"task_id": "t1", "operation": "op", "input": "in", "output": "out", "params": {}}),
        json!({"task_id": "t2", "operation": "op2", "input": "in2", "output": "out2", "params": {}}),
    ];
    let tx_id = "tx123";
    let staging_prefix = "pref";

    let keys = staging::stage_tx(prov.clone(), tx_id, staging_prefix, &tasks)
        .await
        .expect("stage_tx");
    assert_eq!(keys.len(), 2);

    // promote
    staging::promote_tx(prov.clone(), tx_id, staging_prefix)
        .await
        .expect("promote");

    // abort should be able to run (no staged objects remain)
    staging::abort_tx(prov.clone(), tx_id, staging_prefix)
        .await
        .expect("abort");
}
