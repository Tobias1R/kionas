use super::{stage_exchange_data_key, stage_exchange_metadata_key, stage_exchange_prefix};

#[test]
fn builds_exchange_keys_with_sanitized_components() {
    let prefix = stage_exchange_prefix("Run-A", "s/1", 7);
    assert_eq!(prefix, "distributed_exchange/run-a/s_1/stage-7/");

    let data_key = stage_exchange_data_key("Run-A", "s/1", 7, 3);
    assert_eq!(
        data_key,
        "distributed_exchange/run-a/s_1/stage-7/part-00003.parquet"
    );

    let metadata_key = stage_exchange_metadata_key("Run-A", "s/1", 7, 3);
    assert_eq!(
        metadata_key,
        "distributed_exchange/run-a/s_1/stage-7/part-00003.metadata.json"
    );
}
