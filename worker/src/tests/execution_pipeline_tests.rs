use super::maybe_prune_scan_keys;
use crate::execution::planner::{RuntimeScanHints, RuntimeScanMode};
use crate::storage::StorageProvider;
use crate::storage::mock::MockProvider;
use std::sync::Arc;

fn metadata_pruned_hints(predicate_ast: serde_json::Value, pin: u64) -> RuntimeScanHints {
    RuntimeScanHints {
        mode: RuntimeScanMode::MetadataPruned,
        pruning_hints_json: Some(
            serde_json::json!({
                "hint_version": 2,
                "eligible": true,
                "reason": "eligible_foundation_and_comparisons",
                "predicate_ast": predicate_ast,
            })
            .to_string(),
        ),
        delta_version_pin: Some(pin),
        pruning_eligible: true,
        pruning_reason: Some("eligible_foundation_and_comparisons".to_string()),
    }
}

fn as_storage(provider: Arc<MockProvider>) -> Arc<dyn StorageProvider + Send + Sync> {
    provider
}

#[tokio::test]
async fn pruning_falls_back_on_pin_mismatch() {
    let provider = MockProvider::new().into_arc();
    provider
        .put_object(
            "databases/sales/schemas/public/tables/users/_delta_log/00000000000000000001.json",
            b"{}".to_vec(),
        )
        .await
        .expect("delta log should be seeded");

    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = metadata_pruned_hints(
        serde_json::json!({
            "kind": "comparison",
            "column": "id",
            "op": ">",
            "value": "5",
        }),
        99,
    );

    let pruned = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(pruned, keys);
}

#[tokio::test]
async fn pruning_falls_back_when_stats_missing() {
    let provider = MockProvider::new().into_arc();
    provider
        .put_object(
            "databases/sales/schemas/public/tables/users/_delta_log/00000000000000000001.json",
            br#"{"add":{"path":"staging/a.parquet"}}"#.to_vec(),
        )
        .await
        .expect("delta log should be seeded");

    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = metadata_pruned_hints(
        serde_json::json!({
            "kind": "comparison",
            "column": "id",
            "op": ">",
            "value": "5",
        }),
        1,
    );

    let pruned = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(pruned, keys);
}

#[tokio::test]
async fn pruning_removes_definitely_false_file() {
    let provider = MockProvider::new().into_arc();
    provider
        .put_object(
            "databases/sales/schemas/public/tables/users/_delta_log/00000000000000000001.json",
            br#"
{"add":{"path":"staging/a.parquet","stats":"{\"minValues\":{\"id\":1},\"maxValues\":{\"id\":3}}"}}
{"add":{"path":"staging/b.parquet","stats":"{\"minValues\":{\"id\":9},\"maxValues\":{\"id\":10}}"}}
"#
            .to_vec(),
        )
        .await
        .expect("delta log should be seeded");

    let keys = vec![
        "databases/sales/schemas/public/tables/users/staging/a.parquet".to_string(),
        "databases/sales/schemas/public/tables/users/staging/b.parquet".to_string(),
    ];
    let hints = metadata_pruned_hints(
        serde_json::json!({
            "kind": "comparison",
            "column": "id",
            "op": ">",
            "value": "5",
        }),
        1,
    );

    let pruned = maybe_prune_scan_keys(
        &as_storage(provider),
        "databases/sales/schemas/public/tables/users/staging/",
        &keys,
        &hints,
    )
    .await;

    assert_eq!(pruned.len(), 1);
    assert_eq!(
        pruned[0],
        "databases/sales/schemas/public/tables/users/staging/b.parquet"
    );
}
