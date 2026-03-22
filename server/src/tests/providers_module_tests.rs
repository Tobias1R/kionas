use crate::providers::{
    KionasRelationMetadata, build_session_context_with_kionas_providers, normalize_identifier,
};
use crate::services::metastore_client::metastore_service as ms;

#[test]
fn normalizes_identifiers_consistently() {
    assert_eq!(normalize_identifier("\"Sales\""), "sales");
    assert_eq!(normalize_identifier("`Users`"), "users");
    assert_eq!(normalize_identifier("[Orders]"), "orders");
}

#[test]
fn relation_metadata_normalizes_columns_and_location() {
    let relation = KionasRelationMetadata::from_metastore(
        "Sales",
        "Public",
        "Users",
        Some(ms::TableMetadata {
            uuid: "u1".to_string(),
            schema_name: "Public".to_string(),
            table_name: "Users".to_string(),
            table_type: "delta".to_string(),
            location: "s3://bucket/path".to_string(),
            container: String::new(),
            columns: vec![
                ms::ColumnSchema {
                    name: "Id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                },
                ms::ColumnSchema {
                    name: "Name".to_string(),
                    data_type: "string".to_string(),
                    nullable: true,
                },
            ],
            database_name: "Sales".to_string(),
        }),
    );

    assert_eq!(relation.database, "sales");
    assert_eq!(relation.column_names(), vec!["id", "name"]);
    assert_eq!(relation.location.as_deref(), Some("s3://bucket/path"));
}

#[tokio::test]
async fn builds_session_context_with_kionas_providers() {
    let relation = KionasRelationMetadata {
        database: "db1".to_string(),
        schema: "sc1".to_string(),
        table: "tbl1".to_string(),
        location: None,
        columns: vec![super::relation_metadata::KionasColumnMetadata {
            name: "c1".to_string(),
            data_type: "bigint".to_string(),
            nullable: true,
        }],
    };

    let context =
        build_session_context_with_kionas_providers(&[relation]).expect("provider context");
    let state = context.state();
    let plan = state
        .create_logical_plan("SELECT c1 FROM db1.sc1.tbl1")
        .await
        .expect("logical plan should resolve table provider");

    assert!(
        plan.display_indent_schema()
            .to_string()
            .contains("db1.sc1.tbl1")
    );
}
