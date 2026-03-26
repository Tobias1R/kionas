use super::derive_table_uri_from_storage;
use serde_json::json;

#[test]
fn derive_insert_uri_uses_canonical_three_part_layout() {
    let storage = json!({ "bucket": "warehouse" });
    let uri = derive_table_uri_from_storage(&storage, "testdb_4.testschema_2.testtable_2")
        .expect("uri must be derived");
    assert_eq!(
        uri,
        "s3://warehouse/databases/testdb_4/schemas/testschema_2/tables/testtable_2"
    );
}

#[test]
fn derive_insert_uri_normalizes_quoted_three_part_layout() {
    let storage = json!({ "bucket": "warehouse" });
    let uri = derive_table_uri_from_storage(&storage, "\"TestDB\".\"Schema\".\"Table\"")
        .expect("uri must be derived");
    assert_eq!(
        uri,
        "s3://warehouse/databases/testdb/schemas/schema/tables/table"
    );
}
