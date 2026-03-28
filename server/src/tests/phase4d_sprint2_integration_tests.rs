use super::translate_datafusion_to_kionas_physical_plan_with_providers;
use crate::parser::sql::parse_query;
use crate::providers::KionasRelationMetadata;
use crate::services::metastore_client::metastore_service as ms;
use kionas::sql::query_model::build_select_query_model;

fn customers_relation_metadata(
    database: &str,
    schema: &str,
    table: &str,
) -> KionasRelationMetadata {
    KionasRelationMetadata::from_metastore(
        database,
        schema,
        table,
        Some(ms::TableMetadata {
            uuid: format!("{}-{}-{}", database, schema, table),
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            table_type: "delta".to_string(),
            location: String::new(),
            container: String::new(),
            columns: vec![
                ms::ColumnSchema {
                    name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                },
                ms::ColumnSchema {
                    name: "name".to_string(),
                    data_type: "varchar".to_string(),
                    nullable: true,
                },
            ],
            database_name: database.to_string(),
        }),
    )
}

fn orders_relation_metadata(database: &str, schema: &str, table: &str) -> KionasRelationMetadata {
    KionasRelationMetadata::from_metastore(
        database,
        schema,
        table,
        Some(ms::TableMetadata {
            uuid: format!("{}-{}-{}", database, schema, table),
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            table_type: "delta".to_string(),
            location: String::new(),
            container: String::new(),
            columns: vec![
                ms::ColumnSchema {
                    name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                },
                ms::ColumnSchema {
                    name: "customer_id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                },
                ms::ColumnSchema {
                    name: "product_id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                },
                ms::ColumnSchema {
                    name: "quantity".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                },
            ],
            database_name: database.to_string(),
        }),
    )
}

fn products_relation_metadata(database: &str, schema: &str, table: &str) -> KionasRelationMetadata {
    KionasRelationMetadata::from_metastore(
        database,
        schema,
        table,
        Some(ms::TableMetadata {
            uuid: format!("{}-{}-{}", database, schema, table),
            schema_name: schema.to_string(),
            table_name: table.to_string(),
            table_type: "delta".to_string(),
            location: String::new(),
            container: String::new(),
            columns: vec![
                ms::ColumnSchema {
                    name: "id".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: false,
                },
                ms::ColumnSchema {
                    name: "name".to_string(),
                    data_type: "varchar".to_string(),
                    nullable: true,
                },
            ],
            database_name: database.to_string(),
        }),
    )
}

#[tokio::test]
async fn phase4d_sprint2_cte_alias_outer_join_resolves_to_source_column() {
    let customers = customers_relation_metadata("bench4", "seed1", "customers");
    let orders = orders_relation_metadata("bench4", "seed1", "orders");
    let products = products_relation_metadata("bench4", "seed1", "products");

    let statements = parse_query(
        "WITH customer_orders AS (SELECT c.id, o.quantity, c.name, o.product_id AS ppid2 FROM bench4.seed1.customers c JOIN bench4.seed1.orders o ON c.id = o.customer_id WHERE c.id = 700) SELECT * FROM customer_orders co JOIN bench4.seed1.products p ON p.id = co.ppid2",
    )
    .expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "bench4", "seed1")
        .expect("query model should build")
        .model;

    let translated = translate_datafusion_to_kionas_physical_plan_with_providers(
        &model,
        &[customers, orders, products],
    )
    .await
    .expect("cte alias join translation should succeed");

    let join_keys = translated
        .operators
        .iter()
        .filter_map(|op| match op {
            kionas::planner::PhysicalOperator::HashJoin { spec } => Some(spec.keys.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(
        join_keys.iter().flatten().any(|key| {
            (key.left == "product_id" && key.right == "id")
                || (key.left == "id" && key.right == "product_id")
        }),
        "outer join should resolve CTE alias to source column before join execution"
    );
}

#[tokio::test]
async fn phase4d_sprint2_cte_unqualified_alias_outer_join_resolves_to_source_column() {
    let customers = customers_relation_metadata("bench4", "seed1", "customers");
    let orders = orders_relation_metadata("bench4", "seed1", "orders");
    let products = products_relation_metadata("bench4", "seed1", "products");

    let statements = parse_query(
        "WITH customer_orders AS (SELECT c.id, o.quantity, c.name, o.product_id AS ppid2 FROM bench4.seed1.customers c JOIN bench4.seed1.orders o ON c.id = o.customer_id WHERE c.id = 700) SELECT * FROM customer_orders co JOIN bench4.seed1.products p ON p.id = ppid2",
    )
    .expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "bench4", "seed1")
        .expect("query model should build")
        .model;

    let translated = translate_datafusion_to_kionas_physical_plan_with_providers(
        &model,
        &[customers, orders, products],
    )
    .await
    .expect("unqualified alias join translation should succeed");

    let join_keys = translated
        .operators
        .iter()
        .filter_map(|op| match op {
            kionas::planner::PhysicalOperator::HashJoin { spec } => Some(spec.keys.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert!(
        join_keys.iter().flatten().any(|key| {
            (key.left == "product_id" && key.right == "id")
                || (key.left == "id" && key.right == "product_id")
        }),
        "unqualified CTE alias should resolve to source column before join execution"
    );
}
