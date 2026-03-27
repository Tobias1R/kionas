use super::translate_datafusion_to_kionas_physical_plan_with_providers;
use crate::parser::sql::parse_query;
use crate::providers::KionasRelationMetadata;
use crate::services::metastore_client::metastore_service as ms;
use kionas::sql::query_model::build_select_query_model;

fn relation_metadata(database: &str, schema: &str, table: &str) -> KionasRelationMetadata {
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
                    nullable: true,
                },
            ],
            database_name: database.to_string(),
        }),
    )
}

#[tokio::test]
async fn phase4d_sprint1_translates_complex_where_tree() {
    let relation = relation_metadata("sales", "public", "users");

    let statements = parse_query(
        "SELECT id FROM sales.public.users WHERE (id = 1 AND customer_id > 2) OR id BETWEEN 10 AND 20",
    )
    .expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "sales", "public")
        .expect("query model should build")
        .model;

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[relation])
            .await
            .expect("translation should succeed");

    let filter_predicate = translated
        .operators
        .iter()
        .find_map(|op| match op {
            kionas::planner::PhysicalOperator::Filter { predicate } => Some(predicate),
            _ => None,
        })
        .expect("translated plan should include filter operator");

    match filter_predicate {
        kionas::planner::PhysicalExpr::Predicate { predicate } => match predicate {
            kionas::planner::PredicateExpr::Or { clauses } => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(
                    clauses[0],
                    kionas::planner::PredicateExpr::And { .. }
                ));
                assert!(matches!(
                    clauses[1],
                    kionas::planner::PredicateExpr::Between { .. }
                ));
            }
            _ => panic!("expected OR predicate for complex WHERE"),
        },
        _ => panic!("filter must carry structured predicate"),
    }
}

#[tokio::test]
async fn phase4d_sprint1_translates_not_and_or_null_predicates() {
    let relation = relation_metadata("sales", "public", "users");

    let statements = parse_query(
        "SELECT id FROM sales.public.users WHERE NOT (customer_id IS NULL) AND (id = 3 OR customer_id = 4)",
    )
    .expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "sales", "public")
        .expect("query model should build")
        .model;

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[relation])
            .await
            .expect("translation should succeed");

    let filter_predicate = translated
        .operators
        .iter()
        .find_map(|op| match op {
            kionas::planner::PhysicalOperator::Filter { predicate } => Some(predicate),
            _ => None,
        })
        .expect("translated plan should include filter operator");

    match filter_predicate {
        kionas::planner::PhysicalExpr::Predicate { predicate } => match predicate {
            kionas::planner::PredicateExpr::And { clauses } => {
                assert_eq!(clauses.len(), 2);
                assert!(matches!(
                    clauses[0],
                    kionas::planner::PredicateExpr::Not { .. }
                ));
                assert!(matches!(
                    clauses[1],
                    kionas::planner::PredicateExpr::Or { .. }
                ));
            }
            _ => panic!("expected AND predicate for NOT/OR composite WHERE"),
        },
        _ => panic!("filter must carry structured predicate"),
    }
}
