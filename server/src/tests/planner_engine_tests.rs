use super::{
    DataFusionQueryPlanner, build_datafusion_plan_artifacts_with_providers,
    is_fallback_eligible_datafusion_error,
    translate_datafusion_to_kionas_physical_plan_with_providers,
};
use crate::parser::sql::parse_query;
use crate::providers::KionasRelationMetadata;
use crate::services::metastore_client::metastore_service as ms;
use kionas::sql::query_model::build_select_query_model;
use std::collections::BTreeSet;

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
                    nullable: false,
                },
            ],
            database_name: database.to_string(),
        }),
    )
}

fn relation_metadata_with_location(
    database: &str,
    schema: &str,
    table: &str,
    location: &str,
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
            location: location.to_string(),
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
                    name: "name".to_string(),
                    data_type: "varchar".to_string(),
                    nullable: true,
                },
                ms::ColumnSchema {
                    name: "quantity".to_string(),
                    data_type: "bigint".to_string(),
                    nullable: true,
                },
            ],
            database_name: database.to_string(),
        }),
    )
}

#[test]
fn fallback_error_classifier_accepts_object_store_resolution_failures() {
    let object_store_error = "Internal error: No suitable object store found for s3://warehouse/path. See RuntimeEnv::register_object_store";
    assert!(is_fallback_eligible_datafusion_error(object_store_error));
}

#[test]
fn fallback_error_classifier_accepts_missing_table_failures() {
    let table_missing_error = "table 'abc.schema1.table1' not found";
    assert!(is_fallback_eligible_datafusion_error(table_missing_error));
}

#[test]
fn fallback_error_classifier_rejects_unrelated_errors() {
    let unrelated_error =
        "aggregate translation mismatch: datafusion_has_aggregate=true model_has_aggregate=false";
    assert!(!is_fallback_eligible_datafusion_error(unrelated_error));
}

#[tokio::test]
async fn plan_artifacts_include_stage_extraction_diagnostics() {
    let relation = relation_metadata("sales", "public", "users");

    let artifacts = build_datafusion_plan_artifacts_with_providers(
        "SELECT id FROM sales.public.users",
        &[relation],
    )
    .await
    .expect("artifacts should build with providers");

    assert!(artifacts.stage_extraction.stage_count >= 1);
    assert_eq!(
        artifacts.stage_extraction.stage_count,
        artifacts.stage_extraction.stages.len()
    );
    assert_eq!(artifacts.stage_extraction.stages[0].stage_id, 0);
    assert!(!artifacts.stage_extraction.stages[0].node_names.is_empty());
}

#[tokio::test]
async fn stage_extraction_diagnostics_form_valid_dependency_graph_for_join_query() {
    let users = relation_metadata("sales", "public", "users");
    let orders = relation_metadata("sales", "public", "orders");

    let artifacts = build_datafusion_plan_artifacts_with_providers(
        "SELECT u.id FROM sales.public.users u JOIN sales.public.orders o ON u.id = o.customer_id",
        &[users, orders],
    )
    .await
    .expect("artifacts should build for join query");

    assert_eq!(
        artifacts.stage_extraction.stage_count,
        artifacts.stage_extraction.stages.len()
    );
    assert!(artifacts.stage_extraction.stage_count >= 1);

    let stage_ids = artifacts
        .stage_extraction
        .stages
        .iter()
        .map(|stage| stage.stage_id)
        .collect::<Vec<_>>();
    let unique_stage_ids = stage_ids.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(unique_stage_ids.len(), stage_ids.len());

    for stage in &artifacts.stage_extraction.stages {
        for input_stage_id in &stage.input_stage_ids {
            assert!(
                unique_stage_ids.contains(input_stage_id),
                "input stage id {} must reference known stage ids {:?}",
                input_stage_id,
                unique_stage_ids
            );
            assert_ne!(
                *input_stage_id, stage.stage_id,
                "stage {} must not depend on itself",
                stage.stage_id
            );
        }
    }
}

#[tokio::test]
async fn stage_extraction_partitioning_metadata_is_mappable_for_dispatch() {
    let relation = relation_metadata("sales", "public", "users");

    let artifacts = build_datafusion_plan_artifacts_with_providers(
        "SELECT id, COUNT(*) FROM sales.public.users GROUP BY id",
        &[relation],
    )
    .await
    .expect("artifacts should build for aggregate query");

    let allowed_partitioning = ["single", "round_robin_batch", "hash", "unknown"];
    for stage in &artifacts.stage_extraction.stages {
        assert!(
            allowed_partitioning.contains(&stage.output_partitioning.as_str()),
            "unexpected output partitioning kind: {}",
            stage.output_partitioning
        );
        assert!(stage.partitions_out >= 1);

        if stage.output_partitioning == "hash" {
            assert!(
                !stage.output_partitioning_keys.is_empty(),
                "hash partitioned stage {} must include output partitioning keys",
                stage.stage_id
            );
        }
    }
}

#[tokio::test]
async fn planner_wrapper_build_plan_artifacts_matches_direct_function() {
    let relation = relation_metadata("sales", "public", "users");
    let planner = DataFusionQueryPlanner::new();

    let wrapped = planner
        .build_plan_artifacts(
            "SELECT id FROM sales.public.users",
            std::slice::from_ref(&relation),
        )
        .await
        .expect("wrapper planning should succeed");
    let direct = build_datafusion_plan_artifacts_with_providers(
        "SELECT id FROM sales.public.users",
        &[relation],
    )
    .await
    .expect("direct planning should succeed");

    assert_eq!(wrapped.logical_plan_text, direct.logical_plan_text);
    assert_eq!(
        wrapped.optimized_logical_plan_text,
        direct.optimized_logical_plan_text
    );
    assert_eq!(wrapped.physical_plan_text, direct.physical_plan_text);
    assert_eq!(wrapped.stage_extraction, direct.stage_extraction);
}

#[tokio::test]
async fn planner_wrapper_translate_to_kionas_plan_matches_direct_function() {
    let relation = relation_metadata("sales", "public", "users");
    let planner = DataFusionQueryPlanner::new();

    let statements =
        parse_query("SELECT id FROM sales.public.users WHERE id > 1").expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "sales", "public")
        .expect("query model should build")
        .model;

    let wrapped = planner
        .translate_to_kionas_plan(&model, std::slice::from_ref(&relation))
        .await
        .expect("wrapper translation should succeed");
    let direct = translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[relation])
        .await
        .expect("direct translation should succeed");

    assert_eq!(wrapped, direct);
}

#[tokio::test]
async fn translates_union_all_query_to_union_operator() {
    let users = relation_metadata("sales", "public", "users");
    let users_ca = relation_metadata("sales", "public", "users_ca");
    let users_mx = relation_metadata("sales", "public", "users_mx");

    let statements = parse_query(
        "SELECT id FROM sales.public.users UNION ALL SELECT id FROM sales.public.users_ca UNION ALL SELECT id FROM sales.public.users_mx",
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

    let plan = translate_datafusion_to_kionas_physical_plan_with_providers(
        &model,
        &[users, users_ca, users_mx],
    )
    .await
    .expect("union translation should succeed");

    let union = plan
        .operators
        .iter()
        .find_map(|op| match op {
            kionas::planner::PhysicalOperator::Union { operands, distinct } => {
                Some((operands, distinct))
            }
            _ => None,
        })
        .expect("translated plan should include union operator");

    assert_eq!(union.0.len(), 3);
    assert!(!(*union.1));
    assert!(union.0[0].filter.is_none());
    assert!(union.0[1].filter.is_none());
    assert!(union.0[2].filter.is_none());
}

#[tokio::test]
async fn translates_union_with_operand_where_filters() {
    let customers = relation_metadata("bench", "seed1", "customers");

    let statements = parse_query(
        "SELECT id FROM bench.seed1.customers WHERE id <= 5 UNION ALL SELECT id FROM bench.seed1.customers WHERE id > 5 AND id <= 10 ORDER BY id",
    )
    .expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "bench", "seed1")
        .expect("query model should build")
        .model;

    let plan = translate_datafusion_to_kionas_physical_plan_with_providers(
        &model,
        std::slice::from_ref(&customers),
    )
    .await
    .expect("union translation should succeed");

    let operands = plan
        .operators
        .iter()
        .find_map(|op| match op {
            kionas::planner::PhysicalOperator::Union { operands, .. } => Some(operands),
            _ => None,
        })
        .expect("translated plan should include union operator");

    assert_eq!(operands.len(), 2);
    assert!(operands[0].filter.is_some());
    assert!(operands[1].filter.is_some());
}

#[tokio::test]
async fn translates_cte_join_query_via_object_store_fallback_without_join_mismatch() {
    let customers = relation_metadata_with_location(
        "bench4",
        "seed1",
        "customers",
        "s3://warehouse/databases/bench4/schemas/seed1/tables/customers",
    );
    let orders = relation_metadata_with_location(
        "bench4",
        "seed1",
        "orders",
        "s3://warehouse/databases/bench4/schemas/seed1/tables/orders",
    );

    let statements = parse_query(
        "WITH customer_orders AS (SELECT c.name, o.quantity FROM bench4.seed1.customers c JOIN bench4.seed1.orders o ON c.id = o.customer_id WHERE c.name = 'Alice Clark') SELECT sum(quantity) FROM customer_orders",
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

    let plan =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[customers, orders])
            .await
            .expect("cte join query should translate without join mismatch");

    assert!(
        plan.operators
            .iter()
            .any(|op| matches!(op, kionas::planner::PhysicalOperator::HashJoin { .. }))
    );
    assert!(plan.operators.iter().any(|op| matches!(
        op,
        kionas::planner::PhysicalOperator::AggregatePartial { .. }
    )));
}
