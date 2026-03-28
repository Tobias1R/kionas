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
async fn translates_window_query_to_window_operator() {
    let relation = relation_metadata("sales", "public", "users");

    let statements = parse_query(
        "SELECT id, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY id) AS rn FROM sales.public.users",
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
            .expect("window query translation should succeed");

    assert!(
        translated
            .operators
            .iter()
            .any(|op| matches!(op, kionas::planner::PhysicalOperator::WindowAggr { .. })),
        "translated plan must include WindowAggr operator"
    );
}

#[tokio::test]
async fn translates_cte_row_number_query_without_global_sort_mismatch() {
    let relation = orders_relation_metadata("bench4", "seed1", "orders");

    let statements = parse_query(
        "WITH ranked_orders AS (SELECT o.*, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY quantity DESC) AS rn FROM bench4.seed1.orders o) SELECT * FROM ranked_orders WHERE rn <= 3",
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

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[relation])
            .await
            .expect("cte row_number query translation should succeed");

    assert!(
        translated
            .operators
            .iter()
            .any(|op| matches!(op, kionas::planner::PhysicalOperator::WindowAggr { .. })),
        "translated plan must include WindowAggr operator"
    );
}

#[tokio::test]
async fn translates_cte_rank_query_with_upstream_aggregate_for_window_sort_column() {
    let relation = orders_relation_metadata("bench4", "seed1", "orders");

    let statements = parse_query(
        "WITH product_totals AS (SELECT product_id, SUM(quantity) AS total_quantity FROM bench4.seed1.orders GROUP BY product_id), ranked_products AS (SELECT pt.*, RANK() OVER (ORDER BY total_quantity DESC) AS rnk FROM product_totals pt) SELECT * FROM ranked_products WHERE rnk <= 5",
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

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[relation])
            .await
            .expect("cte rank query translation should succeed");

    let aggregate_count = translated
        .operators
        .iter()
        .filter(|op| {
            matches!(
                op,
                kionas::planner::PhysicalOperator::AggregatePartial { .. }
                    | kionas::planner::PhysicalOperator::AggregateFinal { .. }
            )
        })
        .count();
    assert!(
        aggregate_count >= 2,
        "translated plan should include aggregate operators producing total_quantity before ranking"
    );

    assert!(
        translated
            .operators
            .iter()
            .any(|op| matches!(op, kionas::planner::PhysicalOperator::WindowAggr { .. })),
        "translated plan should include WindowAggr operator"
    );
}

#[tokio::test]
async fn translates_cte_window_aggregate_query_without_missing_aggregate_spec_failure() {
    let relation = orders_relation_metadata("bench4", "seed1", "orders");

    let statements = parse_query(
        "WITH order_stats AS (SELECT o.*, COUNT(*) OVER (PARTITION BY customer_id) AS order_count, AVG(quantity) OVER (PARTITION BY customer_id) AS avg_quantity, SUM(quantity) OVER (PARTITION BY customer_id) AS total_quantity, MIN(quantity) OVER (PARTITION BY customer_id) AS min_quantity, MAX(quantity) OVER (PARTITION BY customer_id) AS max_quantity FROM bench4.seed1.orders o) SELECT * FROM order_stats WHERE customer_id = 700",
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

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[relation])
            .await
            .expect("cte window aggregate query translation should succeed");

    assert!(
        translated
            .operators
            .iter()
            .any(|op| matches!(op, kionas::planner::PhysicalOperator::WindowAggr { .. })),
        "translated plan must include WindowAggr operator"
    );
}

#[tokio::test]
async fn translates_cte_dense_rank_query_with_upstream_aggregate_for_window_sort_column() {
    let relation = orders_relation_metadata("bench4", "seed1", "orders");

    let statements = parse_query(
        "WITH customer_totals AS (SELECT customer_id, SUM(quantity) AS total_quantity FROM bench4.seed1.orders GROUP BY customer_id), ranked_customers AS (SELECT ct.*, DENSE_RANK() OVER (ORDER BY total_quantity DESC) AS drnk FROM customer_totals ct) SELECT * FROM ranked_customers WHERE drnk <= 5",
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

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[relation])
            .await
            .expect("cte dense_rank query translation should succeed");

    assert!(
        translated
            .operators
            .iter()
            .any(|op| matches!(op, kionas::planner::PhysicalOperator::AggregateFinal { .. })),
        "translated plan should include aggregate operators producing total_quantity before dense rank"
    );

    assert!(
        translated
            .operators
            .iter()
            .any(|op| matches!(op, kionas::planner::PhysicalOperator::WindowAggr { .. })),
        "translated plan should include WindowAggr operator"
    );
}

#[tokio::test]
async fn translates_cte_with_outer_join_emits_both_join_operators() {
    let customers = customers_relation_metadata("bench4", "seed1", "customers");
    let orders = orders_relation_metadata("bench4", "seed1", "orders");
    let products = products_relation_metadata("bench4", "seed1", "products");

    let statements = parse_query(
        "WITH customer_orders AS (SELECT c.id, o.quantity, c.name, o.product_id FROM bench4.seed1.customers c JOIN bench4.seed1.orders o ON c.id = o.customer_id WHERE c.id = 700) SELECT co.*, p.name FROM customer_orders co JOIN bench4.seed1.products p ON co.product_id = p.id",
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
    .expect("cte plus outer join query translation should succeed");

    let join_count = translated
        .operators
        .iter()
        .filter(|op| matches!(op, kionas::planner::PhysicalOperator::HashJoin { .. }))
        .count();

    assert_eq!(
        join_count, 2,
        "translated plan should include both CTE-internal and outer join operators"
    );
}

#[tokio::test]
async fn translates_pure_cte_select_star_with_cte_projection_columns_only() {
    let customers = customers_relation_metadata("bench4", "seed1", "customers");
    let orders = orders_relation_metadata("bench4", "seed1", "orders");

    let statements = parse_query(
        "WITH customer_orders1 AS (SELECT c.id, o.quantity, c.name, o.product_id FROM bench4.seed1.customers c JOIN bench4.seed1.orders o ON c.id = o.customer_id WHERE c.id = 700) SELECT * FROM customer_orders1 co",
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

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[customers, orders])
            .await
            .expect("pure cte select-star query translation should succeed");

    let projection_exprs = translated
        .operators
        .iter()
        .find_map(|op| match op {
            kionas::planner::PhysicalOperator::Projection { expressions } => Some(expressions),
            _ => None,
        })
        .expect("translated plan should include projection operator");

    assert_eq!(projection_exprs.len(), 4);
    assert!(
        projection_exprs.iter().all(|expr| {
            !matches!(expr, kionas::planner::PhysicalExpr::Raw { sql } if sql.trim() == "*")
        }),
        "projection should expand to CTE-selected columns instead of raw '*'"
    );
}

#[tokio::test]
async fn resolves_outer_join_key_from_cte_alias_to_source_column() {
    let customers = customers_relation_metadata("bench4", "seed1", "customers");
    let orders = orders_relation_metadata("bench4", "seed1", "orders");
    let products = products_relation_metadata("bench4", "seed1", "products");

    let statements = parse_query(
        "WITH customer_orders AS (SELECT c.id, o.quantity, c.name, o.product_id AS ppid FROM bench4.seed1.customers c JOIN bench4.seed1.orders o ON c.id = o.customer_id WHERE c.id = 700) SELECT * FROM customer_orders co JOIN bench4.seed1.products p ON co.ppid = p.id",
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
    .expect("cte alias join query translation should succeed");

    let join_keys = translated
        .operators
        .iter()
        .filter_map(|op| match op {
            kionas::planner::PhysicalOperator::HashJoin { spec } => Some(spec.keys.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        join_keys.len(),
        2,
        "translated plan should include both CTE-internal and outer hash joins"
    );

    assert!(
        join_keys
            .iter()
            .flatten()
            .any(|key| key.left == "product_id" && key.right == "id"),
        "outer join key should resolve to source column so join execution can read pre-projection schema"
    );
}

#[tokio::test]
async fn resolves_unqualified_outer_join_key_from_cte_alias_to_source_column() {
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
    .expect("cte alias join query translation should succeed");

    let join_keys = translated
        .operators
        .iter()
        .filter_map(|op| match op {
            kionas::planner::PhysicalOperator::HashJoin { spec } => Some(spec.keys.clone()),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        join_keys.len(),
        2,
        "translated plan should include both CTE-internal and outer hash joins"
    );

    assert!(
        join_keys.iter().flatten().any(|key| {
            (key.left == "product_id" && key.right == "id")
                || (key.left == "id" && key.right == "product_id")
        }),
        "unqualified alias should resolve to source column in outer join key"
    );
}

#[tokio::test]
async fn routes_theta_join_to_nested_loop_operator() {
    let customers = customers_relation_metadata("bench4", "seed1", "customers");
    let orders = orders_relation_metadata("bench4", "seed1", "orders");

    let statements =
        parse_query("SELECT c.id FROM bench4.seed1.customers c JOIN bench4.seed1.orders o ON c.id < o.customer_id")
            .expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "bench4", "seed1")
        .expect("query model should build")
        .model;

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[customers, orders])
            .await
            .expect("theta join translation should succeed");

    let nested_loop = translated.operators.iter().find_map(|op| match op {
        kionas::planner::PhysicalOperator::NestedLoopJoin { spec } => Some(spec),
        _ => None,
    });

    let nested_loop = nested_loop.expect("translated plan should include NestedLoopJoin");
    assert!(!nested_loop.predicates.is_empty());
}

#[tokio::test]
async fn routes_cross_join_to_nested_loop_operator_without_predicates() {
    let customers = customers_relation_metadata("bench4", "seed1", "customers");
    let orders = orders_relation_metadata("bench4", "seed1", "orders");

    let statements =
        parse_query("SELECT c.id FROM bench4.seed1.customers c CROSS JOIN bench4.seed1.orders o")
            .expect("sql should parse");
    let statement = statements.first().expect("statement expected");
    let query = match statement {
        crate::parser::datafusion_sql::sqlparser::ast::Statement::Query(query) => query,
        _ => panic!("expected query statement"),
    };

    let model = build_select_query_model(query, "s1", "bench4", "seed1")
        .expect("query model should build")
        .model;

    let translated =
        translate_datafusion_to_kionas_physical_plan_with_providers(&model, &[customers, orders])
            .await
            .expect("cross join translation should succeed");

    let nested_loop = translated.operators.iter().find_map(|op| match op {
        kionas::planner::PhysicalOperator::NestedLoopJoin { spec } => Some(spec),
        _ => None,
    });

    let nested_loop = nested_loop.expect("translated plan should include NestedLoopJoin");
    assert!(nested_loop.predicates.is_empty());
    assert!(nested_loop.keys.is_empty());
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

#[tokio::test]
async fn translates_or_where_predicate_to_structured_disjunction() {
    let relation = relation_metadata("sales", "public", "users");

    let statements = parse_query("SELECT id FROM sales.public.users WHERE id = 1 OR id = 2")
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
            .expect("or predicate query translation should succeed");

    let filter_predicate = translated
        .operators
        .iter()
        .find_map(|op| match op {
            kionas::planner::PhysicalOperator::Filter { predicate } => Some(predicate),
            _ => None,
        })
        .expect("translated plan should include filter operator");

    match filter_predicate {
        kionas::planner::PhysicalExpr::Predicate { predicate } => {
            assert!(matches!(
                predicate,
                kionas::planner::PredicateExpr::Or { .. }
            ));
        }
        _ => panic!("filter must carry structured predicate"),
    }
}

#[tokio::test]
async fn translates_not_where_predicate_to_structured_negation() {
    let relation = relation_metadata("sales", "public", "users");

    let statements =
        parse_query("SELECT id FROM sales.public.users WHERE NOT (id = 1 AND customer_id = 2)")
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
            .expect("not predicate query translation should succeed");

    let filter_predicate = translated
        .operators
        .iter()
        .find_map(|op| match op {
            kionas::planner::PhysicalOperator::Filter { predicate } => Some(predicate),
            _ => None,
        })
        .expect("translated plan should include filter operator");

    match filter_predicate {
        kionas::planner::PhysicalExpr::Predicate { predicate } => {
            assert!(matches!(
                predicate,
                kionas::planner::PredicateExpr::Not { .. }
            ));
        }
        _ => panic!("filter must carry structured predicate"),
    }
}
