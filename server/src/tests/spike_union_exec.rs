/// Spike test: Understand DataFusion 52.2.0 UnionExec structure in detail
/// Goal: Align our N-ary UNION model 100% with DataFusion's representation
#[cfg(test)]
mod union_exec_spike {
    use datafusion::physical_plan::ExecutionPlanProperties;
    use datafusion::prelude::*;

    /// Test 1: Examine UnionExec structure for binary UNION
    #[tokio::test]
    async fn test_datafusion_binary_union_structure() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();

        let sql = "SELECT 1 as x, 'a' as y UNION ALL SELECT 2 as x, 'b' as y";

        println!("\n=== SPIKE: Binary UNION Structure ===\n");
        println!("SQL:\n{}\n", sql);

        // Get logical plan
        let logical = ctx.state().create_logical_plan(sql).await?;
        println!("LOGICAL PLAN TYPE: {:#?}\n", logical);

        // Get optimized
        let optimized = ctx.state().optimize(&logical)?;
        println!("OPTIMIZED LOGICAL: {:#?}\n", optimized);

        // Get physical
        let physical = ctx.state().create_physical_plan(&optimized).await?;

        println!("PHYSICAL PLAN TYPE (name): {}", physical.name());
        println!("Number of children: {}", physical.children().len());
        println!("Schema: {:?}\n", physical.schema());

        // Print detailed tree
        print_detailed_plan_tree(&physical, 0);

        Ok(())
    }

    /// Test 2: Examine N-ary UNION (3 inputs)
    #[tokio::test]
    async fn test_datafusion_ternary_union_structure() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();

        let sql = "SELECT 1 as id UNION ALL SELECT 2 as id UNION ALL SELECT 3 as id";

        println!("\n=== SPIKE: Ternary (3-way) UNION Structure ===\n");
        println!("SQL:\n{}\n", sql);

        let logical = ctx.state().create_logical_plan(sql).await?;
        println!("LOGICAL PLAN:\n{:#?}\n", logical);

        let optimized = ctx.state().optimize(&logical)?;
        let physical = ctx.state().create_physical_plan(&optimized).await?;

        println!("PHYSICAL PLAN TYPE: {}", physical.name());
        println!("Number of children: {}", physical.children().len());

        // Key question: Does DataFusion flatten nested UNIONs or keep them nested?
        print_detailed_plan_tree(&physical, 0);

        Ok(())
    }

    /// Test 3: UNION with schema mismatch handling
    #[tokio::test]
    async fn test_datafusion_union_schema_coercion() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();

        // Different types that need coercion
        let sql = "SELECT 1 as x, 'hello' as s UNION ALL SELECT 2, 'world'";

        println!("\n=== SPIKE: Schema Coercion in UNION ===\n");

        let logical = ctx.state().create_logical_plan(sql).await?;
        let optimized = ctx.state().optimize(&logical)?;
        let physical = ctx.state().create_physical_plan(&optimized).await?;

        println!("Schema: {:?}", physical.schema());
        println!("Schema fields:");
        for (i, field) in physical.schema().fields().iter().enumerate() {
            println!("  [{}] {} : {}", i, field.name(), field.data_type());
        }

        Ok(())
    }

    /// Test 4: UNION DISTINCT vs UNION ALL
    #[tokio::test]
    async fn test_datafusion_union_distinct_vs_all() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();

        println!("\n=== SPIKE: UNION ALL ===");
        let sql_all = "SELECT 1 as x UNION ALL SELECT 1 as x";
        let phys_all = ctx
            .state()
            .create_physical_plan(
                &ctx.state()
                    .optimize(&ctx.state().create_logical_plan(sql_all).await?)?,
            )
            .await?;
        println!("Type: {}", phys_all.name());
        println!("Children: {}\n", phys_all.children().len());

        println!("=== SPIKE: UNION (default = DISTINCT) ===");
        let sql_distinct = "SELECT 1 as x UNION SELECT 1 as x";
        let phys_distinct = ctx
            .state()
            .create_physical_plan(
                &ctx.state()
                    .optimize(&ctx.state().create_logical_plan(sql_distinct).await?)?,
            )
            .await?;
        println!("Type: {}", phys_distinct.name());
        println!("Children: {}", phys_distinct.children().len());

        // Print both plans side-by-side
        println!("\nALL structure:");
        print_detailed_plan_tree(&phys_all, 1);
        println!("\nDISTINCT structure:");
        print_detailed_plan_tree(&phys_distinct, 1);

        Ok(())
    }

    /// Test 5: How does a VALUES clause work?
    #[tokio::test]
    async fn test_datafusion_values_clause() -> datafusion::error::Result<()> {
        let ctx = SessionContext::new();

        let sql = "VALUES (1, 'a'), (2, 'b'), (3, 'c')";

        println!("\n=== SPIKE: VALUES Clause ===");
        println!("SQL: {}\n", sql);

        let logical = ctx.state().create_logical_plan(sql).await?;
        println!("LOGICAL:\n{:#?}\n", logical);

        let physical = ctx
            .state()
            .create_physical_plan(&ctx.state().optimize(&logical)?)
            .await?;

        println!("PHYSICAL TYPE: {}", physical.name());
        println!("Schema: {:?}\n", physical.schema());

        Ok(())
    }

    /// Helper: Pretty-print physical plan tree
    fn print_detailed_plan_tree(
        plan: &std::sync::Arc<dyn datafusion::physical_plan::ExecutionPlan>,
        depth: usize,
    ) {
        let indent = "  ".repeat(depth);
        println!("{}├─ Type: {}", indent, plan.name());
        println!(
            "{}│  Schema fields: {}",
            indent,
            plan.schema().fields().len()
        );

        for field in plan.schema().fields().iter().take(3) {
            println!("{}│    - {}: {}", indent, field.name(), field.data_type());
        }
        if plan.schema().fields().len() > 3 {
            println!(
                "{}│    ... ({} more)",
                indent,
                plan.schema().fields().len() - 3
            );
        }

        println!("{}│  Properties:", indent);
        println!(
            "{}│    Output partitioning: {:?}",
            indent,
            plan.output_partitioning()
        );
        println!(
            "{}│    Output ordering: {:?}",
            indent,
            plan.output_ordering()
        );

        let children = plan.children();
        println!("{}│  Children: {}", indent, children.len());

        for (i, child) in children.iter().enumerate() {
            let is_last = i == children.len() - 1;
            let prefix = if is_last { "└" } else { "├" };
            println!("{}│", indent);
            println!("{}{} Child {}:", indent, prefix, i);
            print_detailed_plan_tree(child, depth + 2);
        }
    }
}
