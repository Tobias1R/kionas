use kionas::planner::{
    AggregateFunction, PhysicalAggregateExpr, PhysicalAggregateSpec, PhysicalExpr,
    PhysicalSortExpr, PhysicalWindowFrameBound, PhysicalWindowFrameSpec, PhysicalWindowFrameUnit,
    PhysicalWindowFunctionSpec, PhysicalWindowSpec, PlannerError,
};
use kionas::sql::query_model::{QueryFromSpec, SelectQueryModel};

fn normalize_identifier(raw: &str) -> String {
    raw.trim()
        .trim_matches('"')
        .trim_matches('`')
        .trim_matches('[')
        .trim_matches(']')
        .to_ascii_lowercase()
}

pub(crate) fn parse_projection_alias(expr: &str) -> (String, Option<String>) {
    let lower = expr.to_ascii_lowercase();
    if let Some(index) = lower.rfind(" as ") {
        let lhs = expr[..index].trim().to_string();
        let rhs = expr[index + 4..].trim();
        if !rhs.is_empty() {
            return (lhs, Some(normalize_identifier(rhs)));
        }
    }

    (expr.trim().to_string(), None)
}

pub(crate) fn is_simple_identifier_reference(expr: &str) -> bool {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.contains('(')
        || trimmed.contains(')')
        || trimmed.contains('+')
        || trimmed.contains('-')
        || trimmed.contains('*')
        || trimmed.contains('/')
        || trimmed.contains(',')
        || trimmed.contains(':')
        || trimmed.contains(' ')
    {
        return false;
    }

    let parts = trimmed.split('.').collect::<Vec<_>>();
    if parts.is_empty() || parts.len() > 2 {
        return false;
    }

    parts.into_iter().all(|segment| {
        let raw = segment
            .trim()
            .trim_matches('"')
            .trim_matches('`')
            .trim_matches('[')
            .trim_matches(']');

        !raw.is_empty()
            && raw
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    })
}

fn parse_aggregate_expr_from_projection(expr: &str) -> Option<(AggregateFunction, Option<String>)> {
    let (base_expr, _) = parse_projection_alias(expr);
    let trimmed = base_expr.trim();
    let lower = trimmed.to_ascii_lowercase();

    for (name, function) in [
        ("count", AggregateFunction::Count),
        ("sum", AggregateFunction::Sum),
        ("min", AggregateFunction::Min),
        ("max", AggregateFunction::Max),
        ("avg", AggregateFunction::Avg),
    ] {
        let prefix = format!("{}(", name);
        if lower.starts_with(prefix.as_str()) && lower.ends_with(')') {
            let raw_arg = trimmed[prefix.len()..trimmed.len() - 1].trim();
            if matches!(function, AggregateFunction::Count) && raw_arg == "*" {
                return Some((function, None));
            }

            if is_simple_identifier_reference(raw_arg) {
                return Some((function, Some(raw_arg.to_string())));
            }

            return None;
        }
    }

    None
}

fn parse_window_function_from_projection(expr: &str) -> Option<PhysicalWindowFunctionSpec> {
    let (base_expr, alias) = parse_projection_alias(expr);
    let lower = base_expr.to_ascii_lowercase();
    let over_pos = lower.find(" over (")?;

    let func_expr = base_expr[..over_pos].trim();
    let open_paren = func_expr.find('(')?;
    let close_paren = func_expr.rfind(')')?;
    if close_paren <= open_paren {
        return None;
    }

    let function_name = func_expr[..open_paren].trim();
    if function_name.is_empty() {
        return None;
    }

    let args_sql = func_expr[open_paren + 1..close_paren].trim();
    let args = if args_sql == "*" || args_sql.is_empty() {
        Vec::new()
    } else {
        args_sql
            .split(',')
            .map(|arg| PhysicalExpr::Raw {
                sql: arg.trim().to_string(),
            })
            .collect::<Vec<_>>()
    };

    let output_name = alias.unwrap_or_else(|| projection_output_name(expr));

    Some(PhysicalWindowFunctionSpec {
        function_name: function_name.to_ascii_uppercase(),
        args,
        output_name,
        frame: None,
    })
}

fn parse_window_partition_and_order_from_projection(
    expr: &str,
) -> Option<(Vec<PhysicalExpr>, Vec<PhysicalSortExpr>)> {
    let (base_expr, _) = parse_projection_alias(expr);
    let lower = base_expr.to_ascii_lowercase();
    let over_pos = lower.find(" over (")?;

    let over_clause = base_expr[over_pos + 6..].trim();
    if !(over_clause.starts_with('(') && over_clause.ends_with(')')) {
        return None;
    }

    let inner = over_clause[1..over_clause.len() - 1].trim();
    let lower_inner = inner.to_ascii_lowercase();

    let partition_pos = lower_inner.find("partition by");
    let order_pos = lower_inner.find("order by");

    let mut partition_by = Vec::<PhysicalExpr>::new();
    let mut order_by = Vec::<PhysicalSortExpr>::new();

    if let Some(p_pos) = partition_pos {
        let p_start = p_pos + "partition by".len();
        let p_end = order_pos.unwrap_or(inner.len());
        let partition_sql = inner[p_start..p_end].trim();
        if !partition_sql.is_empty() {
            partition_by = partition_sql
                .split(',')
                .map(|part| PhysicalExpr::Raw {
                    sql: part.trim().to_string(),
                })
                .collect::<Vec<_>>();
        }
    }

    if let Some(o_pos) = order_pos {
        let o_start = o_pos + "order by".len();
        let order_sql = inner[o_start..].trim();
        if !order_sql.is_empty() {
            order_by = order_sql
                .split(',')
                .map(|item| {
                    let trimmed = item.trim();
                    let lower_item = trimmed.to_ascii_lowercase();
                    if lower_item.ends_with(" desc") {
                        PhysicalSortExpr {
                            expression: PhysicalExpr::Raw {
                                sql: trimmed[..trimmed.len() - 5].trim().to_string(),
                            },
                            ascending: false,
                        }
                    } else if lower_item.ends_with(" asc") {
                        PhysicalSortExpr {
                            expression: PhysicalExpr::Raw {
                                sql: trimmed[..trimmed.len() - 4].trim().to_string(),
                            },
                            ascending: true,
                        }
                    } else {
                        PhysicalSortExpr {
                            expression: PhysicalExpr::Raw {
                                sql: trimmed.to_string(),
                            },
                            ascending: true,
                        }
                    }
                })
                .collect::<Vec<_>>();
        }
    }

    Some((partition_by, order_by))
}

pub(crate) fn build_window_spec_from_model(model: &SelectQueryModel) -> Option<PhysicalWindowSpec> {
    let mut functions = model
        .projection
        .iter()
        .filter_map(|expr| parse_window_function_from_projection(expr))
        .collect::<Vec<_>>();

    if functions.is_empty() {
        return None;
    }

    let (partition_by, order_by) = model
        .projection
        .iter()
        .find_map(|expr| parse_window_partition_and_order_from_projection(expr))
        .unwrap_or_default();

    // For partition-only windows, default aggregate window frame should cover the whole partition.
    if order_by.is_empty() {
        for function in &mut functions {
            match function.function_name.as_str() {
                "COUNT" | "SUM" | "AVG" | "MIN" | "MAX" => {
                    function.frame = Some(PhysicalWindowFrameSpec {
                        unit: PhysicalWindowFrameUnit::Rows,
                        start_bound: PhysicalWindowFrameBound::UnboundedPreceding,
                        end_bound: PhysicalWindowFrameBound::UnboundedFollowing,
                    });
                }
                _ => {}
            }
        }
    }

    Some(PhysicalWindowSpec {
        partition_by,
        order_by,
        functions,
    })
}

pub(crate) fn model_contains_window(model: &SelectQueryModel) -> bool {
    model
        .projection
        .iter()
        .any(|expr| expr.to_ascii_lowercase().contains(" over ("))
}

fn is_simple_projection_reference(expr: &str) -> bool {
    let (base_expr, _) = parse_projection_alias(expr);
    is_simple_identifier_reference(base_expr.as_str())
}

pub(crate) fn should_project_cte_source_columns(
    model: &SelectQueryModel,
    source_model: &SelectQueryModel,
    has_aggregate: bool,
    has_window: bool,
) -> bool {
    if has_aggregate || has_window {
        return false;
    }

    if !matches!(model.from, QueryFromSpec::CteRef { .. }) {
        return false;
    }

    if !model.joins.is_empty() {
        return false;
    }

    if model.projection.len() != 1 || model.projection[0].trim() != "*" {
        return false;
    }

    !source_model.projection.is_empty()
        && source_model
            .projection
            .iter()
            .all(|expr| is_simple_projection_reference(expr))
}

pub(crate) fn projection_output_name(expr: &str) -> String {
    let lower = expr.to_ascii_lowercase();
    if let Some(index) = lower.rfind(" as ") {
        return normalize_identifier(&expr[index + 4..]);
    }

    if let Some((prefix, function)) = [
        ("count(", "count"),
        ("sum(", "sum"),
        ("min(", "min"),
        ("max(", "max"),
        ("avg(", "avg"),
    ]
    .iter()
    .find(|(prefix, _)| lower.starts_with(*prefix) && lower.ends_with(')'))
    {
        let arg = expr[prefix.len()..expr.len() - 1].trim();
        if arg == "*" {
            return (*function).to_string();
        }
        return format!("{}_{}", function, normalize_identifier(arg));
    }

    normalize_identifier(expr)
}

pub(crate) fn build_aggregate_spec_from_model(
    model: &SelectQueryModel,
) -> Result<Option<PhysicalAggregateSpec>, PlannerError> {
    if model.group_by.is_empty()
        && !model
            .projection
            .iter()
            .any(|expr| parse_aggregate_expr_from_projection(expr).is_some())
    {
        return Ok(None);
    }

    let grouping_exprs = model
        .group_by
        .iter()
        .map(|expr| PhysicalExpr::Raw { sql: expr.clone() })
        .collect::<Vec<_>>();

    let mut aggregates = Vec::<PhysicalAggregateExpr>::new();
    for expr in &model.projection {
        if let Some((function, input)) = parse_aggregate_expr_from_projection(expr) {
            let (_, alias) = parse_projection_alias(expr);
            let output_name = alias.unwrap_or_else(|| projection_output_name(expr));
            aggregates.push(PhysicalAggregateExpr {
                function,
                input: input.map(|column| PhysicalExpr::Raw { sql: column }),
                output_name,
            });
        }
    }

    if aggregates.is_empty() {
        return Ok(None);
    }

    Ok(Some(PhysicalAggregateSpec {
        grouping_exprs,
        aggregates,
    }))
}

pub(crate) fn model_contains_aggregate(model: &SelectQueryModel) -> bool {
    !model.group_by.is_empty()
        || model
            .projection
            .iter()
            .any(|expr| parse_aggregate_expr_from_projection(expr).is_some())
}
