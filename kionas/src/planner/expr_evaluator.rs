use chrono::{Datelike, NaiveDate};
use std::collections::HashMap;

/// What: Runtime scalar value used by expression evaluation.
///
/// Inputs:
/// - Variant payload carries one typed scalar value.
///
/// Output:
/// - Comparable and serializable scalar consumed by planner-side expression logic.
///
/// Details:
/// - `Null` propagates through most operators and functions unless explicitly handled.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    Bool(bool),
    String(String),
    Date(NaiveDate),
    Null,
}

/// What: Type contract for expression value coercion checks.
///
/// Inputs:
/// - Variant selects one canonical scalar family.
///
/// Output:
/// - Expected type for expression planning and validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExprType {
    Int,
    Float,
    Bool,
    String,
    Date,
    Null,
}

/// What: Binary operators supported by expression evaluation.
///
/// Inputs:
/// - Variant identifies one arithmetic or comparison operator.
///
/// Output:
/// - Operator tag interpreted by evaluator dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOperator {
    Add,
    Sub,
    Mul,
    Div,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

/// What: Planner expression tree for scalar evaluation.
///
/// Inputs:
/// - Variant payload contains expression-specific fields.
///
/// Output:
/// - Recursive expression structure evaluable against a row context.
#[derive(Debug, Clone, PartialEq)]
pub enum PhysicalExpr {
    ColumnRef {
        name: String,
    },
    Literal {
        value: Value,
    },
    FunctionCall {
        name: String,
        args: Vec<PhysicalExpr>,
    },
    BinaryOp {
        left: Box<PhysicalExpr>,
        op: BinaryOperator,
        right: Box<PhysicalExpr>,
    },
    CaseExpr {
        when_then_pairs: Vec<(PhysicalExpr, PhysicalExpr)>,
        else_expr: Option<Box<PhysicalExpr>>,
    },
}

/// What: Trait contract for expression evaluation implementations.
///
/// Inputs:
/// - `expr`: Expression tree to evaluate.
/// - `row`: Runtime row values by column name.
/// - `schema`: Runtime column type contract by column name.
///
/// Output:
/// - Evaluated scalar value or descriptive error.
pub trait ExpressionEvaluator {
    fn evaluate_expression(
        &self,
        expr: &PhysicalExpr,
        row: &HashMap<String, Value>,
        schema: &HashMap<String, ExprType>,
    ) -> Result<Value, String>;
}

/// What: Default planner-side expression evaluator.
///
/// Inputs:
/// - None for construction; methods consume expression + row + schema.
///
/// Output:
/// - Deterministic scalar evaluation for supported expression subset.
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultExpressionEvaluator;

impl ExpressionEvaluator for DefaultExpressionEvaluator {
    fn evaluate_expression(
        &self,
        expr: &PhysicalExpr,
        row: &HashMap<String, Value>,
        schema: &HashMap<String, ExprType>,
    ) -> Result<Value, String> {
        match expr {
            PhysicalExpr::ColumnRef { name } => evaluate_column_ref(name, row, schema),
            PhysicalExpr::Literal { value } => Ok(value.clone()),
            PhysicalExpr::FunctionCall { name, args } => {
                let mut evaluated_args = Vec::with_capacity(args.len());
                for arg in args {
                    evaluated_args.push(self.evaluate_expression(arg, row, schema)?);
                }
                evaluate_builtin_function(name, &evaluated_args)
            }
            PhysicalExpr::BinaryOp { left, op, right } => {
                let left_value = self.evaluate_expression(left, row, schema)?;
                let right_value = self.evaluate_expression(right, row, schema)?;
                evaluate_binary_op(&left_value, *op, &right_value)
            }
            PhysicalExpr::CaseExpr {
                when_then_pairs,
                else_expr,
            } => {
                for (when_expr, then_expr) in when_then_pairs {
                    let when_value = self.evaluate_expression(when_expr, row, schema)?;
                    if as_bool(&when_value)? {
                        return self.evaluate_expression(then_expr, row, schema);
                    }
                }

                if let Some(fallback) = else_expr {
                    return self.evaluate_expression(fallback, row, schema);
                }

                Ok(Value::Null)
            }
        }
    }
}

/// What: Evaluate one planner expression against a row context.
///
/// Inputs:
/// - `expr`: Expression tree to evaluate.
/// - `row`: Runtime row values by column name.
/// - `schema`: Runtime column type contract by column name.
///
/// Output:
/// - Evaluated scalar value or descriptive error.
///
/// Details:
/// - This helper uses `DefaultExpressionEvaluator` for ergonomic function-style calls.
pub fn evaluate_expression(
    expr: &PhysicalExpr,
    row: &HashMap<String, Value>,
    schema: &HashMap<String, ExprType>,
) -> Result<Value, String> {
    DefaultExpressionEvaluator.evaluate_expression(expr, row, schema)
}

fn evaluate_column_ref(
    name: &str,
    row: &HashMap<String, Value>,
    schema: &HashMap<String, ExprType>,
) -> Result<Value, String> {
    if !schema.contains_key(name) {
        return Err(format!(
            "column '{}' is not present in expression schema",
            name
        ));
    }

    row.get(name)
        .cloned()
        .ok_or_else(|| format!("column '{}' is missing from runtime row", name))
}

/// What: Evaluate one supported builtin function.
///
/// Inputs:
/// - `name`: Function name (case-insensitive).
/// - `args`: Evaluated scalar function arguments.
///
/// Output:
/// - Function result scalar value or descriptive error.
pub fn evaluate_builtin_function(name: &str, args: &[Value]) -> Result<Value, String> {
    let upper = name.to_ascii_uppercase();
    match upper.as_str() {
        "YEAR" => {
            if is_null_arg(args, 0) {
                return Ok(Value::Null);
            }
            let date = expect_date_arg("YEAR", args, 0)?;
            Ok(Value::Int(i64::from(date.year())))
        }
        "MONTH" => {
            if is_null_arg(args, 0) {
                return Ok(Value::Null);
            }
            let date = expect_date_arg("MONTH", args, 0)?;
            Ok(Value::Int(i64::from(date.month())))
        }
        "DAY" => {
            if is_null_arg(args, 0) {
                return Ok(Value::Null);
            }
            let date = expect_date_arg("DAY", args, 0)?;
            Ok(Value::Int(i64::from(date.day())))
        }
        "LOWER" => {
            if is_null_arg(args, 0) {
                return Ok(Value::Null);
            }
            let text = expect_string_arg("LOWER", args, 0)?;
            Ok(Value::String(text.to_ascii_lowercase()))
        }
        "UPPER" => {
            if is_null_arg(args, 0) {
                return Ok(Value::Null);
            }
            let text = expect_string_arg("UPPER", args, 0)?;
            Ok(Value::String(text.to_ascii_uppercase()))
        }
        "ABS" => {
            if is_null_arg(args, 0) {
                return Ok(Value::Null);
            }
            let numeric = expect_numeric_arg("ABS", args, 0)?;
            Ok(abs_numeric(numeric))
        }
        "CEIL" => {
            if is_null_arg(args, 0) {
                return Ok(Value::Null);
            }
            let numeric = expect_numeric_arg("CEIL", args, 0)?;
            Ok(ceil_numeric(numeric))
        }
        "COALESCE" => {
            for arg in args {
                if *arg != Value::Null {
                    return Ok(arg.clone());
                }
            }
            Ok(Value::Null)
        }
        _ => Err(format!("unsupported builtin function '{}'", name)),
    }
}

fn is_null_arg(args: &[Value], index: usize) -> bool {
    matches!(args.get(index), Some(Value::Null))
}

/// What: Evaluate one binary operation over two scalar values.
///
/// Inputs:
/// - `left`: Left operand value.
/// - `op`: Binary operator.
/// - `right`: Right operand value.
///
/// Output:
/// - Operation result scalar value or descriptive error.
pub fn evaluate_binary_op(
    left: &Value,
    op: BinaryOperator,
    right: &Value,
) -> Result<Value, String> {
    if *left == Value::Null || *right == Value::Null {
        return Ok(Value::Null);
    }

    match op {
        BinaryOperator::Add => evaluate_numeric_binary(left, right, |l, r| l + r),
        BinaryOperator::Sub => evaluate_numeric_binary(left, right, |l, r| l - r),
        BinaryOperator::Mul => evaluate_numeric_binary(left, right, |l, r| l * r),
        BinaryOperator::Div => {
            let (l, r) = as_f64_pair(left, right)?;
            if r == 0.0 {
                return Err("division by zero in binary expression".to_string());
            }
            Ok(Value::Float(l / r))
        }
        BinaryOperator::Eq => Ok(Value::Bool(left == right)),
        BinaryOperator::Ne => Ok(Value::Bool(left != right)),
        BinaryOperator::Gt => Ok(Value::Bool(compare_values(left, right)? > 0)),
        BinaryOperator::Ge => Ok(Value::Bool(compare_values(left, right)? >= 0)),
        BinaryOperator::Lt => Ok(Value::Bool(compare_values(left, right)? < 0)),
        BinaryOperator::Le => Ok(Value::Bool(compare_values(left, right)? <= 0)),
    }
}

fn as_bool(value: &Value) -> Result<bool, String> {
    match value {
        Value::Bool(v) => Ok(*v),
        Value::Null => Ok(false),
        _ => Err("CASE condition must evaluate to boolean".to_string()),
    }
}

fn expect_date_arg(function_name: &str, args: &[Value], index: usize) -> Result<NaiveDate, String> {
    let value = args.get(index).ok_or_else(|| {
        format!(
            "{} requires at least {} argument(s)",
            function_name,
            index + 1
        )
    })?;

    match value {
        Value::Date(date) => Ok(*date),
        _ => Err(format!(
            "{} expects DATE argument at position {}",
            function_name,
            index + 1
        )),
    }
}

fn expect_string_arg(function_name: &str, args: &[Value], index: usize) -> Result<String, String> {
    let value = args.get(index).ok_or_else(|| {
        format!(
            "{} requires at least {} argument(s)",
            function_name,
            index + 1
        )
    })?;

    match value {
        Value::String(text) => Ok(text.clone()),
        _ => Err(format!(
            "{} expects STRING argument at position {}",
            function_name,
            index + 1
        )),
    }
}

fn expect_numeric_arg(function_name: &str, args: &[Value], index: usize) -> Result<Value, String> {
    let value = args.get(index).ok_or_else(|| {
        format!(
            "{} requires at least {} argument(s)",
            function_name,
            index + 1
        )
    })?;

    match value {
        Value::Int(_) | Value::Float(_) => Ok(value.clone()),
        _ => Err(format!(
            "{} expects numeric argument at position {}",
            function_name,
            index + 1
        )),
    }
}

fn abs_numeric(value: Value) -> Value {
    match value {
        Value::Int(v) => Value::Int(v.abs()),
        Value::Float(v) => Value::Float(v.abs()),
        _ => Value::Null,
    }
}

fn ceil_numeric(value: Value) -> Value {
    match value {
        Value::Int(v) => Value::Int(v),
        Value::Float(v) => Value::Float(v.ceil()),
        _ => Value::Null,
    }
}

fn evaluate_numeric_binary(
    left: &Value,
    right: &Value,
    op: fn(f64, f64) -> f64,
) -> Result<Value, String> {
    let (l, r) = as_f64_pair(left, right)?;
    Ok(Value::Float(op(l, r)))
}

fn as_f64_pair(left: &Value, right: &Value) -> Result<(f64, f64), String> {
    Ok((as_f64(left)?, as_f64(right)?))
}

fn as_f64(value: &Value) -> Result<f64, String> {
    match value {
        Value::Int(v) => Ok(*v as f64),
        Value::Float(v) => Ok(*v),
        _ => Err("binary numeric operation requires INT/FLOAT operands".to_string()),
    }
}

fn compare_values(left: &Value, right: &Value) -> Result<i8, String> {
    match (left, right) {
        (Value::Int(l), Value::Int(r)) => Ok(ordering_to_i8(l.cmp(r))),
        (Value::Float(_), Value::Float(_))
        | (Value::Int(_), Value::Float(_))
        | (Value::Float(_), Value::Int(_)) => {
            let (l, r) = as_f64_pair(left, right)?;
            Ok(ordering_to_i8(
                l.partial_cmp(&r)
                    .ok_or_else(|| "cannot compare NaN values".to_string())?,
            ))
        }
        (Value::String(l), Value::String(r)) => Ok(ordering_to_i8(l.cmp(r))),
        (Value::Date(l), Value::Date(r)) => Ok(ordering_to_i8(l.cmp(r))),
        _ => Err("binary comparison requires compatible operand types".to_string()),
    }
}

fn ordering_to_i8(ordering: std::cmp::Ordering) -> i8 {
    match ordering {
        std::cmp::Ordering::Less => -1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => 1,
    }
}
