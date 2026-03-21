use crate::parser::datafusion_sql::sqlparser::ast::CreateTable as SqlCreateTable;
use serde::{Deserialize, Serialize};

/// What: Stable version marker for datatype contract payloads.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Version number used in serialized datatype payloads.
///
/// Details:
/// - The version allows backward-compatible evolution of table datatype metadata.
pub const DATATYPE_CONTRACT_VERSION: u8 = 1;

/// What: Frozen timezone policy for temporal datatypes in FOUNDATION.
///
/// Inputs:
/// - Variant selected by contract builder.
///
/// Output:
/// - Serializable timezone policy in the table datatype contract.
///
/// Details:
/// - FOUNDATION freezes timezone awareness and UTC normalization while preserving
///   the original offset metadata for diagnostics and replay.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TimezonePolicy {
    TimezoneAwareUtcNormalized,
}

/// What: Frozen decimal coercion behavior for FOUNDATION.
///
/// Inputs:
/// - Variant selected by contract builder.
///
/// Output:
/// - Serializable decimal coercion policy in the table datatype contract.
///
/// Details:
/// - FOUNDATION uses strict schema-aligned coercion to avoid silent fallback.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DecimalCoercionPolicy {
    Strict,
}

/// What: Distinguish temporal logical types that must remain separate.
///
/// Inputs:
/// - Variant inferred from declared SQL type.
///
/// Output:
/// - Serializable temporal logical type marker.
///
/// Details:
/// - DATETIME and TIMESTAMP are intentionally represented as distinct variants.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TemporalLogicalType {
    Timestamp,
    Datetime,
}

/// What: Decimal precision and scale contract for one column.
///
/// Inputs:
/// - `precision`: Declared decimal precision.
/// - `scale`: Declared decimal scale.
///
/// Output:
/// - Serializable decimal constraint descriptor.
///
/// Details:
/// - This is only present for decimal-like declared types.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DecimalSpec {
    pub precision: u16,
    pub scale: u16,
}

/// What: One normalized datatype descriptor for a table column.
///
/// Inputs:
/// - `column`: Canonical column identifier.
/// - `declared_type`: Original SQL type text.
/// - `canonical_type`: Canonical datatype family token.
/// - `temporal_type`: Optional temporal logical type marker.
/// - `timezone_policy`: Optional timezone policy for temporal columns.
/// - `decimal_spec`: Optional decimal precision/scale descriptor.
///
/// Output:
/// - Serializable column datatype representation.
///
/// Details:
/// - The contract is additive and does not alter existing column schema fields.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnDatatypeSpec {
    pub column: String,
    pub declared_type: String,
    pub canonical_type: String,
    pub temporal_type: Option<TemporalLogicalType>,
    pub timezone_policy: Option<TimezonePolicy>,
    pub decimal_spec: Option<DecimalSpec>,
}

/// What: Normalized table datatype contract transported across components.
///
/// Inputs:
/// - `version`: Contract version number.
/// - `timezone_policy`: Global temporal timezone policy.
/// - `decimal_coercion_policy`: Global decimal coercion policy.
/// - `columns`: Column-level datatype entries.
///
/// Output:
/// - Serializable table datatype contract used by server/planner/worker flows.
///
/// Details:
/// - FOUNDATION freezes distinct TIMESTAMP/DATETIME and strict decimal coercion
///   decisions even when execution support is delivered in later phases.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableDatatypeContract {
    pub version: u8,
    pub timezone_policy: TimezonePolicy,
    pub decimal_coercion_policy: DecimalCoercionPolicy,
    pub columns: Vec<ColumnDatatypeSpec>,
}

/// What: Build normalized table datatype contract from CREATE TABLE AST.
///
/// Inputs:
/// - `create_table`: Parsed CREATE TABLE statement.
///
/// Output:
/// - Deterministic column-level datatype contract.
///
/// Details:
/// - Temporal columns are tagged with distinct TIMESTAMP or DATETIME markers.
/// - Decimal columns capture parsed precision and scale when explicitly declared.
pub fn build_datatype_contract_from_create_table(
    create_table: &SqlCreateTable,
) -> TableDatatypeContract {
    let columns = create_table
        .columns
        .iter()
        .map(|col| {
            let declared_type = col.data_type.to_string();
            let lower = declared_type.to_ascii_lowercase();
            let temporal_type = if lower.contains("datetime") {
                Some(TemporalLogicalType::Datetime)
            } else if lower.contains("timestamp") {
                Some(TemporalLogicalType::Timestamp)
            } else {
                None
            };
            let timezone_policy = temporal_type
                .as_ref()
                .map(|_| TimezonePolicy::TimezoneAwareUtcNormalized);
            let decimal_spec = parse_decimal_spec(&lower);
            ColumnDatatypeSpec {
                column: col.name.to_string(),
                declared_type: declared_type.clone(),
                canonical_type: canonical_type_token(&lower, temporal_type.as_ref()),
                temporal_type,
                timezone_policy,
                decimal_spec,
            }
        })
        .collect::<Vec<_>>();

    TableDatatypeContract {
        version: DATATYPE_CONTRACT_VERSION,
        timezone_policy: TimezonePolicy::TimezoneAwareUtcNormalized,
        decimal_coercion_policy: DecimalCoercionPolicy::Strict,
        columns,
    }
}

/// What: Resolve a canonical datatype family token from declared SQL text.
///
/// Inputs:
/// - `declared_type_lower`: Lowercased declared type text.
/// - `temporal_type`: Optional temporal logical marker.
///
/// Output:
/// - Canonical datatype family token.
///
/// Details:
/// - Tokens are descriptive and intentionally stable for contract validation.
fn canonical_type_token(
    declared_type_lower: &str,
    temporal_type: Option<&TemporalLogicalType>,
) -> String {
    if let Some(temporal) = temporal_type {
        return match temporal {
            TemporalLogicalType::Timestamp => "timestamp".to_string(),
            TemporalLogicalType::Datetime => "datetime".to_string(),
        };
    }

    if declared_type_lower.contains("decimal") || declared_type_lower.contains("numeric") {
        return "decimal".to_string();
    }
    if declared_type_lower.contains("bigint") || declared_type_lower == "int8" {
        return "int64".to_string();
    }
    if declared_type_lower.contains("smallint") || declared_type_lower == "int2" {
        return "int16".to_string();
    }
    if declared_type_lower.contains("int") || declared_type_lower == "integer" {
        return "int32".to_string();
    }
    if declared_type_lower.contains("double") {
        return "float64".to_string();
    }
    if declared_type_lower.contains("float") || declared_type_lower.contains("real") {
        return "float32".to_string();
    }
    if declared_type_lower.contains("bool") {
        return "boolean".to_string();
    }
    if declared_type_lower == "date" {
        return "date".to_string();
    }
    if declared_type_lower.contains("binary") {
        return "binary".to_string();
    }

    "string".to_string()
}

/// What: Parse decimal precision and scale from declared SQL text.
///
/// Inputs:
/// - `declared_type_lower`: Lowercased declared type text.
///
/// Output:
/// - Decimal precision/scale when explicitly provided and valid.
///
/// Details:
/// - Accepted forms: `decimal(p,s)` and `numeric(p,s)`.
fn parse_decimal_spec(declared_type_lower: &str) -> Option<DecimalSpec> {
    let is_decimal =
        declared_type_lower.contains("decimal") || declared_type_lower.contains("numeric");
    if !is_decimal {
        return None;
    }

    let start = declared_type_lower.find('(')?;
    let end = declared_type_lower.rfind(')')?;
    if end <= start + 1 {
        return None;
    }

    let inner = &declared_type_lower[start + 1..end];
    let parts = inner
        .split(',')
        .map(|part| part.trim())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if parts.len() != 2 {
        return None;
    }

    let precision = parts[0].parse::<u16>().ok()?;
    let scale = parts[1].parse::<u16>().ok()?;
    Some(DecimalSpec { precision, scale })
}

#[cfg(test)]
mod tests {
    use super::{TemporalLogicalType, build_datatype_contract_from_create_table};
    use crate::parser::datafusion_sql::sqlparser::dialect::GenericDialect;
    use crate::parser::datafusion_sql::sqlparser::parser::Parser;

    #[test]
    fn preserves_distinct_temporal_types() {
        let sql = "create table db.s.t (a timestamp, b datetime)";
        let ast = Parser::parse_sql(&GenericDialect {}, sql).expect("sql should parse");
        let statement = ast.first().expect("statement should exist");
        let create_table = match statement {
            crate::parser::datafusion_sql::sqlparser::ast::Statement::CreateTable(create) => create,
            _ => panic!("expected create table"),
        };

        let contract = build_datatype_contract_from_create_table(create_table);
        assert_eq!(contract.columns.len(), 2);
        assert_eq!(
            contract.columns[0].temporal_type,
            Some(TemporalLogicalType::Timestamp)
        );
        assert_eq!(
            contract.columns[1].temporal_type,
            Some(TemporalLogicalType::Datetime)
        );
    }
}
