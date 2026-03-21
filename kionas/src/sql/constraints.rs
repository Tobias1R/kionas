use crate::parser::datafusion_sql::sqlparser::ast::{ColumnOption, CreateTable as SqlCreateTable};
use serde::{Deserialize, Serialize};

/// What: Stable version marker for the constraint contract payload.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Version number used in serialized constraint payloads.
///
/// Details:
/// - The version allows backward-compatible evolution of table constraint metadata.
pub const CONSTRAINT_CONTRACT_VERSION: u8 = 1;

/// What: One normalized column-level constraint descriptor.
///
/// Inputs:
/// - `column`: Canonical column identifier.
/// - `nullable`: Whether column accepts NULL values.
/// - `not_null`: Whether column explicitly requires non-null values.
/// - `constraint_codes`: Stable machine-readable constraint codes.
///
/// Output:
/// - Serializable column constraint representation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ColumnConstraintSpec {
    pub column: String,
    pub nullable: bool,
    pub not_null: bool,
    pub constraint_codes: Vec<String>,
}

/// What: Normalized table constraint contract transported across components.
///
/// Inputs:
/// - `version`: Contract version number.
/// - `columns`: Column-level constraint entries.
///
/// Output:
/// - Serializable table constraint contract used by server, metastore, and worker flows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableConstraintContract {
    pub version: u8,
    pub columns: Vec<ColumnConstraintSpec>,
}

/// What: Build a normalized table-constraint contract from CREATE TABLE AST.
///
/// Inputs:
/// - `create_table`: Parsed CREATE TABLE statement.
///
/// Output:
/// - Deterministic column-level constraint contract.
///
/// Details:
/// - Current FOUNDATION scope records NOT NULL using the `NOT_NULL` code.
pub fn build_constraint_contract_from_create_table(
    create_table: &SqlCreateTable,
) -> TableConstraintContract {
    let columns = create_table
        .columns
        .iter()
        .map(|col| {
            let not_null = col
                .options
                .iter()
                .any(|opt| matches!(opt.option, ColumnOption::NotNull));
            let mut codes = Vec::new();
            if not_null {
                codes.push("NOT_NULL".to_string());
            }
            ColumnConstraintSpec {
                column: col.name.to_string(),
                nullable: !not_null,
                not_null,
                constraint_codes: codes,
            }
        })
        .collect::<Vec<_>>();

    TableConstraintContract {
        version: CONSTRAINT_CONTRACT_VERSION,
        columns,
    }
}
