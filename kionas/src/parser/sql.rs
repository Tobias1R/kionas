use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::ast::Statement;

pub fn parse_query(query: &str) -> Result<Vec<Statement>, String> {
    let dialect = PostgreSqlDialect {};
    match Parser::parse_sql(&dialect, query) {
        Ok(statements) => Ok(statements),
        Err(e) => Err(format!("SQL parse error: {}", e)),
    }
}
