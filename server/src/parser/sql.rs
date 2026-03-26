use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::SnowflakeDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

#[allow(dead_code)]
pub fn parse_query(query: &str) -> Result<Vec<Statement>, String> {
    let dialect = SnowflakeDialect {};

    if let Ok(tokens) = Tokenizer::new(&dialect, query).tokenize() {
        for i in 0..tokens.len() {
            if let Token::Word(w) = &tokens[i]
                && w.keyword == Keyword::USE
            {
                let mut j = i + 1;
                while j < tokens.len() {
                    match &tokens[j] {
                        Token::Word(w2) => {
                            if w2.keyword == Keyword::WAREHOUSE {
                                let parts: Vec<&str> = query.split_whitespace().collect();
                                if parts.len() >= 3 {
                                    let mut name = parts[2].trim().to_string();
                                    if name.ends_with(';') {
                                        name = name.trim_end_matches(';').to_string();
                                    }
                                    return Err(format!(
                                        "KIONAS_DIRECT_COMMAND:USE_WAREHOUSE:{}",
                                        name
                                    ));
                                }
                                return Err("KIONAS_DIRECT_COMMAND:USE_WAREHOUSE:".to_string());
                            }
                            break;
                        }
                        _ => j += 1,
                    }
                }
            }
        }
    }

    match Parser::parse_sql(&dialect, query) {
        Ok(statements) => Ok(statements),
        Err(e) => Err(format!("SQL parse error: {}", e)),
    }
}
