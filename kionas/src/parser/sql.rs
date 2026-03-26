use sqlparser::ast::Statement;
use sqlparser::dialect::SnowflakeDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::{Token, Tokenizer};

pub fn parse_query(query: &str) -> Result<Vec<Statement>, String> {
    // Use Snowflake dialect for now; later we may switch to `KionasDialect`.
    let dialect = SnowflakeDialect {};

    // Tokenize first so we can detect Kionas-specific direct commands
    // (for example: `USE WAREHOUSE <name>`). If detected, return a
    // distinguished Err so callers can route to handlers. Otherwise,
    // fall back to the full Parser.
    if let Ok(tokens) = Tokenizer::new(&dialect, query).tokenize() {
        // Find a pattern: USE WAREHOUSE <name>
        for i in 0..tokens.len() {
            if let Token::Word(w) = &tokens[i]
                && w.keyword == Keyword::USE
            {
                // look for next meaningful token that is WORD == WAREHOUSE
                let mut j = i + 1;
                while j < tokens.len() {
                    match &tokens[j] {
                        Token::Word(w2) => {
                            if w2.keyword == Keyword::WAREHOUSE {
                                // naive name extraction from original text: third whitespace token
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
                                } else {
                                    return Err("KIONAS_DIRECT_COMMAND:USE_WAREHOUSE:".to_string());
                                }
                            } else {
                                break;
                            }
                        }
                        // skip comments/punctuation
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
