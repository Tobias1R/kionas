use crate::utils::redact_sql;

#[test]
fn redact_sql_replaces_literals_and_limits_length() {
    let sql =
        "SELECT * FROM users WHERE email = 'alice@example.com' AND id = 12345 AND score = 98.6";

    let redacted = redact_sql(sql);

    assert!(redacted.contains("email = '?'"));
    assert!(redacted.contains("id = ?"));
    assert!(redacted.contains("score = ?"));
    assert!(!redacted.contains("alice@example.com"));
    assert!(!redacted.contains("12345"));
}

#[test]
fn redact_sql_truncates_long_queries() {
    let long_sql = format!("SELECT '{}'", "x".repeat(500));

    let redacted = redact_sql(&long_sql);

    assert!(redacted.len() <= 200);
}

#[test]
fn redact_sql_handles_insert_and_no_literal_queries() {
    let insert_sql = "INSERT INTO t VALUES ('secret', 99)";
    let simple_sql = "SELECT id FROM users";

    let insert_redacted = redact_sql(insert_sql);
    let simple_redacted = redact_sql(simple_sql);

    assert_eq!(insert_redacted, "INSERT INTO t VALUES ('?', ?)");
    assert_eq!(simple_redacted, simple_sql);
}
