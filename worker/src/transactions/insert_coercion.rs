pub(crate) fn strip_sql_literal_quotes(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.len() >= 2 {
        let bytes = trimmed.as_bytes();
        let first = bytes[0] as char;
        let last = bytes[trimmed.len() - 1] as char;
        if (first == '\'' && last == '\'') || (first == '"' && last == '"') {
            return trimmed[1..trimmed.len() - 1].to_string();
        }
    }
    trimmed.to_string()
}

pub(crate) fn parse_timestamp_millis_literal(raw: &str) -> Option<i64> {
    let unquoted = strip_sql_literal_quotes(raw);
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(&unquoted) {
        return Some(parsed.timestamp_millis());
    }
    if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(&unquoted, "%Y-%m-%d %H:%M:%S") {
        return Some(parsed.and_utc().timestamp_millis());
    }
    None
}

pub(crate) fn parse_datetime_literal(raw: &str) -> Result<chrono::NaiveDateTime, String> {
    let unquoted = strip_sql_literal_quotes(raw);
    if let Ok(parsed) = chrono::NaiveDateTime::parse_from_str(&unquoted, "%Y-%m-%d %H:%M:%S") {
        return Ok(parsed);
    }
    if let Ok(parsed) = chrono::NaiveDate::parse_from_str(&unquoted, "%Y-%m-%d") {
        return parsed.and_hms_opt(0, 0, 0).ok_or_else(|| {
            format!(
                "TEMPORAL_LITERAL_INVALID: could not normalize DATETIME literal '{}'",
                unquoted
            )
        });
    }

    Err(format!(
        "TEMPORAL_LITERAL_INVALID: unsupported DATETIME literal '{}'",
        unquoted
    ))
}

pub(crate) fn format_datetime_literal(value: chrono::NaiveDateTime) -> String {
    value.format("%Y-%m-%d %H:%M:%S").to_string()
}

pub(crate) fn parse_decimal_precision_scale(declared: &str) -> Option<(u16, u16)> {
    let open = declared.find('(')?;
    let close = declared[open + 1..].find(')')? + open + 1;
    let inner = &declared[open + 1..close];
    let mut parts = inner.split(',').map(str::trim);
    let precision = parts.next()?.parse::<u16>().ok()?;
    let scale = parts.next().unwrap_or("0").parse::<u16>().ok()?;
    Some((precision, scale))
}

pub(crate) fn normalize_decimal_literal(
    raw: &str,
    precision_scale: Option<(u16, u16)>,
) -> Result<String, String> {
    let mut value = strip_sql_literal_quotes(raw);
    if value.is_empty() {
        return Err("DECIMAL_COERCION_FAILED: empty decimal literal".to_string());
    }

    if value.contains('e') || value.contains('E') {
        return Err(format!(
            "DECIMAL_COERCION_FAILED: scientific notation is not supported: {}",
            value
        ));
    }

    let sign = if value.starts_with('-') {
        value.remove(0);
        "-"
    } else if value.starts_with('+') {
        value.remove(0);
        ""
    } else {
        ""
    };

    let mut segments = value.split('.');
    let integer_part = segments.next().unwrap_or_default();
    let fraction_part = segments.next().unwrap_or_default();
    if segments.next().is_some() {
        return Err(format!(
            "DECIMAL_COERCION_FAILED: invalid decimal literal '{}': multiple dots",
            value
        ));
    }

    if !integer_part.chars().all(|ch| ch.is_ascii_digit())
        || !fraction_part.chars().all(|ch| ch.is_ascii_digit())
    {
        return Err(format!(
            "DECIMAL_COERCION_FAILED: invalid decimal literal '{}': non-digit characters",
            value
        ));
    }

    let normalized_integer = if integer_part.is_empty() {
        "0".to_string()
    } else {
        let stripped = integer_part.trim_start_matches('0');
        if stripped.is_empty() {
            "0".to_string()
        } else {
            stripped.to_string()
        }
    };

    let mut normalized_fraction = fraction_part.to_string();

    if let Some((precision, scale)) = precision_scale {
        if normalized_fraction.len() > usize::from(scale) {
            return Err(format!(
                "DECIMAL_COERCION_FAILED: literal '{}' exceeds scale {}",
                value, scale
            ));
        }
        while normalized_fraction.len() < usize::from(scale) {
            normalized_fraction.push('0');
        }

        let digits_total = normalized_integer
            .chars()
            .filter(|ch| ch.is_ascii_digit())
            .count()
            + normalized_fraction
                .chars()
                .filter(|ch| ch.is_ascii_digit())
                .count();
        if digits_total > usize::from(precision) {
            return Err(format!(
                "DECIMAL_COERCION_FAILED: literal '{}' exceeds precision {}",
                value, precision
            ));
        }
    }

    if normalized_fraction.is_empty() {
        Ok(format!("{}{}", sign, normalized_integer))
    } else {
        Ok(format!(
            "{}{}.{}",
            sign, normalized_integer, normalized_fraction
        ))
    }
}
