//! Pure functions - no I/O, no state mutation
//! These functions only compute and return values

/// Convert custom ~= operator to SQL LIKE
/// `col ~= 'pattern'` → `col LIKE '%pattern%'`
#[must_use]
pub fn to_sql_like(expr: &str) -> String {
    if let Some((col, pat)) = expr.split_once(" ~= ") {
        let col = col.trim();
        let pat = pat.trim().trim_matches('\'').trim_matches('"');
        format!("{} LIKE '%{}%'", col, pat)
    } else {
        expr.to_string()
    }
}

/// Combine filter clauses with AND, converting ~= to LIKE
#[must_use]
pub fn combine_filters(prev: Option<&str>, new: &str) -> String {
    let new_sql = to_sql_like(new);
    match prev {
        Some(p) => format!("({}) AND ({})", p, new_sql),
        None => new_sql,
    }
}

/// Build display name for filtered view
#[must_use]
pub fn filter_name(parent: &str, expr: &str) -> String {
    format!("{} & {}", parent, expr)
}

/// Build SQL IN clause
#[must_use]
pub fn in_clause(col: &str, values: &[String], is_str: bool) -> String {
    let vals = values.iter()
        .map(|v| if is_str { format!("'{}'", v) } else { v.clone() })
        .collect::<Vec<_>>()
        .join(",");
    format!("\"{}\" IN ({})", col, vals)
}

/// Build display name for IN filter
#[must_use]
pub fn filter_in_name(col: &str, values: &[String]) -> String {
    if values.len() == 1 {
        format!("{}={}", col, values[0])
    } else {
        format!("{}∈{{{}}}", col, values.len())
    }
}

/// Check if column type is string-like
#[must_use]
pub fn is_string_type(dtype: &str) -> bool {
    dtype.contains("String") || dtype.contains("Utf8")
}

/// Reorder columns: keys first, then rest
#[must_use]
pub fn reorder_cols(all: &[String], keys: &[String]) -> Vec<String> {
    let rest: Vec<String> = all.iter()
        .filter(|c| !keys.contains(c))
        .cloned()
        .collect();
    let mut order = keys.to_vec();
    order.extend(rest);
    order
}

/// Count how many columns in 'deleted' appear before 'sep' in 'all'
#[must_use]
pub fn count_before_sep(all: &[String], deleted: &[String], sep: usize) -> usize {
    deleted.iter()
        .filter(|c| all.iter().position(|n| n == *c).map(|i| i < sep).unwrap_or(false))
        .count()
}

/// Toggle columns in/out of key list
#[must_use]
pub fn toggle_keys(current: &[String], to_toggle: &[String]) -> Vec<String> {
    let mut keys = current.to_vec();
    for col in to_toggle {
        if let Some(pos) = keys.iter().position(|k| k == col) {
            keys.remove(pos);
        } else {
            keys.push(col.clone());
        }
    }
    keys
}

/// Build freq command from columns and separator
#[must_use]
pub fn freq_cmd(cols: &[String], sep: usize, cur: Option<&str>) -> Option<String> {
    if sep > 0 {
        Some(format!("freq {}", cols[..sep].join(",")))
    } else {
        cur.map(|c| format!("freq {}", c))
    }
}

/// Build xkey command from key list
#[must_use]
pub fn xkey_cmd(keys: &[String]) -> String {
    if keys.is_empty() { "xkey".into() } else { format!("xkey {}", keys.join(",")) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_sql_like() {
        assert_eq!(to_sql_like("path ~= 'numeric'"), "path LIKE '%numeric%'");
        assert_eq!(to_sql_like("x > 5"), "x > 5");  // passthrough
    }

    #[test]
    fn test_combine_filters_none() {
        assert_eq!(combine_filters(None, "x > 5"), "x > 5");
    }

    #[test]
    fn test_combine_filters_like() {
        assert_eq!(combine_filters(None, "path ~= 'foo'"), "path LIKE '%foo%'");
    }

    #[test]
    fn test_combine_filters_some() {
        assert_eq!(combine_filters(Some("a = 1"), "b = 2"), "(a = 1) AND (b = 2)");
    }

    #[test]
    fn test_in_clause_str() {
        let vals = vec!["a".into(), "b".into()];
        assert_eq!(in_clause("col", &vals, true), "\"col\" IN ('a','b')");
    }

    #[test]
    fn test_in_clause_num() {
        let vals = vec!["1".into(), "2".into()];
        assert_eq!(in_clause("col", &vals, false), "\"col\" IN (1,2)");
    }

    #[test]
    fn test_filter_in_name_single() {
        assert_eq!(filter_in_name("x", &["val".into()]), "x=val");
    }

    #[test]
    fn test_filter_in_name_multi() {
        assert_eq!(filter_in_name("x", &["a".into(), "b".into()]), "x∈{2}");
    }

    #[test]
    fn test_reorder_cols() {
        let all = vec!["a".into(), "b".into(), "c".into()];
        let keys = vec!["c".into(), "a".into()];
        assert_eq!(reorder_cols(&all, &keys), vec!["c", "a", "b"]);
    }

    #[test]
    fn test_count_before_sep() {
        let all = vec!["a".into(), "b".into(), "c".into(), "d".into()];
        let del = vec!["a".into(), "c".into()];
        assert_eq!(count_before_sep(&all, &del, 2), 1);  // only 'a' is before sep
    }

    #[test]
    fn test_toggle_keys_add() {
        let cur = vec!["a".into()];
        let tog = vec!["b".into()];
        assert_eq!(toggle_keys(&cur, &tog), vec!["a", "b"]);
    }

    #[test]
    fn test_toggle_keys_remove() {
        let cur = vec!["a".into(), "b".into()];
        let tog = vec!["a".into()];
        assert_eq!(toggle_keys(&cur, &tog), vec!["b"]);
    }

    #[test]
    fn test_freq_cmd_with_sep() {
        let cols = vec!["a".into(), "b".into(), "c".into()];
        assert_eq!(freq_cmd(&cols, 2, None), Some("freq a,b".into()));
    }

    #[test]
    fn test_freq_cmd_no_sep() {
        let cols = vec!["a".into(), "b".into()];
        assert_eq!(freq_cmd(&cols, 0, Some("x")), Some("freq x".into()));
    }

    #[test]
    fn test_xkey_cmd_empty() {
        assert_eq!(xkey_cmd(&[]), "xkey");
    }

    #[test]
    fn test_xkey_cmd_cols() {
        let keys = vec!["a".into(), "b".into()];
        assert_eq!(xkey_cmd(&keys), "xkey a,b");
    }
}
