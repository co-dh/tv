//! Pure functions - no I/O, no state mutation
//! These functions only compute and return values

/// Convert custom ~= operator to SQL LIKE
/// `col ~= 'pattern'` → `col LIKE '%pattern%'`
#[must_use]
#[allow(dead_code)]  // used in tests
pub fn to_sql_like(expr: &str) -> String {
    if let Some((col, pat)) = expr.split_once(" ~= ") {
        let col = col.trim();
        let pat = pat.trim().trim_matches('\'').trim_matches('"');
        format!("{} LIKE '%{}%'", col, pat)
    } else {
        expr.to_string()
    }
}

/// Convert ~= to PRQL s-string with SQL LIKE
/// `col ~= 'pattern'` → `s"col LIKE '%pattern%'"`
#[must_use]
pub fn to_prql_filter(expr: &str) -> String {
    if let Some((col, pat)) = expr.split_once(" ~= ") {
        let col = col.trim();
        let pat = pat.trim().trim_matches('\'').trim_matches('"');
        format!("s\"{} LIKE '%{}%'\"", col, pat)
    } else {
        expr.to_string()
    }
}

/// Combine filter clauses with AND, converting ~= to LIKE
#[must_use]
#[allow(dead_code)]  // used in tests
pub fn combine_filters(prev: Option<&str>, new: &str) -> String {
    let new_sql = to_sql_like(new);
    match prev {
        Some(p) => format!("({}) AND ({})", p, new_sql),
        None => new_sql,
    }
}

/// Build display name for filtered view (shows command)
#[must_use]
pub fn filter_name(_parent: &str, expr: &str) -> String {
    format!("filter {}", expr)
}

/// Build PRQL filter for multiple values (col == a || col == b)
#[must_use]
pub fn in_clause(col: &str, values: &[String], is_str: bool) -> String {
    let q = if is_str { "'" } else { "" };
    values.iter().map(|v| format!("`{}` == {}{}{}", col, q, v, q)).collect::<Vec<_>>().join(" || ")
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

/// Build xkey command from key list
#[must_use]
pub fn xkey_cmd(keys: &[String]) -> String {
    if keys.is_empty() { "xkey".into() } else { format!("xkey {}", keys.join(",")) }
}

/// Append sort to PRQL, replacing previous sort if consecutive
/// Uses this.col to avoid PRQL reserved names (date, time, etc)
#[must_use]
pub fn append_sort(prql: &str, col: &str, desc: bool) -> String {
    let sort_expr = if desc { format!("-this.`{}`", col) } else { format!("this.`{}`", col) };
    // Check if ends with | sort {...} - replace it
    if let Some(i) = prql.rfind(" | sort {") {
        format!("{} | sort {{{}}}", &prql[..i], sort_expr)
    } else {
        format!("{} | sort {{{}}}", prql, sort_expr)
    }
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
        assert_eq!(in_clause("col", &vals, true), "`col` == 'a' || `col` == 'b'");
    }

    #[test]
    fn test_in_clause_num() {
        let vals = vec!["1".into(), "2".into()];
        assert_eq!(in_clause("col", &vals, false), "`col` == 1 || `col` == 2");
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
    fn test_xkey_cmd_empty() {
        assert_eq!(xkey_cmd(&[]), "xkey");
    }

    #[test]
    fn test_xkey_cmd_cols() {
        let keys = vec!["a".into(), "b".into()];
        assert_eq!(xkey_cmd(&keys), "xkey a,b");
    }

    #[test]
    fn test_append_sort_new() {
        assert_eq!(append_sort("from df", "col", false), "from df | sort {this.`col`}");
    }

    #[test]
    fn test_append_sort_replace() {
        assert_eq!(append_sort("from df | sort {`a`}", "b", true), "from df | sort {-this.`b`}");
    }

    #[test]
    fn test_append_sort_after_filter() {
        assert_eq!(append_sort("from df | filter x > 5 | sort {`a`}", "b", false), "from df | filter x > 5 | sort {this.`b`}");
    }
}
