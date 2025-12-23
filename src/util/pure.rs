//! Pure functions - no I/O, no state mutation
//! These functions only compute and return values

/// PRQL reserved words that need backtick quoting
const RESERVED: &[&str] = &[
    "date", "time", "from", "select", "filter", "take", "group", "aggregate",
    "derive", "sort", "join", "window", "func", "let", "prql", "case", "type",
    "module", "internal", "true", "false", "null", "and", "or", "not", "in",
    "into", "average", "count", "min", "max", "sum", "stddev", "first", "last",
];

/// Quote column name only if reserved word or has special chars
#[must_use]
pub fn qcol(col: &str) -> String {
    if RESERVED.contains(&col.to_lowercase().as_str()) || col.contains(|c: char| !c.is_alphanumeric() && c != '_') {
        format!("`{}`", col)
    } else { col.to_string() }
}

/// Quote multiple columns, join with comma (no spaces)
#[must_use]
pub fn qcols(cols: &[String]) -> String {
    cols.iter().map(|c| qcol(c)).collect::<Vec<_>>().join(",")
}

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
    let c = qcol(col);
    let q = if is_str { "'" } else { "" };
    values.iter().map(|v| format!("{}=={}{}{}", c, q, v, q)).collect::<Vec<_>>().join("||")
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

/// Build xkey command from key list (terse format)
#[must_use]
pub fn xkey_cmd(keys: &[String]) -> String {
    if keys.is_empty() { "xkey".into() } else { format!("xkey{{{}}}", qcols(keys)) }
}

/// Append sort to PRQL, replacing previous sort if consecutive
#[must_use]
pub fn append_sort(prql: &str, col: &str, desc: bool) -> String {
    let c = qcol(col);
    let expr = if desc { format!("-this.{}", c) } else { format!("this.{}", c) };
    // Replace existing sort or append
    if let Some(i) = prql.rfind("|sort{") {
        format!("{}|sort{{{}}}", &prql[..i], expr)
    } else {
        format!("{}|sort{{{}}}", prql, expr)
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
        assert_eq!(in_clause("col", &vals, true), "col=='a'||col=='b'");
    }

    #[test]
    fn test_in_clause_num() {
        let vals = vec!["1".into(), "2".into()];
        assert_eq!(in_clause("col", &vals, false), "col==1||col==2");
    }

    #[test]
    fn test_in_clause_reserved() {
        let vals = vec!["a".into()];
        assert_eq!(in_clause("date", &vals, true), "`date`=='a'");
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
        assert_eq!(xkey_cmd(&keys), "xkey{a,b}");
    }

    #[test]
    fn test_xkey_cmd_reserved() {
        let keys = vec!["date".into(), "time".into()];
        assert_eq!(xkey_cmd(&keys), "xkey{`date`,`time`}");
    }

    #[test]
    fn test_append_sort_new() {
        assert_eq!(append_sort("from df", "col", false), "from df|sort{this.col}");
    }

    #[test]
    fn test_append_sort_desc() {
        assert_eq!(append_sort("from df", "col", true), "from df|sort{-this.col}");
    }

    #[test]
    fn test_append_sort_reserved() {
        assert_eq!(append_sort("from df", "date", false), "from df|sort{this.`date`}");
    }

    #[test]
    fn test_append_sort_replace() {
        assert_eq!(append_sort("from df|sort{a}", "b", true), "from df|sort{-this.b}");
    }

    #[test]
    fn test_qcol_normal() {
        assert_eq!(qcol("name"), "name");
        assert_eq!(qcol("col_1"), "col_1");
    }

    #[test]
    fn test_qcol_reserved() {
        assert_eq!(qcol("date"), "`date`");
        assert_eq!(qcol("time"), "`time`");
        assert_eq!(qcol("from"), "`from`");
    }

    #[test]
    fn test_qcol_special() {
        assert_eq!(qcol("col name"), "`col name`");
        assert_eq!(qcol("col-1"), "`col-1`");
    }
}
