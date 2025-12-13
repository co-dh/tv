//! PRQL to SQL compiler wrapper
//! Compiles PRQL expressions to SQL for use with polars

use anyhow::{anyhow, Result};

/// Compile a PRQL filter expression to SQL WHERE clause
/// Input: "col > 10" or "col == @2008-01-10" or "col > 180days"
/// Output: SQL WHERE expression
/// If expression looks like SQL (contains LIKE, AND, OR, etc.), pass through directly
pub fn filter_to_sql(expr: &str) -> Result<String> {
    let upper = expr.to_uppercase();
    // If it looks like SQL, pass through directly
    if upper.contains(" LIKE ") || upper.contains(" IN ") || upper.contains(" BETWEEN ")
        || upper.contains(" AND ") || upper.contains(" OR ")
        || (expr.contains(" = ") && !expr.contains("==") && !expr.contains(">=") && !expr.contains("<=")) {
        return Ok(expr.to_string());
    }
    // Wrap in full PRQL query to compile
    let prql = format!("from df\nfilter {}", expr);
    let sql = compile(&prql)?;
    // Extract WHERE clause from "SELECT * FROM df WHERE ..."
    extract_where(&sql)
}

/// Compile a PRQL sort expression to SQL ORDER BY
/// Input: "col" (asc) or "-col" (desc)
/// Output: (column_name, is_descending)
pub fn parse_sort(expr: &str) -> (String, bool) {
    let expr = expr.trim();
    if let Some(col) = expr.strip_prefix('-') {
        (col.to_string(), true)
    } else {
        (expr.to_string(), false)
    }
}

/// Compile full PRQL to SQL
pub fn compile(prql: &str) -> Result<String> {
    let opts = prqlc::Options::default().no_signature();
    prqlc::compile(prql, &opts)
        .map_err(|e| anyhow!("PRQL compile error: {}", e.inner[0].reason))
}

/// Extract WHERE clause from SQL and simplify CONCAT patterns for polars
fn extract_where(sql: &str) -> Result<String> {
    let upper = sql.to_uppercase();
    if let Some(pos) = upper.find("WHERE") {
        let rest = &sql[pos + 5..];
        // Find end of WHERE (before ORDER BY, GROUP BY, LIMIT, or end)
        let end_markers = ["ORDER BY", "GROUP BY", "LIMIT", "HAVING"];
        let rest_upper = rest.to_uppercase();
        let end = end_markers.iter()
            .filter_map(|m| rest_upper.find(m))
            .min()
            .unwrap_or(rest.len());
        let clause = rest[..end].trim().to_string();
        // Simplify CONCAT patterns for polars: LIKE CONCAT('a', '%') -> LIKE 'a%'
        Ok(simplify_concat(&clause))
    } else {
        Err(anyhow!("No WHERE clause in compiled SQL"))
    }
}

/// Simplify CONCAT patterns in LIKE clauses for polars compatibility
/// LIKE CONCAT('prefix', '%') -> LIKE 'prefix%'
/// LIKE CONCAT('%', 'mid', '%') -> LIKE '%mid%'
fn simplify_concat(sql: &str) -> String {
    use std::borrow::Cow;
    let mut result = Cow::Borrowed(sql);
    // Pattern: LIKE CONCAT('...', '%') -> LIKE '...%'
    while let Some(pos) = result.to_uppercase().find("LIKE CONCAT(") {
        let start = pos + 5; // after "LIKE "
        if let Some(end) = result[start..].find(')') {
            let concat_expr = &result[start..start + end + 1];
            // Extract parts from CONCAT('a', '%') or CONCAT('%', 'b', '%')
            let inner = &concat_expr[7..concat_expr.len()-1]; // strip CONCAT( and )
            let parts: Vec<&str> = inner.split(", ").collect();
            let simplified: String = parts.iter()
                .map(|p| p.trim_matches('\''))
                .collect();
            let new_sql = format!("{}'{}'{}", &result[..pos+5], simplified, &result[start+end+1..]);
            result = Cow::Owned(new_sql);
        } else {
            break;
        }
    }
    result.into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_simple() {
        let sql = filter_to_sql("val > 10").unwrap();
        assert!(sql.contains("val") && sql.contains("10"));
    }

    #[test]
    fn test_filter_string() {
        let sql = filter_to_sql("name == 'alice'").unwrap();
        assert!(sql.contains("alice"));
    }

    #[test]
    fn test_filter_text_starts_with() {
        let sql = filter_to_sql("(name | text.starts_with 'a')").unwrap();
        eprintln!("starts_with SQL: {}", sql);
        // Should compile to something polars can handle
        assert!(!sql.is_empty());
    }

    #[test]
    fn test_filter_text_contains() {
        let sql = filter_to_sql("(name | text.contains 'app')").unwrap();
        eprintln!("contains SQL: {}", sql);
        assert!(!sql.is_empty());
    }

    #[test]
    fn test_filter_date() {
        let sql = filter_to_sql("dt == @2008-01-10").unwrap();
        assert!(sql.contains("2008") && sql.contains("01") && sql.contains("10"));
    }

    #[test]
    fn test_filter_and() {
        let sql = filter_to_sql("val > 10 && val < 20").unwrap();
        assert!(sql.to_uppercase().contains("AND"));
    }

    #[test]
    fn test_filter_or() {
        let sql = filter_to_sql("val == 1 || val == 2").unwrap();
        assert!(sql.to_uppercase().contains("OR"));
    }

    #[test]
    fn test_filter_like() {
        // SQL LIKE pattern (passed through directly)
        let sql = filter_to_sql("name LIKE '%tests%'").unwrap();
        eprintln!("like SQL: {}", sql);
        assert!(sql.contains("LIKE"));
    }

    #[test]
    fn test_sort_asc() {
        let (col, desc) = parse_sort("name");
        assert_eq!(col, "name");
        assert!(!desc);
    }

    #[test]
    fn test_sort_desc() {
        let (col, desc) = parse_sort("-name");
        assert_eq!(col, "name");
        assert!(desc);
    }

    #[test]
    fn test_compile_full() {
        // PRQL sort syntax: sort {-val} for descending
        let sql = compile("from df\nfilter val > 10\nsort {-val}\ntake 5").unwrap();
        assert!(sql.to_uppercase().contains("SELECT"));
        assert!(sql.to_uppercase().contains("WHERE"));
        assert!(sql.to_uppercase().contains("ORDER BY"));
        assert!(sql.to_uppercase().contains("LIMIT"));
    }
}
