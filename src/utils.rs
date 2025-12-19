//! Utility functions shared across modules.

/// Format number with commas (1234567 -> "1,234,567")
#[must_use]
pub fn commify(s: &str) -> String {
    s.chars().rev().enumerate()
        .flat_map(|(i, c)| if i > 0 && i % 3 == 0 { vec![',', c] } else { vec![c] })
        .collect::<Vec<_>>().into_iter().rev().collect()
}

/// Extract string value without quotes
#[must_use]
pub fn unquote(s: &str) -> String {
    s.trim_matches('"').to_string()
}
