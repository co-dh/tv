//! Utility functions shared across modules.

use polars::prelude::DataType;

/// Check if DataType is numeric (int/uint/float)
#[must_use]
pub fn is_numeric(dt: &DataType) -> bool {
    matches!(dt,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
        DataType::Float32 | DataType::Float64)
}

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
