//! Type conversion helpers for CSV/Parquet data
use polars::prelude::*;

/// Check if column name looks like a datetime field
pub fn is_datetime_name(name: &str) -> bool {
    let n = name.to_lowercase();
    n.contains("time") || n.contains("date") || n.contains("created") || n.contains("updated")
        || n.contains("_at") || n.contains("_ts") || n == "ts" || n == "dt"
}

/// Detect epoch unit from integer value: sec, ms, us, ns
pub fn epoch_unit(v: i64) -> Option<TimeUnit> {
    let abs = v.abs();
    if abs > 1_000_000_000_000_000_000 && abs < 3_000_000_000_000_000_000 { Some(TimeUnit::Nanoseconds) }
    else if abs > 1_000_000_000_000_000 && abs < 3_000_000_000_000_000 { Some(TimeUnit::Microseconds) }
    else if abs > 1_000_000_000_000 && abs < 3_000_000_000_000 { Some(TimeUnit::Milliseconds) }
    else if abs > 1_000_000_000 && abs < 3_000_000_000 { Some(TimeUnit::Milliseconds) }
    else { None }
}

/// Check if value looks like TAQ time format (HHMMSS + fractional ns)
pub fn is_taq_time(v: i64) -> bool {
    if v < 0 { return false; }
    if v < 1_000_000_000_000 || v >= 1_000_000_000_000_000 { return false; }
    let s = format!("{:015}", v);
    let hh: u32 = s[0..2].parse().unwrap_or(99);
    let mm: u32 = s[2..4].parse().unwrap_or(99);
    let ss: u32 = s[4..6].parse().unwrap_or(99);
    hh < 24 && mm < 60 && ss < 60
}

/// Convert TAQ time format to nanoseconds since midnight
pub fn taq_to_ns(v: i64) -> i64 {
    let s = format!("{:015}", v);
    let hh: i64 = s[0..2].parse().unwrap_or(0);
    let mm: i64 = s[2..4].parse().unwrap_or(0);
    let ss: i64 = s[4..6].parse().unwrap_or(0);
    let frac: i64 = s[6..15].parse().unwrap_or(0);
    (hh * 3600 + mm * 60 + ss) * 1_000_000_000 + frac
}

/// Convert integer/float columns with datetime-like names to datetime
/// Note: TAQ time conversion is NOT automatic - use `to_time` command instead
pub fn convert_epoch_cols(df: DataFrame) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for c in df.get_columns() {
        let name = c.name().as_str();
        let is_numeric = c.dtype().is_integer() || c.dtype().is_float();
        if !is_datetime_name(name) || !is_numeric {
            cols.push(c.clone());
            continue;
        }
        let s = c.as_materialized_series();
        let Ok(i64_s) = s.cast(&DataType::Int64) else { cols.push(c.clone()); continue; };
        let Ok(i64_ca) = i64_s.i64() else { cols.push(c.clone()); continue; };
        let Some(v) = i64_ca.into_iter().flatten().next() else { cols.push(c.clone()); continue; };

        // Try epoch conversion (not TAQ time - that requires explicit command)
        if let Some(unit) = epoch_unit(v) {
            let mult = if v.abs() < 10_000_000_000 { 1000i64 } else { 1 };
            let scaled = i64_ca.clone() * mult;
            if let Ok(dt) = scaled.into_series().cast(&DataType::Datetime(unit, None)) {
                cols.push(dt.into_column());
                continue;
            }
        }
        cols.push(c.clone());
    }
    DataFrame::new(cols).unwrap_or(df)
}

/// Check if string looks like a pure integer
pub fn is_pure_int(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() { return true; }
    let s = s.strip_prefix('-').unwrap_or(s);
    !s.is_empty() && s.chars().all(|c| c.is_ascii_digit())
}

/// Check if string->i64->string round-trips (allows leading zeros)
pub fn int_roundtrip(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() { return true; }
    let (neg, digits) = if let Some(rest) = s.strip_prefix('-') { (true, rest) } else { (false, s) };
    let stripped = digits.trim_start_matches('0');
    let canon = if stripped.is_empty() { "0".to_string() } else if neg { format!("-{}", stripped) } else { stripped.to_string() };
    s.parse::<i64>().map(|n| n.to_string() == canon).unwrap_or(false)
}

/// Check if string->f64->string round-trips
pub fn float_roundtrip(s: &str) -> bool {
    let s = s.trim();
    if s.is_empty() { return true; }
    let Ok(f) = s.parse::<f64>() else { return false; };
    if !f.is_finite() { return false; }
    format!("{}", f).parse::<f64>().map(|b| b == f).unwrap_or(false)
}

/// Convert string columns to appropriate types (lossless, conservative)
pub fn convert_types(df: DataFrame) -> DataFrame {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    for col in df.get_columns() {
        let s = col.as_materialized_series();
        let Ok(str_ca) = s.str() else { cols.push(col.clone()); continue; };

        // Try i64: must be pure integers that round-trip exactly
        let all_int = str_ca.iter().all(|v| v.is_none() || (is_pure_int(v.unwrap()) && int_roundtrip(v.unwrap())));
        if all_int {
            if let Ok(int_s) = s.cast(&DataType::Int64) {
                cols.push(int_s.into_column());
                continue;
            }
        }
        // Try f64: must round-trip
        let all_float = str_ca.iter().all(|v| v.is_none() || float_roundtrip(v.unwrap()));
        if all_float {
            if let Ok(float_s) = s.cast(&DataType::Float64) {
                cols.push(float_s.into_column());
                continue;
            }
        }
        cols.push(col.clone());
    }
    convert_epoch_cols(DataFrame::new(cols).unwrap_or(df))
}

/// Apply a fixed schema to dataframe (cast columns to match)
pub fn apply_schema(df: DataFrame, schema: &Schema) -> (DataFrame, Option<String>) {
    let mut cols: Vec<Column> = Vec::with_capacity(df.width());
    let mut err_msg: Option<String> = None;
    let n_rows = df.height();

    for col in df.get_columns() {
        let name = col.name();
        let target = schema.get(name);
        if target.is_none() || col.dtype() == target.unwrap() {
            cols.push(col.clone());
            continue;
        }
        let target = target.unwrap();

        // Try epoch conversion: String → i64 → Datetime
        if matches!(target, DataType::Datetime(_, _)) && col.dtype() == &DataType::String {
            if let Ok(i64_s) = col.cast(&DataType::Int64) {
                if let Ok(i64_ca) = i64_s.as_materialized_series().i64() {
                    if let Some(v) = i64_ca.iter().flatten().next() {
                        if let Some(unit) = epoch_unit(v) {
                            let mult = if v.abs() < 10_000_000_000 { 1000i64 } else { 1 };
                            let scaled = i64_ca.clone() * mult;
                            if let Ok(dt) = scaled.into_series().cast(&DataType::Datetime(unit, None)) {
                                if dt.len() == n_rows { cols.push(dt.into_column()); continue; }
                            }
                        }
                    }
                }
            }
        }
        // Standard cast
        if let Ok(casted) = col.cast(target) {
            if casted.len() == n_rows { cols.push(casted); continue; }
        }
        if err_msg.is_none() {
            err_msg = Some(format!("Column '{}': failed to convert {:?} to {:?}", name, col.dtype(), target));
        }
        cols.push(col.clone());
    }
    (DataFrame::new(cols).unwrap_or(df), err_msg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_df;

    #[test]
    fn test_is_pure_int() {
        assert!(is_pure_int("123"));
        assert!(is_pure_int("-456"));
        assert!(is_pure_int("0"));
        assert!(is_pure_int(""));
        assert!(!is_pure_int("12.3"));
        assert!(!is_pure_int("1e5"));
    }

    #[test]
    fn test_int_roundtrip() {
        assert!(int_roundtrip("123"));
        assert!(int_roundtrip("-456"));
        assert!(int_roundtrip("0123"));
        assert!(!int_roundtrip("12.3"));
    }

    #[test]
    fn test_float_roundtrip() {
        assert!(float_roundtrip("10.5"));
        assert!(float_roundtrip("-3.14"));
        assert!(!float_roundtrip("abc"));
        assert!(!float_roundtrip("inf"));
    }

    #[test]
    fn test_convert_types_mixed() {
        let df = test_df!(
            "bbo_ind" => &["O", "E", "A"],
            "price" => &["10.5", "11", "12.5"],
            "volume" => &["100", "200", "300"]
        );
        let df = convert_types(df);
        assert_eq!(df.column("bbo_ind").unwrap().dtype(), &DataType::String);
        assert_eq!(df.column("price").unwrap().dtype(), &DataType::Float64);
        assert_eq!(df.column("volume").unwrap().dtype(), &DataType::Int64);
    }
}
