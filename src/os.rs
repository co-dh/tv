use polars::prelude::*;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// List directory contents as DataFrame
pub fn ls(dir: &Path) -> anyhow::Result<DataFrame> {
    let mut names: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut modified: Vec<i64> = Vec::new();
    let mut is_dir: Vec<&str> = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let e = entry?;
        let m = e.metadata()?;
        names.push(e.file_name().to_string_lossy().into());
        sizes.push(m.size());
        is_dir.push(if m.is_dir() { "x" } else { "" });
        modified.push(m.mtime() * 1_000_000); // microseconds
    }

    let modified_series = Series::new("modified".into(), modified)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    Ok(DataFrame::new(vec![
        Series::new("name".into(), names).into(),
        Series::new("size".into(), sizes).into(),
        modified_series.into(),
        Series::new("dir".into(), is_dir).into(),
    ])?)
}

/// List directory recursively as DataFrame
pub fn lr(dir: &Path) -> anyhow::Result<DataFrame> {
    let mut paths: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut modified: Vec<i64> = Vec::new();
    let mut is_dir: Vec<&'static str> = Vec::new();

    fn walk(dir: &Path, base: &Path, paths: &mut Vec<String>, sizes: &mut Vec<u64>, modified: &mut Vec<i64>, is_dir: &mut Vec<&'static str>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                if let Ok(m) = entry.metadata() {
                    let p = entry.path();
                    paths.push(p.strip_prefix(base).unwrap_or(&p).to_string_lossy().into());
                    sizes.push(m.size());
                    is_dir.push(if m.is_dir() { "x" } else { "" });
                    modified.push(m.mtime() * 1_000_000); // microseconds
                    if m.is_dir() { walk(&p, base, paths, sizes, modified, is_dir); }
                }
            }
        }
    }

    walk(dir, dir, &mut paths, &mut sizes, &mut modified, &mut is_dir);

    let modified_series = Series::new("modified".into(), modified)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    Ok(DataFrame::new(vec![
        Series::new("path".into(), paths).into(),
        Series::new("size".into(), sizes).into(),
        modified_series.into(),
        Series::new("dir".into(), is_dir).into(),
    ])?)
}
