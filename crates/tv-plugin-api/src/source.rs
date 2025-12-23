//! System source generators - shared between sqlite and adbc plugins
//! Returns data as simple tables that plugins can use

use std::process::Command;

/// Simple table: column names, types, and row data
pub struct SourceTable {
    pub cols: Vec<String>,
    pub types: Vec<u8>,  // 0=str, 1=int, 2=float
    pub rows: Vec<Vec<String>>,
}

impl SourceTable {
    fn new(cols: &[&str], types: &[u8]) -> Self {
        Self {
            cols: cols.iter().map(|s| s.to_string()).collect(),
            types: types.to_vec(),
            rows: Vec::new(),
        }
    }
    fn push(&mut self, row: Vec<String>) { self.rows.push(row); }
}

/// Run shell command, return stdout
fn run(cmd: &str) -> Option<String> {
    Command::new("sh").arg("-c").arg(cmd).output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
}

/// Process list
pub fn ps() -> SourceTable {
    let mut t = SourceTable::new(&["user", "pid", "cpu", "mem", "cmd"], &[0, 1, 2, 2, 0]);
    if let Some(out) = run("ps aux --no-headers") {
        for line in out.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() >= 11 {
                t.push(vec![f[0].into(), f[1].into(), f[2].into(), f[3].into(), f[10].into()]);
            }
        }
    }
    t
}

/// TCP connections
pub fn tcp() -> SourceTable {
    let mut t = SourceTable::new(&["proto", "local", "remote", "state"], &[0, 0, 0, 0]);
    if let Some(out) = run("ss -tn") {
        for line in out.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() >= 5 {
                t.push(vec!["tcp".into(), f[3].into(), f[4].into(), f[0].into()]);
            }
        }
    }
    t
}

/// UDP connections
pub fn udp() -> SourceTable {
    let mut t = SourceTable::new(&["proto", "local", "remote", "state"], &[0, 0, 0, 0]);
    if let Some(out) = run("ss -un") {
        for line in out.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() >= 5 {
                t.push(vec!["udp".into(), f[3].into(), f[4].into(), f[0].into()]);
            }
        }
    }
    t
}

/// Environment variables
pub fn env() -> SourceTable {
    let mut t = SourceTable::new(&["name", "value"], &[0, 0]);
    for (k, v) in std::env::vars() {
        t.push(vec![k, v]);
    }
    t
}

/// Disk usage
pub fn df() -> SourceTable {
    let mut t = SourceTable::new(&["fs", "size", "used", "avail", "pct", "mount"], &[0, 0, 0, 0, 0, 0]);
    if let Some(out) = run("df -h") {
        for line in out.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() >= 6 {
                t.push(vec![f[0].into(), f[1].into(), f[2].into(), f[3].into(), f[4].into(), f[5].into()]);
            }
        }
    }
    t
}

/// Mount points
pub fn mounts() -> SourceTable {
    let mut t = SourceTable::new(&["dev", "mount", "type", "opts"], &[0, 0, 0, 0]);
    if let Some(out) = run("mount") {
        for line in out.lines() {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() >= 6 {
                t.push(vec![f[0].into(), f[2].into(), f[4].into(), f[5].into()]);
            }
        }
    }
    t
}

/// Get source by name
pub fn get(name: &str) -> Option<SourceTable> {
    match name {
        "ps" => Some(ps()),
        "tcp" => Some(tcp()),
        "udp" => Some(udp()),
        "env" => Some(env()),
        "df" => Some(df()),
        "mounts" => Some(mounts()),
        _ => None,
    }
}

/// Convert to TSV string (for SQLite import)
pub fn to_tsv(t: &SourceTable) -> String {
    let mut s = t.cols.join("\t");
    s.push('\n');
    for row in &t.rows {
        s.push_str(&row.join("\t"));
        s.push('\n');
    }
    s
}
