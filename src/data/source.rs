//! System sources - generates tables from OS data using DuckDB
//! Handles source:ls, source:lr, source:ps, etc.

use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;

/// Source result: SQL to create table + data
pub struct SourceData {
    pub sql: String,
}

/// Parse source path and generate SQL for DuckDB
pub fn query(path: &str) -> Option<SourceData> {
    let rest = path.strip_prefix("source:")?;
    let (typ, arg) = rest.split_once(':').unwrap_or((rest, ""));
    match typ {
        "commands" => Some(commands()),
        "ls" => ls(Path::new(if arg.is_empty() { "." } else { arg })).ok(),
        "lr" => lr(Path::new(if arg.is_empty() { "." } else { arg })).ok(),
        "ps" => ps().ok(),
        "tcp" => tcp().ok(),
        "udp" => udp().ok(),
        "env" => env().ok(),
        "df" => df().ok(),
        "mounts" => mounts().ok(),
        "systemctl" => systemctl().ok(),
        "lsof" => lsof(arg.parse().ok()).ok(),
        "pacman" => pacman().ok(),
        _ => None,
    }
}

/// Available commands
fn commands() -> SourceData {
    let rows = [
        ("ls [dir]", "list directory"),
        ("lr [dir]", "list recursive (rg --files)"),
        ("ps", "process list"),
        ("tcp", "TCP connections"),
        ("udp", "UDP connections"),
        ("env", "environment variables"),
        ("df", "disk free"),
        ("mounts", "mount points"),
        ("systemctl", "systemd services"),
        ("lsof [pid]", "open files"),
        ("pacman", "installed packages"),
    ];
    let vals: Vec<String> = rows.iter()
        .map(|(c, d)| format!("('{}','{}')", c, d))
        .collect();
    SourceData { sql: format!("CREATE OR REPLACE TABLE df(command VARCHAR,description VARCHAR);INSERT INTO df VALUES{}", vals.join(",")) }
}

/// Format unix timestamp to ISO datetime
fn fmt_time(ts: i64) -> String {
    let secs = ts;
    let days = secs / 86400 + 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let h = (secs % 86400) / 3600;
    let mi = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", y, m, d, h, mi, s)
}

/// Escape SQL string
fn esc(s: &str) -> String { s.replace('\'', "''") }

/// Directory listing - path column shows containing folder
fn ls(dir: &Path) -> Result<SourceData, std::io::Error> {
    let mut rows = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    let abs = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let abs_str = abs.to_string_lossy();
    // ".." row: path is parent's parent (grandparent)
    if let Some(p) = abs.parent() {
        let m = p.metadata().ok();
        let sz = m.as_ref().map(|m| m.size() as i64).unwrap_or(0);
        let mt = m.map(|m| fmt_time(m.mtime())).unwrap_or_default();
        let pp = p.parent().map(|g| g.to_string_lossy().to_string()).unwrap_or_else(|| "/".into());
        rows.push(format!("('..','{}',{},'{}','x')", esc(&pp), sz, esc(&mt)));
    }

    for e in entries {
        let m = e.metadata()?;
        let name = e.file_name().to_string_lossy().to_string();
        let is_dir = if m.is_dir() { "x" } else { "" };
        // path = containing folder (current dir for all entries)
        rows.push(format!("('{}','{}',{},'{}','{}')", esc(&name), esc(&abs_str), m.size(), esc(&fmt_time(m.mtime())), is_dir));
    }

    let sql = format!("CREATE OR REPLACE TABLE df(name VARCHAR,path VARCHAR,size BIGINT,modified VARCHAR,dir VARCHAR);INSERT INTO df VALUES{}", rows.join(","));
    Ok(SourceData { sql })
}

/// Recursive listing via rg --files - path column shows containing folder
fn lr(dir: &Path) -> Result<SourceData, std::io::Error> {
    let out = Command::new("rg").args(["--files"]).current_dir(dir).output()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let mut files: Vec<&str> = text.lines().collect();
    files.sort();

    let abs = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    let mut rows = Vec::new();
    for p in files {
        let full = dir.join(p);
        if let Ok(m) = full.metadata() {
            let name = full.file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default();
            // path = containing folder (parent of the file)
            let parent = abs.join(p).parent().map(|d| d.to_string_lossy().to_string()).unwrap_or_default();
            rows.push(format!("('{}','{}',{},'{}','{}')", esc(&name), esc(&parent), m.size(),
                esc(&fmt_time(m.mtime())), if m.is_dir() { "x" } else { "" }));
        }
    }

    let sql = if rows.is_empty() {
        "CREATE OR REPLACE TABLE df(name VARCHAR,path VARCHAR,size BIGINT,modified VARCHAR,dir VARCHAR)".into()
    } else {
        format!("CREATE OR REPLACE TABLE df(name VARCHAR,path VARCHAR,size BIGINT,modified VARCHAR,dir VARCHAR);INSERT INTO df VALUES{}", rows.join(","))
    };
    Ok(SourceData { sql })
}

/// Process list from ps aux
fn ps() -> Result<SourceData, std::io::Error> {
    let out = Command::new("ps").args(["aux"]).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut rows = Vec::new();
    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 11 {
            rows.push(format!("('{}',{},{},{},{},{},'{}','{}','{}','{}','{}')",
                esc(p[0]), p[1].parse::<i64>().unwrap_or(0),
                p[2].parse::<f64>().unwrap_or(0.0), p[3].parse::<f64>().unwrap_or(0.0),
                p[4].parse::<i64>().unwrap_or(0), p[5].parse::<i64>().unwrap_or(0),
                esc(p[6]), esc(p[7]), esc(p[8]), esc(p[9]), esc(&p[10..].join(" "))));
        }
    }

    let sql = format!("CREATE OR REPLACE TABLE df(user VARCHAR,pid BIGINT,cpu DOUBLE,mem DOUBLE,vsz BIGINT,rss BIGINT,tty VARCHAR,stat VARCHAR,start VARCHAR,time VARCHAR,command VARCHAR);INSERT INTO df VALUES{}", rows.join(","));
    Ok(SourceData { sql })
}

/// TCP connections from ss
fn tcp() -> Result<SourceData, std::io::Error> { parse_net("tcp") }

/// UDP connections from ss
fn udp() -> Result<SourceData, std::io::Error> { parse_net("udp") }

fn parse_net(proto: &str) -> Result<SourceData, std::io::Error> {
    let flag = if proto == "tcp" { "-tn" } else { "-un" };
    let out = Command::new("ss").arg(flag).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut rows = Vec::new();
    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            rows.push(format!("('{}','{}','{}','{}')", proto, esc(p[3]), esc(p[4]), esc(p[0])));
        }
    }

    let sql = if rows.is_empty() {
        "CREATE OR REPLACE TABLE df(proto VARCHAR,local VARCHAR,remote VARCHAR,state VARCHAR)".into()
    } else {
        format!("CREATE OR REPLACE TABLE df(proto VARCHAR,local VARCHAR,remote VARCHAR,state VARCHAR);INSERT INTO df VALUES{}", rows.join(","))
    };
    Ok(SourceData { sql })
}

/// Environment variables
fn env() -> Result<SourceData, std::io::Error> {
    let rows: Vec<String> = std::env::vars()
        .map(|(k, v)| format!("('{}','{}')", esc(&k), esc(&v)))
        .collect();

    let sql = format!("CREATE OR REPLACE TABLE df(name VARCHAR,value VARCHAR);INSERT INTO df VALUES{}", rows.join(","));
    Ok(SourceData { sql })
}

/// Disk free from df command
fn df() -> Result<SourceData, std::io::Error> {
    let out = Command::new("df").arg("-h").output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut rows = Vec::new();
    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 6 {
            rows.push(format!("('{}','{}','{}','{}','{}','{}')",
                esc(p[0]), esc(p[1]), esc(p[2]), esc(p[3]), esc(p[4]), esc(p[5])));
        }
    }

    let sql = format!("CREATE OR REPLACE TABLE df(filesystem VARCHAR,size VARCHAR,used VARCHAR,avail VARCHAR,pct VARCHAR,mount VARCHAR);INSERT INTO df VALUES{}", rows.join(","));
    Ok(SourceData { sql })
}

/// Mount points from /proc/mounts
fn mounts() -> Result<SourceData, std::io::Error> {
    let mut rows = Vec::new();
    for line in fs::read_to_string("/proc/mounts")?.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 4 {
            rows.push(format!("('{}','{}','{}','{}')", esc(p[0]), esc(p[1]), esc(p[2]), esc(p[3])));
        }
    }

    let sql = format!("CREATE OR REPLACE TABLE df(device VARCHAR,mount VARCHAR,type VARCHAR,options VARCHAR);INSERT INTO df VALUES{}", rows.join(","));
    Ok(SourceData { sql })
}

/// Systemd services
fn systemctl() -> Result<SourceData, std::io::Error> {
    let out = Command::new("systemctl")
        .args(["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])
        .output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut rows = Vec::new();
    for line in text.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            rows.push(format!("('{}','{}','{}','{}','{}')",
                esc(p[0]), esc(p[1]), esc(p[2]), esc(p[3]), esc(&p[4..].join(" "))));
        }
    }

    let sql = if rows.is_empty() {
        "CREATE OR REPLACE TABLE df(unit VARCHAR,load VARCHAR,active VARCHAR,sub VARCHAR,description VARCHAR)".into()
    } else {
        format!("CREATE OR REPLACE TABLE df(unit VARCHAR,load VARCHAR,active VARCHAR,sub VARCHAR,description VARCHAR);INSERT INTO df VALUES{}", rows.join(","))
    };
    Ok(SourceData { sql })
}

/// Open file descriptors
fn lsof(pid: Option<i32>) -> Result<SourceData, std::io::Error> {
    let mut rows = Vec::new();
    let dirs: Vec<i32> = if let Some(p) = pid { vec![p] } else {
        fs::read_dir("/proc")?.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok()).collect()
    };

    for p in dirs {
        if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", p)) {
            for e in entries.flatten() {
                if let Ok(fd) = e.file_name().to_string_lossy().parse::<i64>() {
                    let link = fs::read_link(e.path()).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    rows.push(format!("({},{},'{}' )", p, fd, esc(&link)));
                }
            }
        }
    }

    let sql = if rows.is_empty() {
        "CREATE OR REPLACE TABLE df(pid BIGINT,fd BIGINT,path VARCHAR)".into()
    } else {
        format!("CREATE OR REPLACE TABLE df(pid BIGINT,fd BIGINT,path VARCHAR);INSERT INTO df VALUES{}", rows.join(","))
    };
    Ok(SourceData { sql })
}

/// Installed packages from pacman
fn pacman() -> Result<SourceData, std::io::Error> {
    use std::collections::{HashMap, HashSet};

    let orphan_text = Command::new("pacman").args(["-Qdt"]).output()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())?;
    let orphans: HashSet<String> = orphan_text.lines()
        .filter_map(|l| l.split_whitespace().next()).map(String::from).collect();

    let out = Command::new("pacman").args(["-Qi"]).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let parse_size = |s: &str| -> u64 {
        let p: Vec<&str> = s.split_whitespace().collect();
        if p.len() != 2 { return 0; }
        let n: f64 = p[0].parse().unwrap_or(0.0);
        let m: f64 = match p[1] { "B" => 1.0, "KiB" => 1024.0, "MiB" => 1048576.0, "GiB" => 1073741824.0, _ => 1.0 };
        (n * m) as u64
    };

    let parse_deps = |v: &str| -> Vec<String> {
        if v == "None" { vec![] }
        else { v.split_whitespace().map(|s| s.split(&['<','>','='][..]).next().unwrap_or(s).to_string()).collect() }
    };

    let (mut pkg_size, mut pkg_req_by): (HashMap<String, u64>, HashMap<String, Vec<String>>) = (HashMap::new(), HashMap::new());
    let (mut name, mut size, mut req_list) = (String::new(), 0u64, vec![]);

    for line in text.lines() {
        if line.is_empty() {
            if !name.is_empty() {
                pkg_size.insert(name.clone(), size);
                pkg_req_by.insert(std::mem::take(&mut name), std::mem::take(&mut req_list));
                size = 0;
            }
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            match k.trim() {
                "Name" => name = v.trim().into(),
                "Installed Size" => size = parse_size(v.trim()),
                "Required By" => req_list = parse_deps(v.trim()),
                _ => {}
            }
        }
    }
    if !name.is_empty() { pkg_size.insert(name.clone(), size); pkg_req_by.insert(name, req_list); }

    let mut rows = Vec::new();
    let (mut name, mut ver, mut desc, mut inst, mut reason) = (String::new(), String::new(), String::new(), String::new(), String::new());
    let (mut size, mut deps, mut reqs, mut deps_list) = (0u64, 0i64, 0i64, vec![]);

    for line in text.lines() {
        if line.is_empty() {
            if !name.is_empty() {
                let rsize = deps_list.iter().fold(size, |acc, dep| {
                    acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name)
                        .map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
                });
                let orphan = if orphans.contains(&name) { "x" } else { "" };
                rows.push(format!("('{}','{}',{},{},{},{},'{}','{}','{}','{}')",
                    esc(&name), esc(&ver), size / 1024, rsize / 1024, deps, reqs, orphan, esc(&reason), esc(&inst), esc(&desc)));
                name.clear(); ver.clear(); desc.clear(); inst.clear(); reason.clear();
                size = 0; deps = 0; reqs = 0; deps_list.clear();
            }
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim();
            match k.trim() {
                "Name" => name = v.into(),
                "Version" => ver = v.into(),
                "Description" => desc = v.into(),
                "Installed Size" => size = parse_size(v),
                "Install Date" => {
                    // Format: "Sat Nov 15 11:06:03 2025" (dayname month day time year)
                    let p: Vec<&str> = v.split_whitespace().collect();
                    if p.len() >= 5 {
                        let mon = match p[1] { "Jan"=>"01","Feb"=>"02","Mar"=>"03","Apr"=>"04","May"=>"05","Jun"=>"06",
                            "Jul"=>"07","Aug"=>"08","Sep"=>"09","Oct"=>"10","Nov"=>"11","Dec"=>"12",_=>"01" };
                        inst = format!("{}-{}-{:02}", p[4], mon, p[2].parse::<u32>().unwrap_or(0));
                    }
                }
                "Install Reason" => reason = if v.contains("dependency") { "dep".into() } else { "explicit".into() },
                "Depends On" => { deps_list = parse_deps(v); deps = deps_list.len() as i64; }
                "Required By" => reqs = if v == "None" { 0 } else { v.split_whitespace().count() as i64 },
                _ => {}
            }
        }
    }

    let sql = if rows.is_empty() {
        "CREATE OR REPLACE TABLE df(name VARCHAR,version VARCHAR,\"size(k)\" BIGINT,\"rsize(k)\" BIGINT,deps BIGINT,req_by BIGINT,orphan VARCHAR,reason VARCHAR,installed VARCHAR,description VARCHAR)".into()
    } else {
        format!("CREATE OR REPLACE TABLE df(name VARCHAR,version VARCHAR,\"size(k)\" BIGINT,\"rsize(k)\" BIGINT,deps BIGINT,req_by BIGINT,orphan VARCHAR,reason VARCHAR,installed VARCHAR,description VARCHAR);INSERT INTO df VALUES{}", rows.join(","))
    };
    Ok(SourceData { sql })
}
