//! System plugin - OS commands (ps, tcp, pacman, etc.) and file operations (ls, lr)

use crate::app::AppContext;
use crate::command::Command;
use crate::plugin::Plugin;
use crate::state::{ViewKind, ViewState};
use crate::data::table::{SimpleTable, Col};
use anyhow::Result;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

pub struct SystemPlugin;

impl Plugin for SystemPlugin {
    fn name(&self) -> &str { "system" }
    fn tab(&self) -> &str { "table" }

    fn matches(&self, name: &str) -> bool {
        matches!(name, "ps" | "mounts" | "tcp" | "udp" | "env" | "systemctl" | "pacman" | "cargo")
            || name.starts_with("lsof") || name.starts_with("journalctl")
    }

    fn handle(&self, _cmd: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> { None }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "ps" => Some(Box::new(SysCmd::Ps)),
            "mounts" => Some(Box::new(SysCmd::Mounts)),
            "tcp" => Some(Box::new(SysCmd::Tcp)),
            "udp" => Some(Box::new(SysCmd::Udp)),
            "env" => Some(Box::new(SysCmd::Env)),
            "systemctl" => Some(Box::new(SysCmd::Systemctl)),
            "pacman" => Some(Box::new(SysCmd::Pacman)),
            "cargo" => Some(Box::new(SysCmd::Cargo)),
            "lsof" => Some(Box::new(Lsof { pid: arg.parse().ok() })),
            "journalctl" => Some(Box::new(Journalctl { n: arg.parse().unwrap_or(1000) })),
            _ => None,
        }
    }
}

/// Unified system command enum
#[derive(Clone, Copy)]
pub enum SysCmd { Ps, Mounts, Tcp, Udp, Env, Systemctl, Pacman, Cargo }

impl Command for SysCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, t) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
            SysCmd::Cargo => ("cargo", cargo()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new_memory(id, name, ViewKind::Table, Box::new(t)));
        Ok(())
    }
    fn to_str(&self) -> String {
        match self {
            SysCmd::Ps => "ps", SysCmd::Mounts => "mounts",
            SysCmd::Tcp => "tcp", SysCmd::Udp => "udp", SysCmd::Env => "env",
            SysCmd::Systemctl => "systemctl", SysCmd::Pacman => "pacman", SysCmd::Cargo => "cargo",
        }.into()
    }
}

/// lsof with optional pid filter
pub struct Lsof { pub pid: Option<i32> }

impl Command for Lsof {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let t = lsof(self.pid)?;
        let name = self.pid.map(|p| format!("lsof:{}", p)).unwrap_or("lsof".into());
        let id = app.next_id();
        app.stack.push(ViewState::new_memory(id, name, ViewKind::Table, Box::new(t)));
        Ok(())
    }
    fn to_str(&self) -> String { self.pid.map(|p| format!("lsof {}", p)).unwrap_or("lsof".into()) }
}

/// journalctl with line count
pub struct Journalctl { pub n: usize }

impl Command for Journalctl {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let t = journalctl(self.n)?;
        let id = app.next_id();
        app.stack.push(ViewState::new_memory(id, "journalctl", ViewKind::Table, Box::new(t)));
        Ok(())
    }
    fn to_str(&self) -> String { format!("journalctl {}", self.n) }
}

// ============================================================================
// OS Data Functions
// ============================================================================

/// Run command and return stdout as String
fn run_cmd(cmd: &str, args: &[&str]) -> Result<String> {
    let out = std::process::Command::new(cmd).args(args).output()?;
    Ok(String::from_utf8_lossy(&out.stdout).into())
}

/// List directory contents as SimpleTable
pub fn ls(dir: &Path) -> Result<SimpleTable> {
    let mut names: Vec<String> = Vec::new();
    let mut paths: Vec<String> = Vec::new();
    let mut sizes: Vec<i64> = Vec::new();
    let mut modified: Vec<String> = Vec::new();
    let mut is_dir: Vec<String> = Vec::new();

    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    // Add ".." for parent directory
    let abs_dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    if let Some(parent) = abs_dir.parent() {
        names.push("..".into());
        paths.push(parent.to_string_lossy().into());
        let m = parent.metadata().ok();
        sizes.push(m.as_ref().map(|m| m.size() as i64).unwrap_or(0));
        is_dir.push("x".into());
        modified.push(m.map(|m| fmt_time(m.mtime())).unwrap_or_default());
    }

    for e in entries {
        let m = e.metadata()?;
        let full_path = e.path().canonicalize().unwrap_or_else(|_| e.path());
        names.push(e.file_name().to_string_lossy().into());
        paths.push(full_path.to_string_lossy().into());
        sizes.push(m.size() as i64);
        is_dir.push(if m.is_dir() { "x" } else { "" }.into());
        modified.push(fmt_time(m.mtime()));
    }

    Ok(SimpleTable::from_cols(vec![
        Col::str("name", names), Col::str("path", paths), Col::int("size", sizes),
        Col::str("modified", modified), Col::str("dir", is_dir),
    ]))
}

/// Format unix timestamp to ISO datetime string
fn fmt_time(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

/// List directory recursively (respects .gitignore via rg)
pub fn lr(dir: &Path) -> Result<SimpleTable> {
    use std::process::Command;
    let out = Command::new("rg").args(["--files"]).current_dir(dir).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut paths: Vec<String> = Vec::new();
    let mut sizes: Vec<i64> = Vec::new();
    let mut modified: Vec<String> = Vec::new();

    let mut files: Vec<&str> = text.lines().collect();
    files.sort();
    for p in files {
        let full = dir.join(p);
        if let Ok(m) = full.metadata() {
            paths.push(p.to_string());  // relative path
            sizes.push(m.size() as i64);
            modified.push(fmt_time(m.mtime()));
        }
    }

    Ok(SimpleTable::from_cols(vec![
        Col::str("path", paths), Col::int("size", sizes), Col::str("modified", modified),
    ]))
}

/// Process list from ps aux
fn ps() -> Result<SimpleTable> {
    let text = run_cmd("ps", &["aux"])?;

    let (mut users, mut pids, mut cpus, mut mems) = (vec![], vec![], vec![], vec![]);
    let (mut vszs, mut rsss, mut ttys, mut stats) = (vec![], vec![], vec![], vec![]);
    let (mut starts, mut times, mut cmds) = (vec![], vec![], vec![]);

    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 11 {
            users.push(p[0].to_string()); pids.push(p[1].parse::<i64>().unwrap_or(0));
            cpus.push(p[2].parse::<f64>().unwrap_or(0.0)); mems.push(p[3].parse::<f64>().unwrap_or(0.0));
            vszs.push(p[4].parse::<i64>().unwrap_or(0)); rsss.push(p[5].parse::<i64>().unwrap_or(0));
            ttys.push(p[6].to_string()); stats.push(p[7].to_string());
            starts.push(p[8].to_string()); times.push(p[9].to_string());
            cmds.push(p[10..].join(" "));
        }
    }

    Ok(SimpleTable::from_cols(vec![
        Col::str("user", users), Col::int("pid", pids), Col::float("%cpu", cpus), Col::float("%mem", mems),
        Col::int("vsz", vszs), Col::int("rss", rsss), Col::str("tty", ttys), Col::str("stat", stats),
        Col::str("start", starts), Col::str("time", times), Col::str("command", cmds),
    ]))
}

/// Mount points from /proc/mounts
fn mounts() -> Result<SimpleTable> {
    let (mut devs, mut mps, mut types, mut opts) = (vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string("/proc/mounts")?.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 4 {
            devs.push(p[0].to_string()); mps.push(p[1].to_string());
            types.push(p[2].to_string()); opts.push(p[3].to_string());
        }
    }
    Ok(SimpleTable::from_cols(vec![
        Col::str("device", devs), Col::str("mount", mps), Col::str("type", types), Col::str("options", opts),
    ]))
}

/// TCP connections from /proc/net/tcp
fn tcp() -> Result<SimpleTable> { parse_net("/proc/net/tcp") }

/// UDP connections from /proc/net/udp
fn udp() -> Result<SimpleTable> { parse_net("/proc/net/udp") }

/// Parse /proc/net/tcp or udp
fn parse_net(path: &str) -> Result<SimpleTable> {
    let (mut la, mut lp, mut ra, mut rp, mut st, mut ino) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string(path)?.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 10 {
            let (a1, p1) = parse_addr(p[1]); let (a2, p2) = parse_addr(p[2]);
            la.push(a1); lp.push(p1 as i64); ra.push(a2); rp.push(p2 as i64);
            st.push(parse_tcp_state(p[3])); ino.push(p[9].parse::<i64>().unwrap_or(0));
        }
    }
    Ok(SimpleTable::from_cols(vec![
        Col::str("local_addr", la), Col::int("local_port", lp), Col::str("remote_addr", ra),
        Col::int("remote_port", rp), Col::str("state", st), Col::int("inode", ino),
    ]))
}

/// Parse hex IP:port to (dotted IP, port)
fn parse_addr(s: &str) -> (String, u32) {
    let p: Vec<&str> = s.split(':').collect();
    if p.len() == 2 {
        let a = u32::from_str_radix(p[0], 16).unwrap_or(0);
        let port = u32::from_str_radix(p[1], 16).unwrap_or(0);
        (format!("{}.{}.{}.{}", a & 0xff, (a >> 8) & 0xff, (a >> 16) & 0xff, (a >> 24) & 0xff), port)
    } else { (String::new(), 0) }
}

/// TCP state code to string
fn parse_tcp_state(s: &str) -> String {
    match s {
        "01" => "ESTABLISHED", "02" => "SYN_SENT", "03" => "SYN_RECV",
        "04" => "FIN_WAIT1", "05" => "FIN_WAIT2", "06" => "TIME_WAIT",
        "07" => "CLOSE", "08" => "CLOSE_WAIT", "09" => "LAST_ACK",
        "0A" => "LISTEN", "0B" => "CLOSING", _ => "UNKNOWN",
    }.into()
}

/// Open file descriptors from /proc/[pid]/fd
fn lsof(pid: Option<i32>) -> Result<SimpleTable> {
    let (mut pids, mut fds, mut paths) = (vec![], vec![], vec![]);
    let dirs: Vec<i32> = if let Some(p) = pid { vec![p] } else {
        fs::read_dir("/proc")?.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok()).collect()
    };
    for p in dirs {
        if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", p)) {
            for e in entries.flatten() {
                if let Ok(fd) = e.file_name().to_string_lossy().parse::<i64>() {
                    let link = fs::read_link(e.path()).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    pids.push(p as i64); fds.push(fd); paths.push(link);
                }
            }
        }
    }
    Ok(SimpleTable::from_cols(vec![
        Col::int("pid", pids), Col::int("fd", fds), Col::str("path", paths),
    ]))
}

/// Environment variables
fn env() -> Result<SimpleTable> {
    let (names, vals): (Vec<String>, Vec<String>) = std::env::vars().unzip();
    Ok(SimpleTable::from_cols(vec![Col::str("name", names), Col::str("value", vals)]))
}

/// Total system memory in bytes
pub fn mem_total() -> u64 {
    fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()).map(|kb| kb * 1024))
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

/// Systemd services from systemctl
fn systemctl() -> Result<SimpleTable> {
    let text = run_cmd("systemctl", &["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])?;
    let (mut units, mut loads, mut actives, mut subs, mut descs) = (vec![], vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            units.push(p[0].to_string()); loads.push(p[1].to_string());
            actives.push(p[2].to_string()); subs.push(p[3].to_string());
            descs.push(p[4..].join(" "));
        }
    }
    Ok(SimpleTable::from_cols(vec![
        Col::str("unit", units), Col::str("load", loads), Col::str("active", actives),
        Col::str("sub", subs), Col::str("description", descs),
    ]))
}

/// Journal logs from journalctl
fn journalctl(n: usize) -> Result<SimpleTable> {
    let ns = n.to_string();
    let text = run_cmd("journalctl", &["--no-pager", "-o", "short-iso", "-n", &ns])?;
    let (mut times, mut hosts, mut units, mut msgs) = (vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        if p.len() >= 4 {
            times.push(p[0].to_string()); hosts.push(p[1].to_string());
            units.push(p[2].split('[').next().unwrap_or(p[2]).trim_end_matches(':').to_string());
            msgs.push(p[3].to_string());
        } else {
            times.push("".into()); hosts.push("".into()); units.push("".into()); msgs.push(line.into());
        }
    }
    Ok(SimpleTable::from_cols(vec![
        Col::str("time", times), Col::str("host", hosts), Col::str("unit", units), Col::str("message", msgs),
    ]))
}

/// Parse pacman size "136.04 KiB" to bytes
fn parse_size(s: &str) -> u64 {
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() != 2 { return 0; }
    let n: f64 = p[0].parse().unwrap_or(0.0);
    let m: f64 = match p[1] { "B" => 1.0, "KiB" => 1024.0, "MiB" => 1048576.0, "GiB" => 1073741824.0, _ => 1.0 };
    (n * m) as u64
}

/// Parse pacman date to ISO format
fn parse_date(s: &str) -> String {
    let p: Vec<&str> = s.split_whitespace().collect();
    if p.len() < 5 { return s.into(); }
    let mon = match p[1] {
        "Jan" => "01", "Feb" => "02", "Mar" => "03", "Apr" => "04", "May" => "05", "Jun" => "06",
        "Jul" => "07", "Aug" => "08", "Sep" => "09", "Oct" => "10", "Nov" => "11", "Dec" => "12", _ => return s.into(),
    };
    format!("{}-{}-{:02}", p[4], mon, p[2].parse::<u32>().unwrap_or(0))
}

/// Parse dependency list from pacman field value
fn parse_deps(v: &str) -> Vec<String> {
    if v == "None" { vec![] } else { v.split_whitespace().map(|s| s.split(&['<','>','='][..]).next().unwrap_or(s).to_string()).collect() }
}

/// Calculate rsize: package size + exclusive deps (deps only required by this package)
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &std::collections::HashMap<String, u64>, pkg_req_by: &std::collections::HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

/// Cache file path for cargo latest versions
fn cargo_cache_path() -> std::path::PathBuf {
    dirs::cache_dir().unwrap_or_else(|| std::path::PathBuf::from(".")).join("tv/cargo_versions.csv")
}

/// Load version cache from disk: name -> (version, timestamp)
fn load_ver_cache() -> std::collections::HashMap<String, (String, i64)> {
    let mut cache = std::collections::HashMap::new();
    let path = cargo_cache_path();
    if let Ok(content) = fs::read_to_string(&path) {
        for line in content.lines().skip(1) {  // skip header
            let p: Vec<&str> = line.split(',').collect();
            if p.len() >= 3 {
                cache.insert(p[0].to_string(), (p[1].to_string(), p[2].parse().unwrap_or(0)));
            }
        }
    }
    cache
}

/// Save version cache to disk
fn save_ver_cache(cache: &std::collections::HashMap<String, (String, i64)>) {
    let path = cargo_cache_path();
    if let Some(parent) = path.parent() { fs::create_dir_all(parent).ok(); }
    let mut content = String::from("name,version,timestamp\n");
    for (name, (ver, ts)) in cache {
        content.push_str(&format!("{},{},{}\n", name, ver, ts));
    }
    fs::write(path, content).ok();
}

/// Fetch latest version from crates.io (fully detached from terminal)
fn fetch_latest(name: &str) -> Option<String> {
    use std::os::unix::process::CommandExt;
    let mut cmd = std::process::Command::new("cargo");
    cmd.args(["search", name, "--limit", "1", "--color=never"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    // Detach from controlling terminal via new session
    unsafe { cmd.pre_exec(|| { nix::unistd::setsid().ok(); Ok(()) }); }
    cmd.output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .and_then(|s| s.lines().next().map(String::from))
        .and_then(|l| l.split('"').nth(1).map(String::from))
}

/// Update stale cache entries in background (older than 1 day)
fn update_ver_cache_bg(names: Vec<String>) {
    std::thread::spawn(move || {
        let now = chrono::Utc::now().timestamp();
        let day = 86400;
        let mut cache = load_ver_cache();
        let mut cnt = 0;
        for name in names {
            let stale = cache.get(&name).map(|(_, ts)| now - ts > day).unwrap_or(true);
            if stale {
                if let Some(ver) = fetch_latest(&name) {
                    cache.insert(name, (ver, now));
                    cnt += 1;
                    if cnt % 10 == 0 { save_ver_cache(&cache); }  // save every 10 fetches
                }
            }
        }
        save_ver_cache(&cache);
    });
}

/// Get latest version from cache (returns cached value, updates in background)
fn latest_ver(name: &str, cache: &std::collections::HashMap<String, (String, i64)>) -> String {
    cache.get(name).map(|(v, _)| v.clone()).unwrap_or_default()
}

/// Project dependencies from `cargo metadata` (like pacman for Rust)
fn cargo() -> Result<SimpleTable> {
    use std::collections::{HashMap, HashSet};
    let text = run_cmd("cargo", &["metadata", "--format-version", "1"])?;
    let json: serde_json::Value = serde_json::from_str(&text)?;

    // Get linux-compiled packages via --filter-platform
    let linux_text = run_cmd("cargo", &["metadata", "--format-version", "1", "--filter-platform", "x86_64-unknown-linux-gnu"])?;
    let linux_json: serde_json::Value = serde_json::from_str(&linux_text)?;
    let linux_pkgs: HashSet<String> = linux_json["packages"].as_array()
        .map(|a| a.iter().filter_map(|p| p["name"].as_str().map(String::from)).collect())
        .unwrap_or_default();

    // Build package info map: id -> (name, ver, desc, deps)
    let mut pkg_info: HashMap<String, (String, String, String, Vec<String>)> = HashMap::new();
    let mut pkg_size: HashMap<String, u64> = HashMap::new();
    if let Some(pkgs) = json["packages"].as_array() {
        for p in pkgs {
            let id = p["id"].as_str().unwrap_or("").to_string();
            let name = p["name"].as_str().unwrap_or("").to_string();
            let ver = p["version"].as_str().unwrap_or("").to_string();
            let desc = p["description"].as_str().unwrap_or("").to_string();
            let deps: Vec<String> = p["dependencies"].as_array()
                .map(|a| a.iter().filter_map(|d| d["name"].as_str().map(String::from)).collect())
                .unwrap_or_default();
            // Estimate size from manifest_path's parent dir
            let size = p["manifest_path"].as_str()
                .and_then(|mp| std::path::Path::new(mp).parent())
                .map(|d| dir_size(d)).unwrap_or(0);
            pkg_size.insert(name.clone(), size);
            pkg_info.insert(id, (name, ver, desc, deps));
        }
    }

    // Build reverse deps from resolve graph
    let mut req_by: HashMap<String, Vec<String>> = HashMap::new();
    let mut resolved: Vec<String> = vec![];
    if let Some(resolve) = json["resolve"].as_object() {
        if let Some(nodes) = resolve["nodes"].as_array() {
            for n in nodes {
                let id = n["id"].as_str().unwrap_or("");
                let name = pkg_info.get(id).map(|i| i.0.clone()).unwrap_or_default();
                if name.is_empty() { continue; }
                resolved.push(id.to_string());
                if let Some(deps) = n["deps"].as_array() {
                    for d in deps {
                        let dep_name = d["name"].as_str().unwrap_or("");
                        req_by.entry(dep_name.to_string()).or_default().push(name.clone());
                    }
                }
            }
        }
    }

    // Build output vectors
    let (mut names, mut vers, mut latest, mut descs, mut plat) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut sizes, mut rsizes, mut deps_cnt, mut req_cnt): (Vec<i64>, Vec<i64>, Vec<i64>, Vec<i64>) = (vec![], vec![], vec![], vec![]);
    let ver_cache = load_ver_cache();
    let mut all_names: Vec<String> = vec![];

    for id in &resolved {
        if let Some((name, ver, desc, deps)) = pkg_info.get(id) {
            let size = pkg_size.get(name).copied().unwrap_or(0);
            let rsize = calc_rsize(name, size, deps, &pkg_size, &req_by);
            let reqs = req_by.get(name).map(|v| v.len()).unwrap_or(0);
            let lat = latest_ver(name, &ver_cache);
            all_names.push(name.clone());
            names.push(name.clone()); vers.push(ver.clone()); latest.push(lat); descs.push(desc.clone());
            sizes.push((size / 1024) as i64); rsizes.push((rsize / 1024) as i64);
            deps_cnt.push(deps.len() as i64); req_cnt.push(reqs as i64);
            // Infer platform from package name or linux compilation
            let p = if linux_pkgs.contains(name) { "linux" }
                else if name.contains("windows") { "windows" }
                else if name.contains("macos") || name.contains("core-foundation") || name.contains("objc") { "macos" }
                else if name.contains("android") { "android" }
                else if name.contains("wasm") || name.contains("js-sys") || name.contains("web-sys") { "wasm" }
                else { "" };
            plat.push(p.to_string());
        }
    }

    // Update stale cache entries in background
    update_ver_cache_bg(all_names);

    Ok(SimpleTable::from_cols(vec![
        Col::str("name", names), Col::str("version", vers), Col::str("latest", latest),
        Col::int("size(k)", sizes), Col::int("rsize(k)", rsizes),
        Col::int("deps", deps_cnt), Col::int("req_by", req_cnt),
        Col::str("platform", plat), Col::str("description", descs),
    ]))
}

/// Calculate directory size recursively
fn dir_size(path: &std::path::Path) -> u64 {
    fs::read_dir(path).ok().map(|entries| {
        entries.filter_map(|e| e.ok()).map(|e| {
            let m = e.metadata().ok();
            if m.as_ref().map(|m| m.is_dir()).unwrap_or(false) {
                dir_size(&e.path())
            } else {
                m.map(|m| m.len()).unwrap_or(0)
            }
        }).sum()
    }).unwrap_or(0)
}

/// Installed packages from pacman (Arch Linux)
fn pacman() -> Result<SimpleTable> {
    use std::collections::{HashSet, HashMap};

    let orphan_text = run_cmd("pacman", &["-Qdt"])?;
    let orphans: HashSet<String> = orphan_text.lines().filter_map(|l| l.split_whitespace().next()).map(String::from).collect();

    let text = run_cmd("pacman", &["-Qi"])?;

    // First pass: collect sizes and who requires each package
    let (mut pkg_size, mut pkg_req_by): (HashMap<String, u64>, HashMap<String, Vec<String>>) = (HashMap::new(), HashMap::new());
    let (mut name, mut size, mut req_list) = (String::new(), 0u64, vec![]);

    for line in text.lines() {
        if line.is_empty() {
            if !name.is_empty() { pkg_size.insert(name.clone(), size); pkg_req_by.insert(std::mem::take(&mut name), std::mem::take(&mut req_list)); size = 0; }
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

    // Second pass: build output
    let (mut names, mut vers, mut descs, mut sizes, mut rsizes): (Vec<String>, Vec<String>, Vec<String>, Vec<i64>, Vec<i64>) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut installed, mut reasons, mut deps_cnt, mut req_cnt, mut orphan_flags): (Vec<String>, Vec<String>, Vec<i64>, Vec<i64>, Vec<String>) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut name, mut ver, mut desc, mut inst, mut reason) = (String::new(), String::new(), String::new(), String::new(), String::new());
    let (mut size, mut deps, mut reqs, mut deps_list) = (0u64, 0i64, 0i64, vec![]);

    // Helper to push current package to output vectors
    let mut push_pkg = |n: &mut String, v: &mut String, d: &mut String, i: &mut String, r: &mut String, sz: &mut u64, dc: &mut i64, rc: &mut i64, dl: &mut Vec<String>| {
        if n.is_empty() { return; }
        rsizes.push((calc_rsize(n, *sz, dl, &pkg_size, &pkg_req_by) / 1024) as i64);
        orphan_flags.push(if orphans.contains(n.as_str()) { "x".into() } else { "".into() });
        names.push(std::mem::take(n)); vers.push(std::mem::take(v)); descs.push(std::mem::take(d));
        sizes.push((*sz / 1024) as i64); installed.push(std::mem::take(i)); reasons.push(std::mem::take(r));
        deps_cnt.push(*dc); req_cnt.push(*rc); dl.clear(); *sz = 0; *dc = 0; *rc = 0;
    };

    for line in text.lines() {
        if line.is_empty() { push_pkg(&mut name, &mut ver, &mut desc, &mut inst, &mut reason, &mut size, &mut deps, &mut reqs, &mut deps_list); continue; }
        if let Some((k, v)) = line.split_once(':') {
            let v = v.trim();
            match k.trim() {
                "Name" => name = v.into(), "Version" => ver = v.into(), "Description" => desc = v.into(),
                "Installed Size" => size = parse_size(v), "Install Date" => inst = parse_date(v),
                "Install Reason" => reason = if v.contains("dependency") { "dep".into() } else { "explicit".into() },
                "Depends On" => { deps_list = parse_deps(v); deps = deps_list.len() as i64; },
                "Required By" => reqs = if v == "None" { 0 } else { v.split_whitespace().count() as i64 },
                _ => {}
            }
        }
    }
    push_pkg(&mut name, &mut ver, &mut desc, &mut inst, &mut reason, &mut size, &mut deps, &mut reqs, &mut deps_list);

    Ok(SimpleTable::from_cols(vec![
        Col::str("name", names), Col::str("version", vers),
        Col::int("size(k)", sizes), Col::int("rsize(k)", rsizes),
        Col::int("deps", deps_cnt), Col::int("req_by", req_cnt),
        Col::str("orphan", orphan_flags), Col::str("reason", reasons),
        Col::str("installed", installed), Col::str("description", descs),
    ]))
}
