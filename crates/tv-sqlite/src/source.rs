//! System data sources - generate tables from OS data
//! Used for "source:ps", "source:ls:/path" etc.

use crate::{Cell, ColType, SimpleTable};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;

/// Available source commands with descriptions
const COMMANDS: &[(&str, &str)] = &[
    ("ls [dir]", "list directory"),
    ("lr [dir]", "list recursive (rg --files)"),
    ("ps", "process list"),
    ("mounts", "mount points"),
    ("tcp", "TCP connections"),
    ("udp", "UDP connections"),
    ("env", "environment variables"),
    ("df", "disk free"),
    ("lsof [pid]", "open files"),
    ("systemctl", "systemd services"),
    ("journalctl [n]", "journal logs"),
    ("pacman", "installed packages"),
    ("cargo", "cargo dependencies"),
];

/// Return table of available source commands
pub fn commands() -> SimpleTable {
    let names: Vec<Cell> = COMMANDS.iter().map(|(n, _)| Cell::Str(n.to_string())).collect();
    let descs: Vec<Cell> = COMMANDS.iter().map(|(_, d)| Cell::Str(d.to_string())).collect();
    cols_to_table(vec![
        ("command", ColType::Str, names),
        ("description", ColType::Str, descs),
    ])
}

/// Parse source path "source:type:args" and generate table
pub fn query(path: &str) -> Option<SimpleTable> {
    let rest = path.strip_prefix("source:")?;
    let (typ, arg) = rest.split_once(':').unwrap_or((rest, ""));
    match typ {
        "commands" => Some(commands()),
        "ls" => ls(Path::new(if arg.is_empty() { "." } else { arg })).ok(),
        "lr" => lr(Path::new(if arg.is_empty() { "." } else { arg })).ok(),
        "ps" => ps().ok(),
        "mounts" => mounts().ok(),
        "tcp" => tcp().ok(),
        "udp" => udp().ok(),
        "env" => env().ok(),
        "systemctl" => systemctl().ok(),
        "pacman" => pacman().ok(),
        "cargo" => cargo().ok(),
        "df" => df().ok(),
        "lsof" => lsof(arg.parse().ok()).ok(),
        "journalctl" => journalctl(arg.parse().unwrap_or(1000)).ok(),
        _ => None,
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Build table from column vectors
fn cols_to_table(cols: Vec<(&str, ColType, Vec<Cell>)>) -> SimpleTable {
    let names = cols.iter().map(|(n, _, _)| n.to_string()).collect();
    let types = cols.iter().map(|(_, t, _)| *t).collect();
    let rows = if cols.is_empty() { 0 } else { cols[0].2.len() };
    let data = (0..rows).map(|r| cols.iter().map(|(_, _, v)| v[r].clone()).collect()).collect();
    SimpleTable::new(names, types, data)
}

/// Run command and return stdout
fn run_cmd(cmd: &str, args: &[&str]) -> Result<String, std::io::Error> {
    let out = Command::new(cmd).args(args).output()?;
    Ok(String::from_utf8_lossy(&out.stdout).into())
}

/// Format unix timestamp to ISO datetime
fn fmt_time(ts: i64) -> String {
    chrono::DateTime::from_timestamp(ts, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

// ── Directory Listing ────────────────────────────────────────────────────────

/// List directory contents
pub fn ls(dir: &Path) -> Result<SimpleTable, std::io::Error> {
    let mut names: Vec<Cell> = Vec::new();
    let mut paths: Vec<Cell> = Vec::new();
    let mut sizes: Vec<Cell> = Vec::new();
    let mut modified: Vec<Cell> = Vec::new();
    let mut is_dir: Vec<Cell> = Vec::new();

    let mut entries: Vec<_> = fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    // Add ".." for parent directory
    let abs_dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    if let Some(parent) = abs_dir.parent() {
        names.push(Cell::Str("..".into()));
        paths.push(Cell::Str(parent.to_string_lossy().into()));
        let m = parent.metadata().ok();
        sizes.push(Cell::Int(m.as_ref().map(|m| m.size() as i64).unwrap_or(0)));
        is_dir.push(Cell::Str("x".into()));
        modified.push(Cell::Str(m.map(|m| fmt_time(m.mtime())).unwrap_or_default()));
    }

    for e in entries {
        let m = e.metadata()?;
        let full_path = e.path().canonicalize().unwrap_or_else(|_| e.path());
        names.push(Cell::Str(e.file_name().to_string_lossy().into()));
        paths.push(Cell::Str(full_path.to_string_lossy().into()));
        sizes.push(Cell::Int(m.size() as i64));
        is_dir.push(Cell::Str(if m.is_dir() { "x" } else { "" }.into()));
        modified.push(Cell::Str(fmt_time(m.mtime())));
    }

    Ok(cols_to_table(vec![
        ("name", ColType::Str, names), ("path", ColType::Str, paths),
        ("size", ColType::Int, sizes), ("modified", ColType::Str, modified),
        ("dir", ColType::Str, is_dir),
    ]))
}

/// List directory recursively (respects .gitignore via rg)
pub fn lr(dir: &Path) -> Result<SimpleTable, std::io::Error> {
    let out = Command::new("rg").args(["--files"]).current_dir(dir).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut paths: Vec<Cell> = Vec::new();
    let mut sizes: Vec<Cell> = Vec::new();
    let mut modified: Vec<Cell> = Vec::new();

    let mut files: Vec<&str> = text.lines().collect();
    files.sort();
    for p in files {
        let full = dir.join(p);
        if let Ok(m) = full.metadata() {
            paths.push(Cell::Str(p.to_string()));
            sizes.push(Cell::Int(m.size() as i64));
            modified.push(Cell::Str(fmt_time(m.mtime())));
        }
    }

    Ok(cols_to_table(vec![
        ("path", ColType::Str, paths), ("size", ColType::Int, sizes),
        ("modified", ColType::Str, modified),
    ]))
}

// ── Process List ─────────────────────────────────────────────────────────────

/// Process list from ps aux
fn ps() -> Result<SimpleTable, std::io::Error> {
    let text = run_cmd("ps", &["aux"])?;

    let (mut users, mut pids, mut cpus, mut mems) = (vec![], vec![], vec![], vec![]);
    let (mut vszs, mut rsss, mut ttys, mut stats) = (vec![], vec![], vec![], vec![]);
    let (mut starts, mut times, mut cmds) = (vec![], vec![], vec![]);

    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 11 {
            users.push(Cell::Str(p[0].into()));
            pids.push(Cell::Int(p[1].parse().unwrap_or(0)));
            cpus.push(Cell::Float(p[2].parse().unwrap_or(0.0)));
            mems.push(Cell::Float(p[3].parse().unwrap_or(0.0)));
            vszs.push(Cell::Int(p[4].parse().unwrap_or(0)));
            rsss.push(Cell::Int(p[5].parse().unwrap_or(0)));
            ttys.push(Cell::Str(p[6].into()));
            stats.push(Cell::Str(p[7].into()));
            starts.push(Cell::Str(p[8].into()));
            times.push(Cell::Str(p[9].into()));
            cmds.push(Cell::Str(p[10..].join(" ")));
        }
    }

    Ok(cols_to_table(vec![
        ("user", ColType::Str, users), ("pid", ColType::Int, pids),
        ("%cpu", ColType::Float, cpus), ("%mem", ColType::Float, mems),
        ("vsz", ColType::Int, vszs), ("rss", ColType::Int, rsss),
        ("tty", ColType::Str, ttys), ("stat", ColType::Str, stats),
        ("start", ColType::Str, starts), ("time", ColType::Str, times),
        ("command", ColType::Str, cmds),
    ]))
}

// ── Mount Points ─────────────────────────────────────────────────────────────

/// Mount points from /proc/mounts
fn mounts() -> Result<SimpleTable, std::io::Error> {
    let (mut devs, mut mps, mut types, mut opts) = (vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string("/proc/mounts")?.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 4 {
            devs.push(Cell::Str(p[0].into()));
            mps.push(Cell::Str(p[1].into()));
            types.push(Cell::Str(p[2].into()));
            opts.push(Cell::Str(p[3].into()));
        }
    }
    Ok(cols_to_table(vec![
        ("device", ColType::Str, devs), ("mount", ColType::Str, mps),
        ("type", ColType::Str, types), ("options", ColType::Str, opts),
    ]))
}

// ── Disk Free ────────────────────────────────────────────────────────────────

/// Disk free space from df command
fn df() -> Result<SimpleTable, std::io::Error> {
    let text = run_cmd("df", &["-h"])?;
    let (mut fs, mut size, mut used, mut avail, mut pct, mut mount) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 6 {
            fs.push(Cell::Str(p[0].into()));
            size.push(Cell::Str(p[1].into()));
            used.push(Cell::Str(p[2].into()));
            avail.push(Cell::Str(p[3].into()));
            pct.push(Cell::Str(p[4].into()));
            mount.push(Cell::Str(p[5].into()));
        }
    }
    Ok(cols_to_table(vec![
        ("filesystem", ColType::Str, fs), ("size", ColType::Str, size),
        ("used", ColType::Str, used), ("avail", ColType::Str, avail),
        ("use%", ColType::Str, pct), ("mount", ColType::Str, mount),
    ]))
}

// ── Network Connections ──────────────────────────────────────────────────────

/// TCP connections
fn tcp() -> Result<SimpleTable, std::io::Error> { parse_net("/proc/net/tcp") }

/// UDP connections
fn udp() -> Result<SimpleTable, std::io::Error> { parse_net("/proc/net/udp") }

/// Parse /proc/net/tcp or udp
fn parse_net(path: &str) -> Result<SimpleTable, std::io::Error> {
    let (mut la, mut lp, mut ra, mut rp, mut st, mut ino) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string(path)?.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 10 {
            let (a1, p1) = parse_addr(p[1]);
            let (a2, p2) = parse_addr(p[2]);
            la.push(Cell::Str(a1)); lp.push(Cell::Int(p1 as i64));
            ra.push(Cell::Str(a2)); rp.push(Cell::Int(p2 as i64));
            st.push(Cell::Str(parse_tcp_state(p[3]))); ino.push(Cell::Int(p[9].parse().unwrap_or(0)));
        }
    }
    Ok(cols_to_table(vec![
        ("local_addr", ColType::Str, la), ("local_port", ColType::Int, lp),
        ("remote_addr", ColType::Str, ra), ("remote_port", ColType::Int, rp),
        ("state", ColType::Str, st), ("inode", ColType::Int, ino),
    ]))
}

/// Parse hex IP:port
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

// ── Open Files ───────────────────────────────────────────────────────────────

/// Open file descriptors from /proc/[pid]/fd
fn lsof(pid: Option<i32>) -> Result<SimpleTable, std::io::Error> {
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
                    pids.push(Cell::Int(p as i64)); fds.push(Cell::Int(fd)); paths.push(Cell::Str(link));
                }
            }
        }
    }
    Ok(cols_to_table(vec![
        ("pid", ColType::Int, pids), ("fd", ColType::Int, fds), ("path", ColType::Str, paths),
    ]))
}

// ── Environment Variables ────────────────────────────────────────────────────

/// Environment variables
fn env() -> Result<SimpleTable, std::io::Error> {
    let (names, vals): (Vec<Cell>, Vec<Cell>) = std::env::vars()
        .map(|(k, v)| (Cell::Str(k), Cell::Str(v))).unzip();
    Ok(cols_to_table(vec![
        ("name", ColType::Str, names), ("value", ColType::Str, vals),
    ]))
}

// ── Systemd Services ─────────────────────────────────────────────────────────

/// Systemd services from systemctl
fn systemctl() -> Result<SimpleTable, std::io::Error> {
    let text = run_cmd("systemctl", &["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])?;
    let (mut units, mut loads, mut actives, mut subs, mut descs) = (vec![], vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 5 {
            units.push(Cell::Str(p[0].into()));
            loads.push(Cell::Str(p[1].into()));
            actives.push(Cell::Str(p[2].into()));
            subs.push(Cell::Str(p[3].into()));
            descs.push(Cell::Str(p[4..].join(" ")));
        }
    }
    Ok(cols_to_table(vec![
        ("unit", ColType::Str, units), ("load", ColType::Str, loads),
        ("active", ColType::Str, actives), ("sub", ColType::Str, subs),
        ("description", ColType::Str, descs),
    ]))
}

// ── Journal Logs ─────────────────────────────────────────────────────────────

/// Journal logs from journalctl
fn journalctl(n: usize) -> Result<SimpleTable, std::io::Error> {
    let ns = n.to_string();
    let text = run_cmd("journalctl", &["--no-pager", "-o", "short-iso", "-n", &ns])?;
    let (mut times, mut hosts, mut units, mut msgs) = (vec![], vec![], vec![], vec![]);
    for line in text.lines() {
        let p: Vec<&str> = line.splitn(4, ' ').collect();
        if p.len() >= 4 {
            times.push(Cell::Str(p[0].into()));
            hosts.push(Cell::Str(p[1].into()));
            units.push(Cell::Str(p[2].split('[').next().unwrap_or(p[2]).trim_end_matches(':').into()));
            msgs.push(Cell::Str(p[3].into()));
        } else {
            times.push(Cell::Str(String::new()));
            hosts.push(Cell::Str(String::new()));
            units.push(Cell::Str(String::new()));
            msgs.push(Cell::Str(line.into()));
        }
    }
    Ok(cols_to_table(vec![
        ("time", ColType::Str, times), ("host", ColType::Str, hosts),
        ("unit", ColType::Str, units), ("message", ColType::Str, msgs),
    ]))
}

// ── Pacman Packages ──────────────────────────────────────────────────────────

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
        "Jul" => "07", "Aug" => "08", "Sep" => "09", "Oct" => "10", "Nov" => "11", "Dec" => "12",
        _ => return s.into(),
    };
    format!("{}-{}-{:02}", p[4], mon, p[2].parse::<u32>().unwrap_or(0))
}

/// Parse dependency list
fn parse_deps(v: &str) -> Vec<String> {
    if v == "None" { vec![] }
    else { v.split_whitespace().map(|s| s.split(&['<','>','='][..]).next().unwrap_or(s).to_string()).collect() }
}

/// Calculate rsize: package size + exclusive deps
fn calc_rsize(name: &str, size: u64, deps: &[String], pkg_size: &HashMap<String, u64>, pkg_req_by: &HashMap<String, Vec<String>>) -> u64 {
    deps.iter().fold(size, |acc, dep| {
        acc + pkg_req_by.get(dep).filter(|r| r.len() == 1 && r[0] == name).map(|_| pkg_size.get(dep).copied().unwrap_or(0)).unwrap_or(0)
    })
}

/// Installed packages from pacman
fn pacman() -> Result<SimpleTable, std::io::Error> {
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
    let (mut names, mut vers, mut descs, mut sizes, mut rsizes): (Vec<Cell>, Vec<Cell>, Vec<Cell>, Vec<Cell>, Vec<Cell>) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut installed, mut reasons, mut deps_cnt, mut req_cnt, mut orphan_flags): (Vec<Cell>, Vec<Cell>, Vec<Cell>, Vec<Cell>, Vec<Cell>) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut name, mut ver, mut desc, mut inst, mut reason) = (String::new(), String::new(), String::new(), String::new(), String::new());
    let (mut size, mut deps, mut reqs, mut deps_list) = (0u64, 0i64, 0i64, vec![]);

    let mut push_pkg = |n: &mut String, v: &mut String, d: &mut String, i: &mut String, r: &mut String, sz: &mut u64, dc: &mut i64, rc: &mut i64, dl: &mut Vec<String>| {
        if n.is_empty() { return; }
        rsizes.push(Cell::Int((calc_rsize(n, *sz, dl, &pkg_size, &pkg_req_by) / 1024) as i64));
        orphan_flags.push(Cell::Str(if orphans.contains(n.as_str()) { "x".into() } else { "".into() }));
        names.push(Cell::Str(std::mem::take(n))); vers.push(Cell::Str(std::mem::take(v))); descs.push(Cell::Str(std::mem::take(d)));
        sizes.push(Cell::Int((*sz / 1024) as i64)); installed.push(Cell::Str(std::mem::take(i))); reasons.push(Cell::Str(std::mem::take(r)));
        deps_cnt.push(Cell::Int(*dc)); req_cnt.push(Cell::Int(*rc)); dl.clear(); *sz = 0; *dc = 0; *rc = 0;
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

    Ok(cols_to_table(vec![
        ("name", ColType::Str, names), ("version", ColType::Str, vers),
        ("size(k)", ColType::Int, sizes), ("rsize(k)", ColType::Int, rsizes),
        ("deps", ColType::Int, deps_cnt), ("req_by", ColType::Int, req_cnt),
        ("orphan", ColType::Str, orphan_flags), ("reason", ColType::Str, reasons),
        ("installed", ColType::Str, installed), ("description", ColType::Str, descs),
    ]))
}

// ── Cargo Dependencies ───────────────────────────────────────────────────────

/// Cache file path for cargo latest versions
fn cargo_cache_path() -> std::path::PathBuf {
    dirs::cache_dir().unwrap_or_else(|| std::path::PathBuf::from(".")).join("tv/cargo_versions.csv")
}

/// Load version cache from disk
fn load_ver_cache() -> HashMap<String, (String, i64)> {
    let mut cache = HashMap::new();
    let path = cargo_cache_path();
    if let Ok(content) = fs::read_to_string(&path) {
        for line in content.lines().skip(1) {
            let p: Vec<&str> = line.split(',').collect();
            if p.len() >= 3 {
                cache.insert(p[0].to_string(), (p[1].to_string(), p[2].parse().unwrap_or(0)));
            }
        }
    }
    cache
}

/// Save version cache to disk
fn save_ver_cache(cache: &HashMap<String, (String, i64)>) {
    let path = cargo_cache_path();
    if let Some(parent) = path.parent() { fs::create_dir_all(parent).ok(); }
    let mut content = String::from("name,version,timestamp\n");
    for (name, (ver, ts)) in cache { content.push_str(&format!("{},{},{}\n", name, ver, ts)); }
    fs::write(path, content).ok();
}

/// Fetch latest version from crates.io (detached)
fn fetch_latest(name: &str) -> Option<String> {
    use std::os::unix::process::CommandExt;
    let mut cmd = Command::new("cargo");
    cmd.args(["search", name, "--limit", "1", "--color=never"])
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null());
    unsafe { cmd.pre_exec(|| { nix::unistd::setsid().ok(); Ok(()) }); }
    cmd.output().ok()
        .map(|o| String::from_utf8_lossy(&o.stdout).to_string())
        .and_then(|s| s.lines().next().map(String::from))
        .and_then(|l| l.split('"').nth(1).map(String::from))
}

/// Update stale cache entries in background
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
                    if cnt % 10 == 0 { save_ver_cache(&cache); }
                }
            }
        }
        save_ver_cache(&cache);
    });
}

/// Get latest version from cache
fn latest_ver(name: &str, cache: &HashMap<String, (String, i64)>) -> String {
    cache.get(name).map(|(v, _)| v.clone()).unwrap_or_default()
}

/// Calculate directory size recursively
fn dir_size(path: &Path) -> u64 {
    fs::read_dir(path).ok().map(|entries| {
        entries.filter_map(|e| e.ok()).map(|e| {
            let m = e.metadata().ok();
            if m.as_ref().map(|m| m.is_dir()).unwrap_or(false) { dir_size(&e.path()) }
            else { m.map(|m| m.len()).unwrap_or(0) }
        }).sum()
    }).unwrap_or(0)
}

/// Project dependencies from cargo metadata
fn cargo() -> Result<SimpleTable, std::io::Error> {
    let text = run_cmd("cargo", &["metadata", "--format-version", "1"])?;
    let json: serde_json::Value = serde_json::from_str(&text).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    // Get linux-compiled packages
    let linux_text = run_cmd("cargo", &["metadata", "--format-version", "1", "--filter-platform", "x86_64-unknown-linux-gnu"])?;
    let linux_json: serde_json::Value = serde_json::from_str(&linux_text).unwrap_or_default();
    let linux_pkgs: HashSet<String> = linux_json["packages"].as_array()
        .map(|a| a.iter().filter_map(|p| p["name"].as_str().map(String::from)).collect())
        .unwrap_or_default();

    // Build package info map
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
            let size = p["manifest_path"].as_str()
                .and_then(|mp| Path::new(mp).parent())
                .map(dir_size).unwrap_or(0);
            pkg_size.insert(name.clone(), size);
            pkg_info.insert(id, (name, ver, desc, deps));
        }
    }

    // Build reverse deps
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

    // Build output
    let (mut names, mut vers, mut latest, mut descs, mut plat) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut sizes, mut rsizes, mut deps_cnt, mut req_cnt): (Vec<Cell>, Vec<Cell>, Vec<Cell>, Vec<Cell>) = (vec![], vec![], vec![], vec![]);
    let ver_cache = load_ver_cache();
    let mut all_names: Vec<String> = vec![];

    for id in &resolved {
        if let Some((name, ver, desc, deps)) = pkg_info.get(id) {
            let size = pkg_size.get(name).copied().unwrap_or(0);
            let rsize = calc_rsize(name, size, deps, &pkg_size, &req_by);
            let reqs = req_by.get(name).map(|v| v.len()).unwrap_or(0);
            let lat = latest_ver(name, &ver_cache);
            all_names.push(name.clone());
            names.push(Cell::Str(name.clone())); vers.push(Cell::Str(ver.clone()));
            latest.push(Cell::Str(lat)); descs.push(Cell::Str(desc.clone()));
            sizes.push(Cell::Int((size / 1024) as i64)); rsizes.push(Cell::Int((rsize / 1024) as i64));
            deps_cnt.push(Cell::Int(deps.len() as i64)); req_cnt.push(Cell::Int(reqs as i64));
            let p = if linux_pkgs.contains(name) { "linux" }
                else if name.contains("windows") { "windows" }
                else if name.contains("macos") || name.contains("core-foundation") || name.contains("objc") { "macos" }
                else if name.contains("android") { "android" }
                else if name.contains("wasm") || name.contains("js-sys") || name.contains("web-sys") { "wasm" }
                else { "" };
            plat.push(Cell::Str(p.into()));
        }
    }

    update_ver_cache_bg(all_names);

    Ok(cols_to_table(vec![
        ("name", ColType::Str, names), ("version", ColType::Str, vers), ("latest", ColType::Str, latest),
        ("size(k)", ColType::Int, sizes), ("rsize(k)", ColType::Int, rsizes),
        ("deps", ColType::Int, deps_cnt), ("req_by", ColType::Int, req_cnt),
        ("platform", ColType::Str, plat), ("description", ColType::Str, descs),
    ]))
}
