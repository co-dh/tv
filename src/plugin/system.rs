//! System plugin - OS commands (ps, tcp, pacman, etc.) and file operations (ls, lr)

use crate::app::AppContext;
use crate::command::Command;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::Result;
use polars::prelude::*;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

pub struct SystemPlugin;

impl Plugin for SystemPlugin {
    fn name(&self) -> &str { "system" }
    fn tab(&self) -> &str { "table" }

    fn matches(&self, name: &str) -> bool {
        matches!(name, "ps" | "mounts" | "tcp" | "udp" | "env" | "systemctl" | "pacman")
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
            "lsof" => Some(Box::new(Lsof { pid: arg.parse().ok() })),
            "journalctl" => Some(Box::new(Journalctl { n: arg.parse().unwrap_or(1000) })),
            _ => None,
        }
    }
}

/// Unified system command enum
#[derive(Clone, Copy)]
pub enum SysCmd { Ps, Mounts, Tcp, Udp, Env, Systemctl, Pacman }

impl Command for SysCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, df) = match self {
            SysCmd::Ps => ("ps", ps()?),
            SysCmd::Mounts => ("mounts", mounts()?),
            SysCmd::Tcp => ("tcp", tcp()?),
            SysCmd::Udp => ("udp", udp()?),
            SysCmd::Env => ("env", env()?),
            SysCmd::Systemctl => ("systemctl", systemctl()?),
            SysCmd::Pacman => ("pacman", pacman()?),
        };
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name.into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String {
        match self {
            SysCmd::Ps => "ps", SysCmd::Mounts => "mounts",
            SysCmd::Tcp => "tcp", SysCmd::Udp => "udp", SysCmd::Env => "env",
            SysCmd::Systemctl => "systemctl", SysCmd::Pacman => "pacman",
        }.into()
    }
}

/// lsof with optional pid filter
pub struct Lsof { pub pid: Option<i32> }

impl Command for Lsof {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = lsof(self.pid)?;
        let name = self.pid.map(|p| format!("lsof:{}", p)).unwrap_or("lsof".into());
        let id = app.next_id();
        app.stack.push(ViewState::new(id, name, df, None));
        Ok(())
    }
    fn to_str(&self) -> String { self.pid.map(|p| format!("lsof {}", p)).unwrap_or("lsof".into()) }
}

/// journalctl with line count
pub struct Journalctl { pub n: usize }

impl Command for Journalctl {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = journalctl(self.n)?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "journalctl".into(), df, None));
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

/// List directory contents as DataFrame
pub fn ls(dir: &Path) -> Result<DataFrame> {
    let mut names: Vec<String> = Vec::new();
    let mut paths: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut modified: Vec<i64> = Vec::new();
    let mut is_dir: Vec<&str> = Vec::new();

    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    // Add ".." for parent directory
    let abs_dir = dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    if let Some(parent) = abs_dir.parent() {
        names.push("..".into());
        paths.push(parent.to_string_lossy().into());
        let m = parent.metadata().ok();
        sizes.push(m.as_ref().map(|m| m.size()).unwrap_or(0));
        is_dir.push("x");
        modified.push(m.map(|m| m.mtime() * 1_000_000).unwrap_or(0));
    }

    for e in entries {
        let m = e.metadata()?;
        let full_path = e.path().canonicalize().unwrap_or_else(|_| e.path());
        names.push(e.file_name().to_string_lossy().into());
        paths.push(full_path.to_string_lossy().into());
        sizes.push(m.size());
        is_dir.push(if m.is_dir() { "x" } else { "" });
        modified.push(m.mtime() * 1_000_000);
    }

    let modified_series = Series::new("modified".into(), modified)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    Ok(DataFrame::new(vec![
        Series::new("name".into(), names).into(),
        Series::new("path".into(), paths).into(),
        Series::new("size".into(), sizes).into(),
        modified_series.into(),
        Series::new("dir".into(), is_dir).into(),
    ])?)
}

/// List directory recursively (respects .gitignore via rg)
pub fn lr(dir: &Path) -> Result<DataFrame> {
    use std::process::Command;
    let out = Command::new("rg").args(["--files"]).current_dir(dir).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut paths: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut modified: Vec<i64> = Vec::new();

    let mut files: Vec<&str> = text.lines().collect();
    files.sort();
    for p in files {
        let full = dir.join(p);
        if let Ok(m) = full.metadata() {
            paths.push(p.to_string());
            sizes.push(m.size());
            modified.push(m.mtime() * 1_000_000);
        }
    }

    let modified_series = Series::new("modified".into(), modified)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    Ok(DataFrame::new(vec![
        Series::new("path".into(), paths).into(),
        Series::new("size".into(), sizes).into(),
        modified_series.into(),
    ])?)
}

/// Process list from ps aux
fn ps() -> Result<DataFrame> {
    let text = run_cmd("ps", &["aux"])?;

    let (mut users, mut pids, mut cpus, mut mems) = (vec![], vec![], vec![], vec![]);
    let (mut vszs, mut rsss, mut ttys, mut stats) = (vec![], vec![], vec![], vec![]);
    let (mut starts, mut times, mut cmds) = (vec![], vec![], vec![]);

    for line in text.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 11 {
            users.push(p[0].to_string()); pids.push(p[1].parse::<i32>().unwrap_or(0));
            cpus.push(p[2].parse::<f64>().unwrap_or(0.0)); mems.push(p[3].parse::<f64>().unwrap_or(0.0));
            vszs.push(p[4].parse::<u64>().unwrap_or(0)); rsss.push(p[5].parse::<u64>().unwrap_or(0));
            ttys.push(p[6].to_string()); stats.push(p[7].to_string());
            starts.push(p[8].to_string()); times.push(p[9].to_string());
            cmds.push(p[10..].join(" "));
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("user".into(), users).into(), Series::new("pid".into(), pids).into(),
        Series::new("%cpu".into(), cpus).into(), Series::new("%mem".into(), mems).into(),
        Series::new("vsz".into(), vszs).into(), Series::new("rss".into(), rsss).into(),
        Series::new("tty".into(), ttys).into(), Series::new("stat".into(), stats).into(),
        Series::new("start".into(), starts).into(), Series::new("time".into(), times).into(),
        Series::new("command".into(), cmds).into(),
    ])?)
}

/// Mount points from /proc/mounts
fn mounts() -> Result<DataFrame> {
    let (mut devs, mut mps, mut types, mut opts) = (vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string("/proc/mounts")?.lines() {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 4 {
            devs.push(p[0].to_string()); mps.push(p[1].to_string());
            types.push(p[2].to_string()); opts.push(p[3].to_string());
        }
    }
    Ok(DataFrame::new(vec![
        Series::new("device".into(), devs).into(), Series::new("mount".into(), mps).into(),
        Series::new("type".into(), types).into(), Series::new("options".into(), opts).into(),
    ])?)
}

/// TCP connections from /proc/net/tcp
fn tcp() -> Result<DataFrame> { parse_net("/proc/net/tcp") }

/// UDP connections from /proc/net/udp
fn udp() -> Result<DataFrame> { parse_net("/proc/net/udp") }

/// Parse /proc/net/tcp or udp
fn parse_net(path: &str) -> Result<DataFrame> {
    let (mut la, mut lp, mut ra, mut rp, mut st, mut ino) = (vec![], vec![], vec![], vec![], vec![], vec![]);
    for line in fs::read_to_string(path)?.lines().skip(1) {
        let p: Vec<&str> = line.split_whitespace().collect();
        if p.len() >= 10 {
            let (a1, p1) = parse_addr(p[1]); let (a2, p2) = parse_addr(p[2]);
            la.push(a1); lp.push(p1); ra.push(a2); rp.push(p2);
            st.push(parse_tcp_state(p[3])); ino.push(p[9].parse::<u64>().unwrap_or(0));
        }
    }
    Ok(DataFrame::new(vec![
        Series::new("local_addr".into(), la).into(), Series::new("local_port".into(), lp).into(),
        Series::new("remote_addr".into(), ra).into(), Series::new("remote_port".into(), rp).into(),
        Series::new("state".into(), st).into(), Series::new("inode".into(), ino).into(),
    ])?)
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
fn lsof(pid: Option<i32>) -> Result<DataFrame> {
    let (mut pids, mut fds, mut paths) = (vec![], vec![], vec![]);
    let dirs: Vec<i32> = if let Some(p) = pid { vec![p] } else {
        fs::read_dir("/proc")?.filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok()).collect()
    };
    for p in dirs {
        if let Ok(entries) = fs::read_dir(format!("/proc/{}/fd", p)) {
            for e in entries.flatten() {
                if let Ok(fd) = e.file_name().to_string_lossy().parse::<i32>() {
                    let link = fs::read_link(e.path()).map(|p| p.to_string_lossy().to_string()).unwrap_or_default();
                    pids.push(p); fds.push(fd); paths.push(link);
                }
            }
        }
    }
    Ok(DataFrame::new(vec![
        Series::new("pid".into(), pids).into(), Series::new("fd".into(), fds).into(),
        Series::new("path".into(), paths).into(),
    ])?)
}

/// Environment variables
fn env() -> Result<DataFrame> {
    let (names, vals): (Vec<String>, Vec<String>) = std::env::vars().unzip();
    Ok(DataFrame::new(vec![
        Series::new("name".into(), names).into(), Series::new("value".into(), vals).into(),
    ])?)
}

/// Total system memory in bytes
pub fn mem_total() -> u64 {
    fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| s.lines().find(|l| l.starts_with("MemTotal:"))
            .and_then(|l| l.split_whitespace().nth(1)?.parse::<u64>().ok()).map(|kb| kb * 1024))
        .unwrap_or(8 * 1024 * 1024 * 1024)
}

/// Systemd services from systemctl
fn systemctl() -> Result<DataFrame> {
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
    Ok(DataFrame::new(vec![
        Series::new("unit".into(), units).into(), Series::new("load".into(), loads).into(),
        Series::new("active".into(), actives).into(), Series::new("sub".into(), subs).into(),
        Series::new("description".into(), descs).into(),
    ])?)
}

/// Journal logs from journalctl
fn journalctl(n: usize) -> Result<DataFrame> {
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
    Ok(DataFrame::new(vec![
        Series::new("time".into(), times).into(), Series::new("host".into(), hosts).into(),
        Series::new("unit".into(), units).into(), Series::new("message".into(), msgs).into(),
    ])?)
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

/// Installed packages from pacman (Arch Linux)
fn pacman() -> Result<DataFrame> {
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
    let (mut names, mut vers, mut descs, mut sizes, mut rsizes) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut installed, mut reasons, mut deps_cnt, mut req_cnt, mut orphan_flags): (Vec<String>, Vec<String>, Vec<u32>, Vec<u32>, Vec<String>) = (vec![], vec![], vec![], vec![], vec![]);
    let (mut name, mut ver, mut desc, mut inst, mut reason) = (String::new(), String::new(), String::new(), String::new(), String::new());
    let (mut size, mut deps, mut reqs, mut deps_list) = (0u64, 0u32, 0u32, vec![]);

    // Helper to push current package to output vectors
    let mut push_pkg = |n: &mut String, v: &mut String, d: &mut String, i: &mut String, r: &mut String, sz: &mut u64, dc: &mut u32, rc: &mut u32, dl: &mut Vec<String>| {
        if n.is_empty() { return; }
        rsizes.push(calc_rsize(n, *sz, dl, &pkg_size, &pkg_req_by));
        orphan_flags.push(if orphans.contains(n.as_str()) { "x".into() } else { "".into() });
        names.push(std::mem::take(n)); vers.push(std::mem::take(v)); descs.push(std::mem::take(d));
        sizes.push(*sz); installed.push(std::mem::take(i)); reasons.push(std::mem::take(r));
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
                "Depends On" => { deps_list = parse_deps(v); deps = deps_list.len() as u32; },
                "Required By" => reqs = if v == "None" { 0 } else { v.split_whitespace().count() as u32 },
                _ => {}
            }
        }
    }
    push_pkg(&mut name, &mut ver, &mut desc, &mut inst, &mut reason, &mut size, &mut deps, &mut reqs, &mut deps_list);

    Ok(DataFrame::new(vec![
        Series::new("name".into(), names).into(), Series::new("version".into(), vers).into(),
        Series::new("size(k)".into(), sizes.iter().map(|b| b / 1024).collect::<Vec<u64>>()).into(),
        Series::new("rsize(k)".into(), rsizes.iter().map(|b| b / 1024).collect::<Vec<u64>>()).into(),
        Series::new("deps".into(), deps_cnt).into(), Series::new("req_by".into(), req_cnt).into(),
        Series::new("orphan".into(), orphan_flags).into(), Series::new("reason".into(), reasons).into(),
        Series::new("installed".into(), installed).into(), Series::new("description".into(), descs).into(),
    ])?)
}
