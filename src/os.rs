use polars::prelude::*;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

/// List directory contents as DataFrame
pub fn ls(dir: &Path) -> anyhow::Result<DataFrame> {
    let mut names: Vec<String> = Vec::new();
    let mut paths: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut modified: Vec<i64> = Vec::new();
    let mut is_dir: Vec<&str> = Vec::new();

    // Collect entries and sort by name
    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by(|a, b| a.file_name().cmp(&b.file_name()));

    // Add ".." first to go to parent directory
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
        modified.push(m.mtime() * 1_000_000); // microseconds
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

/// List directory recursively as DataFrame
pub fn lr(dir: &Path) -> anyhow::Result<DataFrame> {
    use std::process::Command;
    // rg --files respects .gitignore
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
            modified.push(m.mtime() * 1_000_000); // microseconds
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

/// Process list from ps aux (more columns than /proc parsing)
pub fn ps() -> anyhow::Result<DataFrame> {
    use std::process::Command;
    let out = Command::new("ps").args(["aux"]).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut users: Vec<String> = Vec::new();
    let mut pids: Vec<i32> = Vec::new();
    let mut cpus: Vec<f64> = Vec::new();
    let mut mems: Vec<f64> = Vec::new();
    let mut vszs: Vec<u64> = Vec::new();
    let mut rsss: Vec<u64> = Vec::new();
    let mut ttys: Vec<String> = Vec::new();
    let mut stats: Vec<String> = Vec::new();
    let mut starts: Vec<String> = Vec::new();
    let mut times: Vec<String> = Vec::new();
    let mut cmds: Vec<String> = Vec::new();

    for line in text.lines().skip(1) {  // skip header
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 11 {
            users.push(parts[0].into());
            pids.push(parts[1].parse().unwrap_or(0));
            cpus.push(parts[2].parse().unwrap_or(0.0));
            mems.push(parts[3].parse().unwrap_or(0.0));
            vszs.push(parts[4].parse().unwrap_or(0));
            rsss.push(parts[5].parse().unwrap_or(0));
            ttys.push(parts[6].into());
            stats.push(parts[7].into());
            starts.push(parts[8].into());
            times.push(parts[9].into());
            cmds.push(parts[10..].join(" "));
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("user".into(), users).into(),
        Series::new("pid".into(), pids).into(),
        Series::new("%cpu".into(), cpus).into(),
        Series::new("%mem".into(), mems).into(),
        Series::new("vsz".into(), vszs).into(),
        Series::new("rss".into(), rsss).into(),
        Series::new("tty".into(), ttys).into(),
        Series::new("stat".into(), stats).into(),
        Series::new("start".into(), starts).into(),
        Series::new("time".into(), times).into(),
        Series::new("command".into(), cmds).into(),
    ])?)
}

/// Disk usage from statvfs (like df)
pub fn df() -> anyhow::Result<DataFrame> {
    let mut filesystems: Vec<String> = Vec::new();
    let mut mount_points: Vec<String> = Vec::new();
    let mut total: Vec<u64> = Vec::new();
    let mut used: Vec<u64> = Vec::new();
    let mut avail: Vec<u64> = Vec::new();

    let mounts = fs::read_to_string("/proc/mounts")?;
    for line in mounts.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            let fs = parts[0];
            let mp = parts[1];

            // Skip virtual filesystems
            if fs.starts_with('/') || mp == "/" {
                if let Ok(stat) = nix::sys::statvfs::statvfs(mp) {
                    let block_size = stat.block_size() as u64;
                    let total_bytes = stat.blocks() * block_size;
                    let avail_bytes = stat.blocks_available() * block_size;
                    let free_bytes = stat.blocks_free() * block_size;
                    let used_bytes = total_bytes - free_bytes;

                    filesystems.push(fs.to_string());
                    mount_points.push(mp.to_string());
                    total.push(total_bytes);
                    used.push(used_bytes);
                    avail.push(avail_bytes);
                }
            }
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("filesystem".into(), filesystems).into(),
        Series::new("mount".into(), mount_points).into(),
        Series::new("total".into(), total).into(),
        Series::new("used".into(), used).into(),
        Series::new("avail".into(), avail).into(),
    ])?)
}

/// Mount points from /proc/mounts
pub fn mounts() -> anyhow::Result<DataFrame> {
    let mut devices: Vec<String> = Vec::new();
    let mut mount_points: Vec<String> = Vec::new();
    let mut fs_types: Vec<String> = Vec::new();
    let mut options: Vec<String> = Vec::new();

    let content = fs::read_to_string("/proc/mounts")?;
    for line in content.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 4 {
            devices.push(parts[0].to_string());
            mount_points.push(parts[1].to_string());
            fs_types.push(parts[2].to_string());
            options.push(parts[3].to_string());
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("device".into(), devices).into(),
        Series::new("mount".into(), mount_points).into(),
        Series::new("type".into(), fs_types).into(),
        Series::new("options".into(), options).into(),
    ])?)
}

/// TCP connections from /proc/net/tcp
pub fn tcp() -> anyhow::Result<DataFrame> {
    parse_net_file("/proc/net/tcp")
}

/// UDP connections from /proc/net/udp
pub fn udp() -> anyhow::Result<DataFrame> {
    parse_net_file("/proc/net/udp")
}

/// Parse /proc/net/tcp or /proc/net/udp into DataFrame
fn parse_net_file(path: &str) -> anyhow::Result<DataFrame> {
    let mut local_addrs: Vec<String> = Vec::new();
    let mut local_ports: Vec<u32> = Vec::new();
    let mut remote_addrs: Vec<String> = Vec::new();
    let mut remote_ports: Vec<u32> = Vec::new();
    let mut states: Vec<String> = Vec::new();
    let mut inodes: Vec<u64> = Vec::new();

    let content = fs::read_to_string(path)?;
    for line in content.lines().skip(1) {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 10 {
            let (local_addr, local_port) = parse_addr(parts[1]);
            let (remote_addr, remote_port) = parse_addr(parts[2]);
            let state = parse_tcp_state(parts[3]);
            let inode: u64 = parts[9].parse().unwrap_or(0);

            local_addrs.push(local_addr);
            local_ports.push(local_port);
            remote_addrs.push(remote_addr);
            remote_ports.push(remote_port);
            states.push(state);
            inodes.push(inode);
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("local_addr".into(), local_addrs).into(),
        Series::new("local_port".into(), local_ports).into(),
        Series::new("remote_addr".into(), remote_addrs).into(),
        Series::new("remote_port".into(), remote_ports).into(),
        Series::new("state".into(), states).into(),
        Series::new("inode".into(), inodes).into(),
    ])?)
}

/// Parse hex IP:port string from /proc/net/* into (dotted IP, port)
fn parse_addr(s: &str) -> (String, u32) {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() == 2 {
        let addr = u32::from_str_radix(parts[0], 16).unwrap_or(0);
        let port = u32::from_str_radix(parts[1], 16).unwrap_or(0);
        let ip = format!("{}.{}.{}.{}", addr & 0xff, (addr >> 8) & 0xff, (addr >> 16) & 0xff, (addr >> 24) & 0xff);
        (ip, port)
    } else {
        (String::new(), 0)
    }
}

/// Convert hex TCP state code to readable string
fn parse_tcp_state(s: &str) -> String {
    match s {
        "01" => "ESTABLISHED",
        "02" => "SYN_SENT",
        "03" => "SYN_RECV",
        "04" => "FIN_WAIT1",
        "05" => "FIN_WAIT2",
        "06" => "TIME_WAIT",
        "07" => "CLOSE",
        "08" => "CLOSE_WAIT",
        "09" => "LAST_ACK",
        "0A" => "LISTEN",
        "0B" => "CLOSING",
        _ => "UNKNOWN",
    }.to_string()
}

/// Block devices from /sys/block
pub fn lsblk() -> anyhow::Result<DataFrame> {
    let mut names: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut removable: Vec<String> = Vec::new();
    let mut ro: Vec<String> = Vec::new();

    for entry in fs::read_dir("/sys/block")? {
        let e = entry?;
        let name = e.file_name().to_string_lossy().to_string();
        let base = format!("/sys/block/{}", name);

        let size: u64 = fs::read_to_string(format!("{}/size", base))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .map(|sectors: u64| sectors * 512)  // sectors to bytes
            .unwrap_or(0);

        let is_removable = fs::read_to_string(format!("{}/removable", base))
            .map(|s| if s.trim() == "1" { "x" } else { "" })
            .unwrap_or("");

        let is_ro = fs::read_to_string(format!("{}/ro", base))
            .map(|s| if s.trim() == "1" { "x" } else { "" })
            .unwrap_or("");

        names.push(name);
        sizes.push(size);
        removable.push(is_removable.to_string());
        ro.push(is_ro.to_string());
    }

    Ok(DataFrame::new(vec![
        Series::new("name".into(), names).into(),
        Series::new("size".into(), sizes).into(),
        Series::new("removable".into(), removable).into(),
        Series::new("ro".into(), ro).into(),
    ])?)
}

/// Logged in users from /var/run/utmp
pub fn who() -> anyhow::Result<DataFrame> {
    let mut users: Vec<String> = Vec::new();
    let mut ttys: Vec<String> = Vec::new();
    let mut hosts: Vec<String> = Vec::new();
    let mut times: Vec<i64> = Vec::new();

    let data = fs::read("/var/run/utmp")?;
    // utmp entry is 384 bytes on x86_64 Linux
    const UTMP_SIZE: usize = 384;

    for chunk in data.chunks(UTMP_SIZE) {
        if chunk.len() < UTMP_SIZE { break; }

        let ut_type = i32::from_ne_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
        if ut_type == 7 {  // USER_PROCESS
            let user = String::from_utf8_lossy(&chunk[8..40]).trim_matches('\0').to_string();
            let tty = String::from_utf8_lossy(&chunk[44..76]).trim_matches('\0').to_string();
            let host = String::from_utf8_lossy(&chunk[76..332]).trim_matches('\0').to_string();
            let tv_sec = i32::from_ne_bytes([chunk[332], chunk[333], chunk[334], chunk[335]]) as i64;

            if !user.is_empty() {
                users.push(user);
                ttys.push(tty);
                hosts.push(host);
                times.push(tv_sec * 1_000_000);
            }
        }
    }

    let time_series = Series::new("login".into(), times)
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?;

    Ok(DataFrame::new(vec![
        Series::new("user".into(), users).into(),
        Series::new("tty".into(), ttys).into(),
        Series::new("host".into(), hosts).into(),
        time_series.into(),
    ])?)
}

/// Open file descriptors for a process from /proc/[pid]/fd
pub fn lsof(pid: Option<i32>) -> anyhow::Result<DataFrame> {
    let mut pids: Vec<i32> = Vec::new();
    let mut fds: Vec<i32> = Vec::new();
    let mut paths: Vec<String> = Vec::new();

    let proc_dirs: Vec<i32> = if let Some(p) = pid {
        vec![p]
    } else {
        fs::read_dir("/proc")?
            .filter_map(|e| e.ok())
            .filter_map(|e| e.file_name().to_string_lossy().parse().ok())
            .collect()
    };

    for p in proc_dirs {
        let fd_dir = format!("/proc/{}/fd", p);
        if let Ok(entries) = fs::read_dir(&fd_dir) {
            for entry in entries.flatten() {
                if let Ok(fd) = entry.file_name().to_string_lossy().parse::<i32>() {
                    let link = fs::read_link(entry.path())
                        .map(|p| p.to_string_lossy().to_string())
                        .unwrap_or_default();
                    pids.push(p);
                    fds.push(fd);
                    paths.push(link);
                }
            }
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("pid".into(), pids).into(),
        Series::new("fd".into(), fds).into(),
        Series::new("path".into(), paths).into(),
    ])?)
}

/// Environment variables as DataFrame
pub fn env() -> anyhow::Result<DataFrame> {
    let mut names: Vec<String> = Vec::new();
    let mut values: Vec<String> = Vec::new();

    for (key, value) in std::env::vars() {
        names.push(key);
        values.push(value);
    }

    Ok(DataFrame::new(vec![
        Series::new("name".into(), names).into(),
        Series::new("value".into(), values).into(),
    ])?)
}

/// Get total system memory in bytes from /proc/meminfo
pub fn mem_total() -> u64 {
    fs::read_to_string("/proc/meminfo").ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("MemTotal:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse::<u64>().ok())
                .map(|kb| kb * 1024)
        })
        .unwrap_or(8 * 1024 * 1024 * 1024)  // default 8GB
}

/// Systemd services from systemctl
pub fn systemctl() -> anyhow::Result<DataFrame> {
    use std::process::Command;
    let out = Command::new("systemctl")
        .args(["list-units", "--type=service", "--all", "--no-pager", "--no-legend"])
        .output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut units: Vec<String> = Vec::new();
    let mut loads: Vec<String> = Vec::new();
    let mut actives: Vec<String> = Vec::new();
    let mut subs: Vec<String> = Vec::new();
    let mut descs: Vec<String> = Vec::new();

    for line in text.lines() {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 5 {
            units.push(parts[0].into());
            loads.push(parts[1].into());
            actives.push(parts[2].into());
            subs.push(parts[3].into());
            descs.push(parts[4..].join(" "));
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("unit".into(), units).into(),
        Series::new("load".into(), loads).into(),
        Series::new("active".into(), actives).into(),
        Series::new("sub".into(), subs).into(),
        Series::new("description".into(), descs).into(),
    ])?)
}

/// Journal logs from journalctl (last 1000 entries)
pub fn journalctl(n: usize) -> anyhow::Result<DataFrame> {
    use std::process::Command;
    let out = Command::new("journalctl")
        .args(["--no-pager", "-o", "short-iso", "-n", &n.to_string()])
        .output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut times: Vec<String> = Vec::new();
    let mut hosts: Vec<String> = Vec::new();
    let mut units: Vec<String> = Vec::new();
    let mut msgs: Vec<String> = Vec::new();

    for line in text.lines() {
        // Format: 2025-01-15T10:30:00+0000 hostname unit[pid]: message
        let parts: Vec<&str> = line.splitn(4, ' ').collect();
        if parts.len() >= 4 {
            times.push(parts[0].into());
            hosts.push(parts[1].into());
            // unit[pid]: or unit: - extract unit name
            let unit_part = parts[2];
            let unit = unit_part.split('[').next()
                .unwrap_or(unit_part)
                .trim_end_matches(':');
            units.push(unit.into());
            msgs.push(parts[3].into());
        } else if parts.len() >= 1 {
            // continuation line
            times.push("".into());
            hosts.push("".into());
            units.push("".into());
            msgs.push(line.into());
        }
    }

    Ok(DataFrame::new(vec![
        Series::new("time".into(), times).into(),
        Series::new("host".into(), hosts).into(),
        Series::new("unit".into(), units).into(),
        Series::new("message".into(), msgs).into(),
    ])?)
}

/// Parse pacman size string like "136.04 KiB" or "6.55 MiB" to bytes
fn parse_size(s: &str) -> u64 {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 { return 0; }
    let num: f64 = parts[0].parse().unwrap_or(0.0);
    let mult: f64 = match parts[1] {
        "B" => 1.0,
        "KiB" => 1024.0,
        "MiB" => 1024.0 * 1024.0,
        "GiB" => 1024.0 * 1024.0 * 1024.0,
        _ => 1.0,
    };
    (num * mult) as u64
}

/// Installed packages from pacman (Arch Linux)
pub fn pacman() -> anyhow::Result<DataFrame> {
    use std::process::Command;
    use std::collections::HashSet;

    // Get orphaned packages
    let orphan_out = Command::new("pacman").args(["-Qdt"]).output()?;
    let orphan_text = String::from_utf8_lossy(&orphan_out.stdout).to_string();
    let orphans: HashSet<&str> = orphan_text.lines()
        .filter_map(|l| l.split_whitespace().next()).collect();

    let out = Command::new("pacman").args(["-Qi"]).output()?;
    let text = String::from_utf8_lossy(&out.stdout);

    let mut names: Vec<String> = Vec::new();
    let mut versions: Vec<String> = Vec::new();
    let mut descs: Vec<String> = Vec::new();
    let mut sizes: Vec<u64> = Vec::new();
    let mut installed: Vec<String> = Vec::new();
    let mut reasons: Vec<String> = Vec::new();
    let mut deps_cnt: Vec<u32> = Vec::new();
    let mut req_cnt: Vec<u32> = Vec::new();
    let mut orphan_flags: Vec<&str> = Vec::new();

    // Parse each package block (separated by empty lines)
    let (mut name, mut ver, mut desc, mut inst, mut reason) =
        (String::new(), String::new(), String::new(), String::new(), String::new());
    let (mut size, mut deps, mut reqs) = (0u64, 0u32, 0u32);

    for line in text.lines() {
        if line.is_empty() {
            if !name.is_empty() {
                orphan_flags.push(if orphans.contains(name.as_str()) { "x" } else { "" });
                names.push(std::mem::take(&mut name));
                versions.push(std::mem::take(&mut ver));
                descs.push(std::mem::take(&mut desc));
                sizes.push(size);
                installed.push(std::mem::take(&mut inst));
                reasons.push(std::mem::take(&mut reason));
                deps_cnt.push(deps); req_cnt.push(reqs);
                size = 0; deps = 0; reqs = 0;
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
                "Install Date" => inst = v.into(),
                "Install Reason" => reason = if v.contains("dependency") { "dep".into() } else { "explicit".into() },
                "Depends On" => deps = if v == "None" { 0 } else { v.split_whitespace().count() as u32 },
                "Required By" => reqs = if v == "None" { 0 } else { v.split_whitespace().count() as u32 },
                _ => {}
            }
        }
    }
    // Last package
    if !name.is_empty() {
        orphan_flags.push(if orphans.contains(name.as_str()) { "x" } else { "" });
        names.push(name); versions.push(ver); descs.push(desc);
        sizes.push(size); installed.push(inst); reasons.push(reason);
        deps_cnt.push(deps); req_cnt.push(reqs);
    }

    Ok(DataFrame::new(vec![
        Series::new("name".into(), names).into(),
        Series::new("version".into(), versions).into(),
        Series::new("size".into(), sizes).into(),
        Series::new("deps".into(), deps_cnt).into(),
        Series::new("req_by".into(), req_cnt).into(),
        Series::new("orphan".into(), orphan_flags).into(),
        Series::new("reason".into(), reasons).into(),
        Series::new("installed".into(), installed).into(),
        Series::new("description".into(), descs).into(),
    ])?)
}
