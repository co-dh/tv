//! System source functions - shell commands to ADBC SQLite
//! Used by system plugin for ps, tcp, udp, env, df, mounts

use std::process::Command as ShellCmd;

/// Shell source definition: (name, header, shell_cmd)
const SOURCES: &[(&str, &str, &str)] = &[
    ("ps", "user\tpid\t%cpu\t%mem\tvsz\trss\ttty\tstat\tstart\ttime\tcommand",
        r#"ps aux --no-headers | awk '{printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11}'"#),
    ("tcp", "proto\tlocal\tremote\tstate",
        r#"ss -tn | tail -n+2 | awk '{printf "tcp\t%s\t%s\t%s\n",$4,$5,$1}'"#),
    ("udp", "proto\tlocal\tremote\tstate",
        r#"ss -un | tail -n+2 | awk '{printf "udp\t%s\t%s\t%s\n",$4,$5,$1}'"#),
    ("env", "name\tvalue", r#"env | sed 's/=/\t/'"#),
    ("df", "fs\tsize\tused\tavail\tpct\tmount",
        r#"df -h | awk 'NR>1{printf "%s\t%s\t%s\t%s\t%s\t%s\n",$1,$2,$3,$4,$5,$6}'"#),
    ("mounts", "dev\tmount\ttype\topts",
        r#"mount | awk '{printf "%s\t%s\t%s\t%s\n",$1,$3,$5,$6}'"#),
];

/// SQLite ADBC driver path
pub const SQLITE_DRIVER: &str = "/usr/local/lib/libadbc_driver_sqlite.so";

/// Get user/process-specific ADBC SQLite database path
pub fn sys_db() -> String {
    let pid = std::process::id();
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        format!("{}/tv_sys_{}.db", dir, pid)
    } else {
        let user = std::env::var("USER").unwrap_or_else(|_| "tv".into());
        format!("/tmp/tv_sys_{}_{}.db", user, pid)
    }
}

/// Check if ADBC SQLite driver is available
pub fn available() -> bool {
    std::path::Path::new(SQLITE_DRIVER).exists()
}

/// Generate ADBC path for system command (shell → SQLite import)
/// Returns None if driver not available or command not supported
pub fn source(cmd: &str) -> Option<String> {
    if !available() { return None; }
    let (_, header, shell) = SOURCES.iter().find(|(c, _, _)| *c == cmd)?;
    // Run shell, import to SQLite
    let data = ShellCmd::new("sh").arg("-c").arg(shell).output().ok()?;
    let tsv = format!("{}\n{}", header, String::from_utf8_lossy(&data.stdout));
    let db = sys_db();
    let tsv_path = format!("{}.{}.tsv", db, cmd);
    std::fs::write(&tsv_path, &tsv).ok()?;
    let import_cmd = format!(
        "sqlite3 '{}' 'DROP TABLE IF EXISTS {}' && sqlite3 '{}' -cmd '.mode tabs' '.import {} {}'",
        db, cmd, db, tsv_path, cmd
    );
    ShellCmd::new("sh").arg("-c").arg(&import_cmd).status().ok()?;
    std::fs::remove_file(&tsv_path).ok();
    Some(format!("adbc:sqlite://{}?table={}", db, cmd))
}

/// Check if command is a known simple source (shell-based)
pub fn is_simple(cmd: &str) -> bool {
    SOURCES.iter().any(|(c, _, _)| *c == cmd)
}
