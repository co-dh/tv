//! System plugin - routes OS commands to sqlite plugin or ADBC
//! Simple sources (ps, tcp, etc.) use shared tv_plugin_api::source
//! Complex sources (journalctl, pacman) use sqlite plugin APIs

use crate::app::AppContext;
use crate::command::Command;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::Result;
use std::process::Command as ShellCmd;
use tv_plugin_api::source;

/// Known system commands (cmd, has_arg)
const SYS_CMDS: &[(&str, bool)] = &[
    ("ps", false), ("mounts", false), ("tcp", false), ("udp", false),
    ("env", false), ("df", false), ("systemctl", false),
    ("pacman", false), ("cargo", false),
    ("lsof", true), ("journalctl", true),
];

/// Get user/process-specific ADBC SQLite database path
fn adbc_sys_db() -> String {
    let pid = std::process::id();
    // Use XDG_RUNTIME_DIR (user-specific, e.g. /run/user/1000)
    if let Ok(dir) = std::env::var("XDG_RUNTIME_DIR") {
        format!("{}/tv_sys_{}.db", dir, pid)
    } else {
        // Fallback: /tmp with username and pid
        let user = std::env::var("USER").unwrap_or_else(|_| "tv".into());
        format!("/tmp/tv_sys_{}_{}.db", user, pid)
    }
}

pub struct SystemPlugin;

impl Plugin for SystemPlugin {
    fn name(&self) -> &str { "system" }
    fn tab(&self) -> &str { "table" }

    fn matches(&self, name: &str) -> bool {
        SYS_CMDS.iter().any(|(c, _)| name == *c || name.starts_with(&format!("{}:", c)))
    }

    fn handle(&self, _cmd: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> { None }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        SYS_CMDS.iter().find(|(c, _)| *c == cmd)
            .map(|(c, has_arg)| {
                let arg = if *has_arg && !arg.is_empty() { Some(arg.to_string()) } else { None };
                Box::new(SourceCmd { cmd: c.to_string(), arg }) as Box<dyn Command>
            })
    }
}

/// Generic system command - routes to sqlite source or ADBC
pub struct SourceCmd { pub cmd: String, pub arg: Option<String> }

impl Command for SourceCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        // Try ADBC first if available
        if let Some(path) = adbc_source(&self.cmd) {
            let id = app.next_id();
            app.stack.push(ViewState::build(id, self.cmd.clone()).path(path));
            return Ok(());
        }
        // Fallback to sqlite plugin
        let (name, source) = match &self.arg {
            Some(a) => (format!("{}:{}", self.cmd, a), format!("source:{}:{}", self.cmd, a)),
            None => (self.cmd.clone(), format!("source:{}", self.cmd)),
        };
        let id = app.next_id();
        app.stack.push(ViewState::build(id, name).path(source));
        Ok(())
    }
    fn to_str(&self) -> String {
        match &self.arg {
            Some(a) => format!("{} {}", self.cmd, a),
            None => self.cmd.clone(),
        }
    }
}

/// Generate ADBC path for system command using shared source module
fn adbc_source(cmd: &str) -> Option<String> {
    // Check if ADBC SQLite driver exists
    if !std::path::Path::new("/usr/local/lib/libadbc_driver_sqlite.so").exists() {
        return None;
    }
    // Get data from shared source module
    let table = source::get(cmd)?;
    let tsv = source::to_tsv(&table);
    // Write TSV to temp file
    let db = adbc_sys_db();
    let tsv_path = format!("{}.{}.tsv", db, cmd);
    std::fs::write(&tsv_path, &tsv).ok()?;
    // Import via shell (sqlite3 needs dot commands via stdin or -cmd)
    let import_cmd = format!(
        "sqlite3 '{}' 'DROP TABLE IF EXISTS {}' && sqlite3 '{}' -cmd '.mode tabs' '.import {} {}'",
        db, cmd, db, tsv_path, cmd
    );
    ShellCmd::new("sh").arg("-c").arg(&import_cmd).status().ok()?;
    std::fs::remove_file(&tsv_path).ok();
    Some(format!("adbc:sqlite://{}?table={}", db, cmd))
}
