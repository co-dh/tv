//! Command parser - converts command strings to Command objects

use crate::app::AppContext;
use crate::command::Command;
use crate::command::io::{From, Save};
use crate::command::nav::{Goto, GotoCol, ToggleInfo, Decimals, ToggleSel, ClearSel, SelAll, SelRows};
use crate::command::transform::{DelCol, Derive, Filter, RenameCol, Select, Sort, Take, ToTime, Xkey};
use crate::command::view::{Dup, Pop, Swap};
use crate::plugin::corr::Correlation;
use crate::plugin::meta::Metadata;
use crate::plugin::folder::Ls;
use crate::state::ViewState;
use anyhow::Result;

/// System commands: (name, has_arg)
const SYS_CMDS: &[(&str, bool)] = &[
    ("ps", false), ("mounts", false), ("tcp", false), ("udp", false),
    ("env", false), ("df", false), ("systemctl", false),
    ("pacman", false), ("lsof", true), ("journalctl", true),
];

/// Parse command string into Command object
pub fn parse(line: &str, app: &mut AppContext) -> Option<Box<dyn Command>> {
    let (cmd_raw, arg) = line.split_once(' ').map(|(c, a)| (c, a.trim())).unwrap_or((line, ""));

    // Handle xkey{} before lowercasing (column names are case-sensitive)
    if cmd_raw.to_lowercase().starts_with("xkey{") && cmd_raw.ends_with('}') {
        let inner = &cmd_raw[5..cmd_raw.len()-1];
        let keys: Vec<String> = inner.split(',')
            .map(|s| s.trim().trim_matches('`').to_string())
            .filter(|s| !s.is_empty()).collect();
        return Some(Box::new(Xkey { keys }));
    }

    let cmd = cmd_raw.to_lowercase();

    // Core commands (not in plugins)
    match cmd.as_str() {
        "load" | "from" => return Some(Box::new(From { file_path: arg.to_string() })),
        "save" => return Some(Box::new(Save { file_path: arg.to_string() })),
        "corr" | "correlation" => return Some(Box::new(Correlation)),
        "del_col" | "delcol" => return Some(Box::new(DelCol { cols: arg.split(',').map(|s| s.trim().to_string()).collect() })),
        "filter" => return Some(Box::new(Filter { expr: arg.to_string() })),
        "select" | "sel" => return Some(Box::new(Select {
            col_names: arg.split(',').map(|s| s.trim().to_string()).collect()
        })),
        "sort" => {
            let (col, desc) = if let Some(c) = arg.strip_prefix('-') { (c, true) } else { (arg, false) };
            return Some(Box::new(Sort { col_name: col.to_string(), descending: desc }));
        }
        "sort_desc" | "sortdesc" => return Some(Box::new(Sort { col_name: arg.to_string(), descending: true })),
        "take" => return arg.parse().ok().map(|n| Box::new(Take { n }) as Box<dyn Command>),
        "to_time" => return Some(Box::new(ToTime { col_name: arg.to_string() })),
        "derive" => return Some(Box::new(Derive { col_name: arg.to_string() })),
        "xkey" => {
            let keys: Vec<String> = arg.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
            return Some(Box::new(Xkey { keys }));
        }
        "rename" => {
            let (old, new) = arg.split_once(' ')?;
            return Some(Box::new(RenameCol { old_name: old.into(), new_name: new.into() }));
        }
        "goto" => return Some(Box::new(Goto { arg: arg.to_string() })),
        "goto_col" | "gotocol" => return Some(Box::new(GotoCol { arg: arg.to_string() })),
        "toggle_info" => return Some(Box::new(ToggleInfo)),
        "decimals" => return arg.parse().ok().map(|d| Box::new(Decimals { delta: d }) as Box<dyn Command>),
        "toggle_sel" => return Some(Box::new(ToggleSel)),
        "clear_sel" => return Some(Box::new(ClearSel)),
        "sel_all" => return Some(Box::new(SelAll)),
        "sel_rows" => return Some(Box::new(SelRows { expr: arg.to_string() })),
        "pop" | "quit" => return Some(Box::new(Pop)),
        "swap" => return Some(Box::new(Swap)),
        "dup" => return Some(Box::new(Dup)),
        "meta" => return Some(Box::new(Metadata)),
        "pivot" => return Some(Box::new(crate::plugin::pivot::Pivot)),
        "page_down" => return Some(Box::new(Goto { arg: app.page().to_string() })),
        "page_up" => return Some(Box::new(Goto { arg: (-app.page()).to_string() })),
        "ls" => {
            let dir = if arg.is_empty() { std::env::current_dir().unwrap_or_default() } else { std::path::PathBuf::from(arg) };
            return Some(Box::new(Ls { dir, recursive: false }));
        }
        "lr" => {
            let dir = if arg.is_empty() { std::env::current_dir().unwrap_or_default() } else { std::path::PathBuf::from(arg) };
            return Some(Box::new(Ls { dir, recursive: true }));
        }
        _ => {}
    }

    // System commands (ps, pacman, systemctl, etc.) → source:xxx paths
    if let Some((c, has_arg)) = SYS_CMDS.iter().find(|(c, _)| *c == cmd) {
        let arg = if *has_arg && !arg.is_empty() { Some(arg.to_string()) } else { None };
        return Some(Box::new(SourceCmd { cmd: c.to_string(), arg }));
    }

    // Try plugin commands (parse method)
    if let Some(c) = app.plugins.parse(&cmd, arg) { return Some(c); }

    // Try plugin handle for context-dependent commands (enter, delete_sel, etc.)
    if let Some(name) = app.view().map(|v| v.name.clone()) {
        let plugins = std::mem::take(&mut app.plugins);
        let result = plugins.handle(&name, &cmd, app);
        app.plugins = plugins;
        if result.is_some() { return result; }
    }
    None
}

/// System command - creates view with source:xxx path
struct SourceCmd { cmd: String, arg: Option<String> }

impl Command for SourceCmd {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let id = app.next_id();
        let (name, src) = match &self.arg {
            Some(a) => (format!("{}:{}", self.cmd, a), format!("source:{}:{}", self.cmd, a)),
            None => (self.cmd.clone(), format!("source:{}", self.cmd)),
        };
        app.stack.push(ViewState::build(id, name).path(src));
        Ok(())
    }
    fn to_str(&self) -> String {
        match &self.arg { Some(a) => format!("{} {}", self.cmd, a), None => self.cmd.clone() }
    }
}
