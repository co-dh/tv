//! System plugin - routes OS commands to ADBC via source:xxx paths

use crate::app::AppContext;
use crate::command::Command;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::Result;

/// Known system commands (cmd, has_arg)
const SYS_CMDS: &[(&str, bool)] = &[
    ("ps", false), ("mounts", false), ("tcp", false), ("udp", false),
    ("env", false), ("df", false), ("systemctl", false),
    ("pacman", false), ("cargo", false),
    ("lsof", true), ("journalctl", true),
];

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

/// Generic system command - routes to source:xxx path
pub struct SourceCmd { pub cmd: String, pub arg: Option<String> }

impl Command for SourceCmd {
    /// Route to source:xxx path - ADBC handles via source module
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let (name, src) = match &self.arg {
            Some(a) => (format!("{}:{}", self.cmd, a), format!("source:{}:{}", self.cmd, a)),
            None => (self.cmd.clone(), format!("source:{}", self.cmd)),
        };
        let id = app.next_id();
        app.stack.push(ViewState::build(id, name).path(src));
        Ok(())
    }
    fn to_str(&self) -> String {
        match &self.arg {
            Some(a) => format!("{} {}", self.cmd, a),
            None => self.cmd.clone(),
        }
    }
}
