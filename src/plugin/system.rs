//! System view plugin - OS info commands (ps, df, tcp, etc.)
//! These create table views of system information

use crate::app::AppContext;
use crate::command::Command;
use crate::plugin::Plugin;
use crate::state::ViewState;
use anyhow::Result;

pub struct SystemPlugin;

impl Plugin for SystemPlugin {
    fn name(&self) -> &str { "system" }
    fn tab(&self) -> &str { "table" }  // system views use table keybindings

    fn matches(&self, name: &str) -> bool {
        matches!(name, "ps" | "df" | "mounts" | "tcp" | "udp" | "lsblk" | "who" | "env")
            || name.starts_with("lsof")
    }

    fn handle(&self, _cmd: &str, _app: &mut AppContext) -> Option<Box<dyn Command>> {
        None  // system views use table handler
    }

    fn parse(&self, cmd: &str, arg: &str) -> Option<Box<dyn Command>> {
        match cmd {
            "ps" => Some(Box::new(Ps)),
            "df" => Some(Box::new(Df)),
            "mounts" => Some(Box::new(Mounts)),
            "tcp" => Some(Box::new(Tcp)),
            "udp" => Some(Box::new(Udp)),
            "lsblk" => Some(Box::new(Lsblk)),
            "who" => Some(Box::new(Who)),
            "env" => Some(Box::new(Env)),
            "lsof" => {
                let pid = arg.parse::<i32>().ok();
                Some(Box::new(Lsof { pid }))
            }
            _ => None,
        }
    }

    fn commands(&self) -> Vec<(&str, &str)> {
        vec![
            ("ps", "Process list"),
            ("df", "Disk usage"),
            ("mounts", "Mount points"),
            ("tcp", "TCP connections"),
            ("udp", "UDP connections"),
            ("lsblk", "Block devices"),
            ("who", "Logged in users"),
            ("lsof", "Open files"),
            ("env", "Environment variables"),
        ]
    }
}

// === Commands ===

/// Process list
pub struct Ps;

impl Command for Ps {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::ps()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "ps".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "ps".into() }
}

/// Disk usage
pub struct Df;

impl Command for Df {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::df()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "df".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "df".into() }
}

/// Mount points
pub struct Mounts;

impl Command for Mounts {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::mounts()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "mounts".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "mounts".into() }
}

/// TCP connections
pub struct Tcp;

impl Command for Tcp {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::tcp()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "tcp".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "tcp".into() }
}

/// UDP connections
pub struct Udp;

impl Command for Udp {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::udp()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "udp".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "udp".into() }
}

/// Block devices
pub struct Lsblk;

impl Command for Lsblk {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::lsblk()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "lsblk".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "lsblk".into() }
}

/// Logged in users
pub struct Who;

impl Command for Who {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::who()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "who".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "who".into() }
}

/// Open files
pub struct Lsof { pub pid: Option<i32> }

impl Command for Lsof {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::lsof(self.pid)?;
        let id = app.next_id();
        let name = self.pid.map(|p| format!("lsof:{}", p)).unwrap_or("lsof".into());
        app.stack.push(ViewState::new(id, name, df, None));
        Ok(())
    }
    fn to_str(&self) -> String { self.pid.map(|p| format!("lsof {}", p)).unwrap_or("lsof".into()) }
}

/// Environment variables
pub struct Env;

impl Command for Env {
    fn exec(&mut self, app: &mut AppContext) -> Result<()> {
        let df = crate::os::env()?;
        let id = app.next_id();
        app.stack.push(ViewState::new(id, "env".into(), df, None));
        Ok(())
    }
    fn to_str(&self) -> String { "env".into() }
}
